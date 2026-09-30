package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/jobs"
)

const jobColumns = `
job_id::text, job_type, payload, state, idempotency_key,
claim_attempts, attempt_count, max_attempts, next_attempt_at, lease_owner, lease_expires_at,
last_error, created_at, updated_at, started_at, finished_at`

const updatedJobColumns = `
job.job_id::text, job.job_type, job.payload, job.state, job.idempotency_key,
job.claim_attempts, job.attempt_count, job.max_attempts, job.next_attempt_at, job.lease_owner, job.lease_expires_at,
job.last_error, job.created_at, job.updated_at, job.started_at, job.finished_at`

const expiredLeaseMessage = "running lease expired; provider reconciliation required"

type JobRepository struct {
	pool *pgxpool.Pool
}

var _ jobs.Repository = (*JobRepository)(nil)

func NewJobRepository(pool *pgxpool.Pool) (*JobRepository, error) {
	if pool == nil {
		return nil, errors.New("job database pool is required")
	}
	return &JobRepository{pool: pool}, nil
}

func (repository *JobRepository) Create(ctx context.Context, request jobs.CreateRequest) (jobs.Job, error) {
	return insertJob(ctx, repository.pool, request)
}

type jobQueryRow interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// insertJob is shared by the standalone Job repository and transactions that
// atomically create a Job with another Panta-owned aggregate.
func insertJob(ctx context.Context, db jobQueryRow, request jobs.CreateRequest) (jobs.Job, error) {
	if request.ID == "" || strings.TrimSpace(request.Type) == "" || request.MaxAttempts < 1 || !json.Valid(request.Payload) {
		return jobs.Job{}, jobs.ErrInvalidArgument
	}
	if request.IdempotencyKey != nil && strings.TrimSpace(*request.IdempotencyKey) == "" {
		return jobs.Job{}, jobs.ErrInvalidArgument
	}

	row := db.QueryRow(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    claim_attempts, attempt_count, max_attempts, created_at, updated_at
) VALUES ($1, $2, $3, 'QUEUED', $4, 0, 0, $5, now(), now())
RETURNING `+jobColumns,
		string(request.ID), request.Type, string(request.Payload), request.IdempotencyKey, request.MaxAttempts,
	)
	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "23505" && postgresError.ConstraintName == "jobs_idempotency_key_unique" {
		return jobs.Job{}, fmt.Errorf("%w: %s", jobs.ErrDuplicateIdempotencyKey, *request.IdempotencyKey)
	}
	return jobs.Job{}, fmt.Errorf("create job: %w", err)
}

func (repository *JobRepository) Get(ctx context.Context, id jobs.JobID) (jobs.Job, error) {
	return repository.getOne(ctx, "SELECT "+jobColumns+" FROM jobs WHERE job_id = $1", string(id))
}

func (repository *JobRepository) GetByIdempotencyKey(ctx context.Context, key string) (jobs.Job, error) {
	if strings.TrimSpace(key) == "" {
		return jobs.Job{}, jobs.ErrInvalidArgument
	}
	return repository.getOne(ctx, "SELECT "+jobColumns+" FROM jobs WHERE idempotency_key = $1", key)
}

// ClaimNext claims the next schedulable Job and increments its claim generation.
//
// The failure budget deliberately does not gate claiming: a RETRY_WAIT row is
// always schedulable once next_attempt_at is due. A single Job now carries the
// whole acquisition workflow across provider, visibility, and canonical stages,
// so gating on the failure budget would strand it in RETRY_WAIT forever after
// enough stage transitions, or block the visibility stage right after a
// successful download.
func (repository *JobRepository) ClaimNext(ctx context.Context, request jobs.ClaimRequest) (jobs.Job, error) {
	if strings.TrimSpace(request.Owner) == "" || request.Now.IsZero() || request.LeaseDuration <= 0 {
		return jobs.Job{}, jobs.ErrInvalidArgument
	}
	row := repository.pool.QueryRow(ctx, `
WITH candidate AS (
    SELECT job_id
    FROM jobs
    WHERE state = 'QUEUED'
       OR (state = 'RETRY_WAIT' AND next_attempt_at <= $1)
    ORDER BY COALESCE(next_attempt_at, created_at), created_at, job_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE jobs AS job
SET state = 'RUNNING',
    claim_attempts = job.claim_attempts + 1,
    next_attempt_at = NULL,
    lease_owner = $2,
    lease_expires_at = CURRENT_TIMESTAMP + ($3 * interval '1 microsecond'),
    started_at = COALESCE(job.started_at, $1),
    updated_at = $1
FROM candidate
WHERE job.job_id = candidate.job_id
RETURNING `+updatedJobColumns, request.Now, request.Owner, request.LeaseDuration.Microseconds())
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNoClaimableJob
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("claim next job: %w", err)
	}
	return job, nil
}

func (repository *JobRepository) RenewLease(ctx context.Context, request jobs.RenewLeaseRequest) (jobs.Job, error) {
	if err := validateLeaseRequest(request.LeaseRequest); err != nil || request.LeaseDuration <= 0 {
		return jobs.Job{}, jobs.ErrInvalidArgument
	}
	row := repository.pool.QueryRow(ctx, `
UPDATE jobs AS job
SET lease_expires_at = CURRENT_TIMESTAMP + ($5 * interval '1 microsecond'), updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns,
		string(request.ID), request.Owner, request.ExpectedClaim, request.Now, request.LeaseDuration.Microseconds(),
	)
	return repository.finishLeaseMutation(ctx, request.ID, row, "renew lease")
}

func (repository *JobRepository) Succeed(ctx context.Context, request jobs.LeaseRequest) (jobs.Job, error) {
	if err := validateLeaseRequest(request); err != nil {
		return jobs.Job{}, err
	}
	row := repository.pool.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'SUCCEEDED', lease_owner = NULL, lease_expires_at = NULL,
    next_attempt_at = NULL, last_error = NULL, finished_at = $4, updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns, string(request.ID), request.Owner, request.ExpectedClaim, request.Now)
	return repository.finishLeaseMutation(ctx, request.ID, row, "succeed job")
}

func (repository *JobRepository) Fail(ctx context.Context, request jobs.FailureRequest) (jobs.Job, error) {
	if err := validateFailureRequest(request); err != nil {
		return jobs.Job{}, err
	}
	row := repository.pool.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'FAILED', lease_owner = NULL, lease_expires_at = NULL,
    next_attempt_at = NULL, last_error = $5, finished_at = $4, updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns, string(request.ID), request.Owner, request.ExpectedClaim, request.Now, request.Error)
	return repository.finishLeaseMutation(ctx, request.ID, row, "fail job")
}

// RetryAt schedules a same-stage retry and consumes the failure budget. This is
// the only operation that increments the failure count or consults max_attempts.
func (repository *JobRepository) RetryAt(ctx context.Context, request jobs.RetryRequest) (jobs.Job, error) {
	if err := validateFailureRequest(request.FailureRequest); err != nil || request.RetryAt.IsZero() {
		return jobs.Job{}, jobs.ErrInvalidArgument
	}
	row := repository.pool.QueryRow(ctx, `
UPDATE jobs AS job
SET state = CASE WHEN job.attempt_count + 1 >= job.max_attempts THEN 'FAILED' ELSE 'RETRY_WAIT' END,
    attempt_count = job.attempt_count + 1,
    lease_owner = NULL,
    lease_expires_at = NULL,
    next_attempt_at = CASE WHEN job.attempt_count + 1 >= job.max_attempts THEN NULL ELSE $6::timestamptz END,
    last_error = $5,
    finished_at = CASE WHEN job.attempt_count + 1 >= job.max_attempts THEN $4::timestamptz ELSE NULL END,
    updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns,
		string(request.ID), request.Owner, request.ExpectedClaim, request.Now, request.Error, request.RetryAt,
	)
	return repository.finishLeaseMutation(ctx, request.ID, row, "schedule job retry")
}

func (repository *JobRepository) Cancel(ctx context.Context, request jobs.CancelRequest) (jobs.Job, error) {
	if request.ID == "" || request.Now.IsZero() {
		return jobs.Job{}, jobs.ErrInvalidArgument
	}
	row := repository.pool.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'CANCELED', next_attempt_at = NULL, finished_at = $2, updated_at = $2
WHERE job_id = $1 AND state IN ('QUEUED', 'RETRY_WAIT')
RETURNING `+updatedJobColumns, string(request.ID), request.Now)
	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("cancel job: %w", err)
	}
	current, getErr := repository.Get(ctx, request.ID)
	if getErr != nil {
		return jobs.Job{}, getErr
	}
	return jobs.Job{}, fmt.Errorf("%w: cannot cancel job in state %s", jobs.ErrInvalidTransition, current.State)
}

func (repository *JobRepository) MarkExpiredRunningRecoveryRequired(ctx context.Context, request jobs.RecoveryRequest) (int64, error) {
	if request.Now.IsZero() || request.Limit < 1 {
		return 0, jobs.ErrInvalidArgument
	}
	commandTag, err := repository.pool.Exec(ctx, `
WITH expired AS (
    SELECT job_id
    FROM jobs
    WHERE state = 'RUNNING' AND lease_expires_at <= CURRENT_TIMESTAMP
    ORDER BY lease_expires_at, job_id
    FOR UPDATE SKIP LOCKED
    LIMIT $2
)
UPDATE jobs AS job
SET state = 'RECOVERY_REQUIRED',
    lease_owner = NULL,
    lease_expires_at = NULL,
    last_error = COALESCE(job.last_error, $3),
    updated_at = $1
FROM expired
WHERE job.job_id = expired.job_id`, request.Now, request.Limit, expiredLeaseMessage)
	if err != nil {
		return 0, fmt.Errorf("mark expired jobs for recovery: %w", err)
	}
	return commandTag.RowsAffected(), nil
}

func (repository *JobRepository) getOne(ctx context.Context, query string, argument any) (jobs.Job, error) {
	return getOneJob(ctx, repository.pool, query, argument)
}

func getOneJob(ctx context.Context, db jobQueryRow, query string, argument any) (jobs.Job, error) {
	job, err := scanJob(db.QueryRow(ctx, query, argument))
	if errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, jobs.ErrNotFound
	}
	if err != nil {
		return jobs.Job{}, fmt.Errorf("read job: %w", err)
	}
	return job, nil
}

func (repository *JobRepository) finishLeaseMutation(ctx context.Context, id jobs.JobID, row pgx.Row, operation string) (jobs.Job, error) {
	job, err := scanJob(row)
	if err == nil {
		return job, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, fmt.Errorf("%s: %w", operation, err)
	}
	current, getErr := repository.Get(ctx, id)
	if getErr != nil {
		return jobs.Job{}, getErr
	}
	if current.State != jobs.StateRunning {
		return jobs.Job{}, fmt.Errorf("%w: job state is %s", jobs.ErrInvalidTransition, current.State)
	}
	return jobs.Job{}, jobs.ErrLeaseConflict
}

func validateLeaseRequest(request jobs.LeaseRequest) error {
	if request.ID == "" || strings.TrimSpace(request.Owner) == "" || request.ExpectedClaim < 1 || request.Now.IsZero() {
		return jobs.ErrInvalidArgument
	}
	return nil
}

func validateFailureRequest(request jobs.FailureRequest) error {
	if err := validateLeaseRequest(request.LeaseRequest); err != nil || strings.TrimSpace(request.Error) == "" {
		return jobs.ErrInvalidArgument
	}
	return nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanJob(row rowScanner) (jobs.Job, error) {
	var job jobs.Job
	var id, state string
	var payload []byte
	var idempotencyKey, leaseOwner, lastError sql.NullString
	var nextAttemptAt, leaseExpiresAt, startedAt, finishedAt sql.NullTime
	err := row.Scan(
		&id, &job.Type, &payload, &state, &idempotencyKey,
		&job.ClaimAttempts, &job.FailureCount, &job.MaxAttempts, &nextAttemptAt, &leaseOwner, &leaseExpiresAt,
		&lastError, &job.CreatedAt, &job.UpdatedAt, &startedAt, &finishedAt,
	)
	if err != nil {
		return jobs.Job{}, err
	}
	job.ID = jobs.JobID(id)
	job.Payload = payload
	job.State = jobs.State(state)
	job.IdempotencyKey = stringPointer(idempotencyKey)
	job.NextAttemptAt = timePointer(nextAttemptAt)
	job.LeaseOwner = stringPointer(leaseOwner)
	job.LeaseExpiresAt = timePointer(leaseExpiresAt)
	job.LastError = stringPointer(lastError)
	job.StartedAt = timePointer(startedAt)
	job.FinishedAt = timePointer(finishedAt)
	return job, nil
}

func stringPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func timePointer(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
