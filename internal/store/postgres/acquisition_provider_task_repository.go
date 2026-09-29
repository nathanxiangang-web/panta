package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

const acquisitionProviderTaskColumns = `
manifest_id::text, job_id::text, provider_id, provider_task_ref, state, created_at, updated_at`

// providerTaskFenceLockClass namespaces the transaction-scoped advisory lock that
// serializes claim decisions per Manifest. Without it, two concurrent claimers
// could both observe "no row" before either commits, and both would be authorized
// to start an external provider task.
const providerTaskFenceLockClass = 3401

// providerTaskClaimLockTimeout bounds how long a claim waits for the per-Manifest
// fence lock. Waiting is normal and brief under contention, but it must never be
// unbounded: a claim that cannot obtain the fence fails closed instead of hanging
// a worker forever.
const providerTaskClaimLockTimeout = "5s"

type acquisitionProviderTaskDB interface {
	Begin(context.Context) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// AcquisitionProviderTaskRepository persists the durable provider-task linkage
// and fences the external side effect.
type AcquisitionProviderTaskRepository struct {
	db acquisitionProviderTaskDB
}

var _ acquisition.ProviderTaskStore = (*AcquisitionProviderTaskRepository)(nil)

func NewAcquisitionProviderTaskRepository(db acquisitionProviderTaskDB) (*AcquisitionProviderTaskRepository, error) {
	if db == nil {
		return nil, errors.New("acquisition provider task database is required")
	}
	return &AcquisitionProviderTaskRepository{db: db}, nil
}

func (repository *AcquisitionProviderTaskRepository) GetProviderTask(ctx context.Context, manifestID acquisition.ManifestID) (acquisition.ProviderTask, error) {
	if manifestID == "" {
		return acquisition.ProviderTask{}, acquisition.ErrInvalidProviderTask
	}
	task, err := scanAcquisitionProviderTask(repository.db.QueryRow(ctx, `SELECT `+acquisitionProviderTaskColumns+`
FROM acquisition_provider_tasks
WHERE manifest_id = $1`, string(manifestID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.ProviderTask{}, fmt.Errorf("%w: %s", acquisition.ErrProviderTaskNotFound, manifestID)
	}
	if err != nil {
		return acquisition.ProviderTask{}, providerTaskPersistenceError("get provider task", err)
	}
	return task, nil
}

// ClaimProviderTask is the single atomic entry point that authorizes a provider
// start. It runs inside one transaction holding a per-Manifest advisory lock, so
// exactly one caller can observe a fresh START_RESERVED claim or commit a
// reference for a reserved row.
func (repository *AcquisitionProviderTaskRepository) ClaimProviderTask(
	ctx context.Context,
	request acquisition.ProviderTaskClaimRequest,
) (acquisition.ProviderTaskClaimResult, error) {
	if request.ManifestID == "" || request.JobID == "" || !request.ProviderID.Valid() || request.Now.IsZero() {
		return acquisition.ProviderTaskClaimResult{}, acquisition.ErrInvalidProviderTask
	}
	if request.Reference != "" && !acquisition.ValidProviderTaskRef(request.Reference) {
		return acquisition.ProviderTaskClaimResult{}, acquisition.ErrInvalidProviderTask
	}

	tx, err := repository.db.Begin(ctx)
	if err != nil {
		return acquisition.ProviderTaskClaimResult{}, providerTaskPersistenceError("begin claim transaction", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '`+providerTaskClaimLockTimeout+`'`); err != nil {
		return acquisition.ProviderTaskClaimResult{}, providerTaskPersistenceError("bound fence lock wait", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext($2))`,
		providerTaskFenceLockClass, string(request.ManifestID)); err != nil {
		return acquisition.ProviderTaskClaimResult{}, providerTaskWriteError("lock provider task fence", err)
	}

	existing, err := scanAcquisitionProviderTask(tx.QueryRow(ctx, `SELECT `+acquisitionProviderTaskColumns+`
FROM acquisition_provider_tasks
WHERE manifest_id = $1
FOR UPDATE`, string(request.ManifestID)))
	switch {
	case err == nil:
		result, err := repository.resolveExisting(ctx, tx, request, existing)
		if err != nil {
			return acquisition.ProviderTaskClaimResult{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return acquisition.ProviderTaskClaimResult{}, providerTaskPersistenceError("commit provider task claim", err)
		}
		return result, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return acquisition.ProviderTaskClaimResult{}, providerTaskPersistenceError("lock provider task", err)
	}

	// No row exists: this caller exclusively reserves the start attempt.
	if request.Reference != "" {
		return acquisition.ProviderTaskClaimResult{}, fmt.Errorf("%w: reference supplied without a start reservation",
			acquisition.ErrInvalidProviderTask)
	}
	reserved, err := scanAcquisitionProviderTask(tx.QueryRow(ctx, `
INSERT INTO acquisition_provider_tasks (
    manifest_id, job_id, provider_id, provider_task_ref, state, created_at, updated_at
) VALUES ($1, $2, $3, NULL, 'START_RESERVED', $4, $4)
RETURNING `+acquisitionProviderTaskColumns,
		string(request.ManifestID), string(request.JobID), string(request.ProviderID), request.Now))
	if err != nil {
		return acquisition.ProviderTaskClaimResult{}, providerTaskWriteError("reserve provider start", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return acquisition.ProviderTaskClaimResult{}, providerTaskPersistenceError("commit provider start reservation", err)
	}
	return acquisition.ProviderTaskClaimResult{Task: reserved, ClaimedStart: true}, nil
}

// resolveExisting decides what a claim may do against an already durable row.
func (repository *AcquisitionProviderTaskRepository) resolveExisting(
	ctx context.Context,
	tx pgx.Tx,
	request acquisition.ProviderTaskClaimRequest,
	existing acquisition.ProviderTask,
) (acquisition.ProviderTaskClaimResult, error) {
	if existing.JobID != request.JobID || existing.ProviderID != request.ProviderID {
		return acquisition.ProviderTaskClaimResult{}, fmt.Errorf("%w: Manifest %s",
			acquisition.ErrProviderTaskIdentityChange, request.ManifestID)
	}
	switch existing.State {
	case acquisition.ProviderTaskReferenceKnown:
		if request.Reference == "" {
			return acquisition.ProviderTaskClaimResult{Task: existing}, nil
		}
		if existing.ProviderTaskRef != request.Reference {
			return acquisition.ProviderTaskClaimResult{}, fmt.Errorf("%w: Manifest %s already holds a different task reference",
				acquisition.ErrProviderTaskIdentityChange, request.ManifestID)
		}
		return acquisition.ProviderTaskClaimResult{Task: existing, CommittedReference: true}, nil
	case acquisition.ProviderTaskStartReserved:
		if request.Reference == "" {
			// The caller holds no reference and may not start a second task.
			return acquisition.ProviderTaskClaimResult{Task: existing}, nil
		}
		committed, err := scanAcquisitionProviderTask(tx.QueryRow(ctx, `
UPDATE acquisition_provider_tasks
SET provider_task_ref = $2, state = 'REFERENCE_KNOWN', updated_at = $3
WHERE manifest_id = $1 AND state = 'START_RESERVED'
RETURNING `+acquisitionProviderTaskColumns,
			string(request.ManifestID), request.Reference, request.Now))
		if errors.Is(err, pgx.ErrNoRows) {
			return acquisition.ProviderTaskClaimResult{}, fmt.Errorf("%w: Manifest %s",
				acquisition.ErrProviderTaskIdentityChange, request.ManifestID)
		}
		if err != nil {
			return acquisition.ProviderTaskClaimResult{}, providerTaskWriteError("commit provider task reference", err)
		}
		return acquisition.ProviderTaskClaimResult{Task: committed, CommittedReference: true}, nil
	default:
		return acquisition.ProviderTaskClaimResult{}, fmt.Errorf("%w: unknown durable state",
			acquisition.ErrInvalidProviderTask)
	}
}

func scanAcquisitionProviderTask(row pgx.Row) (acquisition.ProviderTask, error) {
	var task acquisition.ProviderTask
	var manifestID, jobID, providerID string
	var reference *string
	err := row.Scan(
		&manifestID, &jobID, &providerID, &reference, &task.State, &task.CreatedAt, &task.UpdatedAt,
	)
	if err != nil {
		return acquisition.ProviderTask{}, err
	}
	task.ManifestID = acquisition.ManifestID(manifestID)
	task.JobID = jobs.JobID(jobID)
	task.ProviderID = contracts.ProviderID(providerID)
	if reference != nil {
		task.ProviderTaskRef = *reference
	}
	return task, nil
}

func providerTaskWriteError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "23505":
			return fmt.Errorf("%w: %s", acquisition.ErrProviderTaskConflict, postgresError.ConstraintName)
		case "23503":
			return fmt.Errorf("%w: %s", acquisition.ErrInvalidProviderTask, postgresError.ConstraintName)
		case "23514", "22P02":
			return fmt.Errorf("%w: %s", acquisition.ErrInvalidProviderTask, postgresError.ConstraintName)
		case "55P03", "40P01":
			// The fence lock could not be held, so no side-effect authorization
			// was granted. Callers must fail closed rather than start a task.
			return fmt.Errorf("%w: %s", acquisition.ErrProviderTaskContention, postgresError.Message)
		}
	}
	return providerTaskPersistenceError(operation, err)
}

func providerTaskPersistenceError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", acquisition.ErrProviderTaskPersistence, operation, err)
}
