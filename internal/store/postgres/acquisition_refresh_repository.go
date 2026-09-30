package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

// RefreshRepository commits one IndexCore observation handoff as a single
// PostgreSQL transaction over the Acquisition Manifest and its linked Job.
//
// It never calls IndexCore, OpenList, a provider, a registry, a session, search,
// an agent, or auth: the Hint was already accepted by the remote, and this
// repository only records Panta's own durable state.
type RefreshRepository struct {
	db refreshDB
	// afterManifestUpdate is a test-only injection point placed between the two row
	// mutations so rollback atomicity can be proven.
	afterManifestUpdate func(context.Context, pgx.Tx) error
}

type refreshDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

var _ acquisition.RefreshStore = (*RefreshRepository)(nil)

func NewRefreshRepository(db refreshDB) (*RefreshRepository, error) {
	if db == nil {
		return nil, errors.New("acquisition refresh database is required")
	}
	return &RefreshRepository{db: db}, nil
}

func (repository *RefreshRepository) CommitRefresh(
	ctx context.Context,
	plan acquisition.RefreshPlan,
) (acquisition.RefreshResult, error) {
	if err := validateRefreshPlan(plan); err != nil {
		return acquisition.RefreshResult{}, err
	}
	tx, err := repository.db.Begin(ctx)
	if err != nil {
		return acquisition.RefreshResult{}, refreshPersistenceError("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Lock order is Manifest then Job, matching atomic Manifest activation and the
	// provider-stage handoff, so concurrent handoffs cannot deadlock.
	manifest, err := scanAcquisitionManifest(tx.QueryRow(ctx, `SELECT `+acquisitionManifestColumns+`
FROM acquisition_manifests
WHERE manifest_id = $1
FOR UPDATE`, string(plan.ManifestID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.RefreshResult{}, fmt.Errorf("%w: %s", acquisition.ErrRefreshManifestNotFound, plan.ManifestID)
	}
	if err != nil {
		return acquisition.RefreshResult{}, refreshPersistenceError("lock Manifest", err)
	}

	job, err := getOneJob(ctx, tx, "SELECT "+jobColumns+" FROM jobs WHERE job_id = $1 FOR UPDATE", string(plan.JobID))
	if errors.Is(err, jobs.ErrNotFound) {
		return acquisition.RefreshResult{}, fmt.Errorf("%w: %s", acquisition.ErrRefreshJobMismatch, plan.JobID)
	}
	if err != nil {
		return acquisition.RefreshResult{}, refreshPersistenceError("lock Job", err)
	}

	// Exact already-committed pair: report the durable records without rewriting
	// anything. The Hint is not resent, because the caller sees Changed=false.
	if replay, err := isCommittedRefreshHandoff(manifest, job, plan); err != nil {
		return acquisition.RefreshResult{}, err
	} else if replay {
		if err := tx.Commit(ctx); err != nil {
			return acquisition.RefreshResult{}, refreshPersistenceError("commit refresh replay", err)
		}
		return acquisition.RefreshResult{Manifest: manifest, Job: job, Changed: false}, nil
	}

	if manifest.State != acquisition.StateAwaitingVisibility {
		return acquisition.RefreshResult{}, fmt.Errorf("%w: Manifest %s is %s",
			acquisition.ErrRefreshManifestState, manifest.ID, manifest.State)
	}
	if err := verifyRefreshManifestLinkage(manifest, plan); err != nil {
		return acquisition.RefreshResult{}, err
	}
	expired, err := leaseExpiredByDatabaseTime(ctx, tx, job)
	if err != nil {
		return acquisition.RefreshResult{}, err
	}
	if err := verifyRefreshJobFence(manifest, job, plan, expired); err != nil {
		return acquisition.RefreshResult{}, err
	}

	// Mutation 1: advance the Manifest milestone only while it still holds the
	// accepted pre-handoff state.
	updatedManifest, err := scanAcquisitionManifest(tx.QueryRow(ctx, `
UPDATE acquisition_manifests
SET state = $3,
    updated_at = $4
WHERE manifest_id = $1 AND state = $2
RETURNING `+acquisitionManifestColumns,
		string(plan.ManifestID), string(acquisition.StateAwaitingVisibility),
		string(plan.ManifestState), plan.Now))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.RefreshResult{}, fmt.Errorf("%w: Manifest %s changed during handoff",
			acquisition.ErrRefreshConflict, plan.ManifestID)
	}
	if err != nil {
		return acquisition.RefreshResult{}, refreshPersistenceError("advance Manifest milestone", err)
	}
	if repository.afterManifestUpdate != nil {
		if err := repository.afterManifestUpdate(ctx, tx); err != nil {
			return acquisition.RefreshResult{}, refreshPersistenceError("after Manifest update", err)
		}
	}

	// Mutation 2: queue the same Job for the later canonical-confirmation stage under
	// the same claim-generation fence. The claim generation and the failure budget are
	// deliberately not modified: this queues a new stage, it is not a same-stage retry.
	updatedJob, err := scanJob(tx.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'RETRY_WAIT',
    lease_owner = NULL,
    lease_expires_at = NULL,
    next_attempt_at = $5::timestamptz,
    last_error = NULL,
    finished_at = NULL,
    updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns,
		string(plan.JobID), plan.Owner, plan.ExpectedClaim, plan.Now, plan.RetryAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.RefreshResult{}, fmt.Errorf("%w: Job %s fence no longer matches",
			acquisition.ErrRefreshConflict, plan.JobID)
	}
	if err != nil {
		return acquisition.RefreshResult{}, refreshPersistenceError("advance Job state", err)
	}
	if updatedJob.State != jobs.StateRetryWait {
		return acquisition.RefreshResult{}, fmt.Errorf("%w: Job %s landed in %s",
			acquisition.ErrRefreshConflict, updatedJob.ID, updatedJob.State)
	}

	if err := tx.Commit(ctx); err != nil {
		return acquisition.RefreshResult{}, refreshPersistenceError("commit refresh handoff", err)
	}
	return acquisition.RefreshResult{Manifest: updatedManifest, Job: updatedJob, Changed: true}, nil
}

// isCommittedRefreshHandoff reports whether the durable records already hold exactly
// the handoff this plan would commit, and that the linkage is intact.
func isCommittedRefreshHandoff(
	manifest acquisition.Manifest,
	job jobs.Job,
	plan acquisition.RefreshPlan,
) (bool, error) {
	if manifest.ID != plan.ManifestID || manifest.State != plan.ManifestState {
		return false, nil
	}
	if job.ID != plan.JobID {
		return false, fmt.Errorf("%w: requested %s, read %s",
			acquisition.ErrRefreshJobMismatch, plan.JobID, job.ID)
	}
	if manifest.JobID == nil || *manifest.JobID != plan.JobID {
		return false, fmt.Errorf("%w: Manifest %s is not linked to Job %s",
			acquisition.ErrRefreshJobMismatch, manifest.ID, plan.JobID)
	}
	if err := acquisition.ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return false, fmt.Errorf("%w: %w", acquisition.ErrRefreshJobMismatch, err)
	}
	return job.State == plan.JobState, nil
}

func verifyRefreshManifestLinkage(manifest acquisition.Manifest, plan acquisition.RefreshPlan) error {
	if manifest.ID != plan.ManifestID {
		return fmt.Errorf("%w: requested %s, read %s",
			acquisition.ErrExecutionIdentityMismatch, plan.ManifestID, manifest.ID)
	}
	if manifest.JobID == nil || *manifest.JobID != plan.JobID {
		return fmt.Errorf("%w: Manifest %s is not linked to Job %s",
			acquisition.ErrRefreshJobMismatch, manifest.ID, plan.JobID)
	}
	return nil
}

// verifyRefreshJobFence proves the Job is the linked ACQUISITION Job holding the
// fenced RUNNING lease for the expected claim generation.
func verifyRefreshJobFence(
	manifest acquisition.Manifest,
	job jobs.Job,
	plan acquisition.RefreshPlan,
	expired bool,
) error {
	if err := acquisition.ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return fmt.Errorf("%w: %w", acquisition.ErrRefreshJobMismatch, err)
	}
	if job.State != jobs.StateRunning {
		return fmt.Errorf("%w: Job %s is %s", acquisition.ErrRefreshFence, job.ID, job.State)
	}
	if job.LeaseOwner == nil || *job.LeaseOwner != plan.Owner {
		return fmt.Errorf("%w: Job %s is not leased by %s", acquisition.ErrRefreshFence, job.ID, plan.Owner)
	}
	if job.ClaimAttempts != plan.ExpectedClaim {
		return fmt.Errorf("%w: Job %s claim generation %d does not match %d",
			acquisition.ErrRefreshFence, job.ID, job.ClaimAttempts, plan.ExpectedClaim)
	}
	if expired {
		return fmt.Errorf("%w: Job %s lease has expired", acquisition.ErrRefreshFence, job.ID)
	}
	return nil
}

// validateRefreshPlan rejects a tampered plan rather than trusting its caller.
func validateRefreshPlan(plan acquisition.RefreshPlan) error {
	if plan.ManifestID == "" || plan.JobID == "" || plan.Owner == "" ||
		plan.ExpectedClaim < 1 || plan.Now.IsZero() || plan.RetryAt.IsZero() {
		return acquisition.ErrInvalidRefreshRequest
	}
	if plan.ManifestState != acquisition.StateAwaitingCanonical || plan.JobState != jobs.StateRetryWait {
		return fmt.Errorf("%w: unsupported handoff %s/%s",
			acquisition.ErrInvalidRefreshRequest, plan.ManifestState, plan.JobState)
	}
	if !plan.RetryAt.After(plan.Now) {
		return fmt.Errorf("%w: RetryAt must be after Now", acquisition.ErrInvalidRefreshRequest)
	}
	return nil
}

func refreshPersistenceError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", acquisition.ErrRefreshPersistence, operation, err)
}
