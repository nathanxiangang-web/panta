package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

type providerOutcomeDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

// ProviderOutcomeRepository commits one provider-stage outcome as a single
// PostgreSQL transaction over the Acquisition Manifest and its linked Job.
//
// It never calls a provider, registry, session, OpenList, IndexCore, search,
// agent, or auth component: the provider stage only records durable state.
type ProviderOutcomeRepository struct {
	db providerOutcomeDB
	// afterManifestUpdate is a test-only injection point placed between the two
	// row mutations so rollback atomicity can be proven.
	afterManifestUpdate func(context.Context, pgx.Tx) error
}

var _ acquisition.ProviderOutcomeStore = (*ProviderOutcomeRepository)(nil)

func NewProviderOutcomeRepository(db providerOutcomeDB) (*ProviderOutcomeRepository, error) {
	if db == nil {
		return nil, errors.New("acquisition provider outcome database is required")
	}
	return &ProviderOutcomeRepository{db: db}, nil
}

func (repository *ProviderOutcomeRepository) CommitProviderOutcome(
	ctx context.Context,
	plan acquisition.ProviderOutcomePlan,
) (acquisition.ProviderOutcomeResult, error) {
	if err := validateProviderOutcomePlan(plan); err != nil {
		return acquisition.ProviderOutcomeResult{}, err
	}
	tx, err := repository.db.Begin(ctx)
	if err != nil {
		return acquisition.ProviderOutcomeResult{}, providerOutcomePersistenceError("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Lock order is Manifest then Job, matching atomic Manifest activation, so a
	// concurrent handoff cannot deadlock against activation.
	manifest, err := scanAcquisitionManifest(tx.QueryRow(ctx, `SELECT `+acquisitionManifestColumns+`
FROM acquisition_manifests
WHERE manifest_id = $1
FOR UPDATE`, string(plan.ManifestID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.ProviderOutcomeResult{}, fmt.Errorf("%w: %s", acquisition.ErrNotFound, plan.ManifestID)
	}
	if err != nil {
		return acquisition.ProviderOutcomeResult{}, providerOutcomePersistenceError("lock Manifest", err)
	}

	job, err := getOneJob(ctx, tx, "SELECT "+jobColumns+" FROM jobs WHERE job_id = $1 FOR UPDATE", string(plan.JobID))
	if errors.Is(err, jobs.ErrNotFound) {
		return acquisition.ProviderOutcomeResult{}, fmt.Errorf("%w: %s", acquisition.ErrProviderOutcomeJobMismatch, plan.JobID)
	}
	if err != nil {
		return acquisition.ProviderOutcomeResult{}, providerOutcomePersistenceError("lock Job", err)
	}

	// Decide replay versus fresh handoff from the durable pair, not from the
	// Manifest state alone. PROVIDER_IN_PROGRESS legitimately leaves the Manifest
	// ACTIVE, so keying on "Manifest is not ACTIVE" would misclassify its own
	// committed outcome as a fresh handoff and fail its replay.
	replay, err := repository.isCommittedOutcome(manifest, job, plan)
	if err != nil {
		return acquisition.ProviderOutcomeResult{}, err
	}
	if replay {
		if err := tx.Commit(ctx); err != nil {
			return acquisition.ProviderOutcomeResult{}, providerOutcomePersistenceError("commit provider outcome replay", err)
		}
		return acquisition.ProviderOutcomeResult{Manifest: manifest, Job: job, Changed: false}, nil
	}

	if manifest.State != acquisition.StateActive {
		return acquisition.ProviderOutcomeResult{}, fmt.Errorf("%w: Manifest %s is %s",
			acquisition.ErrProviderOutcomeManifestState, manifest.ID, manifest.State)
	}
	if err := verifyActiveManifestHandoff(manifest, plan); err != nil {
		return acquisition.ProviderOutcomeResult{}, err
	}
	expired, err := leaseExpiredByDatabaseTime(ctx, tx, job)
	if err != nil {
		return acquisition.ProviderOutcomeResult{}, err
	}
	if err := verifyRunningJobFence(manifest, job, plan, expired); err != nil {
		return acquisition.ProviderOutcomeResult{}, err
	}

	// Mutation 1: advance the Manifest milestone only while it still holds the
	// accepted pre-handoff state. When the outcome keeps the Manifest at ACTIVE,
	// updated_at is deliberately preserved: nothing about the Manifest changed, so
	// rewriting its timestamp would be a spurious durable write.
	updatedManifest, err := scanAcquisitionManifest(tx.QueryRow(ctx, `
UPDATE acquisition_manifests
SET state = $3,
    updated_at = CASE WHEN state = $3 THEN updated_at ELSE $4 END
WHERE manifest_id = $1 AND state = $2
RETURNING `+acquisitionManifestColumns,
		string(plan.ManifestID), string(acquisition.StateActive), string(plan.ManifestState), plan.Now))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.ProviderOutcomeResult{}, fmt.Errorf("%w: Manifest %s changed during handoff",
			acquisition.ErrProviderOutcomeConflict, plan.ManifestID)
	}
	if err != nil {
		return acquisition.ProviderOutcomeResult{}, providerOutcomePersistenceError("advance Manifest milestone", err)
	}
	if repository.afterManifestUpdate != nil {
		if err := repository.afterManifestUpdate(ctx, tx); err != nil {
			return acquisition.ProviderOutcomeResult{}, providerOutcomePersistenceError("after Manifest update", err)
		}
	}

	// Mutation 2: advance the Job under the same claim-generation fence.
	updatedJob, err := mutateJobForOutcome(ctx, tx, plan)
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.ProviderOutcomeResult{}, fmt.Errorf("%w: Job %s fence no longer matches",
			acquisition.ErrProviderOutcomeConflict, plan.JobID)
	}
	if err != nil {
		return acquisition.ProviderOutcomeResult{}, providerOutcomePersistenceError("advance Job state", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return acquisition.ProviderOutcomeResult{}, providerOutcomePersistenceError("commit provider outcome", err)
	}
	return acquisition.ProviderOutcomeResult{Manifest: updatedManifest, Job: updatedJob, Changed: true}, nil
}

// isCommittedOutcome reports whether the durable records already hold exactly the
// outcome this plan would commit. Both halves of the D-026 pairing must match, so
// a different proposed outcome after a committed one still fails closed.
func (repository *ProviderOutcomeRepository) isCommittedOutcome(
	manifest acquisition.Manifest,
	job jobs.Job,
	plan acquisition.ProviderOutcomePlan,
) (bool, error) {
	if manifest.ID != plan.ManifestID || manifest.State != plan.ManifestState {
		return false, nil
	}
	if manifest.JobID == nil || *manifest.JobID != plan.JobID {
		return false, fmt.Errorf("%w: Manifest %s is not linked to Job %s",
			acquisition.ErrProviderOutcomeJobMismatch, manifest.ID, plan.JobID)
	}
	if job.State != plan.JobState {
		return false, nil
	}
	if err := acquisition.ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return false, fmt.Errorf("%w: %w", acquisition.ErrProviderOutcomeJobMismatch, err)
	}
	return true, nil
}

// mutateJobForOutcome applies the D-026 Job semantics under the claim-generation
// fence. Neither the claim generation nor the failure budget is modified here.
func mutateJobForOutcome(ctx context.Context, tx pgx.Tx, plan acquisition.ProviderOutcomePlan) (jobs.Job, error) {
	switch plan.JobState {
	case jobs.StateRetryWait:
		// RETRY_WAIT here queues the next acquisition stage, so it deliberately
		// does not consult max_attempts the way a same-stage retry does.
		return scanJob(tx.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'RETRY_WAIT',
    lease_owner = NULL,
    lease_expires_at = NULL,
    next_attempt_at = $6::timestamptz,
    last_error = $5,
    finished_at = NULL,
    updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns,
			string(plan.JobID), plan.Owner, plan.ExpectedClaim, plan.Now, plan.ErrorMessage, plan.RetryAt))
	case jobs.StateFailed:
		return scanJob(tx.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'FAILED',
    lease_owner = NULL,
    lease_expires_at = NULL,
    next_attempt_at = NULL,
    last_error = $5,
    finished_at = $4,
    updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns,
			string(plan.JobID), plan.Owner, plan.ExpectedClaim, plan.Now, plan.ErrorMessage))
	case jobs.StateCanceled:
		return scanJob(tx.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'CANCELED',
    lease_owner = NULL,
    lease_expires_at = NULL,
    next_attempt_at = NULL,
    finished_at = $4,
    updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns,
			string(plan.JobID), plan.Owner, plan.ExpectedClaim, plan.Now))
	case jobs.StateRecoveryRequired:
		// Recovery is non-terminal, so finished_at stays NULL and the Job is not
		// claimable by ordinary ClaimNext.
		return scanJob(tx.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'RECOVERY_REQUIRED',
    lease_owner = NULL,
    lease_expires_at = NULL,
    next_attempt_at = NULL,
    last_error = $5,
    finished_at = NULL,
    updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns,
			string(plan.JobID), plan.Owner, plan.ExpectedClaim, plan.Now, plan.ErrorMessage))
	default:
		return jobs.Job{}, acquisition.ErrInvalidProviderOutcome
	}
}

// verifyActiveManifestHandoff checks the exact Manifest identity and linkage for a
// fresh handoff.
func verifyActiveManifestHandoff(manifest acquisition.Manifest, plan acquisition.ProviderOutcomePlan) error {
	if manifest.ID != plan.ManifestID {
		return fmt.Errorf("%w: requested %s, read %s",
			acquisition.ErrProviderOutcomeManifestState, plan.ManifestID, manifest.ID)
	}
	if manifest.JobID == nil || *manifest.JobID != plan.JobID {
		return fmt.Errorf("%w: Manifest %s is not linked to Job %s",
			acquisition.ErrProviderOutcomeJobMismatch, manifest.ID, plan.JobID)
	}
	return nil
}

// verifyRunningJobFence proves the Job is the linked ACQUISITION Job and holds the
// fenced RUNNING lease for the expected claim generation.
func verifyRunningJobFence(manifest acquisition.Manifest, job jobs.Job, plan acquisition.ProviderOutcomePlan, expired bool) error {
	if err := acquisition.ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return fmt.Errorf("%w: %w", acquisition.ErrProviderOutcomeJobMismatch, err)
	}
	if job.State != jobs.StateRunning {
		return fmt.Errorf("%w: Job %s is %s", acquisition.ErrProviderOutcomeFence, job.ID, job.State)
	}
	if job.LeaseOwner == nil || *job.LeaseOwner != plan.Owner {
		return fmt.Errorf("%w: Job %s is not leased by %s", acquisition.ErrProviderOutcomeFence, job.ID, plan.Owner)
	}
	if job.ClaimAttempts != plan.ExpectedClaim {
		return fmt.Errorf("%w: Job %s claim generation %d does not match %d",
			acquisition.ErrProviderOutcomeFence, job.ID, job.ClaimAttempts, plan.ExpectedClaim)
	}
	if expired {
		return fmt.Errorf("%w: Job %s lease has expired", acquisition.ErrProviderOutcomeFence, job.ID)
	}
	return nil
}

// leaseExpiredByDatabaseTime authorizes lease validity with database time, exactly
// as the accepted Job Engine lease mutations do.
func leaseExpiredByDatabaseTime(ctx context.Context, tx pgx.Tx, job jobs.Job) (bool, error) {
	var expired bool
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz <= CURRENT_TIMESTAMP`, job.LeaseExpiresAt).Scan(&expired); err != nil {
		return false, providerOutcomePersistenceError("authorize lease with database time", err)
	}
	return expired, nil
}

func validateProviderOutcomePlan(plan acquisition.ProviderOutcomePlan) error {
	transition, err := acquisition.ProviderOutcomeTransitionFor(plan.Outcome)
	if err != nil {
		return err
	}
	if plan.ManifestID == "" || plan.JobID == "" || plan.Owner == "" || plan.ExpectedClaim < 1 || plan.Now.IsZero() {
		return acquisition.ErrInvalidProviderOutcome
	}
	if plan.ManifestState != transition.ManifestState || plan.JobState != transition.JobState {
		return acquisition.ErrInvalidProviderOutcome
	}
	if transition.RequiresRetryAt && plan.RetryAt == nil {
		return acquisition.ErrInvalidProviderOutcome
	}
	if transition.RequiresError && (plan.ErrorMessage == nil || *plan.ErrorMessage == "") {
		return acquisition.ErrInvalidProviderOutcome
	}
	return nil
}

func providerOutcomePersistenceError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", acquisition.ErrProviderOutcomePersistence, operation, err)
}
