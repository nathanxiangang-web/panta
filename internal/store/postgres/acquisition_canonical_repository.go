package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

// CanonicalStageRepository commits one bounded canonical-confirmation invocation as
// a single PostgreSQL transaction over the Acquisition Manifest, its linked Job, and
// the confirming Copy row.
//
// It never creates or updates a Copy: D-017 keeps Copy creation and availability with
// the Journal Projector. This repository only reads the proposed Copy under lock to
// prove the application-layer read was not stale, and then finalizes the acquisition.
type CanonicalStageRepository struct {
	db canonicalDB
	// afterManifestUpdate is a test-only injection point placed between the row
	// mutations so rollback atomicity can be proven.
	afterManifestUpdate func(context.Context, pgx.Tx) error
}

type canonicalDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

var _ acquisition.CanonicalStageStore = (*CanonicalStageRepository)(nil)

func NewCanonicalStageRepository(db canonicalDB) (*CanonicalStageRepository, error) {
	if db == nil {
		return nil, errors.New("acquisition canonical database is required")
	}
	return &CanonicalStageRepository{db: db}, nil
}

func (repository *CanonicalStageRepository) CommitCanonical(
	ctx context.Context,
	plan acquisition.CanonicalPlan,
) (acquisition.CanonicalResult, error) {
	if err := validateCanonicalPlan(plan); err != nil {
		return acquisition.CanonicalResult{}, err
	}
	tx, err := repository.db.Begin(ctx)
	if err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Lock order stays Manifest then Job, matching activation, the provider-stage
	// handoff, and the scope handoff.
	manifest, err := scanAcquisitionManifest(tx.QueryRow(ctx, `SELECT `+acquisitionManifestColumns+`
FROM acquisition_manifests
WHERE manifest_id = $1
FOR UPDATE`, string(plan.ManifestID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: %s", acquisition.ErrCanonicalManifestNotFnd, plan.ManifestID)
	}
	if err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("lock Manifest", err)
	}

	job, err := getOneJob(ctx, tx, "SELECT "+jobColumns+" FROM jobs WHERE job_id = $1 FOR UPDATE", string(plan.JobID))
	if errors.Is(err, jobs.ErrNotFound) {
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: %s", acquisition.ErrCanonicalFence, plan.JobID)
	}
	if err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("lock Job", err)
	}

	if err := verifyCanonicalManifestLinkage(manifest, plan); err != nil {
		return acquisition.CanonicalResult{}, err
	}

	// Exact replay is decided against the locked Manifest, so a finalized acquisition
	// is reported as an idempotent no-op before any fence or Copy work. A different
	// proposed result Copy after READY fails closed.
	if manifest.State == acquisition.StateReady {
		return repository.commitCanonicalReplay(ctx, tx, manifest, job, plan)
	}
	if manifest.State != acquisition.StateAwaitingCanonical {
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: Manifest %s is %s",
			acquisition.ErrCanonicalManifestState, manifest.ID, manifest.State)
	}

	expired, err := leaseExpiredByDatabaseTime(ctx, tx, job)
	if err != nil {
		return acquisition.CanonicalResult{}, err
	}
	if err := verifyCanonicalJobFence(manifest, job, plan, expired); err != nil {
		return acquisition.CanonicalResult{}, err
	}

	if plan.Ready {
		// Revalidate the exact Copy under its own row lock, so a stale application-layer
		// read can never authorize READY.
		copyRecord, err := scanCanonicalCopy(tx.QueryRow(ctx, `SELECT `+canonicalCopyColumns+`
FROM copies
WHERE copy_id = $1
FOR SHARE`, string(plan.ResultCopyID)))
		if errors.Is(err, pgx.ErrNoRows) {
			return acquisition.CanonicalResult{}, fmt.Errorf("%w: Copy %s",
				acquisition.ErrCanonicalCopyMissing, plan.ResultCopyID)
		}
		if err != nil {
			return acquisition.CanonicalResult{}, canonicalPersistenceError("lock confirming Copy", err)
		}
		if err := verifyCanonicalLockedCopy(copyRecord, plan); err != nil {
			return acquisition.CanonicalResult{}, err
		}
	}

	if plan.Ready {
		return repository.commitCanonicalReady(ctx, tx, manifest, job, plan)
	}
	return repository.commitCanonicalPending(ctx, tx, manifest, job, plan)
}

// commitCanonicalPending reschedules the same Job without touching the Manifest: zero
// canonical matches or a not-yet-projected Copy is normal pending work, never a
// failure, and never consumes the failure budget.
func (repository *CanonicalStageRepository) commitCanonicalPending(
	ctx context.Context,
	tx pgx.Tx,
	manifest acquisition.Manifest,
	job jobs.Job,
	plan acquisition.CanonicalPlan,
) (acquisition.CanonicalResult, error) {
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
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: Job %s fence no longer matches",
			acquisition.ErrCanonicalConflict, plan.JobID)
	}
	if err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("reschedule Job for canonical retry", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("commit canonical pending", err)
	}
	return acquisition.CanonicalResult{Manifest: manifest, Job: updatedJob, Changed: true}, nil
}

// commitCanonicalReady finalizes the acquisition: the Manifest becomes READY with
// its immutable result link, and the same Job becomes SUCCEEDED.
func (repository *CanonicalStageRepository) commitCanonicalReady(
	ctx context.Context,
	tx pgx.Tx,
	manifest acquisition.Manifest,
	job jobs.Job,
	plan acquisition.CanonicalPlan,
) (acquisition.CanonicalResult, error) {
	updatedManifest, err := scanAcquisitionManifest(tx.QueryRow(ctx, `
UPDATE acquisition_manifests
SET state = $3,
    result_copy_id = $5,
    updated_at = $4
WHERE manifest_id = $1 AND state = $2
  AND result_copy_id IS NULL
RETURNING `+acquisitionManifestColumns,
		string(plan.ManifestID), string(acquisition.StateAwaitingCanonical),
		string(acquisition.StateReady), plan.Now, string(plan.ResultCopyID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: Manifest %s changed during finalization",
			acquisition.ErrCanonicalConflict, plan.ManifestID)
	}
	if err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("finalize Manifest as READY", err)
	}
	if repository.afterManifestUpdate != nil {
		if err := repository.afterManifestUpdate(ctx, tx); err != nil {
			return acquisition.CanonicalResult{}, canonicalPersistenceError("after Manifest update", err)
		}
	}

	updatedJob, err := scanJob(tx.QueryRow(ctx, `
UPDATE jobs AS job
SET state = 'SUCCEEDED',
    lease_owner = NULL,
    lease_expires_at = NULL,
    next_attempt_at = NULL,
    last_error = NULL,
    finished_at = $4,
    updated_at = $4
WHERE job_id = $1 AND state = 'RUNNING' AND lease_owner = $2
  AND claim_attempts = $3 AND lease_expires_at > CURRENT_TIMESTAMP
RETURNING `+updatedJobColumns,
		string(plan.JobID), plan.Owner, plan.ExpectedClaim, plan.Now))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: Job %s fence no longer matches",
			acquisition.ErrCanonicalConflict, plan.JobID)
	}
	if err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("finalize Job as SUCCEEDED", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("commit canonical finalization", err)
	}
	return acquisition.CanonicalResult{Manifest: updatedManifest, Job: updatedJob, Changed: true}, nil
}

// commitCanonicalReplay reports an already-finalized acquisition. It performs no
// Copy work and rewrites no timestamp.
func (repository *CanonicalStageRepository) commitCanonicalReplay(
	ctx context.Context,
	tx pgx.Tx,
	manifest acquisition.Manifest,
	job jobs.Job,
	plan acquisition.CanonicalPlan,
) (acquisition.CanonicalResult, error) {
	if manifest.ResultCopyID == nil {
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: Manifest %s is READY without a result Copy",
			acquisition.ErrCanonicalCopyConflict, manifest.ID)
	}
	if plan.ResultCopyID == "" || plan.ResultCopyID != *manifest.ResultCopyID {
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: Manifest %s is confirmed by Copy %s, not the proposed Copy",
			acquisition.ErrCanonicalCopyConflict, manifest.ID, *manifest.ResultCopyID)
	}
	if job.State != jobs.StateSucceeded {
		return acquisition.CanonicalResult{}, fmt.Errorf("%w: Manifest %s is READY but Job %s is %s",
			acquisition.ErrCanonicalConflict, manifest.ID, job.ID, job.State)
	}
	if err := tx.Commit(ctx); err != nil {
		return acquisition.CanonicalResult{}, canonicalPersistenceError("commit canonical replay", err)
	}
	return acquisition.CanonicalResult{Manifest: manifest, Job: job, Changed: false}, nil
}

// verifyCanonicalLockedCopy proves the locked Copy still is the accepted physical
// result at commit time.
func verifyCanonicalLockedCopy(copyRecord catalog.Copy, plan acquisition.CanonicalPlan) error {
	if copyRecord.ID != plan.ResultCopyID {
		return fmt.Errorf("%w: locked Copy %s, want %s",
			acquisition.ErrCanonicalCopyConflict, copyRecord.ID, plan.ResultCopyID)
	}
	if copyRecord.IndexCoreRootID != plan.ExpectedCopyRootID ||
		copyRecord.IndexCoreResourceID != plan.ExpectedCopyResourceID {
		return fmt.Errorf("%w: locked Copy %s is %s/%s, want %s/%s",
			acquisition.ErrCanonicalIdentity, copyRecord.ID,
			copyRecord.IndexCoreRootID, copyRecord.IndexCoreResourceID,
			plan.ExpectedCopyRootID, plan.ExpectedCopyResourceID)
	}
	if string(copyRecord.StorageBindingID) != string(plan.ExpectedCopyBindingID) {
		return fmt.Errorf("%w: locked Copy %s belongs to %s, want %s",
			acquisition.ErrCanonicalCopyBinding, copyRecord.ID,
			copyRecord.StorageBindingID, plan.ExpectedCopyBindingID)
	}
	if copyRecord.Availability != catalog.CopyAvailabilityPresent {
		return fmt.Errorf("%w: locked Copy %s availability is %q, want PRESENT",
			acquisition.ErrCanonicalCopyRemoved, copyRecord.ID, copyRecord.Availability)
	}
	// The classification intent must match at commit time: the Copy must already be
	// bound to the Manifest Variant when the Manifest declares one.
	if plan.ManifestVariantID != nil {
		if copyRecord.VariantID == nil || *copyRecord.VariantID != *plan.ManifestVariantID {
			return fmt.Errorf("%w: locked Copy %s is not bound to Manifest Variant %s",
				acquisition.ErrCanonicalCopyClassified, copyRecord.ID, *plan.ManifestVariantID)
		}
	}
	return nil
}

func verifyCanonicalManifestLinkage(manifest acquisition.Manifest, plan acquisition.CanonicalPlan) error {
	if manifest.ID != plan.ManifestID {
		return fmt.Errorf("%w: requested %s, read %s",
			acquisition.ErrExecutionIdentityMismatch, plan.ManifestID, manifest.ID)
	}
	if manifest.JobID == nil || *manifest.JobID != plan.JobID {
		return fmt.Errorf("%w: Manifest %s is not linked to Job %s",
			acquisition.ErrCanonicalFence, manifest.ID, plan.JobID)
	}
	return nil
}

func verifyCanonicalJobFence(
	manifest acquisition.Manifest,
	job jobs.Job,
	plan acquisition.CanonicalPlan,
	expired bool,
) error {
	if err := acquisition.ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return fmt.Errorf("%w: %w", acquisition.ErrCanonicalFence, err)
	}
	if job.State != jobs.StateRunning {
		return fmt.Errorf("%w: Job %s is %s", acquisition.ErrCanonicalFence, job.ID, job.State)
	}
	if job.LeaseOwner == nil || *job.LeaseOwner != plan.Owner {
		return fmt.Errorf("%w: Job %s is not leased by %s", acquisition.ErrCanonicalFence, job.ID, plan.Owner)
	}
	if job.ClaimAttempts != plan.ExpectedClaim {
		return fmt.Errorf("%w: Job %s claim generation %d does not match %d",
			acquisition.ErrCanonicalFence, job.ID, job.ClaimAttempts, plan.ExpectedClaim)
	}
	if expired {
		return fmt.Errorf("%w: Job %s lease has expired", acquisition.ErrCanonicalFence, job.ID)
	}
	return nil
}

func validateCanonicalPlan(plan acquisition.CanonicalPlan) error {
	if plan.ManifestID == "" || plan.JobID == "" || plan.Owner == "" ||
		plan.ExpectedClaim < 1 || plan.Now.IsZero() {
		return acquisition.ErrInvalidCanonicalRequest
	}
	if plan.Ready {
		if plan.ResultCopyID == "" || plan.ExpectedCopyRootID == "" ||
			plan.ExpectedCopyResourceID == "" || plan.ExpectedCopyBindingID == "" ||
			plan.ExpectedCopyAvailability != catalog.CopyAvailabilityPresent {
			return acquisition.ErrInvalidCanonicalRequest
		}
		if plan.ManifestState != acquisition.StateReady || plan.JobState != jobs.StateSucceeded {
			return acquisition.ErrInvalidCanonicalRequest
		}
		return nil
	}
	if plan.ResultCopyID != "" {
		return fmt.Errorf("%w: a pending canonical plan must not carry a result Copy",
			acquisition.ErrInvalidCanonicalRequest)
	}
	if plan.ManifestState != acquisition.StateAwaitingCanonical || plan.JobState != jobs.StateRetryWait {
		return acquisition.ErrInvalidCanonicalRequest
	}
	if plan.RetryAt == nil || plan.RetryAt.IsZero() || !plan.RetryAt.After(plan.Now) {
		return fmt.Errorf("%w: RetryAt must be after Now", acquisition.ErrInvalidCanonicalRequest)
	}
	return nil
}

const canonicalCopyColumns = `
copy_id::text, variant_id::text, indexcore_root_id, indexcore_resource_id,
storage_binding_id::text, availability, created_at, updated_at`

func scanCanonicalCopy(row pgx.Row) (catalog.Copy, error) {
	var copyRecord catalog.Copy
	var copyID, bindingID string
	var variantID *string
	if err := row.Scan(&copyID, &variantID, &copyRecord.IndexCoreRootID, &copyRecord.IndexCoreResourceID,
		&bindingID, &copyRecord.Availability, &copyRecord.CreatedAt, &copyRecord.UpdatedAt); err != nil {
		return catalog.Copy{}, err
	}
	copyRecord.ID = catalog.CopyID(copyID)
	copyRecord.StorageBindingID = catalog.StorageBindingID(bindingID)
	if variantID != nil {
		value := catalog.VariantID(*variantID)
		copyRecord.VariantID = &value
	}
	return copyRecord, nil
}

// storageBindingOf is a small local alias used by the ready plan plumbing.
type storageBindingOf = storage.BindingID

func canonicalPersistenceError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", acquisition.ErrCanonicalPersistence, operation, err)
}
