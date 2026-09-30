package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

var _ acquisition.ExpiredLeaseRecoveryStore = (*JobRepository)(nil)

const maxAcquisitionRecoveryBatch = 100

// MarkExpiredAcquisitionRecoveryRequired recovers up to Limit expired leases.
// Candidate discovery takes no row lock: each pair is then locked Manifest first,
// Job second, matching every acquisition stage handoff. A stale candidate is
// harmless because all identity, state and DB-time expiry facts are rechecked.
func (repository *JobRepository) MarkExpiredAcquisitionRecoveryRequired(
	ctx context.Context, request jobs.RecoveryRequest,
) (int64, error) {
	if request.Now.IsZero() || request.Limit < 1 || request.Limit > maxAcquisitionRecoveryBatch {
		return 0, jobs.ErrInvalidArgument
	}
	rows, err := repository.pool.Query(ctx, `
SELECT `+jobColumns+`
FROM jobs
WHERE job_type = $1 AND state = 'RUNNING'
  AND lease_expires_at <= CURRENT_TIMESTAMP
ORDER BY lease_expires_at, job_id
LIMIT $2`, acquisition.JobTypeAcquisition, request.Limit)
	if err != nil {
		return 0, fmt.Errorf("find expired acquisition jobs: %w", err)
	}
	var candidates []jobs.Job
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			rows.Close()
			return 0, fmt.Errorf("read expired acquisition job: %w", scanErr)
		}
		candidates = append(candidates, job)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, fmt.Errorf("read expired acquisition candidates: %w", err)
	}

	var recovered int64
	for _, candidate := range candidates {
		changed, recoverErr := repository.recoverExpiredAcquisitionPair(ctx, candidate, request.Now)
		if recoverErr != nil {
			return recovered, recoverErr
		}
		if changed {
			recovered++
		}
	}
	return recovered, nil
}

func (repository *JobRepository) recoverExpiredAcquisitionPair(
	ctx context.Context, candidate jobs.Job, now time.Time,
) (bool, error) {
	manifestID, err := acquisition.LinkedManifestID(candidate)
	if err != nil {
		return false, fmt.Errorf("%w: Job %s invalid linkage: %v", acquisition.ErrLeaseRecoveryDebt, candidate.ID, err)
	}
	var parsedManifestID pgtype.UUID
	if err := parsedManifestID.Scan(string(manifestID)); err != nil {
		return false, fmt.Errorf("%w: Job %s has invalid Manifest ID", acquisition.ErrLeaseRecoveryDebt, candidate.ID)
	}
	tx, err := repository.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin acquisition recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	manifest, err := scanAcquisitionManifest(tx.QueryRow(ctx, `SELECT `+acquisitionManifestColumns+`
FROM acquisition_manifests WHERE manifest_id = $1 FOR UPDATE`, string(manifestID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("%w: Manifest %s missing for Job %s", acquisition.ErrLeaseRecoveryDebt, manifestID, candidate.ID)
	}
	if err != nil {
		return false, fmt.Errorf("lock recovery Manifest: %w", err)
	}
	job, err := getOneJob(ctx, tx, "SELECT "+jobColumns+" FROM jobs WHERE job_id = $1 FOR UPDATE", string(candidate.ID))
	if errors.Is(err, jobs.ErrNotFound) {
		return false, fmt.Errorf("%w: Job %s disappeared", acquisition.ErrLeaseRecoveryDebt, candidate.ID)
	}
	if err != nil {
		return false, fmt.Errorf("lock recovery Job: %w", err)
	}
	if manifest.JobID == nil || *manifest.JobID != job.ID {
		return false, fmt.Errorf("%w: Manifest %s reverse link differs from Job %s", acquisition.ErrLeaseRecoveryDebt, manifest.ID, job.ID)
	}
	if err := acquisition.ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return false, fmt.Errorf("%w: Job %s changed linkage: %v", acquisition.ErrLeaseRecoveryDebt, job.ID, err)
	}
	activeManifest := manifest.State == acquisition.StateActive || manifest.State == acquisition.StateAwaitingVisibility || manifest.State == acquisition.StateAwaitingCanonical
	// Another stage may have committed while these locks were being acquired.
	// Only a valid handoff pair is a harmless stale candidate; a mismatched pair
	// remains explicit recovery debt rather than being silently skipped.
	if job.State != jobs.StateRunning {
		validHandoff := (job.State == jobs.StateRetryWait && activeManifest) ||
			(manifest.State == acquisition.StateReady && job.State == jobs.StateSucceeded && manifest.ResultCopyID != nil) ||
			(manifest.State == acquisition.StateFailed && job.State == jobs.StateFailed) ||
			(manifest.State == acquisition.StateCanceled && job.State == jobs.StateCanceled) ||
			(manifest.State == acquisition.StateRecoveryRequired && job.State == jobs.StateRecoveryRequired)
		if !validHandoff {
			return false, fmt.Errorf("%w: Manifest %s and Job %s have incompatible states %s/%s",
				acquisition.ErrLeaseRecoveryDebt, manifest.ID, job.ID, manifest.State, job.State)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit stale recovery check: %w", err)
		}
		return false, nil
	}
	if !activeManifest || job.LeaseExpiresAt == nil {
		return false, fmt.Errorf("%w: Manifest %s and Job %s have incompatible active state or lease",
			acquisition.ErrLeaseRecoveryDebt, manifest.ID, job.ID)
	}
	var expired bool
	if err := tx.QueryRow(ctx, `SELECT $1::timestamptz <= CURRENT_TIMESTAMP`, job.LeaseExpiresAt).Scan(&expired); err != nil {
		return false, fmt.Errorf("check acquisition lease expiry: %w", err)
	}
	if !expired {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit unexpired recovery check: %w", err)
		}
		return false, nil
	}
	if _, err := tx.Exec(ctx, `UPDATE acquisition_manifests SET state = 'RECOVERY_REQUIRED', updated_at = $2
WHERE manifest_id = $1`, string(manifest.ID), now); err != nil {
		return false, fmt.Errorf("recover acquisition Manifest: %w", err)
	}
	if repository.afterAcquisitionManifestRecovery != nil {
		if err := repository.afterAcquisitionManifestRecovery(ctx, tx); err != nil {
			return false, fmt.Errorf("after acquisition Manifest recovery: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs
SET state = 'RECOVERY_REQUIRED', lease_owner = NULL, lease_expires_at = NULL,
    next_attempt_at = NULL, last_error = $3, finished_at = NULL, updated_at = $2
WHERE job_id = $1`, string(job.ID), now, expiredLeaseMessage); err != nil {
		return false, fmt.Errorf("recover acquisition Job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit acquisition recovery: %w", err)
	}
	return true, nil
}
