package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

type acquisitionActivationDB interface {
	Begin(context.Context) (pgx.Tx, error)
}

type AcquisitionActivationRepository struct {
	db             acquisitionActivationDB
	afterJobInsert func(context.Context, pgx.Tx) error
}

var _ acquisition.ActivationStore = (*AcquisitionActivationRepository)(nil)

func NewAcquisitionActivationRepository(db acquisitionActivationDB) (*AcquisitionActivationRepository, error) {
	if db == nil {
		return nil, errors.New("acquisition activation database is required")
	}
	return &AcquisitionActivationRepository{db: db}, nil
}

func (repository *AcquisitionActivationRepository) ActivateManifest(ctx context.Context, plan acquisition.ActivationPlan) (acquisition.ActivateResult, error) {
	if plan.ManifestID == "" || plan.Job.ID == "" || plan.Job.MaxAttempts < 1 ||
		acquisition.ValidateLinkedAcquisitionJob(plan.ManifestID, jobs.Job{
			ID: plan.Job.ID, Type: plan.Job.Type, Payload: plan.Job.Payload, IdempotencyKey: plan.Job.IdempotencyKey,
		}) != nil {
		return acquisition.ActivateResult{}, acquisition.ErrInvalidActivation
	}
	tx, err := repository.db.Begin(ctx)
	if err != nil {
		return acquisition.ActivateResult{}, activationPersistenceError("begin transaction", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	manifest, err := scanAcquisitionManifest(tx.QueryRow(ctx, `SELECT `+acquisitionManifestColumns+`
FROM acquisition_manifests
WHERE manifest_id = $1
FOR UPDATE`, string(plan.ManifestID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.ActivateResult{}, fmt.Errorf("%w: %s", acquisition.ErrActivationManifestNotFound, plan.ManifestID)
	}
	if err != nil {
		return acquisition.ActivateResult{}, activationPersistenceError("lock Manifest", err)
	}

	switch manifest.State {
	case acquisition.StateActive:
		return repository.replay(ctx, tx, manifest)
	case acquisition.StatePending:
		if manifest.JobID != nil {
			return acquisition.ActivateResult{}, acquisition.ErrCorruptActivation
		}
	default:
		return acquisition.ActivateResult{}, fmt.Errorf("%w: %s", acquisition.ErrInvalidManifestState, manifest.State)
	}

	job, err := insertJob(ctx, tx, plan.Job)
	if err != nil {
		if errors.Is(err, jobs.ErrDuplicateIdempotencyKey) {
			return acquisition.ActivateResult{}, fmt.Errorf("%w: deterministic Job key already exists", acquisition.ErrCorruptActivation)
		}
		if errors.Is(err, jobs.ErrInvalidArgument) {
			return acquisition.ActivateResult{}, acquisition.ErrInvalidActivation
		}
		return acquisition.ActivateResult{}, activationPersistenceError("create Job", err)
	}
	if repository.afterJobInsert != nil {
		if err := repository.afterJobInsert(ctx, tx); err != nil {
			return acquisition.ActivateResult{}, activationPersistenceError("after Job insert", err)
		}
	}

	activated, err := scanAcquisitionManifest(tx.QueryRow(ctx, `
UPDATE acquisition_manifests
SET job_id = $2, state = 'ACTIVE', updated_at = clock_timestamp()
WHERE manifest_id = $1 AND state = 'PENDING' AND job_id IS NULL
RETURNING `+acquisitionManifestColumns, string(plan.ManifestID), string(job.ID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.ActivateResult{}, acquisition.ErrCorruptActivation
	}
	if err != nil {
		return acquisition.ActivateResult{}, activationPersistenceError("activate Manifest", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return acquisition.ActivateResult{}, activationPersistenceError("commit activation", err)
	}
	return acquisition.ActivateResult{Manifest: activated, Job: job, Changed: true}, nil
}

func (repository *AcquisitionActivationRepository) replay(ctx context.Context, tx pgx.Tx, manifest acquisition.Manifest) (acquisition.ActivateResult, error) {
	if manifest.JobID == nil {
		return acquisition.ActivateResult{}, acquisition.ErrCorruptActivation
	}
	job, err := getOneJob(ctx, tx, "SELECT "+jobColumns+" FROM jobs WHERE job_id = $1", string(*manifest.JobID))
	if errors.Is(err, jobs.ErrNotFound) {
		return acquisition.ActivateResult{}, acquisition.ErrCorruptActivation
	}
	if err != nil {
		return acquisition.ActivateResult{}, activationPersistenceError("read linked Job", err)
	}
	if err := acquisition.ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return acquisition.ActivateResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return acquisition.ActivateResult{}, activationPersistenceError("commit activation replay", err)
	}
	return acquisition.ActivateResult{Manifest: manifest, Job: job, Changed: false}, nil
}

func activationPersistenceError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", acquisition.ErrActivationPersistence, operation, err)
}
