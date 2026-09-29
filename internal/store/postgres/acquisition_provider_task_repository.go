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
manifest_id::text, job_id::text, provider_id, provider_task_ref, created_at, updated_at`

type acquisitionProviderTaskDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// AcquisitionProviderTaskRepository persists the durable provider-task linkage.
// It never overwrites an existing task reference: a conflicting identity fails
// closed with ErrProviderTaskIdentityChange so an uncertain external side effect
// is never silently re-linked to a different task.
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

// StoreProviderTask inserts the linkage exactly once. ON CONFLICT DO NOTHING
// keeps a known task reference immutable. A conflict is either the same identity
// (idempotent replay) or a competing linkage that must fail closed.
func (repository *AcquisitionProviderTaskRepository) StoreProviderTask(ctx context.Context, task acquisition.ProviderTask) error {
	if err := acquisition.ValidateProviderTask(task); err != nil {
		return err
	}
	if _, err := repository.db.Exec(ctx, `
INSERT INTO acquisition_provider_tasks (
    manifest_id, job_id, provider_id, provider_task_ref, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING`,
		string(task.ManifestID), string(task.JobID), string(task.ProviderID), task.ProviderTaskRef,
		task.CreatedAt, task.UpdatedAt,
	); err != nil {
		return providerTaskWriteError("store provider task", err)
	}
	stored, err := scanAcquisitionProviderTask(repository.db.QueryRow(ctx, `SELECT `+acquisitionProviderTaskColumns+`
FROM acquisition_provider_tasks
WHERE manifest_id = $1`, string(task.ManifestID)))
	if err == nil {
		if stored.JobID != task.JobID || stored.ProviderID != task.ProviderID || stored.ProviderTaskRef != task.ProviderTaskRef {
			return fmt.Errorf("%w: Manifest %s", acquisition.ErrProviderTaskIdentityChange, task.ManifestID)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return providerTaskPersistenceError("read stored provider task", err)
	}
	// No row for this Manifest: the Job is already linked to a different
	// Manifest, which the MVP uniqueness rule forbids.
	var holder string
	if err := repository.db.QueryRow(ctx, `
SELECT manifest_id::text
FROM acquisition_provider_tasks
WHERE job_id = $1`, string(task.JobID)).Scan(&holder); err == nil {
		return fmt.Errorf("%w: Job %s is already linked to Manifest %s", acquisition.ErrProviderTaskConflict, task.JobID, holder)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return providerTaskPersistenceError("read competing provider task", err)
	}
	return providerTaskPersistenceError("read stored provider task", errors.New("provider task insert was not persisted"))
}

func scanAcquisitionProviderTask(row pgx.Row) (acquisition.ProviderTask, error) {
	var task acquisition.ProviderTask
	var manifestID, jobID, providerID string
	err := row.Scan(
		&manifestID, &jobID, &providerID, &task.ProviderTaskRef, &task.CreatedAt, &task.UpdatedAt,
	)
	if err != nil {
		return acquisition.ProviderTask{}, err
	}
	task.ManifestID = acquisition.ManifestID(manifestID)
	task.JobID = jobs.JobID(jobID)
	task.ProviderID = contracts.ProviderID(providerID)
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
