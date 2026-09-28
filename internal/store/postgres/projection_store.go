package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/nathanxiangang-web/panta/internal/projector"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

type projectionDB interface {
	Begin(context.Context) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// ProjectionStore atomically applies unresolved Copy mutations and advances
// their per-binding IndexCore Journal cursor.
type ProjectionStore struct {
	db projectionDB
}

var _ projector.ProjectionStore = (*ProjectionStore)(nil)

func NewProjectionStore(db projectionDB) (*ProjectionStore, error) {
	if db == nil {
		return nil, errors.New("projection database is required")
	}
	return &ProjectionStore{db: db}, nil
}

func (store *ProjectionStore) Cursor(ctx context.Context, bindingID storage.BindingID) (int64, error) {
	if bindingID == "" {
		return 0, projector.ErrInvalidArgument
	}
	var cursor int64
	err := store.db.QueryRow(ctx, `
SELECT last_event_seq
FROM indexcore_projection_cursors
WHERE storage_binding_id = $1`, string(bindingID)).Scan(&cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read IndexCore projection cursor: %w", err)
	}
	return cursor, nil
}

func (store *ProjectionStore) ApplyBatch(ctx context.Context, batch projector.Batch) (err error) {
	if err := validateProjectionBatch(batch); err != nil {
		return err
	}
	tx, err := store.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin projection transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, `
INSERT INTO indexcore_projection_cursors (storage_binding_id, last_event_seq, updated_at)
VALUES ($1, 0, clock_timestamp())
ON CONFLICT (storage_binding_id) DO NOTHING`, string(batch.StorageBindingID)); err != nil {
		return fmt.Errorf("initialize projection cursor: %w", err)
	}
	var current int64
	if err = tx.QueryRow(ctx, `
SELECT last_event_seq
FROM indexcore_projection_cursors
WHERE storage_binding_id = $1
FOR UPDATE`, string(batch.StorageBindingID)).Scan(&current); err != nil {
		return fmt.Errorf("lock projection cursor: %w", err)
	}
	if current != batch.ExpectedCursor {
		return fmt.Errorf("%w: expected %d, current %d", projector.ErrCursorConflict, batch.ExpectedCursor, current)
	}

	for position, mutation := range batch.Mutations {
		var copyID string
		err = tx.QueryRow(ctx, `
INSERT INTO copies (
    copy_id, variant_id, indexcore_root_id, indexcore_resource_id,
    storage_binding_id, availability, created_at, updated_at
) VALUES (gen_random_uuid(), NULL, $1, $2, $3, $4, clock_timestamp(), clock_timestamp())
ON CONFLICT (indexcore_root_id, indexcore_resource_id) DO UPDATE
SET availability = EXCLUDED.availability,
    updated_at = EXCLUDED.updated_at
WHERE copies.storage_binding_id = EXCLUDED.storage_binding_id
RETURNING copy_id::text`,
			mutation.IndexCoreRootID, mutation.IndexCoreResourceID,
			string(mutation.StorageBindingID), string(mutation.Availability),
		).Scan(&copyID)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: mutation %d root %q resource %q", projector.ErrBindingConflict,
				position, mutation.IndexCoreRootID, mutation.IndexCoreResourceID)
		}
		if err != nil {
			return fmt.Errorf("apply projection mutation %d: %w", position, err)
		}
	}

	command, err := tx.Exec(ctx, `
UPDATE indexcore_projection_cursors
SET last_event_seq = $2, updated_at = clock_timestamp()
WHERE storage_binding_id = $1 AND last_event_seq = $3`,
		string(batch.StorageBindingID), batch.LastEventSeq, batch.ExpectedCursor)
	if err != nil {
		return fmt.Errorf("advance projection cursor: %w", err)
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("%w: expected %d", projector.ErrCursorConflict, batch.ExpectedCursor)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit projection transaction: %w", err)
	}
	return nil
}

func validateProjectionBatch(batch projector.Batch) error {
	if batch.StorageBindingID == "" || batch.ExpectedCursor < 0 || batch.LastEventSeq <= batch.ExpectedCursor {
		return projector.ErrInvalidArgument
	}
	for _, mutation := range batch.Mutations {
		if mutation.StorageBindingID != batch.StorageBindingID || strings.TrimSpace(mutation.IndexCoreRootID) == "" ||
			strings.TrimSpace(mutation.IndexCoreResourceID) == "" ||
			(mutation.Availability != projector.AvailabilityPresent && mutation.Availability != projector.AvailabilityRemoved) {
			return projector.ErrInvalidArgument
		}
	}
	return nil
}
