package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/projector"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestPostgresProjectionStoreAtomicIdempotentConcurrentAndRestartSafe(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if !status.Compatible || status.CurrentVersion != 12 || status.LatestVersion != 12 || len(status.Applied) != 12 {
		t.Fatalf("migration status = %#v", status)
	}

	bindingOne := storage.BindingID("91000000-0000-4000-8000-000000000001")
	bindingTwo := storage.BindingID("91000000-0000-4000-8000-000000000002")
	bindingThree := storage.BindingID("91000000-0000-4000-8000-000000000003")
	seedStorageBinding(t, ctx, pool, bindingOne, "root-1")
	seedStorageBinding(t, ctx, pool, bindingTwo, "root-2")
	seedStorageBinding(t, ctx, pool, bindingThree, "root-3")
	assertCopyBindingForeignKey(t, ctx, pool)

	store, err := NewProjectionStore(pool)
	if err != nil {
		t.Fatalf("NewProjectionStore() error = %v", err)
	}
	if cursor, err := store.Cursor(ctx, bindingOne); err != nil || cursor != 0 {
		t.Fatalf("missing Cursor() = %d, %v", cursor, err)
	}

	resourceMutation := projector.Mutation{
		CopyID:          "93000000-0000-4000-8000-000000000002",
		IndexCoreRootID: "root-1", IndexCoreResourceID: "resource-1",
		StorageBindingID: bindingOne, Availability: projector.AvailabilityPresent,
	}
	bindingRepository, err := NewStorageRepository(pool)
	if err != nil {
		t.Fatalf("NewStorageRepository() error = %v", err)
	}
	resourceID := "resource-1"
	journal := &projectionJournal{events: []indexcore.JournalEvent{{
		EventSeq: 1, EventType: indexcore.EventResourceAdded, ResourceID: &resourceID,
	}}}
	candidateCopyID := catalog.CopyID("93000000-0000-4000-8000-000000000001")
	service, err := projector.NewService(bindingRepository, journal, store,
		projector.WithCopyIDFactory(func() (catalog.CopyID, error) { return candidateCopyID, nil }))
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if result, err := service.ProjectOnce(ctx, bindingOne, 25); err != nil || result.CurrentCursor != 1 || result.Mutations != 1 {
		t.Fatalf("ProjectOnce(first) = %#v, %v", result, err)
	}
	if len(journal.calls) != 1 || journal.calls[0].RootID != "root-1" || journal.calls[0].AfterSeq != 0 || journal.calls[0].Limit != 25 {
		t.Fatalf("ProjectOnce Journal calls = %#v", journal.calls)
	}
	first := readProjectedCopy(t, ctx, pool, "root-1", "resource-1")
	if first.copyID != string(candidateCopyID) || first.variantID != nil || first.availability != "PRESENT" || first.bindingID != string(bindingOne) {
		t.Fatalf("first projected Copy = %#v", first)
	}

	variantID := seedVariant(t, ctx, pool)
	if _, err := pool.Exec(ctx, `UPDATE copies SET variant_id = $1 WHERE copy_id = $2`, variantID, first.copyID); err != nil {
		t.Fatalf("classify projected Copy: %v", err)
	}
	resourceMutation.Availability = projector.AvailabilityRemoved
	if err := store.ApplyBatch(ctx, projector.Batch{
		StorageBindingID: bindingOne, ExpectedCursor: 1, LastEventSeq: 2,
		Mutations: []projector.Mutation{resourceMutation},
	}); err != nil {
		t.Fatalf("ApplyBatch(remove) error = %v", err)
	}
	removed := readProjectedCopy(t, ctx, pool, "root-1", "resource-1")
	if removed.copyID != first.copyID || removed.variantID == nil || *removed.variantID != variantID || removed.availability != "REMOVED" {
		t.Fatalf("removed Copy = %#v, first = %#v", removed, first)
	}

	resourceMutation.Availability = projector.AvailabilityPresent
	if err := store.ApplyBatch(ctx, projector.Batch{
		StorageBindingID: bindingOne, ExpectedCursor: 2, LastEventSeq: 3,
		Mutations: []projector.Mutation{resourceMutation},
	}); err != nil {
		t.Fatalf("ApplyBatch(restore) error = %v", err)
	}
	restored := readProjectedCopy(t, ctx, pool, "root-1", "resource-1")
	if restored.copyID != first.copyID || restored.variantID == nil || *restored.variantID != variantID || restored.availability != "PRESENT" {
		t.Fatalf("restored Copy = %#v", restored)
	}
	if count := countProjectedCopies(t, ctx, pool); count != 1 {
		t.Fatalf("Copy count after replay/update = %d", count)
	}

	if err := store.ApplyBatch(ctx, projector.Batch{
		StorageBindingID: bindingOne, ExpectedCursor: 3, LastEventSeq: 4,
	}); err != nil {
		t.Fatalf("ApplyBatch(root lifecycle) error = %v", err)
	}
	if count := countProjectedCopies(t, ctx, pool); count != 1 {
		t.Fatalf("root lifecycle changed Copy count to %d", count)
	}

	rollbackBatch := projector.Batch{
		StorageBindingID: bindingTwo, ExpectedCursor: 0, LastEventSeq: 2,
		Mutations: []projector.Mutation{
			{CopyID: "93000000-0000-4000-8000-000000000003", IndexCoreRootID: "root-2", IndexCoreResourceID: "must-roll-back", StorageBindingID: bindingTwo, Availability: projector.AvailabilityPresent},
			{CopyID: "93000000-0000-4000-8000-000000000004", IndexCoreRootID: "root-1", IndexCoreResourceID: "resource-1", StorageBindingID: bindingTwo, Availability: projector.AvailabilityRemoved},
		},
	}
	if err := store.ApplyBatch(ctx, rollbackBatch); !errors.Is(err, projector.ErrBindingConflict) {
		t.Fatalf("ApplyBatch(conflicting binding) error = %v", err)
	}
	if cursor, err := store.Cursor(ctx, bindingTwo); err != nil || cursor != 0 {
		t.Fatalf("cursor after rolled-back batch = %d, %v", cursor, err)
	}
	var rolledBackCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM copies WHERE indexcore_resource_id = 'must-roll-back'`).Scan(&rolledBackCount); err != nil || rolledBackCount != 0 {
		t.Fatalf("rolled-back Copy count = %d, %v", rolledBackCount, err)
	}

	if err := store.ApplyBatch(ctx, projector.Batch{
		StorageBindingID: bindingOne, ExpectedCursor: 3, LastEventSeq: 5,
	}); !errors.Is(err, projector.ErrCursorConflict) {
		t.Fatalf("ApplyBatch(stale cursor) error = %v", err)
	}
	if cursor, err := store.Cursor(ctx, bindingOne); err != nil || cursor != 4 {
		t.Fatalf("cursor after stale attempt = %d, %v", cursor, err)
	}

	concurrentBatches := []projector.Batch{
		{
			StorageBindingID: bindingThree, ExpectedCursor: 0, LastEventSeq: 10,
			Mutations: []projector.Mutation{{
				CopyID: "93000000-0000-4000-8000-000000000010", IndexCoreRootID: "root-3",
				IndexCoreResourceID: "winner-a", StorageBindingID: bindingThree, Availability: projector.AvailabilityPresent,
			}},
		},
		{
			StorageBindingID: bindingThree, ExpectedCursor: 0, LastEventSeq: 20,
			Mutations: []projector.Mutation{{
				CopyID: "93000000-0000-4000-8000-000000000020", IndexCoreRootID: "root-3",
				IndexCoreResourceID: "winner-b", StorageBindingID: bindingThree, Availability: projector.AvailabilityPresent,
			}},
		},
	}
	type concurrentResult struct {
		batch projector.Batch
		err   error
	}
	start := make(chan struct{})
	results := make(chan concurrentResult, len(concurrentBatches))
	for _, batch := range concurrentBatches {
		batch := batch
		go func() {
			<-start
			results <- concurrentResult{batch: batch, err: store.ApplyBatch(ctx, batch)}
		}()
	}
	close(start)
	var winningBatch *projector.Batch
	conflicts := 0
	for range concurrentBatches {
		result := <-results
		switch {
		case result.err == nil:
			if winningBatch != nil {
				t.Fatal("both concurrent first projections committed")
			}
			batch := result.batch
			winningBatch = &batch
		case errors.Is(result.err, projector.ErrCursorConflict):
			conflicts++
		default:
			t.Fatalf("concurrent ApplyBatch() error = %v", result.err)
		}
	}
	if winningBatch == nil || conflicts != 1 {
		t.Fatalf("concurrent result: winner=%#v conflicts=%d", winningBatch, conflicts)
	}
	if cursor, err := store.Cursor(ctx, bindingThree); err != nil || cursor != winningBatch.LastEventSeq {
		t.Fatalf("concurrent cursor = %d, %v; winner=%#v", cursor, err, winningBatch)
	}
	var concurrentCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM copies WHERE indexcore_root_id = 'root-3'`).Scan(&concurrentCount); err != nil || concurrentCount != 1 {
		t.Fatalf("concurrent Copy count = %d, %v", concurrentCount, err)
	}
	winningCopy := readProjectedCopy(t, ctx, pool, "root-3", winningBatch.Mutations[0].IndexCoreResourceID)
	if winningCopy.copyID != string(winningBatch.Mutations[0].CopyID) {
		t.Fatalf("concurrent winning CopyID = %q, want %q", winningCopy.copyID, winningBatch.Mutations[0].CopyID)
	}
	loserResourceID := concurrentBatches[0].Mutations[0].IndexCoreResourceID
	if loserResourceID == winningBatch.Mutations[0].IndexCoreResourceID {
		loserResourceID = concurrentBatches[1].Mutations[0].IndexCoreResourceID
	}
	var loserCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM copies WHERE indexcore_root_id = 'root-3' AND indexcore_resource_id = $1`, loserResourceID).Scan(&loserCount); err != nil || loserCount != 0 {
		t.Fatalf("concurrent losing Copy count = %d, %v", loserCount, err)
	}

	reopenedPool, err := pgxpool.New(ctx, os.Getenv("PANTA_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("reopen database pool: %v", err)
	}
	defer reopenedPool.Close()
	if err := reopenedPool.Ping(ctx); err != nil {
		t.Fatalf("ping reopened database pool: %v", err)
	}
	reopenedStore, err := NewProjectionStore(reopenedPool)
	if err != nil {
		t.Fatalf("NewProjectionStore(reopened) error = %v", err)
	}
	if cursor, err := reopenedStore.Cursor(ctx, bindingOne); err != nil || cursor != 4 {
		t.Fatalf("reopened Cursor() = %d, %v", cursor, err)
	}
	reopened := readProjectedCopy(t, ctx, reopenedPool, "root-1", "resource-1")
	if reopened.copyID != first.copyID || reopened.variantID == nil || *reopened.variantID != variantID {
		t.Fatalf("reopened Copy = %#v", reopened)
	}
}

type projectionJournal struct {
	events []indexcore.JournalEvent
	calls  []indexcore.JournalRequest
}

func (journal *projectionJournal) ReadJournal(_ context.Context, request indexcore.JournalRequest) ([]indexcore.JournalEvent, error) {
	journal.calls = append(journal.calls, request)
	return journal.events, nil
}

type projectedCopy struct {
	copyID       string
	variantID    *string
	bindingID    string
	availability string
}

type projectionQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readProjectedCopy(t *testing.T, ctx context.Context, db projectionQuerier, rootID, resourceID string) projectedCopy {
	t.Helper()
	var result projectedCopy
	var variantID sql.NullString
	if err := db.QueryRow(ctx, `
SELECT copy_id::text, variant_id::text, storage_binding_id::text, availability
FROM copies WHERE indexcore_root_id = $1 AND indexcore_resource_id = $2`, rootID, resourceID).Scan(
		&result.copyID, &variantID, &result.bindingID, &result.availability,
	); err != nil {
		t.Fatalf("read projected Copy: %v", err)
	}
	if variantID.Valid {
		result.variantID = &variantID.String
	}
	return result
}

func countProjectedCopies(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM copies`).Scan(&count); err != nil {
		t.Fatalf("count projected Copies: %v", err)
	}
	return count
}

func seedStorageBinding(t *testing.T, ctx context.Context, pool *pgxpool.Pool, bindingID storage.BindingID, rootID string) {
	t.Helper()
	connectionID := string(bindingID)
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_connections (
    storage_connection_id, provider_type, credential_ref, status, created_at, updated_at
) VALUES ($1, 'test', NULL, 'ACTIVE', $2, $2)`, connectionID, now); err != nil {
		t.Fatalf("seed storage connection: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_bindings (
    storage_binding_id, storage_connection_id, openlist_mount_path,
    indexcore_root_id, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, 'ACTIVE', $5, $5)`, bindingID, connectionID, "/"+rootID, rootID, now); err != nil {
		t.Fatalf("seed storage binding: %v", err)
	}
}

func seedVariant(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
	t.Helper()
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	assetID := "92000000-0000-4000-8000-000000000001"
	releaseID := "92000000-0000-4000-8000-000000000002"
	variantID := "92000000-0000-4000-8000-000000000003"
	if _, err := pool.Exec(ctx, `INSERT INTO assets VALUES ($1, 'test', 'test', 'ACTIVE', $2, $2)`, assetID, now); err != nil {
		t.Fatalf("seed asset: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO releases (
    release_id, asset_id, version_raw, version_normalized, version_scheme,
    channel, release_date, source_ref, status, created_at, updated_at
) VALUES ($1, $2, 'unversioned', NULL, 'NONE', 'default', NULL, NULL, 'ACTIVE', $3, $3)`, releaseID, assetID, now); err != nil {
		t.Fatalf("seed release: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO variants (variant_id, release_id, variant_key, attributes, status, created_at, updated_at)
VALUES ($1, $2, 'default', '{}', 'ACTIVE', $3, $3)`, variantID, releaseID, now); err != nil {
		t.Fatalf("seed variant: %v", err)
	}
	return variantID
}

func assertCopyBindingForeignKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM pg_constraint constraint_row
JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
WHERE table_row.relname = 'copies'
  AND constraint_row.contype = 'f'
  AND pg_get_constraintdef(constraint_row.oid) LIKE '%storage_binding_id%storage_bindings%'`).Scan(&count); err != nil {
		t.Fatalf("inspect Copy storage binding FK: %v", err)
	}
	if count != 1 {
		t.Fatalf("Copy storage binding FK count = %d", count)
	}
}
