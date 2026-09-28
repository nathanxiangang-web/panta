package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestPostgresStorageConnectionAndBindingRoundTrip(t *testing.T) {
	ctx := context.Background()
	repository := migratedStorageRepository(t, ctx)
	now := time.Date(2026, 9, 28, 15, 0, 0, 0, time.UTC)
	credentialRef := "secret-store://connections/gate1"
	connection := storage.Connection{
		ID: "c0000000-0000-4000-8000-000000000001", ProviderType: "generic-cloud",
		CredentialRef: &credentialRef, Status: storage.ConnectionStatusActive,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateConnection(ctx, connection); err != nil {
		t.Fatalf("CreateConnection() error = %v", err)
	}
	gotConnection, err := repository.GetConnection(ctx, connection.ID)
	if err != nil || gotConnection.ID != connection.ID || gotConnection.ProviderType != connection.ProviderType || gotConnection.CredentialRef == nil || *gotConnection.CredentialRef != credentialRef || gotConnection.Status != connection.Status {
		t.Fatalf("GetConnection() = %#v, %v", gotConnection, err)
	}

	binding := storage.Binding{
		ID: "d0000000-0000-4000-8000-000000000001", ConnectionID: connection.ID,
		OpenListMountPath: "//media///movies//", IndexCoreRootID: "root-canonical-1",
		Status: storage.BindingStatusActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateBinding(ctx, binding); err != nil {
		t.Fatalf("CreateBinding() error = %v", err)
	}
	gotBinding, err := repository.GetBinding(ctx, binding.ID)
	if err != nil || gotBinding.ID != binding.ID || gotBinding.ConnectionID != connection.ID || gotBinding.OpenListMountPath != "/media/movies" || gotBinding.IndexCoreRootID != binding.IndexCoreRootID || gotBinding.Status != binding.Status {
		t.Fatalf("GetBinding() = %#v, %v", gotBinding, err)
	}
	byRoot, err := repository.GetBindingByIndexCoreRootID(ctx, binding.IndexCoreRootID)
	if err != nil || byRoot.ID != binding.ID {
		t.Fatalf("GetBindingByIndexCoreRootID() = %#v, %v", byRoot, err)
	}
	byMount, err := repository.GetBindingByConnectionMount(ctx, connection.ID, "/media//movies/")
	if err != nil || byMount.ID != binding.ID || byMount.OpenListMountPath != "/media/movies" {
		t.Fatalf("GetBindingByConnectionMount() = %#v, %v", byMount, err)
	}
}

func TestPostgresStorageBindingConstraints(t *testing.T) {
	ctx := context.Background()
	repository := migratedStorageRepository(t, ctx)
	now := time.Date(2026, 9, 28, 16, 0, 0, 0, time.UTC)
	connection := storage.Connection{
		ID: "c0000000-0000-4000-8000-000000000002", ProviderType: "generic-cloud",
		CredentialRef: nil, Status: storage.ConnectionStatusDisabled,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateConnection(ctx, connection); err != nil {
		t.Fatalf("CreateConnection() error = %v", err)
	}
	missingConnection := storage.Binding{
		ID:                "d0000000-0000-4000-8000-000000000002",
		ConnectionID:      "c0000000-0000-4000-8000-999999999999",
		OpenListMountPath: "/missing", IndexCoreRootID: "root-missing",
		Status: storage.BindingStatusActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateBinding(ctx, missingConnection); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("CreateBinding(missing connection) error = %v", err)
	}

	first := storage.Binding{
		ID: "d0000000-0000-4000-8000-000000000003", ConnectionID: connection.ID,
		OpenListMountPath: "/media", IndexCoreRootID: "root-unique",
		Status: storage.BindingStatusActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateBinding(ctx, first); err != nil {
		t.Fatalf("CreateBinding(first) error = %v", err)
	}
	duplicateRoot := first
	duplicateRoot.ID = "d0000000-0000-4000-8000-000000000004"
	duplicateRoot.OpenListMountPath = "/other"
	if err := repository.CreateBinding(ctx, duplicateRoot); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("CreateBinding(duplicate root) error = %v", err)
	}
	duplicateMount := first
	duplicateMount.ID = "d0000000-0000-4000-8000-000000000005"
	duplicateMount.IndexCoreRootID = "root-other"
	duplicateMount.OpenListMountPath = "/media/"
	if err := repository.CreateBinding(ctx, duplicateMount); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("CreateBinding(duplicate normalized mount) error = %v", err)
	}
}

func TestPostgresStorageBindingRejectsNonCanonicalDirectWrite(t *testing.T) {
	ctx := context.Background()
	repository := migratedStorageRepository(t, ctx)
	now := time.Date(2026, 9, 28, 17, 0, 0, 0, time.UTC)
	connection := storage.Connection{
		ID: "c0000000-0000-4000-8000-000000000003", ProviderType: "generic-cloud",
		Status: storage.ConnectionStatusActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateConnection(ctx, connection); err != nil {
		t.Fatalf("CreateConnection() error = %v", err)
	}
	if _, err := repository.db.Exec(ctx, `
INSERT INTO storage_bindings (
    storage_binding_id, storage_connection_id, openlist_mount_path,
    indexcore_root_id, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, 'ACTIVE', $5, $5)`,
		"d0000000-0000-4000-8000-000000000006", string(connection.ID), "/media/../escape", "root-invalid", now,
	); err == nil {
		t.Fatal("database accepted a non-canonical mount path")
	}
}

func TestPostgresStorageBindingHasOnlyProductConnectionForeignKey(t *testing.T) {
	ctx := context.Background()
	repository := migratedStorageRepository(t, ctx)
	var foreignKeys, connectionForeignKeys int
	if err := repository.db.QueryRow(ctx, `
SELECT count(*), count(*) FILTER (
    WHERE confrelid = 'storage_connections'::regclass
)
FROM pg_constraint
WHERE contype = 'f' AND conrelid = 'storage_bindings'::regclass`).Scan(
		&foreignKeys, &connectionForeignKeys,
	); err != nil {
		t.Fatalf("inspect storage binding foreign keys: %v", err)
	}
	if foreignKeys != 1 || connectionForeignKeys != 1 {
		t.Fatalf("storage binding foreign keys: total=%d connection=%d", foreignKeys, connectionForeignKeys)
	}
}

func migratedStorageRepository(t *testing.T, ctx context.Context) *StorageRepository {
	t.Helper()
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
	if !status.Compatible || status.CurrentVersion != 3 || status.LatestVersion != 3 || len(status.Applied) != 3 {
		t.Fatalf("migration status = %#v", status)
	}
	repository, err := NewStorageRepository(pool)
	if err != nil {
		t.Fatalf("NewStorageRepository() error = %v", err)
	}
	return repository
}
