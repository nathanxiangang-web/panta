package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/storage"
	"github.com/nathanxiangang-web/panta/migrations"
)

func TestPostgresProviderScopeMigrationPreservesExistingObservationBinding(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	history, err := migrations.All()
	if err != nil || len(history) != 10 {
		t.Fatalf("migration history = %#v, %v", history, err)
	}
	legacy := &Migrator{pool: pool, migrations: history[:6]}
	if _, err := legacy.Apply(ctx); err != nil {
		t.Fatalf("apply through version 6: %v", err)
	}
	now := time.Date(2026, 9, 29, 18, 0, 0, 0, time.UTC)
	connectionID := "cf000000-0000-4000-8000-000000000001"
	bindingID := storage.BindingID("df000000-0000-4000-8000-000000000001")
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_connections (
    storage_connection_id, provider_type, status, created_at, updated_at
) VALUES ($1, 'observation-provider', 'ACTIVE', $2, $2)`,
		connectionID, now,
	); err != nil {
		t.Fatalf("seed version 6 observation connection: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_bindings (
    storage_binding_id, storage_connection_id, openlist_mount_path,
    indexcore_root_id, status, created_at, updated_at
) VALUES ($1, $2, '/legacy', 'legacy-root', 'ACTIVE', $3, $3)`,
		string(bindingID), connectionID, now,
	); err != nil {
		t.Fatalf("seed version 6 observation binding: %v", err)
	}
	migrator, _ := NewMigrator(pool)
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 10 {
		t.Fatalf("upgrade to version 7 = %#v, %v", status, err)
	}
	repository, _ := NewStorageRepository(pool)
	binding, err := repository.GetBinding(ctx, bindingID)
	if err != nil || binding.ProviderScope != nil || binding.OpenListMountPath != "/legacy" || binding.IndexCoreRootID != "legacy-root" {
		t.Fatalf("legacy observation binding = %#v, %v", binding, err)
	}
}

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

	providerScope := " provider://opaque//scope?root=%2F "
	binding := storage.Binding{
		ID: "d0000000-0000-4000-8000-000000000001", ConnectionID: connection.ID,
		ProviderScope:     &providerScope,
		OpenListMountPath: "//media///movies//", IndexCoreRootID: "root-canonical-1",
		Status: storage.BindingStatusActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateBinding(ctx, binding); err != nil {
		t.Fatalf("CreateBinding() error = %v", err)
	}
	gotBinding, err := repository.GetBinding(ctx, binding.ID)
	if err != nil || gotBinding.ID != binding.ID || gotBinding.ConnectionID != connection.ID || gotBinding.ProviderScope == nil || *gotBinding.ProviderScope != providerScope || gotBinding.OpenListMountPath != "/media/movies" || gotBinding.IndexCoreRootID != binding.IndexCoreRootID || gotBinding.Status != binding.Status {
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
	observationOnly, err := repository.GetBinding(ctx, first.ID)
	if err != nil || observationOnly.ProviderScope != nil {
		t.Fatalf("observation-only binding = %#v, %v", observationOnly, err)
	}
	invalidUTF8 := string([]byte{0xff})
	for index, invalid := range []string{"", "   ", "scope\x00value", invalidUTF8, strings.Repeat("x", storage.MaxProviderScopeLength+1)} {
		candidate := first
		candidate.ID = storage.BindingID([]string{
			"d1000000-0000-4000-8000-000000000001", "d1000000-0000-4000-8000-000000000002",
			"d1000000-0000-4000-8000-000000000003", "d1000000-0000-4000-8000-000000000004",
			"d1000000-0000-4000-8000-000000000005",
		}[index])
		candidate.ProviderScope = &invalid
		if err := repository.CreateBinding(ctx, candidate); !errors.Is(err, storage.ErrInvalidArgument) {
			t.Fatalf("CreateBinding(invalid provider scope %q) error = %v", invalid, err)
		}
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
	if _, err := repository.db.Exec(ctx, `
INSERT INTO storage_bindings (
    storage_binding_id, storage_connection_id, provider_scope, openlist_mount_path,
    indexcore_root_id, status, created_at, updated_at
) VALUES ($1, $2, '   ', '/provider-scope', $3, 'ACTIVE', $4, $4)`,
		"d0000000-0000-4000-8000-000000000007", string(connection.ID), "root-provider-scope-invalid", now,
	); err == nil {
		t.Fatal("database accepted whitespace-only provider_scope")
	}
	var defaultValue sql.NullString
	if err := repository.db.QueryRow(ctx, `
SELECT column_default
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'storage_bindings' AND column_name = 'provider_scope'`).Scan(&defaultValue); err != nil {
		t.Fatalf("inspect provider_scope default: %v", err)
	}
	if defaultValue.Valid {
		t.Fatalf("provider_scope has database default %q", defaultValue.String)
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
	if !status.Compatible || status.CurrentVersion != 10 || status.LatestVersion != 10 || len(status.Applied) != 10 {
		t.Fatalf("migration status = %#v", status)
	}
	repository, err := NewStorageRepository(pool)
	if err != nil {
		t.Fatalf("NewStorageRepository() error = %v", err)
	}
	return repository
}
