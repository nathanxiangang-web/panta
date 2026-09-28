package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/migrations"
)

func TestPostgresMigrations(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	initial, err := migrator.Status(ctx)
	if err != nil {
		t.Fatalf("initial Status() error = %v", err)
	}
	if initial.Compatible || len(initial.Pending) == 0 {
		t.Fatalf("initial status = %#v, want pending incompatible schema", initial)
	}

	applied, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}
	assertCurrentSchema(t, applied)

	reapplied, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("idempotent Apply() error = %v", err)
	}
	assertCurrentSchema(t, reapplied)

	resetTestSchema(t, ctx, pool)
	recreated, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("Apply() after schema recreation error = %v", err)
	}
	assertCurrentSchema(t, recreated)

	if _, err := pool.Exec(ctx, `
INSERT INTO schema_migrations (version, name, checksum)
VALUES (9999, 'future', 'future-checksum')`); err != nil {
		t.Fatalf("insert future migration: %v", err)
	}
	future, err := migrator.Status(ctx)
	if err != nil {
		t.Fatalf("future Status() error = %v", err)
	}
	if future.Compatible || len(future.Unknown) != 1 {
		t.Fatalf("future status = %#v", future)
	}
	if _, err := migrator.Apply(ctx); !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("Apply() with future migration error = %v, want ErrIncompatibleSchema", err)
	}
}

func TestPostgresCatalogRoundTripAndConstraints(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	if _, err := migrator.Apply(ctx); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	seedStorageBinding(t, ctx, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "root-a")
	repository, err := NewCatalogRepository(pool)
	if err != nil {
		t.Fatalf("NewCatalogRepository() error = %v", err)
	}

	now := time.Date(2026, 9, 28, 8, 0, 0, 0, time.UTC)
	asset := catalog.Asset{
		ID: "11111111-1111-4111-8111-111111111111", CanonicalName: "Panta Test Asset",
		Category: "software", Status: "ACTIVE", CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateAsset(ctx, asset); err != nil {
		t.Fatalf("CreateAsset() error = %v", err)
	}
	gotAsset, err := repository.GetAsset(ctx, asset.ID)
	if err != nil || gotAsset.ID != asset.ID || gotAsset.CanonicalName != asset.CanonicalName {
		t.Fatalf("GetAsset() = %#v, %v", gotAsset, err)
	}

	normalized := "1.0.0"
	releaseDate := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	release := catalog.Release{
		ID: "22222222-2222-4222-8222-222222222222", AssetID: asset.ID,
		VersionRaw: "v1.0.0", VersionNormalized: &normalized, VersionScheme: catalog.VersionSchemeSemver,
		Channel: "stable", ReleaseDate: &releaseDate, Status: "ACTIVE", CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateRelease(ctx, release); err != nil {
		t.Fatalf("CreateRelease() error = %v", err)
	}
	gotRelease, err := repository.GetRelease(ctx, release.ID)
	if err != nil || gotRelease.ID != release.ID || gotRelease.AssetID != asset.ID || gotRelease.VersionScheme != release.VersionScheme {
		t.Fatalf("GetRelease() = %#v, %v", gotRelease, err)
	}

	variant := catalog.Variant{
		ID: "33333333-3333-4333-8333-333333333333", ReleaseID: release.ID,
		VariantKey: "linux-amd64", Attributes: json.RawMessage(`{"os":"linux","arch":"amd64"}`),
		Status: "ACTIVE", CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateVariant(ctx, variant); err != nil {
		t.Fatalf("CreateVariant() error = %v", err)
	}
	gotVariant, err := repository.GetVariant(ctx, variant.ID)
	if err != nil || gotVariant.ID != variant.ID || gotVariant.ReleaseID != release.ID || !jsonEquivalent(gotVariant.Attributes, variant.Attributes) {
		t.Fatalf("GetVariant() = %#v, %v", gotVariant, err)
	}

	copyWithVariant := catalog.Copy{
		ID: "44444444-4444-4444-8444-444444444444", VariantID: &variant.ID,
		IndexCoreRootID: "root-a", IndexCoreResourceID: "resource-a",
		StorageBindingID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		Availability:     "PRESENT", CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateCopy(ctx, copyWithVariant); err != nil {
		t.Fatalf("CreateCopy() error = %v", err)
	}
	gotCopy, err := repository.GetCopy(ctx, copyWithVariant.ID)
	if err != nil || gotCopy.VariantID == nil || *gotCopy.VariantID != variant.ID {
		t.Fatalf("GetCopy() = %#v, %v", gotCopy, err)
	}

	unresolvedCopy := catalog.Copy{
		ID: "55555555-5555-4555-8555-555555555555", VariantID: nil,
		IndexCoreRootID: "root-a", IndexCoreResourceID: "unresolved-resource",
		StorageBindingID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		Availability:     "PRESENT", CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateCopy(ctx, unresolvedCopy); err != nil {
		t.Fatalf("CreateCopy(unresolved) error = %v", err)
	}
	gotUnresolved, err := repository.GetCopy(ctx, unresolvedCopy.ID)
	if err != nil || gotUnresolved.VariantID != nil {
		t.Fatalf("GetCopy(unresolved) = %#v, %v", gotUnresolved, err)
	}

	missingAssetRelease := release
	missingAssetRelease.ID = "66666666-6666-4666-8666-666666666666"
	missingAssetRelease.AssetID = "77777777-7777-4777-8777-777777777777"
	if err := repository.CreateRelease(ctx, missingAssetRelease); err == nil {
		t.Fatal("CreateRelease() accepted a missing Asset")
	}
	duplicateVariant := variant
	duplicateVariant.ID = "88888888-8888-4888-8888-888888888888"
	if err := repository.CreateVariant(ctx, duplicateVariant); err == nil {
		t.Fatal("CreateVariant() accepted duplicate (release_id, variant_key)")
	}
	duplicatePhysicalCopy := unresolvedCopy
	duplicatePhysicalCopy.ID = "99999999-9999-4999-8999-999999999999"
	if err := repository.CreateCopy(ctx, duplicatePhysicalCopy); err == nil {
		t.Fatal("CreateCopy() accepted duplicate physical identity")
	}
}

func TestPostgresRejectsGappedMigrationHistory(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	if _, err := pool.Exec(ctx, `
CREATE TABLE schema_migrations (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
)`); err != nil {
		t.Fatalf("create migration history: %v", err)
	}

	known := []migrations.Migration{
		{Version: 1, Name: "gap_one", Checksum: "gap-one", Filename: "0001_gap_one.sql", SQL: "CREATE TABLE gap_migration_one (id bigint PRIMARY KEY)"},
		{Version: 2, Name: "gap_two", Checksum: "gap-two", Filename: "0002_gap_two.sql", SQL: "CREATE TABLE gap_migration_two (id bigint PRIMARY KEY)"},
	}
	if _, err := pool.Exec(ctx,
		"INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
		known[1].Version, known[1].Name, known[1].Checksum,
	); err != nil {
		t.Fatalf("record later migration: %v", err)
	}

	migrator := &Migrator{pool: pool, migrations: known}
	status, err := migrator.Status(ctx)
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if status.Compatible || len(status.HistoryGaps) != 1 || status.HistoryGaps[0].Version != 1 {
		t.Fatalf("gapped status = %#v", status)
	}
	if _, err := migrator.Apply(ctx); !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("Apply() error = %v, want ErrIncompatibleSchema", err)
	}

	var missingMigrationExecuted bool
	if err := pool.QueryRow(ctx, "SELECT to_regclass('public.gap_migration_one') IS NOT NULL").Scan(&missingMigrationExecuted); err != nil {
		t.Fatalf("inspect missing migration side effect: %v", err)
	}
	if missingMigrationExecuted {
		t.Fatal("Apply() executed missing migration SQL after detecting a history gap")
	}
	var missingHistoryRow bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = 1)").Scan(&missingHistoryRow); err != nil {
		t.Fatalf("inspect missing migration history: %v", err)
	}
	if missingHistoryRow {
		t.Fatal("Apply() recorded missing migration after detecting a history gap")
	}
}

func integrationPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("PANTA_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("PANTA_TEST_DATABASE_URL is not set")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse PANTA_TEST_DATABASE_URL: %v", err)
	}
	if config.ConnConfig.Database != "panta_test" && !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("refusing destructive integration test for database %q: name must be panta_test or end with _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect integration database: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func resetTestSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
		t.Fatalf("reset test schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	})
}

func assertCurrentSchema(t *testing.T, status SchemaStatus) {
	t.Helper()
	if !status.Compatible || len(status.Pending) != 0 || len(status.Unknown) != 0 || len(status.Modified) != 0 || status.CurrentVersion != status.LatestVersion {
		t.Fatalf("schema status = %#v", status)
	}
}

func jsonEquivalent(first, second []byte) bool {
	var firstValue, secondValue any
	if json.Unmarshal(first, &firstValue) != nil || json.Unmarshal(second, &secondValue) != nil {
		return false
	}
	firstJSON, _ := json.Marshal(firstValue)
	secondJSON, _ := json.Marshal(secondValue)
	return string(firstJSON) == string(secondJSON)
}
