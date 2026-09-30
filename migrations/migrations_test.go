package migrations

import (
	"strings"
	"testing"
)

func TestAllReturnsOrderedMigrationHistory(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	if len(all) == 0 {
		t.Fatal("All() returned no migrations")
	}
	for index, migration := range all {
		if migration.Version <= 0 || migration.Name == "" || migration.Checksum == "" || migration.SQL == "" {
			t.Fatalf("migration %d is incomplete: %#v", index, migration)
		}
		if index > 0 && all[index-1].Version >= migration.Version {
			t.Fatalf("migrations are not strictly ordered: %d then %d", all[index-1].Version, migration.Version)
		}
	}
}

// TestMigrationHistoryEndsAtExpectedVersion keeps the hardcoded version
// assertions in the PostgreSQL integration tests honest: adding a migration
// without updating them fails here first, in a unit test with no database.
func TestMigrationHistoryEndsAtExpectedVersion(t *testing.T) {
	const expectedLatestVersion = 12
	all, err := All()
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	if len(all) != expectedLatestVersion {
		t.Fatalf("migration count = %d, want %d", len(all), expectedLatestVersion)
	}
	latest := all[len(all)-1]
	if latest.Version != expectedLatestVersion {
		t.Fatalf("latest migration version = %d, want %d", latest.Version, expectedLatestVersion)
	}
	if latest.Filename != "0012_acquisition_result_copy.sql" {
		t.Fatalf("latest migration = %q, want the job claim-generation split", latest.Filename)
	}
}

// TestProviderTaskSideEffectFenceMigrationAddsNoDefaults asserts the fence
// migration declares no column default: the state column is backfilled by the
// migration itself and then the default is explicitly dropped, so the
// application always supplies state, timestamps, and references.
func TestProviderTaskSideEffectFenceMigrationAddsNoDefaults(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	var sql string
	for _, migration := range all {
		if migration.Version == 9 {
			sql = strings.ToLower(migration.SQL)
			break
		}
	}
	if sql == "" {
		t.Fatal("migration 9 is missing")
	}
	for _, forbidden := range []string{"create extension", "gen_random_uuid", "uuid_generate"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 9 contains database UUID generation %q", forbidden)
		}
	}
	if !strings.Contains(sql, "drop default") {
		t.Fatal("migration 9 must drop the temporary state default after backfilling")
	}
	if strings.Contains(sql, "default now()") || strings.Contains(sql, "default current_timestamp") {
		t.Fatal("migration 9 introduces a database timestamp default")
	}
}

func TestAcquisitionManifestMigrationUsesApplicationSuppliedUUIDs(t *testing.T) {
	all, err := All()
	if err != nil {
		t.Fatalf("All() error = %v", err)
	}
	var sql string
	for _, migration := range all {
		if migration.Version == 5 {
			sql = strings.ToLower(migration.SQL)
			break
		}
	}
	if sql == "" {
		t.Fatal("migration 5 is missing")
	}
	for _, forbidden := range []string{"create extension", "gen_random_uuid", "uuid_generate"} {
		if strings.Contains(sql, forbidden) {
			t.Fatalf("migration 5 contains database UUID generation %q", forbidden)
		}
	}
	if strings.Contains(sql, "manifest_id uuid default") {
		t.Fatal("manifest_id has a database default")
	}
}
