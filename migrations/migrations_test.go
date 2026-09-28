package migrations

import "testing"

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
