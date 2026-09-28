package postgres

import (
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/migrations"
)

func TestCompareMigrationsFailsClosed(t *testing.T) {
	known := []migrations.Migration{{Version: 1, Name: "catalog", Checksum: "expected"}}

	tests := []struct {
		name    string
		applied []AppliedMigration
		assert  func(*testing.T, SchemaStatus)
	}{
		{
			name: "missing required migration",
			assert: func(t *testing.T, status SchemaStatus) {
				if status.Compatible || len(status.Pending) != 1 {
					t.Fatalf("status = %#v", status)
				}
			},
		},
		{
			name: "future migration",
			applied: []AppliedMigration{
				{MigrationStatus: MigrationStatus{Version: 1, Name: "catalog", Checksum: "expected"}, AppliedAt: time.Now()},
				{MigrationStatus: MigrationStatus{Version: 2, Name: "future", Checksum: "future"}, AppliedAt: time.Now()},
			},
			assert: func(t *testing.T, status SchemaStatus) {
				if status.Compatible || len(status.Unknown) != 1 {
					t.Fatalf("status = %#v", status)
				}
			},
		},
		{
			name: "rewritten migration",
			applied: []AppliedMigration{
				{MigrationStatus: MigrationStatus{Version: 1, Name: "catalog", Checksum: "changed"}, AppliedAt: time.Now()},
			},
			assert: func(t *testing.T, status SchemaStatus) {
				if status.Compatible || len(status.Modified) != 1 {
					t.Fatalf("status = %#v", status)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.assert(t, compareMigrations(known, test.applied))
		})
	}
}
