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

func TestCompareMigrationsRequiresAppliedHistoryPrefix(t *testing.T) {
	tests := []struct {
		name            string
		knownVersions   []int64
		appliedVersions []int64
		wantPending     []int64
		wantHistoryGaps []int64
		wantOutOfOrder  []int64
		wantCompatible  bool
	}{
		{
			name:          "later migration without first migration is a gap",
			knownVersions: []int64{1, 2}, appliedVersions: []int64{2},
			wantHistoryGaps: []int64{1},
		},
		{
			name:          "missing migration inside applied history is a gap",
			knownVersions: []int64{1, 2, 3}, appliedVersions: []int64{1, 3},
			wantHistoryGaps: []int64{2},
		},
		{
			name:          "missing suffix is valid pending history",
			knownVersions: []int64{1, 2, 3}, appliedVersions: []int64{1},
			wantPending: []int64{2, 3},
		},
		{
			name:          "complete prefix is compatible",
			knownVersions: []int64{1, 2}, appliedVersions: []int64{1, 2},
			wantCompatible: true,
		},
		{
			name:          "reversed complete history is out of order",
			knownVersions: []int64{1, 2}, appliedVersions: []int64{2, 1},
			wantOutOfOrder: []int64{1},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status := compareMigrations(testMigrations(test.knownVersions), testAppliedMigrations(test.appliedVersions))
			if status.Compatible != test.wantCompatible {
				t.Fatalf("Compatible = %t, want %t; status = %#v", status.Compatible, test.wantCompatible, status)
			}
			if got := migrationStatusVersions(status.Pending); !equalVersions(got, test.wantPending) {
				t.Fatalf("Pending versions = %v, want %v", got, test.wantPending)
			}
			if got := migrationStatusVersions(status.HistoryGaps); !equalVersions(got, test.wantHistoryGaps) {
				t.Fatalf("HistoryGaps versions = %v, want %v", got, test.wantHistoryGaps)
			}
			if got := appliedMigrationVersions(status.OutOfOrder); !equalVersions(got, test.wantOutOfOrder) {
				t.Fatalf("OutOfOrder versions = %v, want %v", got, test.wantOutOfOrder)
			}
		})
	}
}

func testMigrations(versions []int64) []migrations.Migration {
	result := make([]migrations.Migration, 0, len(versions))
	for _, version := range versions {
		result = append(result, migrations.Migration{Version: version, Name: "migration", Checksum: "checksum"})
	}
	return result
}

func testAppliedMigrations(versions []int64) []AppliedMigration {
	result := make([]AppliedMigration, 0, len(versions))
	for _, version := range versions {
		result = append(result, AppliedMigration{MigrationStatus: MigrationStatus{Version: version, Name: "migration", Checksum: "checksum"}})
	}
	return result
}

func migrationStatusVersions(statuses []MigrationStatus) []int64 {
	versions := make([]int64, 0, len(statuses))
	for _, status := range statuses {
		versions = append(versions, status.Version)
	}
	return versions
}

func appliedMigrationVersions(statuses []AppliedMigration) []int64 {
	versions := make([]int64, 0, len(statuses))
	for _, status := range statuses {
		versions = append(versions, status.Version)
	}
	return versions
}

func equalVersions(first, second []int64) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}
