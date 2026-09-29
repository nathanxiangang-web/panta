package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

// seedAcquisitionProviderTaskManifest seeds one ACTIVE Manifest linked to one
// durable ACQUISITION Job and returns the linked Job identity.
func seedAcquisitionProviderTaskManifest(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	manifestSuffix string,
	jobSuffix string,
) (acquisition.ManifestID, jobs.JobID) {
	t.Helper()
	now := time.Date(2026, 9, 30, 5, 0, 0, 0, time.UTC)
	manifestID := acquisition.ManifestID("24000000-0000-4000-8000-0000000000" + manifestSuffix)
	jobID := jobs.JobID("24000000-0000-4000-8000-0000000000" + jobSuffix)
	jobRepository, err := NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}
	key := acquisition.AcquisitionJobIdempotencyKey(manifestID)
	if _, err := jobRepository.Create(ctx, jobs.CreateRequest{
		ID: jobID, Type: acquisition.JobTypeAcquisition,
		Payload:        []byte(fmt.Sprintf(`{"manifest_id":"%s","schema_version":1}`, manifestID)),
		IdempotencyKey: &key, MaxAttempts: 3,
	}); err != nil {
		t.Fatalf("Create(ACQUISITION job) error = %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id, target_path,
    job_id, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, '/downloads/item', $3, 'ACTIVE', $4, $4)`,
		string(manifestID), string(bindingID), string(jobID), now,
	); err != nil {
		t.Fatalf("seed ACTIVE Manifest: %v", err)
	}
	return manifestID, jobID
}

func TestPostgresAcquisitionProviderTaskRoundTripAndConstraints(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 8 || status.LatestVersion != 8 || len(status.Applied) != 8 {
		t.Fatalf("Apply() = %#v, %v", status, err)
	}

	bindingID := storage.BindingID("24000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "provider-task-root")
	manifestID, jobID := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID, "10", "20")

	repository, err := NewAcquisitionProviderTaskRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionProviderTaskRepository() error = %v", err)
	}
	if _, err := repository.GetProviderTask(ctx, manifestID); !errors.Is(err, acquisition.ErrProviderTaskNotFound) {
		t.Fatalf("GetProviderTask(missing) error = %v, want ErrProviderTaskNotFound", err)
	}

	// Exact opaque round-trip, including surrounding whitespace and characters
	// that Panta must never normalize or interpret.
	opaqueRef := "  provider://task//opaque?root=%2F&token=abc  "
	now := time.Date(2026, 9, 30, 5, 0, 0, 0, time.UTC)
	task := acquisition.ProviderTask{
		ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider",
		ProviderTaskRef: opaqueRef, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.StoreProviderTask(ctx, task); err != nil {
		t.Fatalf("StoreProviderTask() error = %v", err)
	}
	stored, err := repository.GetProviderTask(ctx, manifestID)
	if err != nil {
		t.Fatalf("GetProviderTask() error = %v", err)
	}
	if stored.ManifestID != task.ManifestID || stored.JobID != task.JobID || stored.ProviderID != task.ProviderID ||
		stored.ProviderTaskRef != opaqueRef || !stored.CreatedAt.Equal(now) || !stored.UpdatedAt.Equal(now) {
		t.Fatalf("provider task round trip = %#v, want %#v", stored, task)
	}

	// Replay with identical identity is idempotent, not a conflict.
	if err := repository.StoreProviderTask(ctx, task); err != nil {
		t.Fatalf("StoreProviderTask(replay) error = %v", err)
	}

	// A different task reference must never overwrite the durable one.
	conflict := task
	conflict.ProviderTaskRef = "provider-task-other"
	if err := repository.StoreProviderTask(ctx, conflict); !errors.Is(err, acquisition.ErrProviderTaskIdentityChange) {
		t.Fatalf("StoreProviderTask(conflict) error = %v, want ErrProviderTaskIdentityChange", err)
	}
	afterConflict, err := repository.GetProviderTask(ctx, manifestID)
	if err != nil {
		t.Fatalf("GetProviderTask(after conflict) error = %v", err)
	}
	if afterConflict.ProviderTaskRef != opaqueRef {
		t.Fatalf("durable task reference was overwritten: %q", afterConflict.ProviderTaskRef)
	}

	// One Job maps to at most one provider task.
	secondManifest, _ := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID, "11", "21")
	reused := task
	reused.ManifestID = secondManifest
	if err := repository.StoreProviderTask(ctx, reused); !errors.Is(err, acquisition.ErrProviderTaskConflict) {
		t.Fatalf("StoreProviderTask(reused Job) error = %v, want ErrProviderTaskConflict", err)
	}

	// Both foreign keys are enforced.
	missingManifest, _ := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID, "12", "22")
	missing := task
	missing.ManifestID = missingManifest
	missing.JobID = jobs.JobID("24000000-0000-4000-8000-0000000000fe")
	if err := repository.StoreProviderTask(ctx, missing); !errors.Is(err, acquisition.ErrInvalidProviderTask) {
		t.Fatalf("StoreProviderTask(missing Job) error = %v, want ErrInvalidProviderTask", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_provider_tasks (
    manifest_id, job_id, provider_id, provider_task_ref, created_at, updated_at
) VALUES ($1, $2, 'provider-x', 'task-x', $3, $3)`,
		string(secondManifest), "24000000-0000-4000-8000-0000000000fe", now); err == nil {
		t.Fatal("database accepted a provider task with a missing Job reference")
	}

	// Application-level and database-level opaque bounds.
	invalidValues := []struct {
		name   string
		mutate func(*acquisition.ProviderTask)
	}{
		{name: "empty provider id", mutate: func(value *acquisition.ProviderTask) { value.ProviderID = "" }},
		{name: "whitespace provider id", mutate: func(value *acquisition.ProviderTask) { value.ProviderID = "  " }},
		{name: "overlong provider id", mutate: func(value *acquisition.ProviderTask) {
			value.ProviderID = contracts.ProviderID("provider-" + strings.Repeat("x", acquisition.MaxProviderIDLength))
		}},
		{name: "empty task ref", mutate: func(value *acquisition.ProviderTask) { value.ProviderTaskRef = "" }},
		{name: "whitespace task ref", mutate: func(value *acquisition.ProviderTask) { value.ProviderTaskRef = "  " }},
		{name: "NUL task ref", mutate: func(value *acquisition.ProviderTask) { value.ProviderTaskRef = "task\x00ref" }},
		{name: "overlong task ref", mutate: func(value *acquisition.ProviderTask) {
			value.ProviderTaskRef = strings.Repeat("x", acquisition.MaxProviderTaskRefLength+1)
		}},
	}
	for index, test := range invalidValues {
		t.Run(test.name, func(t *testing.T) {
			freshManifest, freshJob := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID,
				fmt.Sprintf("3%d", index), fmt.Sprintf("4%d", index))
			candidate := task
			candidate.ManifestID = freshManifest
			candidate.JobID = freshJob
			test.mutate(&candidate)
			if err := repository.StoreProviderTask(ctx, candidate); err == nil {
				t.Fatalf("StoreProviderTask(%s) succeeded, want rejection", test.name)
			}
		})
	}

	// The database enforces the opaque bounds even for direct writes.
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_provider_tasks (
    manifest_id, job_id, provider_id, provider_task_ref, created_at, updated_at
) VALUES ($1, $2, '   ', 'task', $3, $3)`, string(secondManifest), "24000000-0000-4000-8000-000000000030", now); err == nil {
		t.Fatal("database accepted a whitespace-only provider_id")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_provider_tasks (
    manifest_id, job_id, provider_id, provider_task_ref, created_at, updated_at
) VALUES ($1, $2, 'provider-x', $3, $4, $4)`, string(secondManifest), "24000000-0000-4000-8000-000000000030",
		strings.Repeat("x", acquisition.MaxProviderTaskRefLength+1), now); err == nil {
		t.Fatal("database accepted an overlong provider_task_ref")
	}

	// No database defaults.
	for _, column := range []string{"manifest_id", "job_id", "provider_id", "provider_task_ref", "created_at", "updated_at"} {
		var defaultValue sql.NullString
		if err := pool.QueryRow(ctx, `
SELECT column_default
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'acquisition_provider_tasks' AND column_name = $1`, column).Scan(&defaultValue); err != nil {
			t.Fatalf("inspect %s default: %v", column, err)
		}
		if defaultValue.Valid {
			t.Fatalf("column %s has database default %q", column, defaultValue.String)
		}
	}

	// No provider-specific columns: the table is provider-neutral and minimal.
	rows, err := pool.Query(ctx, `
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'acquisition_provider_tasks'
ORDER BY column_name`)
	if err != nil {
		t.Fatalf("list provider task columns: %v", err)
	}
	defer rows.Close()
	columns := []string{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("scan provider task column: %v", err)
		}
		columns = append(columns, column)
	}
	want := []string{"created_at", "job_id", "manifest_id", "provider_id", "provider_task_ref", "updated_at"}
	if strings.Join(columns, ",") != strings.Join(want, ",") {
		t.Fatalf("provider task columns = %v, want %v", columns, want)
	}
}
