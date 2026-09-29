package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	if err != nil || !status.Compatible || status.CurrentVersion != 9 || status.LatestVersion != 9 || len(status.Applied) != 9 {
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

	// Phase 1: an exclusive start reservation whose reference is not yet known.
	now := time.Date(2026, 9, 30, 5, 0, 0, 0, time.UTC)
	claim, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
		ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider", Now: now,
	})
	if err != nil {
		t.Fatalf("ClaimProviderTask(reserve) error = %v", err)
	}
	if !claim.ClaimedStart || claim.CommittedReference {
		t.Fatalf("reserve claim = %#v, want ClaimedStart only", claim)
	}
	if claim.Task.State != acquisition.ProviderTaskStartReserved || claim.Task.ProviderTaskRef != "" {
		t.Fatalf("reserved task = %#v, want START_RESERVED with no reference", claim.Task)
	}

	// A second caller must not be granted a start while the reservation holds.
	second, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
		ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider", Now: now,
	})
	if err != nil {
		t.Fatalf("ClaimProviderTask(second) error = %v", err)
	}
	if second.ClaimedStart || second.CommittedReference {
		t.Fatalf("second claim = %#v, want no start and no commit", second)
	}

	// Phase 2: the opaque reference is committed against the reservation.
	opaqueRef := "  provider://task//opaque?root=%2F&token=abc  "
	commit, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
		ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider", Reference: opaqueRef,
		Now: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimProviderTask(commit) error = %v", err)
	}
	if commit.ClaimedStart || !commit.CommittedReference {
		t.Fatalf("commit claim = %#v, want CommittedReference only", commit)
	}
	stored, err := repository.GetProviderTask(ctx, manifestID)
	if err != nil {
		t.Fatalf("GetProviderTask() error = %v", err)
	}
	if !stored.ReferenceKnown() || stored.ProviderTaskRef != opaqueRef {
		t.Fatalf("stored provider task = %#v, want exact opaque %q", stored, opaqueRef)
	}
	if stored.JobID != jobID || stored.ProviderID != "opaque-provider" || !stored.CreatedAt.Equal(now) {
		t.Fatalf("stored provider task identity = %#v", stored)
	}

	// Replay of a known reference is idempotent and never reports a new start.
	replay, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
		ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider", Now: now,
	})
	if err != nil {
		t.Fatalf("ClaimProviderTask(replay) error = %v", err)
	}
	if replay.ClaimedStart || replay.CommittedReference || !replay.Task.ReferenceKnown() {
		t.Fatalf("replay claim = %#v, want the existing known reference only", replay)
	}

	// A competing identity or reference must fail closed.
	conflictCases := []struct {
		name    string
		request acquisition.ProviderTaskClaimRequest
	}{
		{name: "different provider", request: acquisition.ProviderTaskClaimRequest{
			ManifestID: manifestID, JobID: jobID, ProviderID: "other-provider", Now: now,
		}},
		{name: "different Job", request: acquisition.ProviderTaskClaimRequest{
			ManifestID: manifestID, JobID: jobs.JobID("24000000-0000-4000-8000-0000000000ff"), ProviderID: "opaque-provider", Now: now,
		}},
		{name: "different reference", request: acquisition.ProviderTaskClaimRequest{
			ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider", Reference: "provider-task-other", Now: now,
		}},
	}
	for _, test := range conflictCases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := repository.ClaimProviderTask(ctx, test.request); !errors.Is(err, acquisition.ErrProviderTaskIdentityChange) {
				t.Fatalf("ClaimProviderTask() error = %v, want ErrProviderTaskIdentityChange", err)
			}
		})
	}
	afterConflicts, err := repository.GetProviderTask(ctx, manifestID)
	if err != nil {
		t.Fatalf("GetProviderTask(after conflicts) error = %v", err)
	}
	if afterConflicts.ProviderTaskRef != opaqueRef {
		t.Fatalf("durable task reference was overwritten: %q", afterConflicts.ProviderTaskRef)
	}

	// A reference with no reservation is rejected outright.
	freshManifest, freshJob := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID, "13", "23")
	if _, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
		ManifestID: freshManifest, JobID: freshJob, ProviderID: "opaque-provider",
		Reference: "provider-task-unreserved", Now: now,
	}); !errors.Is(err, acquisition.ErrInvalidProviderTask) {
		t.Fatalf("unreserved reference commit error = %v, want ErrInvalidProviderTask", err)
	}

	// One Job maps to at most one provider task.
	reusedManifest, _ := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID, "11", "21")
	if _, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
		ManifestID: reusedManifest, JobID: jobID, ProviderID: "opaque-provider", Now: now,
	}); !errors.Is(err, acquisition.ErrProviderTaskConflict) {
		t.Fatalf("ClaimProviderTask(reused Job) error = %v, want ErrProviderTaskConflict", err)
	}

	// Foreign keys are enforced.
	missingJobManifest, _ := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID, "12", "22")
	if _, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
		ManifestID: missingJobManifest, JobID: jobs.JobID("24000000-0000-4000-8000-0000000000fe"),
		ProviderID: "opaque-provider", Now: now,
	}); !errors.Is(err, acquisition.ErrInvalidProviderTask) {
		t.Fatalf("ClaimProviderTask(missing Job) error = %v, want ErrInvalidProviderTask", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_provider_tasks (
    manifest_id, job_id, provider_id, provider_task_ref, state, created_at, updated_at
) VALUES ($1, $2, 'provider-x', NULL, 'START_RESERVED', $3, $3)`,
		string(reusedManifest), "24000000-0000-4000-8000-0000000000fe", now); err == nil {
		t.Fatal("database accepted a provider task with a missing Job reference")
	}

	// Application-level and database-level opaque bounds.
	invalidValues := []struct {
		name   string
		mutate func(*acquisition.ProviderTaskClaimRequest)
	}{
		{name: "empty provider id", mutate: func(value *acquisition.ProviderTaskClaimRequest) { value.ProviderID = "" }},
		{name: "whitespace provider id", mutate: func(value *acquisition.ProviderTaskClaimRequest) { value.ProviderID = "  " }},
		{name: "overlong provider id", mutate: func(value *acquisition.ProviderTaskClaimRequest) {
			value.ProviderID = contracts.ProviderID("provider-" + strings.Repeat("x", acquisition.MaxProviderIDLength))
		}},
		{name: "whitespace task ref", mutate: func(value *acquisition.ProviderTaskClaimRequest) { value.Reference = "  " }},
		{name: "NUL task ref", mutate: func(value *acquisition.ProviderTaskClaimRequest) { value.Reference = "task\x00ref" }},
		{name: "overlong task ref", mutate: func(value *acquisition.ProviderTaskClaimRequest) {
			value.Reference = strings.Repeat("x", acquisition.MaxProviderTaskRefLength+1)
		}},
	}
	for index, test := range invalidValues {
		t.Run(test.name, func(t *testing.T) {
			invalidManifest, invalidJob := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID,
				fmt.Sprintf("3%d", index), fmt.Sprintf("4%d", index))
			request := acquisition.ProviderTaskClaimRequest{
				ManifestID: invalidManifest, JobID: invalidJob, ProviderID: "opaque-provider", Now: now,
			}
			test.mutate(&request)
			if _, err := repository.ClaimProviderTask(ctx, request); !errors.Is(err, acquisition.ErrInvalidProviderTask) {
				t.Fatalf("ClaimProviderTask(%s) error = %v, want ErrInvalidProviderTask", test.name, err)
			}
		})
	}

	// The database enforces the side-effect state invariant even for direct
	// writes, so an uncommitted reservation can never carry a reference and a
	// committed row can never lose it.
	directWrites := []struct {
		name      string
		reference any
		state     string
	}{
		{name: "reservation with a reference", reference: "task", state: "START_RESERVED"},
		{name: "known state without a reference", reference: nil, state: "REFERENCE_KNOWN"},
		{name: "unknown state", reference: nil, state: "SOMETHING_ELSE"},
	}
	for index, test := range directWrites {
		t.Run("direct write: "+test.name, func(t *testing.T) {
			holder, _ := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID,
				fmt.Sprintf("5%d", index), fmt.Sprintf("6%d", index))
			if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_provider_tasks (
    manifest_id, job_id, provider_id, provider_task_ref, state, created_at, updated_at
) VALUES ($1, $2, 'provider-x', $3, $4, $5, $5)`,
				string(holder), fmt.Sprintf("24000000-0000-4000-8000-00000000007%d", index),
				test.reference, test.state, now); err == nil {
				t.Fatalf("database accepted %s", test.name)
			}
		})
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_provider_tasks (
    manifest_id, job_id, provider_id, provider_task_ref, state, created_at, updated_at
) VALUES ($1, $2, 'provider-x', $3, 'REFERENCE_KNOWN', $4, $4)`,
		string(reusedManifest), "24000000-0000-4000-8000-000000000030",
		strings.Repeat("x", acquisition.MaxProviderTaskRefLength+1), now); err == nil {
		t.Fatal("database accepted an overlong provider_task_ref")
	}

	// No database defaults.
	for _, column := range []string{"manifest_id", "job_id", "provider_id", "state", "created_at", "updated_at"} {
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

	// The table stays provider-neutral and minimal: one side-effect state column,
	// no provider task lifecycle status column.
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
	want := []string{"created_at", "job_id", "manifest_id", "provider_id", "provider_task_ref", "state", "updated_at"}
	if strings.Join(columns, ",") != strings.Join(want, ",") {
		t.Fatalf("provider task columns = %v, want %v", columns, want)
	}
}

// TestPostgresConcurrentProviderStartClaimAdmitsExactlyOneStart proves the
// advisory-lock fence: concurrent claimers for the same Manifest produce exactly
// one START_RESERVED winner.
func TestPostgresConcurrentProviderStartClaimAdmitsExactlyOneStart(t *testing.T) {
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
	bindingID := storage.BindingID("25000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "provider-task-fence-root")
	manifestID, jobID := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID, "10", "20")
	repository, err := NewAcquisitionProviderTaskRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionProviderTaskRepository() error = %v", err)
	}

	const workers = 8
	now := time.Date(2026, 9, 30, 5, 0, 0, 0, time.UTC)
	var waitGroup sync.WaitGroup
	claimed := make([]bool, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			<-start
			result, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
				ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider", Now: now,
			})
			claimed[index], errs[index] = result.ClaimedStart, err
		}(worker)
	}
	close(start)
	waitGroup.Wait()

	starts := 0
	for index := 0; index < workers; index++ {
		if errs[index] != nil {
			t.Fatalf("worker %d claim error = %v", index, errs[index])
		}
		if claimed[index] {
			starts++
		}
	}
	if starts != 1 {
		t.Fatalf("concurrent start claims = %d, want exactly 1", starts)
	}
	stored, err := repository.GetProviderTask(ctx, manifestID)
	if err != nil {
		t.Fatalf("GetProviderTask() error = %v", err)
	}
	if stored.State != acquisition.ProviderTaskStartReserved || stored.ProviderTaskRef != "" {
		t.Fatalf("durable task = %#v, want one START_RESERVED fence", stored)
	}
}

// TestPostgresProviderTaskClaimLockIsBounded proves the fence wait is bounded: a
// claim that cannot obtain the lock fails closed with the contention cause instead
// of blocking a worker indefinitely.
func TestPostgresProviderTaskClaimLockIsBounded(t *testing.T) {
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
	bindingID := storage.BindingID("26000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "provider-task-lock-root")
	manifestID, jobID := seedAcquisitionProviderTaskManifest(t, ctx, pool, bindingID, "10", "20")

	// Hold the per-Manifest fence lock in a separate session.
	holder, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin holder transaction: %v", err)
	}
	defer func() { _ = holder.Rollback(context.Background()) }()
	if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock($1, hashtext($2))`,
		3401, string(manifestID)); err != nil {
		t.Fatalf("holder lock: %v", err)
	}

	// The claim must give up rather than wait forever.
	claimCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	repository, err := NewAcquisitionProviderTaskRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionProviderTaskRepository() error = %v", err)
	}
	started := time.Now()
	_, claimErr := repository.ClaimProviderTask(claimCtx, acquisition.ProviderTaskClaimRequest{
		ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider", Now: time.Now().UTC(),
	})
	elapsed := time.Since(started)
	if claimErr == nil {
		t.Fatal("ClaimProviderTask succeeded while the fence lock was held elsewhere")
	}
	if !errors.Is(claimErr, acquisition.ErrProviderTaskContention) {
		t.Fatalf("ClaimProviderTask error = %v, want ErrProviderTaskContention", claimErr)
	}
	if elapsed > 9*time.Second {
		t.Fatalf("claim waited %s, want it bounded near the 5s lock timeout", elapsed)
	}

	// No authorization was granted, so no durable row may exist.
	if _, err := repository.GetProviderTask(ctx, manifestID); !errors.Is(err, acquisition.ErrProviderTaskNotFound) {
		t.Fatalf("GetProviderTask() error = %v, want ErrProviderTaskNotFound after a bounded lock failure", err)
	}

	// Once the holder releases, the claim succeeds normally.
	if err := holder.Rollback(ctx); err != nil {
		t.Fatalf("release holder transaction: %v", err)
	}
	claim, err := repository.ClaimProviderTask(ctx, acquisition.ProviderTaskClaimRequest{
		ManifestID: manifestID, JobID: jobID, ProviderID: "opaque-provider", Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("ClaimProviderTask() after release error = %v", err)
	}
	if !claim.ClaimedStart {
		t.Fatalf("claim after release = %#v, want ClaimedStart", claim)
	}
}
