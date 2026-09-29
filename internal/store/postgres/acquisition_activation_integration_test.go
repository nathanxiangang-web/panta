package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestPostgresAcquisitionActivationContract(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	applyIntegrationMigrations(t, ctx, pool)
	bindingID := storage.BindingID("25000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "activation-root")
	manifestRepository, _ := NewAcquisitionManifestRepository(pool)
	activationRepository, _ := NewAcquisitionActivationRepository(pool)
	service, _ := acquisition.NewActivationService(activationRepository)

	if _, err := service.Activate(ctx, acquisition.ActivateRequest{
		ManifestID:  "25000000-0000-4000-8000-000000000099",
		JobID:       "25000000-0000-4000-8000-000000000098",
		MaxAttempts: 1,
	}); !errors.Is(err, acquisition.ErrActivationManifestNotFound) {
		t.Fatalf("missing Manifest error = %v", err)
	}

	manifest := seedActivationManifest(t, ctx, manifestRepository, bindingID, "25000000-0000-4000-8000-000000000010", acquisition.StatePending, nil)
	request := acquisition.ActivateRequest{
		ManifestID: manifest.ID, JobID: "25000000-0000-4000-8000-000000000011", MaxAttempts: 3,
	}
	first, err := service.Activate(ctx, request)
	if err != nil {
		t.Fatalf("Activate() error = %v", err)
	}
	if !first.Changed || first.Manifest.State != acquisition.StateActive || first.Manifest.JobID == nil ||
		*first.Manifest.JobID != request.JobID || first.Job.ID != request.JobID || first.Job.State != jobs.StateQueued ||
		first.Job.Type != acquisition.JobTypeAcquisition || first.Job.MaxAttempts != request.MaxAttempts ||
		first.Job.AttemptCount != 0 || !first.Manifest.UpdatedAt.After(manifest.UpdatedAt) {
		t.Fatalf("first activation = %#v", first)
	}
	assertAcquisitionJobContract(t, first.Job, manifest.ID)

	activatedAt := first.Manifest.UpdatedAt
	replay, err := service.Activate(ctx, acquisition.ActivateRequest{
		ManifestID: manifest.ID, JobID: "25000000-0000-4000-8000-000000000012", MaxAttempts: 9,
	})
	if err != nil {
		t.Fatalf("replay error = %v", err)
	}
	if replay.Changed || replay.Job.ID != first.Job.ID || replay.Manifest.JobID == nil ||
		*replay.Manifest.JobID != first.Job.ID || !replay.Manifest.UpdatedAt.Equal(activatedAt) || replay.Job.MaxAttempts != request.MaxAttempts {
		t.Fatalf("replay = %#v; first = %#v", replay, first)
	}
	assertJobCount(t, ctx, pool, 1, request.JobID, "25000000-0000-4000-8000-000000000012")
}

func TestPostgresAcquisitionActivationRejectsInvalidMilestonesAndCorruptLinks(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	applyIntegrationMigrations(t, ctx, pool)
	bindingID := storage.BindingID("25100000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "activation-invalid-root")
	manifestRepository, _ := NewAcquisitionManifestRepository(pool)
	activationRepository, _ := NewAcquisitionActivationRepository(pool)
	service, _ := acquisition.NewActivationService(activationRepository)

	invalidStates := []acquisition.State{
		acquisition.StateAwaitingVisibility, acquisition.StateAwaitingCanonical, acquisition.StateReady,
		acquisition.StateFailed, acquisition.StateRecoveryRequired, acquisition.StateCanceled,
	}
	for index, state := range invalidStates {
		manifestID := acquisition.ManifestID(fmt.Sprintf("25100000-0000-4000-8000-%012d", 10+index))
		seedActivationManifest(t, ctx, manifestRepository, bindingID, manifestID, state, nil)
		_, err := service.Activate(ctx, acquisition.ActivateRequest{
			ManifestID:  manifestID,
			JobID:       jobs.JobID(fmt.Sprintf("25100000-0000-4000-8000-%012d", 30+index)),
			MaxAttempts: 1,
		})
		if !errors.Is(err, acquisition.ErrInvalidManifestState) {
			t.Fatalf("state %s error = %v", state, err)
		}
	}

	activeWithoutJob := seedActivationManifest(t, ctx, manifestRepository, bindingID, "25100000-0000-4000-8000-000000000050", acquisition.StateActive, nil)
	if _, err := service.Activate(ctx, activationRequest(activeWithoutJob.ID, "25100000-0000-4000-8000-000000000051")); !errors.Is(err, acquisition.ErrCorruptActivation) {
		t.Fatalf("ACTIVE without Job error = %v", err)
	}

	pendingJobID := seedContractJob(t, ctx, pool, "25100000-0000-4000-8000-000000000052", "25100000-0000-4000-8000-000000000053", acquisition.JobTypeAcquisition, "")
	pendingLinked := seedActivationManifest(t, ctx, manifestRepository, bindingID, "25100000-0000-4000-8000-000000000053", acquisition.StatePending, &pendingJobID)
	if _, err := service.Activate(ctx, activationRequest(pendingLinked.ID, "25100000-0000-4000-8000-000000000054")); !errors.Is(err, acquisition.ErrCorruptActivation) {
		t.Fatalf("PENDING with Job error = %v", err)
	}

	corruptions := []struct {
		name       string
		manifestID acquisition.ManifestID
		jobID      jobs.JobID
		jobType    string
		payload    string
		key        string
	}{
		{name: "wrong type", manifestID: "25100000-0000-4000-8000-000000000060", jobID: "25100000-0000-4000-8000-000000000061", jobType: "OTHER"},
		{name: "wrong payload manifest", manifestID: "25100000-0000-4000-8000-000000000062", jobID: "25100000-0000-4000-8000-000000000063", jobType: acquisition.JobTypeAcquisition, payload: `{"schema_version":1,"manifest_id":"25100000-0000-4000-8000-000000000099"}`},
		{name: "wrong key", manifestID: "25100000-0000-4000-8000-000000000064", jobID: "25100000-0000-4000-8000-000000000065", jobType: acquisition.JobTypeAcquisition, key: "acquisition:other"},
	}
	for _, test := range corruptions {
		jobID := seedContractJob(t, ctx, pool, test.jobID, test.manifestID, test.jobType, test.key, test.payload)
		manifest := seedActivationManifest(t, ctx, manifestRepository, bindingID, test.manifestID, acquisition.StateActive, &jobID)
		if _, err := service.Activate(ctx, activationRequest(manifest.ID, "25100000-0000-4000-8000-000000000090")); !errors.Is(err, acquisition.ErrCorruptActivation) {
			t.Fatalf("%s error = %v", test.name, err)
		}
	}

	// A disposable schema lets this test prove the application fails closed even
	// if database corruption bypasses the normal foreign key.
	missingManifestID := acquisition.ManifestID("25100000-0000-4000-8000-000000000070")
	missingJobID := jobs.JobID("25100000-0000-4000-8000-000000000071")
	if _, err := pool.Exec(ctx, "ALTER TABLE acquisition_manifests DROP CONSTRAINT acquisition_manifests_job_id_fkey"); err != nil {
		t.Fatalf("drop test-only foreign key: %v", err)
	}
	seedActivationManifest(t, ctx, manifestRepository, bindingID, missingManifestID, acquisition.StateActive, &missingJobID)
	if _, err := service.Activate(ctx, activationRequest(missingManifestID, "25100000-0000-4000-8000-000000000072")); !errors.Is(err, acquisition.ErrCorruptActivation) {
		t.Fatalf("missing linked Job error = %v", err)
	}
}

func TestPostgresConcurrentAcquisitionActivationConverges(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	applyIntegrationMigrations(t, ctx, pool)
	bindingID := storage.BindingID("25200000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "activation-concurrency-root")
	manifestRepository, _ := NewAcquisitionManifestRepository(pool)
	activationRepository, _ := NewAcquisitionActivationRepository(pool)
	service, _ := acquisition.NewActivationService(activationRepository)

	tests := []struct {
		name   string
		base   int
		sameID bool
	}{
		{name: "different proposed Job IDs", base: 10},
		{name: "same proposed Job ID", base: 20, sameID: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifestID := acquisition.ManifestID(fmt.Sprintf("25200000-0000-4000-8000-%012d", test.base))
			firstJobID := jobs.JobID(fmt.Sprintf("25200000-0000-4000-8000-%012d", test.base+1))
			secondJobID := jobs.JobID(fmt.Sprintf("25200000-0000-4000-8000-%012d", test.base+2))
			if test.sameID {
				secondJobID = firstJobID
			}
			seedActivationManifest(t, ctx, manifestRepository, bindingID, manifestID, acquisition.StatePending, nil)

			start := make(chan struct{})
			results := make(chan activationCallResult, 2)
			var workers sync.WaitGroup
			for _, jobID := range []jobs.JobID{firstJobID, secondJobID} {
				workers.Add(1)
				go func(jobID jobs.JobID) {
					defer workers.Done()
					<-start
					result, err := service.Activate(ctx, activationRequest(manifestID, jobID))
					results <- activationCallResult{result: result, err: err}
				}(jobID)
			}
			close(start)
			workers.Wait()
			close(results)

			var got []acquisition.ActivateResult
			for call := range results {
				if call.err != nil {
					t.Fatalf("concurrent Activate() error = %v", call.err)
				}
				got = append(got, call.result)
			}
			if len(got) != 2 || got[0].Job.ID != got[1].Job.ID || got[0].Manifest.JobID == nil || got[1].Manifest.JobID == nil ||
				*got[0].Manifest.JobID != *got[1].Manifest.JobID || got[0].Changed == got[1].Changed {
				t.Fatalf("concurrent results = %#v", got)
			}
			assertJobCount(t, ctx, pool, 1, firstJobID, secondJobID)
			var keyCount int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE idempotency_key = $1", acquisition.AcquisitionJobIdempotencyKey(manifestID)).Scan(&keyCount); err != nil || keyCount != 1 {
				t.Fatalf("idempotency key count = %d, %v", keyCount, err)
			}
		})
	}
}

func TestPostgresAcquisitionActivationRollsBackBetweenJobAndManifest(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	applyIntegrationMigrations(t, ctx, pool)
	bindingID := storage.BindingID("25300000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "activation-rollback-root")
	manifestRepository, _ := NewAcquisitionManifestRepository(pool)
	manifest := seedActivationManifest(t, ctx, manifestRepository, bindingID, "25300000-0000-4000-8000-000000000010", acquisition.StatePending, nil)
	activationRepository, _ := NewAcquisitionActivationRepository(pool)
	injected := errors.New("forced failure after Job insert")
	activationRepository.afterJobInsert = func(context.Context, pgx.Tx) error { return injected }
	service, _ := acquisition.NewActivationService(activationRepository)
	jobID := jobs.JobID("25300000-0000-4000-8000-000000000011")

	if _, err := service.Activate(ctx, activationRequest(manifest.ID, jobID)); !errors.Is(err, acquisition.ErrActivationPersistence) {
		t.Fatalf("forced failure error = %v", err)
	}
	durable, err := manifestRepository.GetManifest(ctx, manifest.ID)
	if err != nil || durable.State != acquisition.StatePending || durable.JobID != nil || !durable.UpdatedAt.Equal(manifest.UpdatedAt) {
		t.Fatalf("durable Manifest after rollback = %#v, %v", durable, err)
	}
	assertJobCount(t, ctx, pool, 0, jobID)
}

type activationCallResult struct {
	result acquisition.ActivateResult
	err    error
}

func applyIntegrationMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	if _, err := migrator.Apply(ctx); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
}

func seedActivationManifest(t *testing.T, ctx context.Context, repository *AcquisitionManifestRepository, bindingID storage.BindingID, id acquisition.ManifestID, state acquisition.State, jobID *jobs.JobID) acquisition.Manifest {
	t.Helper()
	now := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	manifest := acquisition.Manifest{
		ID: id, SourceType: "opaque", SourceRef: "opaque://source", TargetStorageBindingID: bindingID,
		TargetPath: "/downloads/item", JobID: jobID, State: state, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateManifest(ctx, manifest); err != nil {
		t.Fatalf("CreateManifest(%s) error = %v", id, err)
	}
	return manifest
}

func activationRequest(manifestID acquisition.ManifestID, jobID jobs.JobID) acquisition.ActivateRequest {
	return acquisition.ActivateRequest{ManifestID: manifestID, JobID: jobID, MaxAttempts: 2}
}

func seedContractJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID jobs.JobID, manifestID acquisition.ManifestID, jobType, key string, payloadOverride ...string) jobs.JobID {
	t.Helper()
	if key == "" {
		key = acquisition.AcquisitionJobIdempotencyKey(manifestID)
	}
	payload := fmt.Sprintf(`{"schema_version":1,"manifest_id":%q}`, manifestID)
	if len(payloadOverride) > 0 && payloadOverride[0] != "" {
		payload = payloadOverride[0]
	}
	repository, _ := NewJobRepository(pool)
	if _, err := repository.Create(ctx, jobs.CreateRequest{
		ID: jobID, Type: jobType, Payload: json.RawMessage(payload), IdempotencyKey: &key, MaxAttempts: 1,
	}); err != nil {
		t.Fatalf("seed Job %s: %v", jobID, err)
	}
	return jobID
}

func assertAcquisitionJobContract(t *testing.T, job jobs.Job, manifestID acquisition.ManifestID) {
	t.Helper()
	if err := acquisition.ValidateLinkedAcquisitionJob(manifestID, job); err != nil {
		t.Fatalf("linked Job contract error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(job.Payload, &payload); err != nil || len(payload) != 2 ||
		payload["schema_version"] != float64(1) || payload["manifest_id"] != string(manifestID) {
		t.Fatalf("Job payload = %#v, %v", payload, err)
	}
}

func assertJobCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int, ids ...jobs.JobID) {
	t.Helper()
	arguments := make([]any, len(ids))
	placeholders := ""
	for index, id := range ids {
		arguments[index] = string(id)
		if index > 0 {
			placeholders += ","
		}
		placeholders += fmt.Sprintf("$%d", index+1)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM jobs WHERE job_id IN ("+placeholders+")", arguments...).Scan(&count); err != nil || count != want {
		t.Fatalf("Job count = %d, %v; want %d", count, err, want)
	}
}
