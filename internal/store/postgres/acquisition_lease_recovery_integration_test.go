package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

func TestPostgresAcquisitionScopedClaim(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	other := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "48000000-0000-4000-8000-000000000001", Type: "OTHER", Payload: []byte(`{}`), MaxAttempts: 3,
	})
	acquired := mustCreateJob(t, ctx, repository, jobs.CreateRequest{
		ID: "48000000-0000-4000-8000-000000000002", Type: acquisition.JobTypeAcquisition,
		Payload: []byte(`{}`), MaxAttempts: 3,
	})
	now := time.Now().UTC()
	request := jobs.ClaimRequest{Owner: "acquisition-worker", Now: now, LeaseDuration: time.Minute}
	for _, jobType := range []string{"", "OTHER", "UNKNOWN", " ACQUISITION "} {
		if _, err := repository.ClaimNextByType(ctx, jobType, request); !errors.Is(err, jobs.ErrInvalidArgument) {
			t.Fatalf("type %q error = %v", jobType, err)
		}
	}
	if _, err := repository.ClaimNextByType(ctx, acquisition.JobTypeAcquisition, jobs.ClaimRequest{}); !errors.Is(err, jobs.ErrInvalidArgument) {
		t.Fatalf("malformed claim error = %v", err)
	}
	claimed, err := repository.ClaimNextByType(ctx, acquisition.JobTypeAcquisition, request)
	if err != nil || claimed.ID != acquired.ID || claimed.ClaimAttempts != 1 {
		t.Fatalf("scoped claim = %#v, %v", claimed, err)
	}
	unchanged, err := repository.Get(ctx, other.ID)
	if err != nil || unchanged.State != jobs.StateQueued || unchanged.ClaimAttempts != 0 {
		t.Fatalf("unrelated Job = %#v, %v", unchanged, err)
	}
	if _, err := repository.ClaimNextByType(ctx, acquisition.JobTypeAcquisition, request); !errors.Is(err, jobs.ErrNoClaimableJob) {
		t.Fatalf("second claim = %v, want no ACQUISITION Job", err)
	}
	// A due retry remains schedulable regardless of its failure budget.
	if _, err := repository.pool.Exec(ctx, `UPDATE jobs SET state = 'RETRY_WAIT', attempt_count = 2,
next_attempt_at = CURRENT_TIMESTAMP - interval '1 second', lease_owner = NULL, lease_expires_at = NULL
WHERE job_id = $1`, string(acquired.ID)); err != nil {
		t.Fatal(err)
	}
	retry, err := repository.ClaimNextByType(ctx, acquisition.JobTypeAcquisition, request)
	if err != nil || retry.ID != acquired.ID || retry.ClaimAttempts != 2 || retry.FailureCount != 2 {
		t.Fatalf("due retry claim = %#v, %v", retry, err)
	}
}

func TestPostgresAcquisitionScopedClaimConcurrentOneWinner(t *testing.T) {
	ctx := context.Background()
	repository := migratedJobRepository(t, ctx)
	mustCreateJob(t, ctx, repository, jobs.CreateRequest{ID: "48000000-0000-4000-8000-000000000003",
		Type: acquisition.JobTypeAcquisition, Payload: []byte(`{}`), MaxAttempts: 3})
	start := make(chan struct{})
	results := make(chan error, 2)
	var group sync.WaitGroup
	for _, owner := range []string{"worker-a", "worker-b"} {
		group.Add(1)
		go func(owner string) {
			defer group.Done()
			<-start
			_, err := repository.ClaimNextByType(ctx, acquisition.JobTypeAcquisition,
				jobs.ClaimRequest{Owner: owner, Now: time.Now().UTC(), LeaseDuration: time.Minute})
			results <- err
		}(owner)
	}
	close(start)
	group.Wait()
	close(results)
	success, empty := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, jobs.ErrNoClaimableJob) {
			empty++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || empty != 1 {
		t.Fatalf("claim results success/empty = %d/%d", success, empty)
	}
}

func TestPostgresAcquisitionExpiredLeasePairedRecovery(t *testing.T) {
	for _, stage := range []struct {
		name, suffix string
		state        acquisition.State
	}{
		{"provider", "48", acquisition.StateActive},
		{"visibility", "49", acquisition.StateAwaitingVisibility},
		{"canonical", "4a", acquisition.StateAwaitingCanonical},
	} {
		t.Run(stage.name, func(t *testing.T) {
			fixture := newCanonicalPgFixture(t)
			seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, stage.suffix,
				stage.state, "worker", 3, 2, 5, canonicalPgExpiredLeaseEnd(), nil)
			repository := mustGate311JobStore(t, fixture)
			tasks, err := NewAcquisitionProviderTaskRepository(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			reserve, err := tasks.ClaimProviderTask(fixture.ctx, acquisition.ProviderTaskClaimRequest{
				ManifestID: seeded.manifestID, JobID: seeded.jobID, ProviderID: "test", Now: time.Now().UTC()})
			if err != nil || !reserve.ClaimedStart {
				t.Fatalf("reserve = %#v, %v", reserve, err)
			}
			if stage.state == acquisition.StateAwaitingCanonical {
				if _, err := tasks.ClaimProviderTask(fixture.ctx, acquisition.ProviderTaskClaimRequest{
					ManifestID: seeded.manifestID, JobID: seeded.jobID, ProviderID: "test",
					Reference: "provider-ref", Now: time.Now().UTC()}); err != nil {
					t.Fatal(err)
				}
			}
			beforeTask, err := tasks.GetProviderTask(fixture.ctx, seeded.manifestID)
			if err != nil {
				t.Fatal(err)
			}
			beforeManifest, err := fixture.manifests.GetManifest(fixture.ctx, seeded.manifestID)
			if err != nil {
				t.Fatal(err)
			}
			beforeJob := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
			request := jobs.RecoveryRequest{Now: time.Now().UTC(), Limit: 10}
			if generic, err := repository.MarkExpiredRunningRecoveryRequired(fixture.ctx, request); err != nil || generic != 0 {
				t.Fatalf("generic recovery touched Acquisition: %d, %v", generic, err)
			}
			count, err := repository.MarkExpiredAcquisitionRecoveryRequired(fixture.ctx, request)
			if err != nil || count != 1 {
				t.Fatalf("paired recovery = %d, %v", count, err)
			}
			manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
			resultManifest, err := fixture.manifests.GetManifest(fixture.ctx, seeded.manifestID)
			if err != nil {
				t.Fatal(err)
			}
			job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
			storedTask, err := tasks.GetProviderTask(fixture.ctx, seeded.manifestID)
			if err != nil {
				t.Fatal(err)
			}
			if manifest.State != acquisition.StateRecoveryRequired || job.State != jobs.StateRecoveryRequired ||
				job.LeaseOwner != nil || job.LeaseExpiresAt != nil || job.NextAttemptAt != nil || job.LastError == nil ||
				job.ClaimAttempts != beforeJob.ClaimAttempts || job.FailureCount != beforeJob.FailureCount ||
				resultManifest.ResultName == nil || *resultManifest.ResultName != *beforeManifest.ResultName ||
				storedTask != beforeTask {
				t.Fatalf("recovered Manifest/Job/task = %#v / %#v / %#v", manifest, job, storedTask)
			}
			if count, err := repository.MarkExpiredAcquisitionRecoveryRequired(fixture.ctx, request); err != nil || count != 0 {
				t.Fatalf("repeat recovery = %d, %v", count, err)
			}
			repeatManifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
			repeatJob := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
			if !repeatManifest.UpdatedAt.Equal(manifest.UpdatedAt) || !repeatJob.UpdatedAt.Equal(job.UpdatedAt) {
				t.Fatal("repeat recovery rewrote timestamps")
			}
			outcomeStore, err := NewProviderOutcomeRepository(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			outcomes, err := acquisition.NewProviderOutcomeService(outcomeStore)
			if err != nil {
				t.Fatal(err)
			}
			provider := &gate311Provider{}
			visibility := &gate311UnexpectedVisibility{}
			canonical := &gate311UnexpectedCanonical{}
			dispatcher, err := acquisition.NewStageDispatcher(repository, fixture.manifests,
				provider, outcomes, visibility, canonical)
			if err != nil {
				t.Fatal(err)
			}
			replay, err := dispatcher.Dispatch(fixture.ctx, acquisition.StageDispatchRequest{
				JobID: seeded.jobID, Owner: seeded.owner, ExpectedClaim: seeded.claim})
			if err != nil || replay.Stage != acquisition.StageRecovery || !replay.Terminal || replay.Changed ||
				provider.calls != 0 || visibility.calls != 0 || canonical.calls != 0 {
				t.Fatalf("recovery replay = %#v, %v; external calls %d/%d/%d", replay, err,
					provider.calls, visibility.calls, canonical.calls)
			}
		})
	}
}

func TestPostgresAcquisitionRecoveryDatabaseTimeAndRollback(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "4b",
		acquisition.StateActive, "worker", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	repository := mustGate311JobStore(t, fixture)
	for _, invalid := range []jobs.RecoveryRequest{{Limit: 1}, {Now: time.Now().UTC(), Limit: 0},
		{Now: time.Now().UTC(), Limit: maxAcquisitionRecoveryBatch + 1}} {
		if _, err := repository.MarkExpiredAcquisitionRecoveryRequired(fixture.ctx, invalid); !errors.Is(err, jobs.ErrInvalidArgument) {
			t.Fatalf("invalid recovery request %#v: %v", invalid, err)
		}
	}
	request := jobs.RecoveryRequest{Now: time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC), Limit: 10}
	if count, err := repository.MarkExpiredAcquisitionRecoveryRequired(fixture.ctx, request); err != nil || count != 0 {
		t.Fatalf("future caller time recovered live lease: %d, %v", count, err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE jobs SET lease_expires_at = CURRENT_TIMESTAMP - interval '1 second' WHERE job_id = $1`, string(seeded.jobID)); err != nil {
		t.Fatal(err)
	}
	repository.afterAcquisitionManifestRecovery = func(context.Context, pgx.Tx) error { return errors.New("injected recovery failure") }
	if count, err := repository.MarkExpiredAcquisitionRecoveryRequired(fixture.ctx, request); err == nil || count != 0 {
		t.Fatalf("injected recovery = %d, %v", count, err)
	}
	if manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID); manifest.State != acquisition.StateActive {
		t.Fatalf("Manifest changed after rollback: %s", manifest.State)
	}
	if job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID); job.State != jobs.StateRunning {
		t.Fatalf("Job changed after rollback: %s", job.State)
	}
	repository.afterAcquisitionManifestRecovery = nil
	if count, err := repository.MarkExpiredAcquisitionRecoveryRequired(fixture.ctx, request); err != nil || count != 1 {
		t.Fatalf("recovery after rollback = %d, %v", count, err)
	}
}

func TestPostgresAcquisitionRecoveryDebtPreservesRecords(t *testing.T) {
	for _, test := range []struct {
		name, suffix, mutation string
	}{
		{"missing Manifest", "4c", `DELETE FROM acquisition_manifests WHERE manifest_id = $1`},
		{"wrong reverse link", "4d", `UPDATE acquisition_manifests SET job_id = '48000000-0000-4000-8000-000000000099' WHERE manifest_id = $1`},
		{"terminal Manifest", "4e", `UPDATE acquisition_manifests SET state = 'FAILED' WHERE manifest_id = $1`},
		{"malformed payload", "4f", `UPDATE jobs SET payload = '{"schema_version":99}'::jsonb WHERE job_id = $1`},
		{"wrong idempotency", "50", `UPDATE jobs SET idempotency_key = 'wrong' WHERE job_id = $1`},
		{"invalid Manifest ID", "52", `UPDATE jobs SET payload = '{"schema_version":1,"manifest_id":"not-uuid"}'::jsonb,
idempotency_key = 'acquisition:not-uuid' WHERE job_id = $1`},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCanonicalPgFixture(t)
			seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, test.suffix,
				acquisition.StateActive, "worker", 2, 1, 5, canonicalPgExpiredLeaseEnd(), nil)
			if test.name == "wrong reverse link" {
				mustCreateJob(t, fixture.ctx, mustGate311JobStore(t, fixture), jobs.CreateRequest{
					ID: "48000000-0000-4000-8000-000000000099", Type: "OTHER",
					Payload: []byte(`{}`), MaxAttempts: 1})
			}
			target := string(seeded.manifestID)
			if test.name == "malformed payload" || test.name == "wrong idempotency" || test.name == "invalid Manifest ID" {
				target = string(seeded.jobID)
			}
			if _, err := fixture.pool.Exec(fixture.ctx, test.mutation, target); err != nil {
				t.Fatal(err)
			}
			repository := mustGate311JobStore(t, fixture)
			beforeJob := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
			var beforeManifest canonicalPgDurableManifest
			if test.name != "missing Manifest" {
				beforeManifest = readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
			}
			count, err := repository.MarkExpiredAcquisitionRecoveryRequired(fixture.ctx,
				jobs.RecoveryRequest{Now: time.Now().UTC(), Limit: 10})
			if !errors.Is(err, acquisition.ErrLeaseRecoveryDebt) || count != 0 {
				t.Fatalf("recovery debt = %d, %v", count, err)
			}
			afterJob := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
			if afterJob.State != jobs.StateRunning || !afterJob.UpdatedAt.Equal(beforeJob.UpdatedAt) ||
				afterJob.ClaimAttempts != beforeJob.ClaimAttempts || afterJob.FailureCount != beforeJob.FailureCount {
				t.Fatalf("corrupt Job changed: %#v", afterJob)
			}
			if test.name != "missing Manifest" {
				afterManifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
				if afterManifest.State != beforeManifest.State || !afterManifest.UpdatedAt.Equal(beforeManifest.UpdatedAt) {
					t.Fatalf("corrupt Manifest changed: %#v", afterManifest)
				}
			}
		})
	}
}

func TestPostgresAcquisitionRecoveryConcurrentProviderHandoff(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "51",
		acquisition.StateActive, "worker", 1, 0, 5, canonicalPgLeaseEnd(), nil)
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE jobs SET lease_expires_at = CURRENT_TIMESTAMP + interval '2 seconds'
WHERE job_id = $1`, string(seeded.jobID)); err != nil {
		t.Fatal(err)
	}
	outcomeStore, err := NewProviderOutcomeRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	outcomeStore.afterManifestUpdate = func(context.Context, pgx.Tx) error {
		close(entered)
		<-release
		return nil
	}
	outcomes, err := acquisition.NewProviderOutcomeService(outcomeStore)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	retryAt := now.Add(time.Minute)
	handoffDone := make(chan error, 1)
	go func() {
		_, err := outcomes.Commit(fixture.ctx, acquisition.ProviderOutcomeRequest{
			ManifestID: seeded.manifestID, JobID: seeded.jobID, Owner: seeded.owner,
			ExpectedClaim: seeded.claim, Outcome: acquisition.ProviderOutcomeInProgress,
			Now: now, RetryAt: &retryAt})
		handoffDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("stage handoff did not reach locked Manifest")
	}
	// The sweep discovers the now-expired RUNNING candidate while the ordinary
	// handoff holds Manifest first. Its subsequent locked recheck must not recover
	// a Job that the handoff committed to RETRY_WAIT.
	time.Sleep(2100 * time.Millisecond)
	recoveryDone := make(chan struct {
		count int64
		err   error
	}, 1)
	repository := mustGate311JobStore(t, fixture)
	go func() {
		count, err := repository.MarkExpiredAcquisitionRecoveryRequired(fixture.ctx,
			jobs.RecoveryRequest{Now: time.Now().UTC(), Limit: 10})
		recoveryDone <- struct {
			count int64
			err   error
		}{count, err}
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-handoffDone; err != nil {
		t.Fatalf("provider handoff: %v", err)
	}
	result := <-recoveryDone
	if result.err != nil || result.count != 0 {
		t.Fatalf("concurrent recovery = %d, %v", result.count, result.err)
	}
	manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
	if manifest.State != acquisition.StateActive || job.State != jobs.StateRetryWait {
		t.Fatalf("concurrent pair = Manifest %s, Job %s", manifest.State, job.State)
	}
}
