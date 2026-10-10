package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

func gate313Runner(t *testing.T, store *JobRepository, dispatcher *acquisition.StageDispatcher) *app.AcquisitionRunOnce {
	t.Helper()
	runner, err := app.NewAcquisitionRunOnce(store, store, dispatcher, app.RunOnceLimits{
		MinLeaseDuration: time.Minute, MaxLeaseDuration: time.Hour,
		MaxRecoveryLimit: 10, MaxProjectorPageLimit: acquisition.MaxCanonicalProjectorLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func gate313Request(owner string, now time.Time) app.RunOnceRequest {
	return app.RunOnceRequest{Owner: owner, Now: now, LeaseDuration: time.Hour,
		RetryAt: now.Add(time.Minute), ProjectorPageLimit: 25, RecoveryLimit: 10}
}

func TestPostgresAcquisitionRunOnceRecoveryThenTwoSeparateStages(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	ctx := fixture.ctx
	jobStore := mustGate311JobStore(t, fixture)
	expired := seedCanonicalPgManifestJob(t, ctx, fixture.pool, fixture.bindingID, "60",
		acquisition.StateActive, "expired-worker", 1, 2, 5, canonicalPgExpiredLeaseEnd(), nil)
	other := mustCreateJob(t, ctx, jobStore, jobs.CreateRequest{
		ID: "50000000-0000-4000-8000-000000000070", Type: "OTHER", Payload: []byte(`{}`), MaxAttempts: 3,
	})
	manifestID, jobID := seedAcquisitionProviderTaskManifest(t, ctx, fixture.pool, fixture.bindingID, "61", "62")
	providerName := "episode.mkv"
	provider := &gate311Provider{result: acquisition.StepResult{
		Outcome: acquisition.OutcomeProviderSucceeded, ResultName: &providerName}}
	outcomeStore, err := NewProviderOutcomeRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	outcomes, err := acquisition.NewProviderOutcomeService(outcomeStore)
	if err != nil {
		t.Fatal(err)
	}
	storageStore, err := NewStorageRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	refreshStore, err := NewRefreshRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	hint := &gate311Hint{}
	refresh, err := acquisition.NewRefreshStep(fixture.manifests, jobStore, storageStore, hint, refreshStore)
	if err != nil {
		t.Fatal(err)
	}
	canonical := &gate311UnexpectedCanonical{}
	dispatcher, err := acquisition.NewStageDispatcher(jobStore, fixture.manifests, provider, outcomes, refresh, canonical)
	if err != nil {
		t.Fatal(err)
	}
	runner := gate313Runner(t, jobStore, dispatcher)
	now := time.Now().UTC()
	first, err := runner.RunOnce(ctx, gate313Request("worker-provider", now))
	if err != nil || first.State != app.RunOnceStageCompleted || first.Recovered != 1 ||
		first.ClaimedJobID != jobID || first.ClaimAttempts != 1 || first.Stage == nil ||
		first.Stage.Stage != acquisition.StageProvider || first.Stage.Manifest.State != acquisition.StateAwaitingVisibility ||
		first.Stage.Job.State != jobs.StateRetryWait || provider.calls != 1 || hint.calls != 0 {
		t.Fatalf("first tick = %#v, %v; provider/hint calls %d/%d", first, err, provider.calls, hint.calls)
	}
	if manifest := readCanonicalPgManifest(t, ctx, fixture.pool, expired.manifestID); manifest.State != acquisition.StateRecoveryRequired {
		t.Fatalf("expired Manifest state = %s", manifest.State)
	}
	if job := readCanonicalPgJob(t, ctx, fixture.pool, expired.jobID); job.State != jobs.StateRecoveryRequired || job.FailureCount != 2 {
		t.Fatalf("expired Job = %#v", job)
	}
	if job, err := jobStore.Get(ctx, other.ID); err != nil || job.State != jobs.StateQueued || job.ClaimAttempts != 0 {
		t.Fatalf("unrelated Job = %#v, %v", job, err)
	}
	second, err := runner.RunOnce(ctx, gate313Request("worker-hint", now.Add(2*time.Minute)))
	if err != nil || second.State != app.RunOnceStageCompleted || second.Recovered != 0 ||
		second.ClaimedJobID != jobID || second.ClaimAttempts != 2 || second.Stage == nil ||
		second.Stage.Stage != acquisition.StageVisibility || second.Stage.Manifest.State != acquisition.StateAwaitingCanonical ||
		second.Stage.Job.State != jobs.StateRetryWait || provider.calls != 1 || hint.calls != 1 || canonical.calls != 0 {
		t.Fatalf("second tick = %#v, %v; provider/hint/canonical calls %d/%d/%d",
			second, err, provider.calls, hint.calls, canonical.calls)
	}
	if second.Stage.Job.FailureCount != 0 || first.Stage.Job.FailureCount != 0 {
		t.Fatalf("ordinary waits consumed failure budget: first/second %d/%d",
			first.Stage.Job.FailureCount, second.Stage.Job.FailureCount)
	}
	manifest, err := fixture.manifests.GetManifest(ctx, manifestID)
	if err != nil || manifest.ResultName == nil || *manifest.ResultName != providerName {
		t.Fatalf("result locator = %#v, %v", manifest.ResultName, err)
	}
}

type gate318StartDiagnostic struct{}

func (gate318StartDiagnostic) Error() string {
	return "115 StartDownload failed: RESPONSE_DECODE_FAILURE"
}
func (gate318StartDiagnostic) DiagnosticCategory() string       { return "RESPONSE_DECODE_FAILURE" }
func (gate318StartDiagnostic) DiagnosticElapsed() time.Duration { return 484 * time.Millisecond }
func (gate318StartDiagnostic) DiagnosticCauseType() string      { return "*json.SyntaxError" }
func (gate318StartDiagnostic) DiagnosticHTTPStatus() int        { return 200 }

func TestPostgresAcquisitionRunOnceRealStartUncertainty(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	ctx := fixture.ctx
	if _, err := fixture.pool.Exec(ctx, `UPDATE storage_bindings SET provider_scope = 'test-scope' WHERE storage_binding_id = $1`, string(fixture.bindingID)); err != nil {
		t.Fatal(err)
	}
	manifestID, jobID := seedAcquisitionProviderTaskManifest(t, ctx, fixture.pool, fixture.bindingID, "63", "64")
	jobStore := mustGate311JobStore(t, fixture)
	storageStore, err := NewStorageRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := acquisition.NewExecutionInputResolver(fixture.manifests, storageStore)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := NewAcquisitionProviderTaskRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	downloader := &gate311StartFailureDownloader{err: gate318StartDiagnostic{}}
	execution, err := acquisition.NewExecutionStepService(inputs, fixture.manifests, jobStore,
		gate311ExecutionSession{downloader: downloader}, tasks)
	if err != nil {
		t.Fatal(err)
	}
	outcomeStore, err := NewProviderOutcomeRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	outcomes, err := acquisition.NewProviderOutcomeService(outcomeStore)
	if err != nil {
		t.Fatal(err)
	}
	visibility := &gate311UnexpectedVisibility{}
	canonical := &gate311UnexpectedCanonical{}
	dispatcher, err := acquisition.NewStageDispatcher(jobStore, fixture.manifests, execution, outcomes, visibility, canonical)
	if err != nil {
		t.Fatal(err)
	}
	runner := gate313Runner(t, jobStore, dispatcher)
	now := time.Now().UTC()
	first, err := runner.RunOnce(ctx, gate313Request("worker", now))
	if err != nil || first.State != app.RunOnceStageCompleted || first.Stage == nil ||
		first.Stage.Manifest.State != acquisition.StateRecoveryRequired || first.Stage.Job.State != jobs.StateRecoveryRequired ||
		first.ClaimedJobID != jobID || downloader.startCalls != 1 {
		t.Fatalf("uncertain tick = %#v, %v; starts %d", first, err, downloader.startCalls)
	}
	if first.Stage.DiagnosticCategory != "RESPONSE_DECODE_FAILURE" || first.Stage.DiagnosticElapsedMS != 484 ||
		first.Stage.DiagnosticCauseType != "*json.SyntaxError" || first.Stage.DiagnosticHTTPStatus != 200 {
		t.Fatalf("committed recovery lost safe start diagnostic: %#v", first.Stage)
	}
	stored, err := tasks.GetProviderTask(ctx, manifestID)
	if err != nil || stored.State != acquisition.ProviderTaskStartReserved || stored.ProviderTaskRef != "" {
		t.Fatalf("provider reservation = %#v, %v", stored, err)
	}
	second, err := runner.RunOnce(ctx, gate313Request("worker", now.Add(2*time.Minute)))
	if err != nil || second.State != app.RunOnceIdle || downloader.startCalls != 1 ||
		visibility.calls != 0 || canonical.calls != 0 {
		t.Fatalf("replayed tick = %#v, %v; starts %d", second, err, downloader.startCalls)
	}
}

func TestPostgresAcquisitionRunOnceRecoveryDebtBlocksClaim(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	ctx := fixture.ctx
	jobStore := mustGate311JobStore(t, fixture)
	good := seedCanonicalPgManifestJob(t, ctx, fixture.pool, fixture.bindingID, "65",
		acquisition.StateActive, "expired-a", 1, 0, 5, canonicalPgExpiredLeaseEnd().Add(-time.Hour), nil)
	bad := seedCanonicalPgManifestJob(t, ctx, fixture.pool, fixture.bindingID, "66",
		acquisition.StateActive, "expired-b", 1, 0, 5, canonicalPgExpiredLeaseEnd(), nil)
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET idempotency_key = 'wrong' WHERE job_id = $1`, string(bad.jobID)); err != nil {
		t.Fatal(err)
	}
	_, queuedID := seedAcquisitionProviderTaskManifest(t, ctx, fixture.pool, fixture.bindingID, "67", "68")
	outcomeStore, err := NewProviderOutcomeRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	outcomes, err := acquisition.NewProviderOutcomeService(outcomeStore)
	if err != nil {
		t.Fatal(err)
	}
	provider := &gate311Provider{}
	dispatcher, err := acquisition.NewStageDispatcher(jobStore, fixture.manifests, provider, outcomes,
		&gate311UnexpectedVisibility{}, &gate311UnexpectedCanonical{})
	if err != nil {
		t.Fatal(err)
	}
	runner := gate313Runner(t, jobStore, dispatcher)
	result, err := runner.RunOnce(ctx, gate313Request("worker", time.Now().UTC()))
	var typed *app.RunOnceError
	if !errors.As(err, &typed) || typed.Kind != app.RunOnceRecoveryDebt || result.Recovered != 1 ||
		result.ClaimedJobID != "" || provider.calls != 0 {
		t.Fatalf("debt tick = %#v, %v", result, err)
	}
	if manifest := readCanonicalPgManifest(t, ctx, fixture.pool, good.manifestID); manifest.State != acquisition.StateRecoveryRequired {
		t.Fatalf("first pair state = %s", manifest.State)
	}
	if manifest := readCanonicalPgManifest(t, ctx, fixture.pool, bad.manifestID); manifest.State != acquisition.StateActive {
		t.Fatalf("corrupt pair state = %s", manifest.State)
	}
	if job, err := jobStore.Get(ctx, queuedID); err != nil || job.State != jobs.StateQueued || job.ClaimAttempts != 0 {
		t.Fatalf("queued Job was claimed: %#v, %v", job, err)
	}
}

func TestPostgresAcquisitionRunOnceStageErrorLeavesPairRunning(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	ctx := fixture.ctx
	manifestID, jobID := seedAcquisitionProviderTaskManifest(t, ctx, fixture.pool, fixture.bindingID, "69", "6a")
	jobStore := mustGate311JobStore(t, fixture)
	outcomeStore, err := NewProviderOutcomeRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	outcomes, err := acquisition.NewProviderOutcomeService(outcomeStore)
	if err != nil {
		t.Fatal(err)
	}
	stageErr := errors.New("provider session unavailable")
	provider := &gate311Provider{err: stageErr}
	dispatcher, err := acquisition.NewStageDispatcher(jobStore, fixture.manifests, provider, outcomes,
		&gate311UnexpectedVisibility{}, &gate311UnexpectedCanonical{})
	if err != nil {
		t.Fatal(err)
	}
	runner := gate313Runner(t, jobStore, dispatcher)
	result, err := runner.RunOnce(ctx, gate313Request("worker", time.Now().UTC()))
	var typed *app.RunOnceError
	if !errors.As(err, &typed) || typed.Kind != app.RunOnceStageError || !errors.Is(err, stageErr) ||
		typed.JobID != jobID || typed.ClaimAttempts != 1 || result.Stage != nil {
		t.Fatalf("stage error tick = %#v, %v", result, err)
	}
	manifest := readCanonicalPgManifest(t, ctx, fixture.pool, manifestID)
	job := readCanonicalPgJob(t, ctx, fixture.pool, jobID)
	if manifest.State != acquisition.StateActive || job.State != jobs.StateRunning || job.FailureCount != 0 {
		t.Fatalf("stage error mutated pair: Manifest %s Job %s failures %d", manifest.State, job.State, job.FailureCount)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = CURRENT_TIMESTAMP - interval '1 second'
WHERE job_id = $1`, string(jobID)); err != nil {
		t.Fatal(err)
	}
	next, err := runner.RunOnce(ctx, gate313Request("worker-next", time.Now().UTC().Add(time.Minute)))
	if err != nil || next.State != app.RunOnceRecoveryOnly || next.Recovered != 1 || next.ClaimedJobID != "" {
		t.Fatalf("next tick paired recovery = %#v, %v", next, err)
	}
	manifest = readCanonicalPgManifest(t, ctx, fixture.pool, manifestID)
	job = readCanonicalPgJob(t, ctx, fixture.pool, jobID)
	if manifest.State != acquisition.StateRecoveryRequired || job.State != jobs.StateRecoveryRequired || provider.calls != 1 {
		t.Fatalf("paired recovery after stage error = Manifest %s Job %s provider calls %d",
			manifest.State, job.State, provider.calls)
	}
}
