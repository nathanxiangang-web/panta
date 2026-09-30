package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

type gate314Clock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *gate314Clock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}
func (clock *gate314Clock) Advance(delay time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(delay)
	clock.mu.Unlock()
}

type gate314AdvanceWaiter struct {
	clock *gate314Clock
	calls int
}

func (waiter *gate314AdvanceWaiter) Wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	waiter.calls++
	waiter.clock.Advance(2 * delay)
	return nil
}

func gate314Policy() config.AcquisitionWorker {
	return config.AcquisitionWorker{Enabled: true, OwnerPrefix: "pg-worker", Interval: 100 * time.Millisecond,
		TickTimeout: time.Second, LeaseDuration: 10 * time.Second, RetryDelay: 100 * time.Millisecond,
		RecoveryLimit: 10, ProjectorPageLimit: 25}
}

func gate314RunOnce(t *testing.T, jobStore *JobRepository, dispatcher *acquisition.StageDispatcher) *app.AcquisitionRunOnce {
	t.Helper()
	runner, err := app.NewAcquisitionRunOnce(jobStore, jobStore, dispatcher, app.RunOnceLimits{
		MinLeaseDuration: time.Second, MaxLeaseDuration: time.Hour,
		MaxRecoveryLimit: 10, MaxProjectorPageLimit: acquisition.MaxCanonicalProjectorLimit})
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func TestPostgresAcquisitionWorkerRunsSeparateDueStages(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	ctx := fixture.ctx
	manifestID, jobID := seedAcquisitionProviderTaskManifest(t, ctx, fixture.pool, fixture.bindingID, "70", "71")
	jobStore := mustGate311JobStore(t, fixture)
	name := "episode.mkv"
	provider := &gate311Provider{result: acquisition.StepResult{Outcome: acquisition.OutcomeProviderSucceeded, ResultName: &name}}
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
	runner := gate314RunOnce(t, jobStore, dispatcher)
	clock := &gate314Clock{now: time.Now().UTC()}
	waiter := &gate314AdvanceWaiter{clock: clock}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	var stages []app.WorkerEvent
	worker, err := app.NewAcquisitionWorker(runner, gate314Policy(),
		app.WithWorkerClock(clock.Now), app.WithWorkerWaiter(waiter),
		app.WithWorkerEventSink(func(event app.WorkerEvent) {
			if event.Kind == app.WorkerStage {
				stages = append(stages, event)
				if len(stages) == 2 {
					stop()
				}
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Run(workerCtx); err != nil {
		t.Fatal(err)
	}
	if len(stages) != 2 || stages[0].Stage != acquisition.StageProvider || stages[1].Stage != acquisition.StageVisibility ||
		stages[0].JobID != jobID || stages[1].JobID != jobID ||
		stages[0].ClaimAttempts != 1 || stages[1].ClaimAttempts != 2 ||
		stages[0].Owner != worker.Owner() || stages[1].Owner != worker.Owner() ||
		provider.calls != 1 || hint.calls != 1 || canonical.calls != 0 || waiter.calls != 1 {
		t.Fatalf("stage events = %#v; provider/hint/canonical/waits = %d/%d/%d/%d",
			stages, provider.calls, hint.calls, canonical.calls, waiter.calls)
	}
	manifest := readCanonicalPgManifest(t, ctx, fixture.pool, manifestID)
	job := readCanonicalPgJob(t, ctx, fixture.pool, jobID)
	if manifest.State != acquisition.StateAwaitingCanonical || job.State != jobs.StateRetryWait ||
		job.ClaimAttempts != 2 || job.FailureCount != 0 {
		t.Fatalf("durable pair = Manifest %s Job %s claim %d failures %d",
			manifest.State, job.State, job.ClaimAttempts, job.FailureCount)
	}
}

type gate314CancelingDownloader struct {
	cancel context.CancelFunc
	starts int
}

func (provider *gate314CancelingDownloader) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{ID: "test", DisplayName: "canceling downloader",
		Capabilities: contracts.CapabilitySet{Downloader: true}}
}
func (provider *gate314CancelingDownloader) StartDownload(context.Context, contracts.DownloadRequest) (contracts.TaskReference, error) {
	provider.starts++
	provider.cancel()
	return contracts.TaskReference{}, errors.New("provider response lost after request")
}
func (provider *gate314CancelingDownloader) DownloadStatus(context.Context, contracts.TaskReference) (contracts.TaskStatus, error) {
	return contracts.TaskStatus{}, errors.New("unexpected status poll")
}
func (provider *gate314CancelingDownloader) CancelDownload(context.Context, contracts.TaskReference) error {
	return nil
}

func TestPostgresAcquisitionWorkerCancellationKeepsStartReservation(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	ctx := fixture.ctx
	if _, err := fixture.pool.Exec(ctx, `UPDATE storage_bindings SET provider_scope = 'test-scope' WHERE storage_binding_id = $1`, string(fixture.bindingID)); err != nil {
		t.Fatal(err)
	}
	manifestID, jobID := seedAcquisitionProviderTaskManifest(t, ctx, fixture.pool, fixture.bindingID, "72", "73")
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
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	downloader := &gate314CancelingDownloader{cancel: stop}
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
	dispatcher, err := acquisition.NewStageDispatcher(jobStore, fixture.manifests, execution, outcomes,
		&gate311UnexpectedVisibility{}, &gate311UnexpectedCanonical{})
	if err != nil {
		t.Fatal(err)
	}
	runner := gate314RunOnce(t, jobStore, dispatcher)
	worker, err := app.NewAcquisitionWorker(runner, gate314Policy())
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Run(workerCtx); err != nil {
		t.Fatalf("cancelled worker = %v", err)
	}
	stored, err := tasks.GetProviderTask(ctx, manifestID)
	if err != nil || stored.State != acquisition.ProviderTaskStartReserved || stored.ProviderTaskRef != "" || downloader.starts != 1 {
		t.Fatalf("post-cancel fence = %#v, %v; starts %d", stored, err, downloader.starts)
	}
	manifest := readCanonicalPgManifest(t, ctx, fixture.pool, manifestID)
	job := readCanonicalPgJob(t, ctx, fixture.pool, jobID)
	if manifest.State != acquisition.StateActive || job.State != jobs.StateRunning {
		t.Fatalf("post-cancel pair = %s/%s", manifest.State, job.State)
	}
	now := time.Now().UTC()
	request := app.RunOnceRequest{Owner: "another-worker", Now: now, LeaseDuration: 10 * time.Second,
		RetryAt: now.Add(time.Second), RecoveryLimit: 10, ProjectorPageLimit: 25}
	second, err := runner.RunOnce(ctx, request)
	if err != nil || second.State != app.RunOnceIdle || downloader.starts != 1 {
		t.Fatalf("premature replay = %#v, %v; starts %d", second, err, downloader.starts)
	}
	if _, err := fixture.pool.Exec(ctx, `UPDATE jobs SET lease_expires_at = CURRENT_TIMESTAMP - interval '1 second' WHERE job_id = $1`, string(jobID)); err != nil {
		t.Fatal(err)
	}
	third, err := runner.RunOnce(ctx, request)
	if err != nil || third.State != app.RunOnceRecoveryOnly || third.Recovered != 1 || downloader.starts != 1 {
		t.Fatalf("expiry recovery = %#v, %v; starts %d", third, err, downloader.starts)
	}
	manifest = readCanonicalPgManifest(t, ctx, fixture.pool, manifestID)
	job = readCanonicalPgJob(t, ctx, fixture.pool, jobID)
	if manifest.State != acquisition.StateRecoveryRequired || job.State != jobs.StateRecoveryRequired {
		t.Fatalf("recovered pair = %s/%s", manifest.State, job.State)
	}
}
