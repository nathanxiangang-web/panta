package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

type gate311StartFailureDownloader struct {
	startCalls  int
	statusCalls int
	reference   contracts.TaskReference
	err         error
}

func (provider *gate311StartFailureDownloader) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{ID: "test", DisplayName: "test downloader",
		Capabilities: contracts.CapabilitySet{Downloader: true}}
}

func (provider *gate311StartFailureDownloader) StartDownload(context.Context, contracts.DownloadRequest) (contracts.TaskReference, error) {
	provider.startCalls++
	return provider.reference, provider.err
}

func (provider *gate311StartFailureDownloader) DownloadStatus(context.Context, contracts.TaskReference) (contracts.TaskStatus, error) {
	provider.statusCalls++
	return contracts.TaskStatus{}, errors.New("unexpected provider poll")
}

func (provider *gate311StartFailureDownloader) CancelDownload(context.Context, contracts.TaskReference) error {
	return errors.New("unexpected provider cancel")
}

type gate311ExecutionSession struct{ downloader contracts.DownloaderProvider }

func (session gate311ExecutionSession) ResolveDownloader(_ context.Context, request acquisition.DownloaderSessionRequest) (contracts.DownloaderBinding, error) {
	if request.ProviderID != "test" || request.ConnectionID == "" || request.CredentialRef != nil {
		return contracts.DownloaderBinding{}, errors.New("unexpected downloader session identity")
	}
	return contracts.DownloaderBinding{Descriptor: session.downloader.Descriptor(), Downloader: session.downloader}, nil
}

type gate311Provider struct {
	calls  int
	result acquisition.StepResult
	err    error
}

func (stage *gate311Provider) Execute(context.Context, acquisition.StepRequest) (acquisition.StepResult, error) {
	stage.calls++
	return stage.result, stage.err
}

type gate311Hint struct{ calls int }

func (stage *gate311Hint) SubmitMutationHint(_ context.Context, hint acquisition.MutationHint) (acquisition.MutationHintReceipt, error) {
	stage.calls++
	return acquisition.MutationHintReceipt{Status: "accepted", RootID: hint.RootID, ScopeKey: hint.ScopeKey}, nil
}

type gate311Resolver struct {
	calls                int
	visible              bool
	path, root, resource string
}

func (stage *gate311Resolver) ResolveCanonical(_ context.Context, request acquisition.CanonicalResolveRequest) (acquisition.CanonicalResolution, error) {
	stage.calls++
	if request.RootID != stage.root || request.Path != stage.path {
		return acquisition.CanonicalResolution{}, errors.New("unexpected canonical identity")
	}
	if !stage.visible {
		return acquisition.CanonicalResolution{}, nil
	}
	return acquisition.CanonicalResolution{Matches: []acquisition.CanonicalResource{{
		RootID: stage.root, ResourceID: stage.resource, CanonicalPath: stage.path,
		Presence: acquisition.CanonicalPresencePresent,
	}}}, nil
}

type gate311Projector struct{ calls int }

func (stage *gate311Projector) ProjectOnce(context.Context, storage.BindingID, int) (acquisition.CanonicalProjection, error) {
	stage.calls++
	return acquisition.CanonicalProjection{}, nil
}

type gate311Classifier struct{ calls int }

func (stage *gate311Classifier) Bind(context.Context, catalog.CopyID, catalog.VariantID) (catalog.Copy, error) {
	stage.calls++
	return catalog.Copy{}, errors.New("classification must join final PostgreSQL transaction")
}

type gate311UnexpectedVisibility struct{ calls int }

func (stage *gate311UnexpectedVisibility) Submit(context.Context, acquisition.RefreshRequest) (acquisition.RefreshResult, error) {
	stage.calls++
	return acquisition.RefreshResult{}, errors.New("visibility stage must not run")
}

type gate311UnexpectedCanonical struct{ calls int }

func (stage *gate311UnexpectedCanonical) Confirm(context.Context, acquisition.CanonicalRequest) (acquisition.CanonicalResult, error) {
	stage.calls++
	return acquisition.CanonicalResult{}, errors.New("canonical stage must not run")
}

func TestPostgresAcquisitionStageDispatcherMultiClaim(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	ctx := fixture.ctx
	seeded := seedCanonicalPgManifestJob(t, ctx, fixture.pool, fixture.bindingID, "40",
		acquisition.StateActive, "worker-provider", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	if _, err := fixture.pool.Exec(ctx, `UPDATE acquisition_manifests SET result_name = NULL WHERE manifest_id = $1`, string(seeded.manifestID)); err != nil {
		t.Fatalf("clear pre-provider result_name: %v", err)
	}
	providerName := "Episode.mkv"
	provider := &gate311Provider{result: acquisition.StepResult{
		Outcome: acquisition.OutcomeProviderSucceeded, ResultName: &providerName,
	}}
	hint := &gate311Hint{}
	resolver := &gate311Resolver{root: canonicalPgCopyRoot,
		path: "/downloads/item/Episode.mkv", resource: "resource-stage-40"}
	projector := &gate311Projector{}
	classifier := &gate311Classifier{}

	outcomeStore, err := NewProviderOutcomeRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	outcomes, err := acquisition.NewProviderOutcomeService(outcomeStore)
	if err != nil {
		t.Fatal(err)
	}
	refreshStore, err := NewRefreshRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	storageStore, err := NewStorageRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	refresh, err := acquisition.NewRefreshStep(fixture.manifests, mustGate311JobStore(t, fixture), storageStore, hint, refreshStore)
	if err != nil {
		t.Fatal(err)
	}
	catalogStore, err := NewCatalogRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := acquisition.NewCanonicalConfirmation(fixture.manifests, mustGate311JobStore(t, fixture),
		storageStore, resolver, projector, catalogStore, classifier, fixture.repository)
	if err != nil {
		t.Fatal(err)
	}
	jobStore := mustGate311JobStore(t, fixture)
	newDispatcher := func() *acquisition.StageDispatcher {
		t.Helper()
		dispatcher, err := acquisition.NewStageDispatcher(jobStore, fixture.manifests,
			provider, outcomes, refresh, canonical)
		if err != nil {
			t.Fatal(err)
		}
		return dispatcher
	}
	clock := canonicalPgNow()
	request := func(job jobs.Job, owner string, now time.Time) acquisition.StageDispatchRequest {
		return acquisition.StageDispatchRequest{JobID: job.ID, Owner: owner,
			ExpectedClaim: job.ClaimAttempts, Now: now, RetryAt: now.Add(time.Minute),
			ProjectorPageLimit: 25}
	}
	claim := func(owner string, now time.Time) jobs.Job {
		t.Helper()
		job, err := jobStore.ClaimNext(ctx, jobs.ClaimRequest{Owner: owner, Now: now, LeaseDuration: time.Hour})
		if err != nil {
			t.Fatalf("ClaimNext(%s) error = %v", owner, err)
		}
		if job.ID != seeded.jobID {
			t.Fatalf("claimed Job %s, want %s", job.ID, seeded.jobID)
		}
		return job
	}
	assertDurable := func(wantManifest acquisition.State, wantJob jobs.State, wantClaim int) {
		t.Helper()
		manifest := readCanonicalPgManifest(t, ctx, fixture.pool, seeded.manifestID)
		job := readCanonicalPgJob(t, ctx, fixture.pool, seeded.jobID)
		if manifest.State != wantManifest || job.State != wantJob || job.ClaimAttempts != wantClaim || job.FailureCount != seeded.failureBudget {
			t.Fatalf("durable state = Manifest %s, Job %s, claim %d, failure %d; want %s/%s/%d/%d",
				manifest.State, job.State, job.ClaimAttempts, job.FailureCount,
				wantManifest, wantJob, wantClaim, seeded.failureBudget)
		}
	}

	firstJob, err := jobStore.Get(ctx, seeded.jobID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := newDispatcher().Dispatch(ctx, request(firstJob, seeded.owner, clock))
	if err != nil || first.Stage != acquisition.StageProvider || first.Manifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("provider dispatch = %#v, %v", first, err)
	}
	if first.Manifest.ResultName == nil || *first.Manifest.ResultName != providerName {
		t.Fatalf("durable provider result_name = %v, want %q", first.Manifest.ResultName, providerName)
	}
	assertDurable(acquisition.StateAwaitingVisibility, jobs.StateRetryWait, 1)
	if provider.calls != 1 || hint.calls != 0 || resolver.calls != 0 || projector.calls != 0 {
		t.Fatalf("first claim cascaded: provider/hint/Q5/projector = %d/%d/%d/%d",
			provider.calls, hint.calls, resolver.calls, projector.calls)
	}

	// Recreate the coordinator after each durable boundary. No in-memory stage
	// cursor is carried into the next claim.
	secondAt := clock.Add(2 * time.Minute)
	secondJob := claim("worker-hint", secondAt)
	second, err := newDispatcher().Dispatch(ctx, request(secondJob, "worker-hint", secondAt))
	if err != nil || second.Stage != acquisition.StageVisibility || second.Manifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("visibility dispatch = %#v, %v", second, err)
	}
	assertDurable(acquisition.StateAwaitingCanonical, jobs.StateRetryWait, 2)
	if provider.calls != 1 || hint.calls != 1 || resolver.calls != 0 || projector.calls != 0 {
		t.Fatalf("second claim cascaded: provider/hint/Q5/projector = %d/%d/%d/%d",
			provider.calls, hint.calls, resolver.calls, projector.calls)
	}

	thirdAt := clock.Add(4 * time.Minute)
	thirdJob := claim("worker-canonical", thirdAt)
	third, err := newDispatcher().Dispatch(ctx, request(thirdJob, "worker-canonical", thirdAt))
	if err != nil || third.Stage != acquisition.StageCanonical || !third.Pending {
		t.Fatalf("canonical pending dispatch = %#v, %v", third, err)
	}
	assertDurable(acquisition.StateAwaitingCanonical, jobs.StateRetryWait, 3)
	if resolver.calls != 1 || projector.calls != 0 || classifier.calls != 0 {
		t.Fatalf("pending canonical calls = Q5 %d, projector %d, classifier %d", resolver.calls, projector.calls, classifier.calls)
	}

	copyRecord := seedCanonicalPgCopy(t, ctx, fixture.pool, fixture.bindingID, "40",
		resolver.resource, nil, catalog.CopyAvailabilityPresent)
	resolver.visible = true
	fourthAt := clock.Add(6 * time.Minute)
	fourthJob := claim("worker-canonical", fourthAt)
	fourth, err := newDispatcher().Dispatch(ctx, request(fourthJob, "worker-canonical", fourthAt))
	if err != nil || fourth.Stage != acquisition.StageCanonical || !fourth.Terminal || fourth.Manifest.State != acquisition.StateReady {
		t.Fatalf("canonical READY dispatch = %#v, %v", fourth, err)
	}
	assertDurable(acquisition.StateReady, jobs.StateSucceeded, 4)
	if fourth.Manifest.ResultCopyID == nil || *fourth.Manifest.ResultCopyID != copyRecord.ID ||
		provider.calls != 1 || hint.calls != 1 || resolver.calls != 2 || projector.calls != 1 || classifier.calls != 0 {
		t.Fatalf("final result = %#v; calls provider/hint/Q5/projector/classifier = %d/%d/%d/%d/%d",
			fourth.Manifest.ResultCopyID, provider.calls, hint.calls, resolver.calls, projector.calls, classifier.calls)
	}

	// A historical replay does not require another Claim or any external stage.
	replayed, err := newDispatcher().Dispatch(ctx, request(fourthJob, "worker-canonical", fourthAt))
	if err != nil || replayed.Stage != acquisition.StageTerminal || replayed.Changed || !replayed.Terminal {
		t.Fatalf("terminal replay = %#v, %v", replayed, err)
	}
	if provider.calls != 1 || hint.calls != 1 || resolver.calls != 2 || projector.calls != 1 {
		t.Fatal("terminal replay invoked an external stage")
	}
}

func mustGate311JobStore(t *testing.T, fixture canonicalPgIntegration) *JobRepository {
	t.Helper()
	store, err := NewJobRepository(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestPostgresAcquisitionStageDispatcherUncertainSideEffectRequiresRecovery(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "41",
		acquisition.StateActive, "worker-provider", 1, 0, 5, canonicalPgLeaseEnd(), nil)
	provider := &gate311Provider{err: acquisition.ErrExecutionSideEffectUncertain}
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
	jobStore := mustGate311JobStore(t, fixture)
	dispatcher, err := acquisition.NewStageDispatcher(jobStore, fixture.manifests, provider, outcomes, visibility, canonical)
	if err != nil {
		t.Fatal(err)
	}
	request := acquisition.StageDispatchRequest{JobID: seeded.jobID, Owner: seeded.owner,
		ExpectedClaim: seeded.claim, Now: canonicalPgNow(), RetryAt: canonicalPgNow().Add(time.Minute)}
	result, err := dispatcher.Dispatch(fixture.ctx, request)
	if err != nil || result.Manifest.State != acquisition.StateRecoveryRequired || result.Job.State != jobs.StateRecoveryRequired || !result.Terminal {
		t.Fatalf("uncertain provider dispatch = %#v, %v", result, err)
	}
	manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
	if manifest.State != acquisition.StateRecoveryRequired || job.State != jobs.StateRecoveryRequired || job.FailureCount != 0 {
		t.Fatalf("durable recovery = Manifest %s, Job %s, failures %d", manifest.State, job.State, job.FailureCount)
	}
	replayed, err := dispatcher.Dispatch(fixture.ctx, request)
	if err != nil || replayed.Stage != acquisition.StageRecovery || replayed.Changed || !replayed.Terminal {
		t.Fatalf("recovery replay = %#v, %v", replayed, err)
	}
	if provider.calls != 1 || visibility.calls != 0 || canonical.calls != 0 {
		t.Fatalf("recovery replay called stages provider/visibility/canonical = %d/%d/%d",
			provider.calls, visibility.calls, canonical.calls)
	}
}

func TestPostgresAcquisitionStageDispatcherRealExecutionStartFailure(t *testing.T) {
	for _, test := range []struct {
		name               string
		suffix             string
		startErr           error
		wantReferenceError bool
	}{
		{name: "lost response", suffix: "42", startErr: errors.New("provider response lost")},
		{name: "invalid reference", suffix: "43", wantReferenceError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCanonicalPgFixture(t)
			ctx := fixture.ctx
			if _, err := fixture.pool.Exec(ctx, `UPDATE storage_bindings SET provider_scope = 'test-scope' WHERE storage_binding_id = $1`, string(fixture.bindingID)); err != nil {
				t.Fatalf("set provider scope: %v", err)
			}
			seeded := seedCanonicalPgManifestJob(t, ctx, fixture.pool, fixture.bindingID, test.suffix,
				acquisition.StateActive, "worker-provider", 1, 0, 5, canonicalPgLeaseEnd(), nil)
			jobStore := mustGate311JobStore(t, fixture)
			storageStore, err := NewStorageRepository(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			inputs, err := acquisition.NewExecutionInputResolver(fixture.manifests, storageStore)
			if err != nil {
				t.Fatal(err)
			}
			providerTasks, err := NewAcquisitionProviderTaskRepository(fixture.pool)
			if err != nil {
				t.Fatal(err)
			}
			downloader := &gate311StartFailureDownloader{err: test.startErr}
			execution, err := acquisition.NewExecutionStepService(inputs, fixture.manifests, jobStore,
				gate311ExecutionSession{downloader: downloader}, providerTasks)
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
			dispatcher, err := acquisition.NewStageDispatcher(jobStore, fixture.manifests,
				execution, outcomes, visibility, canonical)
			if err != nil {
				t.Fatal(err)
			}
			request := acquisition.StageDispatchRequest{JobID: seeded.jobID, Owner: seeded.owner,
				ExpectedClaim: seeded.claim, Now: canonicalPgNow(), RetryAt: canonicalPgNow().Add(time.Minute)}

			// The first real Execute must classify the post-reservation error before
			// the dispatcher commits a durable recovery outcome in PostgreSQL.
			result, err := dispatcher.Dispatch(ctx, request)
			if err != nil || result.Stage != acquisition.StageProvider || !result.Terminal ||
				result.Manifest.State != acquisition.StateRecoveryRequired || result.Job.State != jobs.StateRecoveryRequired {
				t.Fatalf("first dispatch = %#v, %v; want immediate recovery", result, err)
			}
			stored, err := providerTasks.GetProviderTask(ctx, seeded.manifestID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.State != acquisition.ProviderTaskStartReserved || stored.ProviderTaskRef != "" {
				t.Fatalf("provider task = %#v, want uncommitted START_RESERVED", stored)
			}
			manifest := readCanonicalPgManifest(t, ctx, fixture.pool, seeded.manifestID)
			job := readCanonicalPgJob(t, ctx, fixture.pool, seeded.jobID)
			if manifest.State != acquisition.StateRecoveryRequired || job.State != jobs.StateRecoveryRequired || job.FailureCount != 0 {
				t.Fatalf("durable state = Manifest %s, Job %s, failures %d", manifest.State, job.State, job.FailureCount)
			}
			if downloader.startCalls != 1 || downloader.statusCalls != 0 {
				t.Fatalf("provider calls start/status = %d/%d", downloader.startCalls, downloader.statusCalls)
			}

			replayed, err := dispatcher.Dispatch(ctx, request)
			if err != nil || replayed.Stage != acquisition.StageRecovery || replayed.Changed || !replayed.Terminal {
				t.Fatalf("recovery replay = %#v, %v", replayed, err)
			}
			if downloader.startCalls != 1 || downloader.statusCalls != 0 || visibility.calls != 0 || canonical.calls != 0 {
				t.Fatalf("replay called external stages: start/status/visibility/canonical = %d/%d/%d/%d",
					downloader.startCalls, downloader.statusCalls, visibility.calls, canonical.calls)
			}
		})
	}
}
