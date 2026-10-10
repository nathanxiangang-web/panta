package acquisition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

type dispatchStartDiagnostic struct{}

func (dispatchStartDiagnostic) Error() string                    { return "private upstream response" }
func (dispatchStartDiagnostic) DiagnosticCategory() string       { return "SOURCE_REJECTED" }
func (dispatchStartDiagnostic) DiagnosticElapsed() time.Duration { return 321 * time.Millisecond }

type dispatchJobReader struct{ job jobs.Job }

func (reader *dispatchJobReader) Get(context.Context, jobs.JobID) (jobs.Job, error) {
	return reader.job, nil
}

type dispatchManifestReader struct{ manifest Manifest }

func (reader *dispatchManifestReader) GetManifest(context.Context, ManifestID) (Manifest, error) {
	return reader.manifest, nil
}

type dispatchProvider struct {
	calls  int
	result StepResult
	err    error
}

func (stage *dispatchProvider) Execute(context.Context, StepRequest) (StepResult, error) {
	stage.calls++
	return stage.result, stage.err
}

type dispatchOutcomes struct {
	calls    int
	request  ProviderOutcomeRequest
	manifest Manifest
	job      jobs.Job
}

func (stage *dispatchOutcomes) Commit(_ context.Context, request ProviderOutcomeRequest) (ProviderOutcomeResult, error) {
	stage.calls++
	stage.request = request
	transition, err := ProviderOutcomeTransitionFor(request.Outcome)
	if err != nil {
		return ProviderOutcomeResult{}, err
	}
	manifest, job := stage.manifest, stage.job
	manifest.State, job.State = transition.ManifestState, transition.JobState
	return ProviderOutcomeResult{Manifest: manifest, Job: job, Changed: true}, nil
}

type dispatchVisibility struct {
	calls    int
	manifest Manifest
	job      jobs.Job
}

func (stage *dispatchVisibility) Submit(context.Context, RefreshRequest) (RefreshResult, error) {
	stage.calls++
	manifest, job := stage.manifest, stage.job
	manifest.State, job.State = StateAwaitingCanonical, jobs.StateRetryWait
	return RefreshResult{Manifest: manifest, Job: job, Changed: true}, nil
}

type dispatchCanonical struct {
	calls    int
	ready    bool
	manifest Manifest
	job      jobs.Job
}

func (stage *dispatchCanonical) Confirm(context.Context, CanonicalRequest) (CanonicalResult, error) {
	stage.calls++
	manifest, job := stage.manifest, stage.job
	if stage.ready {
		manifest.State, job.State = StateReady, jobs.StateSucceeded
		copyID := catalog.CopyID("copy-result")
		manifest.ResultCopyID = &copyID
	} else {
		manifest.State, job.State = StateAwaitingCanonical, jobs.StateRetryWait
	}
	return CanonicalResult{Manifest: manifest, Job: job, Changed: true}, nil
}

type dispatchFixture struct {
	jobs       *dispatchJobReader
	manifests  *dispatchManifestReader
	provider   *dispatchProvider
	outcomes   *dispatchOutcomes
	visibility *dispatchVisibility
	canonical  *dispatchCanonical
	dispatcher *StageDispatcher
	request    StageDispatchRequest
}

func newDispatchFixture(t *testing.T, state State) *dispatchFixture {
	t.Helper()
	manifestID := ManifestID("manifest-1")
	jobID := jobs.JobID("job-1")
	owner := "worker-a"
	key := AcquisitionJobIdempotencyKey(manifestID)
	payload, err := json.Marshal(acquisitionJobPayload{SchemaVersion: AcquisitionPayloadSchemaVersion, ManifestID: manifestID})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	leaseEnd := now.Add(time.Hour)
	job := jobs.Job{ID: jobID, Type: JobTypeAcquisition, Payload: payload, State: jobs.StateRunning,
		IdempotencyKey: &key, LeaseOwner: &owner, LeaseExpiresAt: &leaseEnd, ClaimAttempts: 3}
	manifest := Manifest{ID: manifestID, JobID: &jobID, State: state}
	fixture := &dispatchFixture{
		jobs: &dispatchJobReader{job: job}, manifests: &dispatchManifestReader{manifest: manifest},
		provider: &dispatchProvider{}, outcomes: &dispatchOutcomes{manifest: manifest, job: job},
		visibility: &dispatchVisibility{manifest: manifest, job: job},
		canonical:  &dispatchCanonical{manifest: manifest, job: job},
		request: StageDispatchRequest{JobID: jobID, Owner: owner, ExpectedClaim: 3,
			Now: now, RetryAt: now.Add(time.Minute), ProjectorPageLimit: 50},
	}
	fixture.dispatcher, err = NewStageDispatcher(fixture.jobs, fixture.manifests, fixture.provider,
		fixture.outcomes, fixture.visibility, fixture.canonical)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture *dispatchFixture) assertCalls(t *testing.T, provider, outcomes, visibility, canonical int) {
	t.Helper()
	if fixture.provider.calls != provider || fixture.outcomes.calls != outcomes ||
		fixture.visibility.calls != visibility || fixture.canonical.calls != canonical {
		t.Fatalf("stage calls = provider %d, outcome %d, visibility %d, canonical %d; want %d/%d/%d/%d",
			fixture.provider.calls, fixture.outcomes.calls, fixture.visibility.calls, fixture.canonical.calls,
			provider, outcomes, visibility, canonical)
	}
}

func TestStageDispatcherProviderOutcomeTable(t *testing.T) {
	for _, test := range []struct {
		name         string
		step         StepResult
		stepErr      error
		want         ProviderOutcome
		wantManifest State
		wantJob      jobs.State
	}{
		{"pending", StepResult{Outcome: OutcomeProviderInProgress}, nil, ProviderOutcomeInProgress, StateActive, jobs.StateRetryWait},
		{"succeeded", StepResult{Outcome: OutcomeProviderSucceeded}, nil, ProviderOutcomeSucceeded, StateAwaitingVisibility, jobs.StateRetryWait},
		{"failed", StepResult{Outcome: OutcomeProviderFailed}, nil, ProviderOutcomeFailed, StateFailed, jobs.StateFailed},
		{"canceled", StepResult{Outcome: OutcomeProviderCanceled}, nil, ProviderOutcomeCanceled, StateCanceled, jobs.StateCanceled},
		{"uncertain", StepResult{}, ErrExecutionSideEffectUncertain, ProviderOutcomeRecovery, StateRecoveryRequired, jobs.StateRecoveryRequired},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDispatchFixture(t, StateActive)
			fixture.provider.result, fixture.provider.err = test.step, test.stepErr
			if test.want == ProviderOutcomeSucceeded {
				name := "  Exact Result.mkv  "
				fixture.provider.result.ResultName = &name
			}
			result, err := fixture.dispatcher.Dispatch(context.Background(), fixture.request)
			if err != nil {
				t.Fatalf("Dispatch() error = %v", err)
			}
			fixture.assertCalls(t, 1, 1, 0, 0)
			if result.Stage != StageProvider || result.Manifest.State != test.wantManifest || result.Job.State != test.wantJob ||
				fixture.outcomes.request.Outcome != test.want {
				t.Fatalf("result = %#v, handoff = %#v", result, fixture.outcomes.request)
			}
			if test.want == ProviderOutcomeSucceeded {
				if fixture.outcomes.request.ProviderResultName == nil || *fixture.outcomes.request.ProviderResultName != "  Exact Result.mkv  " {
					t.Fatalf("provider result_name changed: %v", fixture.outcomes.request.ProviderResultName)
				}
			}
			if test.want == ProviderOutcomeRecovery && fixture.outcomes.request.ErrorMessage == nil {
				t.Fatal("uncertain side effect did not record explicit recovery cause")
			}
		})
	}
}

func TestStageDispatcherRetainsRedactedDiagnosticAfterRecoveryCommit(t *testing.T) {
	fixture := newDispatchFixture(t, StateActive)
	fixture.provider.err = fmt.Errorf("%w: %w", ErrExecutionSideEffectUncertain, dispatchStartDiagnostic{})
	result, err := fixture.dispatcher.Dispatch(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Manifest.State != StateRecoveryRequired || result.Job.State != jobs.StateRecoveryRequired ||
		result.DiagnosticCategory != "SOURCE_REJECTED" || result.DiagnosticElapsedMS != 321 {
		t.Fatalf("recovery diagnostic = %#v", result)
	}
	fixture.assertCalls(t, 1, 1, 0, 0)
}

func TestStageDispatcherRoutesExactlyOnePersistedStage(t *testing.T) {
	for _, test := range []struct {
		state     State
		wantStage Stage
		ready     bool
		pending   bool
	}{
		{StateAwaitingVisibility, StageVisibility, false, true},
		{StateAwaitingCanonical, StageCanonical, false, true},
		{StateAwaitingCanonical, StageCanonical, true, false},
	} {
		fixture := newDispatchFixture(t, test.state)
		fixture.canonical.ready = test.ready
		result, err := fixture.dispatcher.Dispatch(context.Background(), fixture.request)
		if err != nil {
			t.Fatalf("Dispatch(%s) error = %v", test.state, err)
		}
		if result.Stage != test.wantStage || result.Pending != test.pending {
			t.Fatalf("result = %#v", result)
		}
		if test.state == StateAwaitingVisibility {
			fixture.assertCalls(t, 0, 0, 1, 0)
		} else {
			fixture.assertCalls(t, 0, 0, 0, 1)
		}
	}
}

func TestStageDispatcherTerminalReplayAndFences(t *testing.T) {
	ready := newDispatchFixture(t, StateReady)
	ready.jobs.job.State = jobs.StateSucceeded
	copyID := catalog.CopyID("copy-result")
	ready.manifests.manifest.ResultCopyID = &copyID
	ready.request.RetryAt = time.Time{}
	result, err := ready.dispatcher.Dispatch(context.Background(), ready.request)
	if err != nil || result.Stage != StageTerminal || !result.Terminal || result.Changed {
		t.Fatalf("READY replay = %#v, %v", result, err)
	}
	ready.assertCalls(t, 0, 0, 0, 0)
	for _, terminal := range []struct {
		manifest State
		job      jobs.State
		stage    Stage
	}{
		{StateFailed, jobs.StateFailed, StageTerminal},
		{StateCanceled, jobs.StateCanceled, StageTerminal},
		{StateRecoveryRequired, jobs.StateRecoveryRequired, StageRecovery},
	} {
		fixture := newDispatchFixture(t, terminal.manifest)
		fixture.jobs.job.State = terminal.job
		result, err := fixture.dispatcher.Dispatch(context.Background(), fixture.request)
		if err != nil || result.Stage != terminal.stage || !result.Terminal || result.Changed {
			t.Fatalf("terminal %s replay = %#v, %v", terminal.manifest, result, err)
		}
		fixture.assertCalls(t, 0, 0, 0, 0)
	}

	for _, test := range []struct {
		name   string
		mutate func(*dispatchFixture)
	}{
		{"stale generation", func(f *dispatchFixture) { f.request.ExpectedClaim-- }},
		{"wrong owner", func(f *dispatchFixture) { f.request.Owner = "other" }},
		{"wrong Job type", func(f *dispatchFixture) { f.jobs.job.Type = "OTHER" }},
		{"reverse link", func(f *dispatchFixture) { other := jobs.JobID("other"); f.manifests.manifest.JobID = &other }},
		{"pending Manifest", func(f *dispatchFixture) { f.manifests.manifest.State = StatePending }},
		{"contradictory Job", func(f *dispatchFixture) { f.jobs.job.State = jobs.StateSucceeded }},
		{"unknown provider outcome", func(f *dispatchFixture) { f.provider.result.Outcome = "UNKNOWN" }},
		{"ordinary provider error", func(f *dispatchFixture) { f.provider.err = errors.New("provider unavailable") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDispatchFixture(t, StateActive)
			test.mutate(fixture)
			if _, err := fixture.dispatcher.Dispatch(context.Background(), fixture.request); err == nil {
				t.Fatal("Dispatch() succeeded for invalid state")
			}
			if test.name == "unknown provider outcome" || test.name == "ordinary provider error" {
				fixture.assertCalls(t, 1, 0, 0, 0)
			} else {
				fixture.assertCalls(t, 0, 0, 0, 0)
			}
		})
	}
}
