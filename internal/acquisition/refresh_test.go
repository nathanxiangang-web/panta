package acquisition_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var refreshNow = time.Date(2026, 9, 30, 11, 0, 0, 0, time.UTC)

const (
	refreshManifestID = acquisition.ManifestID("f0000000-0000-4000-8000-000000000001")
	refreshJobID      = jobs.JobID("f0000000-0000-4000-8000-000000000002")
	refreshBindingID  = storage.BindingID("f0000000-0000-4000-8000-000000000003")
	refreshOwner      = "acquisition-worker-a"
	// Gate 3.9: the frozen direct-child identity every valid AWAITING_VISIBILITY
	// Manifest now carries.
	refreshExpectedName = "acquired-item.bin"
)

// hintPortDouble records exactly what the step submitted.
type hintPortDouble struct {
	mu        sync.Mutex
	calls     int
	requests  []acquisition.MutationHint
	err       error
	transform func(acquisition.MutationHint) acquisition.MutationHintReceipt
}

func (port *hintPortDouble) SubmitMutationHint(_ context.Context, hint acquisition.MutationHint) (acquisition.MutationHintReceipt, error) {
	port.mu.Lock()
	defer port.mu.Unlock()
	port.calls++
	port.requests = append(port.requests, hint)
	if port.err != nil {
		return acquisition.MutationHintReceipt{}, port.err
	}
	if port.transform != nil {
		return port.transform(hint), nil
	}
	return acquisition.MutationHintReceipt{
		Status: "accepted", RootID: hint.RootID, ScopeKey: hint.ScopeKey,
		WorkState: "PENDING", SignalSeq: 1,
	}, nil
}

func (port *hintPortDouble) snapshot() (int, []acquisition.MutationHint) {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.calls, append([]acquisition.MutationHint(nil), port.requests...)
}

// refreshBindingReaderDouble is the narrow storage read double for the Hint step.
type refreshBindingReaderDouble struct {
	values map[storage.BindingID]storage.Binding
	calls  int
}

func (reader *refreshBindingReaderDouble) GetBinding(_ context.Context, id storage.BindingID) (storage.Binding, error) {
	reader.calls++
	binding, exists := reader.values[id]
	if !exists {
		return storage.Binding{}, storage.ErrNotFound
	}
	return binding, nil
}

// refreshStoreDouble emulates the atomic durable handoff: it only mutates its
// records after the plan is accepted, so a failure leaves both rows untouched.
type refreshStoreDouble struct {
	mu        sync.Mutex
	manifests map[acquisition.ManifestID]acquisition.Manifest
	jobs      map[jobs.JobID]jobs.Job
	plans     []acquisition.RefreshPlan
	err       error
}

func (store *refreshStoreDouble) CommitRefresh(_ context.Context, plan acquisition.RefreshPlan) (acquisition.RefreshResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.plans = append(store.plans, plan)
	if store.err != nil {
		return acquisition.RefreshResult{}, store.err
	}
	manifest, exists := store.manifests[plan.ManifestID]
	if !exists {
		return acquisition.RefreshResult{}, acquisition.ErrRefreshManifestNotFound
	}
	if manifest.State == plan.ManifestState {
		job, jobExists := store.jobs[plan.JobID]
		if jobExists && job.State == plan.JobState {
			return acquisition.RefreshResult{Manifest: manifest, Job: job, Changed: false}, nil
		}
	}
	job := store.jobs[plan.JobID]
	if job.State != jobs.StateRunning {
		return acquisition.RefreshResult{}, fmt.Errorf("%w: job state", acquisition.ErrRefreshConflict)
	}
	manifest.State = plan.ManifestState
	manifest.UpdatedAt = plan.Now
	job.State = plan.JobState
	job.UpdatedAt = plan.Now
	store.manifests[plan.ManifestID] = manifest
	store.jobs[plan.JobID] = job
	return acquisition.RefreshResult{Manifest: manifest, Job: job, Changed: true}, nil
}

func (store *refreshStoreDouble) snapshots() (acquisition.Manifest, jobs.Job, int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.manifests[refreshManifestID], store.jobs[refreshJobID], len(store.plans)
}

type refreshFixture struct {
	step  *acquisition.RefreshStep
	port  *hintPortDouble
	store *refreshStoreDouble
	// manifests and jobs are the same maps the store mutates, so assertions read
	// durable state.
	manifests map[acquisition.ManifestID]acquisition.Manifest
	jobs      map[jobs.JobID]jobs.Job
}

func newRefreshFixture(t *testing.T, options ...func(*storage.Binding, *acquisition.Manifest, *jobs.Job)) *refreshFixture {
	t.Helper()
	scope := "provider-scope-must-not-leak"
	binding := storage.Binding{
		ID: refreshBindingID, ConnectionID: executionConnID,
		ProviderScope:     &scope,
		OpenListMountPath: "/115",
		IndexCoreRootID:   "root-115-a",
		Status:            storage.BindingStatusActive,
		CreatedAt:         refreshNow, UpdatedAt: refreshNow,
	}
	linkedJob := refreshJobID
	expectedName := refreshExpectedName
	manifest := acquisition.Manifest{
		ID: refreshManifestID, SourceType: "opaque-source", SourceRef: "opaque-ref",
		TargetStorageBindingID: refreshBindingID, TargetPath: "/downloads/movies",
		ExpectedName: &expectedName,
		JobID:        &linkedJob,
		State:        acquisition.StateAwaitingVisibility, CreatedAt: refreshNow, UpdatedAt: refreshNow,
	}
	leaseEnd := refreshNow.Add(time.Hour)
	startedAt := refreshNow
	key := acquisition.AcquisitionJobIdempotencyKey(refreshManifestID)
	job := jobs.Job{
		ID: refreshJobID, Type: acquisition.JobTypeAcquisition, State: jobs.StateRunning,
		Payload:        []byte(fmt.Sprintf(`{"manifest_id":%q,"schema_version":1}`, refreshManifestID)),
		IdempotencyKey: &key,
		ClaimAttempts:  3, FailureCount: 1, MaxAttempts: 5,
		LeaseOwner: stringPointer(refreshOwner), LeaseExpiresAt: &leaseEnd,
		CreatedAt: refreshNow, UpdatedAt: refreshNow, StartedAt: &startedAt,
	}
	for _, option := range options {
		option(&binding, &manifest, &job)
	}

	manifests := map[acquisition.ManifestID]acquisition.Manifest{refreshManifestID: manifest}
	jobsByID := map[jobs.JobID]jobs.Job{refreshJobID: job}
	store := &refreshStoreDouble{manifests: manifests, jobs: jobsByID}
	port := &hintPortDouble{}
	step, err := acquisition.NewRefreshStep(
		&manifestReaderDouble{values: manifests},
		&jobReaderDouble{values: jobsByID},
		&refreshBindingReaderDouble{values: map[storage.BindingID]storage.Binding{refreshBindingID: binding}},
		port, store,
	)
	if err != nil {
		t.Fatalf("NewRefreshStep() error = %v", err)
	}
	return &refreshFixture{step: step, port: port, store: store, manifests: manifests, jobs: jobsByID}
}

func (fixture *refreshFixture) request() acquisition.RefreshRequest {
	return acquisition.RefreshRequest{
		ManifestID: refreshManifestID, JobID: refreshJobID, Owner: refreshOwner,
		ExpectedClaim: 3, Now: refreshNow, RetryAt: refreshNow.Add(time.Minute),
	}
}

// --- 1-4: D-029 mapping ------------------------------------------------------

func TestRefreshHintMappingFollowsD029(t *testing.T) {
	fixture := newRefreshFixture(t)
	result, err := fixture.step.Submit(context.Background(), fixture.request())
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Submit() reported Changed=false for a fresh handoff")
	}

	calls, requests := fixture.port.snapshot()
	if calls != 1 {
		t.Fatalf("Hint calls = %d, want exactly 1", calls)
	}
	hint := requests[0]
	if hint.RootID != "root-115-a" {
		t.Fatalf("root_id = %q, want StorageBinding.indexcore_root_id", hint.RootID)
	}
	if hint.ScopeKey != "/downloads/movies" {
		t.Fatalf("scope_key = %q, want Manifest.target_path", hint.ScopeKey)
	}
	if hint.Reason != acquisition.MutationHintPossibleChange {
		t.Fatalf("reason = %q, want POSSIBLE_CHANGE", hint.Reason)
	}
}

func TestRefreshHintIgnoresOpenListMountAndProviderScope(t *testing.T) {
	fixture := newRefreshFixture(t, func(binding *storage.Binding, _ *acquisition.Manifest, _ *jobs.Job) {
		binding.OpenListMountPath = "/totally/different/mount"
		scope := "magnet:?xt=urn:btih:provider-scope"
		binding.ProviderScope = &scope
	})
	if _, err := fixture.step.Submit(context.Background(), fixture.request()); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	_, requests := fixture.port.snapshot()
	hint := requests[0]
	if hint.ScopeKey != "/downloads/movies" {
		t.Fatalf("scope_key = %q, want the target path, not an OpenList mount", hint.ScopeKey)
	}
	if strings.Contains(hint.ScopeKey, "totally") || strings.Contains(hint.RootID, "totally") {
		t.Fatal("the OpenList mount leaked into the Hint")
	}
	if strings.Contains(hint.ScopeKey, "magnet") || strings.Contains(hint.RootID, "magnet") {
		t.Fatal("provider_scope leaked into the Hint")
	}
}

// --- 12-14: preconditions ----------------------------------------------------

func TestRefreshRequiresAwaitingVisibilityManifest(t *testing.T) {
	for _, state := range []acquisition.State{
		acquisition.StatePending, acquisition.StateActive, acquisition.StateReady,
		acquisition.StateFailed, acquisition.StateCanceled, acquisition.StateRecoveryRequired,
		acquisition.StateAwaitingCanonical,
	} {
		t.Run(string(state), func(t *testing.T) {
			fixture := newRefreshFixture(t, func(_ *storage.Binding, manifest *acquisition.Manifest, _ *jobs.Job) {
				manifest.State = state
			})
			if _, err := fixture.step.Submit(context.Background(), fixture.request()); !errors.Is(err, acquisition.ErrRefreshManifestState) {
				t.Fatalf("Submit() error = %v, want ErrRefreshManifestState", err)
			}
			if calls, _ := fixture.port.snapshot(); calls != 0 {
				t.Fatalf("Hint calls = %d, want 0 for a wrong-state Manifest", calls)
			}
		})
	}
}

func TestRefreshRejectsBindingBeforeHint(t *testing.T) {
	t.Run("missing binding", func(t *testing.T) {
		fixture := newRefreshFixture(t)
		step, err := acquisition.NewRefreshStep(
			&manifestReaderDouble{values: fixture.manifests},
			&jobReaderDouble{values: fixture.jobs},
			&refreshBindingReaderDouble{values: map[storage.BindingID]storage.Binding{}},
			fixture.port, fixture.store,
		)
		if err != nil {
			t.Fatalf("NewRefreshStep() error = %v", err)
		}
		if _, err := step.Submit(context.Background(), fixture.request()); !errors.Is(err, acquisition.ErrRefreshBindingMissing) {
			t.Fatalf("Submit() error = %v, want ErrRefreshBindingMissing", err)
		}
		if calls, _ := fixture.port.snapshot(); calls != 0 {
			t.Fatalf("Hint calls = %d, want 0", calls)
		}
	})

	t.Run("disabled binding", func(t *testing.T) {
		fixture := newRefreshFixture(t, func(binding *storage.Binding, _ *acquisition.Manifest, _ *jobs.Job) {
			binding.Status = storage.BindingStatusDisabled
		})
		if _, err := fixture.step.Submit(context.Background(), fixture.request()); !errors.Is(err, acquisition.ErrRefreshBindingState) {
			t.Fatalf("Submit() error = %v, want ErrRefreshBindingState", err)
		}
		if calls, _ := fixture.port.snapshot(); calls != 0 {
			t.Fatalf("Hint calls = %d, want 0", calls)
		}
	})

	t.Run("binding identity mismatch", func(t *testing.T) {
		fixture := newRefreshFixture(t, func(binding *storage.Binding, _ *acquisition.Manifest, _ *jobs.Job) {
			binding.ID = storage.BindingID("f0000000-0000-4000-8000-0000000000ff")
		})
		if _, err := fixture.step.Submit(context.Background(), fixture.request()); !errors.Is(err, acquisition.ErrStorageTopologyMismatch) {
			t.Fatalf("Submit() error = %v, want ErrStorageTopologyMismatch", err)
		}
		if calls, _ := fixture.port.snapshot(); calls != 0 {
			t.Fatalf("Hint calls = %d, want 0", calls)
		}
	})
}

func TestRefreshRejectsInvalidIndexCoreRootBeforeHint(t *testing.T) {
	for _, rootID := range []string{"", "   ", "\t", " padded "} {
		t.Run(fmt.Sprintf("%q", rootID), func(t *testing.T) {
			fixture := newRefreshFixture(t, func(binding *storage.Binding, _ *acquisition.Manifest, _ *jobs.Job) {
				binding.IndexCoreRootID = rootID
			})
			if _, err := fixture.step.Submit(context.Background(), fixture.request()); !errors.Is(err, acquisition.ErrRefreshRootID) {
				t.Fatalf("Submit() error = %v, want ErrRefreshRootID", err)
			}
			if calls, _ := fixture.port.snapshot(); calls != 0 {
				t.Fatalf("Hint calls = %d, want 0 for a blank root", calls)
			}
		})
	}
}

func TestRefreshRejectsNonCanonicalTargetPathBeforeHint(t *testing.T) {
	for _, target := range []string{"", "relative", "/trailing/", "/a//b", "/a/./b", "/a/../b", `/a\b`} {
		t.Run(target, func(t *testing.T) {
			fixture := newRefreshFixture(t, func(_ *storage.Binding, manifest *acquisition.Manifest, _ *jobs.Job) {
				manifest.TargetPath = target
			})
			if _, err := fixture.step.Submit(context.Background(), fixture.request()); !errors.Is(err, acquisition.ErrRefreshScopeKey) {
				t.Fatalf("Submit() error = %v, want ErrRefreshScopeKey", err)
			}
			if calls, _ := fixture.port.snapshot(); calls != 0 {
				t.Fatalf("Hint calls = %d, want 0 for a non-canonical scope", calls)
			}
		})
	}
}

// --- 15-16: job linkage and fence -------------------------------------------

func TestRefreshJobLinkageAndFence(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(*acquisition.Manifest, *jobs.Job)
		request  func(acquisition.RefreshRequest) acquisition.RefreshRequest
		wantKind error
	}{
		{
			name: "wrong Job type",
			mutate: func(_ *acquisition.Manifest, job *jobs.Job) {
				job.Type = "CANONICAL_CONFIRMATION"
			},
			wantKind: acquisition.ErrRefreshJobMismatch,
		},
		{
			name: "Manifest linked to another Job",
			mutate: func(manifest *acquisition.Manifest, _ *jobs.Job) {
				other := jobs.JobID("f0000000-0000-4000-8000-0000000000ee")
				manifest.JobID = &other
			},
			wantKind: acquisition.ErrRefreshJobMismatch,
		},
		{
			name: "wrong owner",
			request: func(request acquisition.RefreshRequest) acquisition.RefreshRequest {
				request.Owner = "someone-else"
				return request
			},
			wantKind: acquisition.ErrRefreshFence,
		},
		{
			name: "stale claim generation",
			request: func(request acquisition.RefreshRequest) acquisition.RefreshRequest {
				request.ExpectedClaim = 2
				return request
			},
			wantKind: acquisition.ErrRefreshFence,
		},
		{
			name: "Job not RUNNING",
			mutate: func(_ *acquisition.Manifest, job *jobs.Job) {
				job.State = jobs.StateRetryWait
			},
			wantKind: acquisition.ErrRefreshFence,
		},
		{
			name: "no lease owner",
			mutate: func(_ *acquisition.Manifest, job *jobs.Job) {
				job.LeaseOwner = nil
			},
			wantKind: acquisition.ErrRefreshFence,
		},
		{
			name: "different Job ID requested",
			request: func(request acquisition.RefreshRequest) acquisition.RefreshRequest {
				request.JobID = jobs.JobID("f0000000-0000-4000-8000-0000000000ee")
				return request
			},
			wantKind: acquisition.ErrRefreshJobMismatch,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRefreshFixture(t, func(_ *storage.Binding, manifest *acquisition.Manifest, job *jobs.Job) {
				if test.mutate != nil {
					test.mutate(manifest, job)
				}
			})
			request := fixture.request()
			if test.request != nil {
				request = test.request(request)
			}
			if _, err := fixture.step.Submit(context.Background(), request); !errors.Is(err, test.wantKind) {
				t.Fatalf("Submit() error = %v, want %v", err, test.wantKind)
			}
			if calls, _ := fixture.port.snapshot(); calls != 0 {
				t.Fatalf("Hint calls = %d, want 0 when the fence does not match", calls)
			}
		})
	}
}

// --- 17-20: accepted handoff -------------------------------------------------

func TestRefreshAcceptedHintCommitsAtomicHandoff(t *testing.T) {
	fixture := newRefreshFixture(t)
	result, err := fixture.step.Submit(context.Background(), fixture.request())
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if result.Manifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("Manifest state = %q, want AWAITING_CANONICAL", result.Manifest.State)
	}
	if result.Job.State != jobs.StateRetryWait {
		t.Fatalf("Job state = %q, want RETRY_WAIT", result.Job.State)
	}
	if result.Manifest.State == acquisition.StateReady {
		t.Fatal("Hint acceptance marked the Manifest READY")
	}
	// 18: the same Job, never SUCCEEDED.
	if result.Job.ID != refreshJobID {
		t.Fatalf("Job ID = %s, want the same Job %s", result.Job.ID, refreshJobID)
	}
	if result.Job.State == jobs.StateSucceeded {
		t.Fatal("Hint acceptance marked the Job SUCCEEDED")
	}
	// 20: the claim generation and failure budget are preserved.
	if result.Job.ClaimAttempts != 3 {
		t.Fatalf("claim generation = %d, want 3 preserved", result.Job.ClaimAttempts)
	}
	if result.Job.FailureCount != 1 {
		t.Fatalf("failure count = %d, want 1 preserved", result.Job.FailureCount)
	}
	if result.Job.MaxAttempts != 5 {
		t.Fatalf("max attempts = %d, want 5 preserved", result.Job.MaxAttempts)
	}
	// The plan carried the accepted transition and the future-stage RetryAt.
	_, _, plans := fixture.store.snapshots()
	if plans != 1 {
		t.Fatalf("store calls = %d, want 1", plans)
	}
}

// --- 21: exact replay --------------------------------------------------------

func TestRefreshExactReplayDoesNotResendHint(t *testing.T) {
	fixture := newRefreshFixture(t)
	if _, err := fixture.step.Submit(context.Background(), fixture.request()); err != nil {
		t.Fatalf("first Submit() error = %v", err)
	}
	// The Manifest and Job maps are the store's durable records, so the fixture's
	// readers now observe the committed pair.
	replay, err := fixture.step.Submit(context.Background(), fixture.request())
	if err != nil {
		t.Fatalf("replay Submit() error = %v", err)
	}
	if replay.Changed {
		t.Fatal("replay reported Changed=true")
	}
	if replay.Manifest.State != acquisition.StateAwaitingCanonical || replay.Job.State != jobs.StateRetryWait {
		t.Fatalf("replay pair = %q/%q", replay.Manifest.State, replay.Job.State)
	}
	calls, _ := fixture.port.snapshot()
	if calls != 1 {
		t.Fatalf("Hint calls = %d, want no second Hint on an exact committed replay", calls)
	}
	_, _, plans := fixture.store.snapshots()
	if plans != 1 {
		t.Fatalf("store calls = %d, want no second commit on replay", plans)
	}
}

// --- 23: the at-least-once window -------------------------------------------

func TestRefreshCommitFailureKeepsAwaitingVisibilityAndRetryResendsHint(t *testing.T) {
	fixture := newRefreshFixture(t)
	fixture.store.err = errors.New("injected Panta commit failure")

	if _, err := fixture.step.Submit(context.Background(), fixture.request()); err == nil {
		t.Fatal("Submit() succeeded, want the injected commit failure")
	}
	// The Hint was accepted and sent once; Panta stayed AWAITING_VISIBILITY.
	if calls, _ := fixture.port.snapshot(); calls != 1 {
		t.Fatalf("Hint calls = %d, want exactly 1 before the commit failure", calls)
	}
	manifest, job, _ := fixture.store.snapshots()
	if manifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("Manifest state = %q, want AWAITING_VISIBILITY to remain", manifest.State)
	}
	if job.State != jobs.StateRunning {
		t.Fatalf("Job state = %q, want RUNNING to remain", job.State)
	}

	// A retry is allowed to resend the same Hint, and then commits.
	fixture.store.err = nil
	result, err := fixture.step.Submit(context.Background(), fixture.request())
	if err != nil {
		t.Fatalf("retry Submit() error = %v", err)
	}
	if !result.Changed || result.Manifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("retry result = %+v", result)
	}
	calls, requests := fixture.port.snapshot()
	if calls != 2 {
		t.Fatalf("Hint calls = %d, want the retry to resend the same Hint", calls)
	}
	if requests[0] != requests[1] {
		t.Fatalf("retry sent a different Hint: %+v vs %+v", requests[0], requests[1])
	}
}

// TestRefreshRepeatedHintsDoNotAuthorizeProviderOrOpenList proves the step has no
// provider or OpenList capability at all: its only collaborators are the Manifest,
// Job, Binding readers, the Hint port, and the store.
func TestRefreshRepeatedHintsDoNotAuthorizeProviderOrOpenList(t *testing.T) {
	fixture := newRefreshFixture(t)
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := fixture.step.Submit(context.Background(), fixture.request()); err != nil {
			t.Fatalf("attempt %d Submit() error = %v", attempt, err)
		}
	}
	calls, requests := fixture.port.snapshot()
	if calls != 1 {
		t.Fatalf("Hint calls = %d, want at most one once the handoff is committed", calls)
	}
	for _, hint := range requests {
		if hint.Reason != acquisition.MutationHintPossibleChange {
			t.Fatalf("reason = %q, want only POSSIBLE_CHANGE", hint.Reason)
		}
	}
	// Repeated receipts never advanced any canonical state.
	manifest, _, _ := fixture.store.snapshots()
	if manifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("Manifest state = %q", manifest.State)
	}
	if manifest.State == acquisition.StateReady {
		t.Fatal("repeated Hints reached READY")
	}
}

// --- hint rejection ----------------------------------------------------------

func TestRefreshHintRejectionFailsClosedWithoutCommit(t *testing.T) {
	tests := []struct {
		name     string
		port     *hintPortDouble
		wantKind error
	}{
		{
			name: "mismatched receipt identity",
			port: &hintPortDouble{transform: func(hint acquisition.MutationHint) acquisition.MutationHintReceipt {
				return acquisition.MutationHintReceipt{
					Status: "accepted", RootID: "root-other", ScopeKey: hint.ScopeKey, WorkState: "PENDING",
				}
			}},
			wantKind: acquisition.ErrRefreshHintRejected,
		},
		{
			name: "non-accepted status",
			port: &hintPortDouble{transform: func(hint acquisition.MutationHint) acquisition.MutationHintReceipt {
				return acquisition.MutationHintReceipt{
					Status: "queued", RootID: hint.RootID, ScopeKey: hint.ScopeKey, WorkState: "PENDING",
				}
			}},
			wantKind: acquisition.ErrRefreshHintRejected,
		},
		{
			name:     "remote backpressure",
			port:     &hintPortDouble{err: errors.New("indexcore hint: backpressure")},
			wantKind: nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRefreshFixture(t)
			step, err := acquisition.NewRefreshStep(
				&manifestReaderDouble{values: fixture.manifests},
				&jobReaderDouble{values: fixture.jobs},
				&refreshBindingReaderDouble{values: map[storage.BindingID]storage.Binding{
					refreshBindingID: buildActiveBinding(),
				}},
				test.port, fixture.store,
			)
			if err != nil {
				t.Fatalf("NewRefreshStep() error = %v", err)
			}
			_, err = step.Submit(context.Background(), fixture.request())
			if err == nil {
				t.Fatal("Submit() succeeded, want a rejection")
			}
			if test.wantKind != nil && !errors.Is(err, test.wantKind) {
				t.Fatalf("error = %v, want %v", err, test.wantKind)
			}
			// No durable handoff happened.
			manifest, job, plans := fixture.store.snapshots()
			if plans != 0 {
				t.Fatalf("store calls = %d, want 0 after a rejected Hint", plans)
			}
			if manifest.State != acquisition.StateAwaitingVisibility || job.State != jobs.StateRunning {
				t.Fatalf("durable pair changed: %q/%q", manifest.State, job.State)
			}
		})
	}
}

func buildActiveBinding() storage.Binding {
	return storage.Binding{
		ID: refreshBindingID, ConnectionID: executionConnID,
		OpenListMountPath: "/115", IndexCoreRootID: "root-115-a",
		Status: storage.BindingStatusActive, CreatedAt: refreshNow, UpdatedAt: refreshNow,
	}
}

func TestBuildRefreshPlanValidatesRequest(t *testing.T) {
	valid := acquisition.RefreshRequest{
		ManifestID: refreshManifestID, JobID: refreshJobID, Owner: refreshOwner,
		ExpectedClaim: 3, Now: refreshNow, RetryAt: refreshNow.Add(time.Minute),
	}
	reason, plan, err := acquisition.BuildRefreshPlan(valid)
	if err != nil {
		t.Fatalf("BuildRefreshPlan() error = %v", err)
	}
	if reason != acquisition.MutationHintPossibleChange {
		t.Fatalf("reason = %q", reason)
	}
	if plan.ManifestState != acquisition.StateAwaitingCanonical || plan.JobState != jobs.StateRetryWait {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.RetryAt.IsZero() || !plan.RetryAt.After(plan.Now) {
		t.Fatalf("plan RetryAt = %v, want after Now", plan.RetryAt)
	}

	invalid := []acquisition.RefreshRequest{
		{JobID: refreshJobID, Owner: refreshOwner, ExpectedClaim: 3, Now: refreshNow, RetryAt: refreshNow.Add(time.Minute)},
		{ManifestID: refreshManifestID, Owner: refreshOwner, ExpectedClaim: 3, Now: refreshNow, RetryAt: refreshNow.Add(time.Minute)},
		{ManifestID: refreshManifestID, JobID: refreshJobID, ExpectedClaim: 3, Now: refreshNow, RetryAt: refreshNow.Add(time.Minute)},
		{ManifestID: refreshManifestID, JobID: refreshJobID, Owner: "   ", ExpectedClaim: 3, Now: refreshNow, RetryAt: refreshNow.Add(time.Minute)},
		{ManifestID: refreshManifestID, JobID: refreshJobID, Owner: refreshOwner, ExpectedClaim: 0, Now: refreshNow, RetryAt: refreshNow.Add(time.Minute)},
		{ManifestID: refreshManifestID, JobID: refreshJobID, Owner: refreshOwner, ExpectedClaim: 3, RetryAt: refreshNow.Add(time.Minute)},
		{ManifestID: refreshManifestID, JobID: refreshJobID, Owner: refreshOwner, ExpectedClaim: 3, Now: refreshNow},
		// RetryAt must queue the future stage in the future.
		{ManifestID: refreshManifestID, JobID: refreshJobID, Owner: refreshOwner, ExpectedClaim: 3, Now: refreshNow, RetryAt: refreshNow},
	}
	for _, request := range invalid {
		if _, _, err := acquisition.BuildRefreshPlan(request); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
			t.Fatalf("BuildRefreshPlan(%+v) error = %v, want ErrInvalidRefreshRequest", request, err)
		}
	}
}

func TestNewRefreshStepRequiresDependencies(t *testing.T) {
	fixture := newRefreshFixture(t)
	manifests := &manifestReaderDouble{values: fixture.manifests}
	jobs := &jobReaderDouble{values: fixture.jobs}
	bindings := &refreshBindingReaderDouble{values: map[storage.BindingID]storage.Binding{}}
	if _, err := acquisition.NewRefreshStep(nil, jobs, bindings, fixture.port, fixture.store); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("nil manifests error = %v", err)
	}
	if _, err := acquisition.NewRefreshStep(manifests, nil, bindings, fixture.port, fixture.store); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("nil jobs error = %v", err)
	}
	if _, err := acquisition.NewRefreshStep(manifests, jobs, nil, fixture.port, fixture.store); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("nil bindings error = %v", err)
	}
	if _, err := acquisition.NewRefreshStep(manifests, jobs, bindings, nil, fixture.store); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("nil hints error = %v", err)
	}
	if _, err := acquisition.NewRefreshStep(manifests, jobs, bindings, fixture.port, nil); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("nil store error = %v", err)
	}
}

func TestRefreshHonoursContextCancellation(t *testing.T) {
	fixture := newRefreshFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fixture.step.Submit(ctx, fixture.request()); err == nil {
		t.Fatal("Submit() succeeded, want cancellation")
	}
}

// --- Issue #42 (D-032): Gate 3.8 is scoped only by root and target_path ---------

// TestRefreshHintIgnoresAcquiredResultLocator proves the Mutation Hint does not
// depend on the acquired-result locator. D-032 keeps Gate 3.8 scoped only by
// indexcore_root_id + target_path: the Hint tells IndexCore to refresh this
// directory scope, and the locator is not an input to that handoff.
func TestRefreshHintIgnoresAcquiredResultLocator(t *testing.T) {
	tests := []struct {
		name       string
		resultName *string
	}{
		{name: "no locator yet", resultName: nil},
		{name: "valid locator", resultName: stringPointer("MiXeD Case & Unicode 影片.mkv")},
		{name: "intent-only, no locator", resultName: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRefreshFixture(t, func(_ *storage.Binding, manifest *acquisition.Manifest, _ *jobs.Job) {
				manifest.ResultName = test.resultName
			})
			result, err := fixture.step.Submit(context.Background(), fixture.request())
			if err != nil {
				t.Fatalf("Submit() error = %v, want the Hint to be unaffected by the locator", err)
			}
			if !result.Changed || result.Manifest.State != acquisition.StateAwaitingCanonical {
				t.Fatalf("result = %+v", result)
			}
			calls, requests := fixture.port.snapshot()
			if calls != 1 {
				t.Fatalf("Hint calls = %d, want exactly 1", calls)
			}
			hint := requests[0]
			if hint.RootID != "root-115-a" {
				t.Fatalf("root_id = %q, want the binding indexcore_root_id", hint.RootID)
			}
			if hint.ScopeKey != "/downloads/movies" {
				t.Fatalf("scope_key = %q, want Manifest.target_path", hint.ScopeKey)
			}
			if hint.Reason != acquisition.MutationHintPossibleChange {
				t.Fatalf("reason = %q, want POSSIBLE_CHANGE", hint.Reason)
			}
			// Neither the locator nor the intent may travel in the Hint.
			for _, leaked := range []string{"MiXeD", "影片", "acquired"} {
				if strings.Contains(hint.ScopeKey, leaked) || strings.Contains(hint.RootID, leaked) {
					t.Fatalf("%q leaked into the Hint", leaked)
				}
			}
		})
	}
}

// TestRefreshDoesNotRewriteExpectedNameOrResultName proves the Gate 3.8 handoff
// leaves both identity fields exactly as they were.
func TestRefreshDoesNotRewriteExpectedNameOrResultName(t *testing.T) {
	fixture := newRefreshFixture(t, func(_ *storage.Binding, manifest *acquisition.Manifest, _ *jobs.Job) {
		manifest.ExpectedName = stringPointer("request-intent.mkv")
		manifest.ResultName = stringPointer("observed-result.mkv")
	})
	if _, err := fixture.step.Submit(context.Background(), fixture.request()); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	manifest, _, _ := fixture.store.snapshots()
	if manifest.ExpectedName == nil || *manifest.ExpectedName != "request-intent.mkv" {
		t.Fatalf("expected_name = %v, want the request intent untouched", manifest.ExpectedName)
	}
	if manifest.ResultName == nil || *manifest.ResultName != "observed-result.mkv" {
		t.Fatalf("result_name = %v, want the locator untouched", manifest.ResultName)
	}
}
