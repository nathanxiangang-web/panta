package acquisition_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

var outcomeNow = time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)

// outcomeStore records the plan it was asked to commit so the service's
// validation and mapping can be asserted without a database.
type outcomeStore struct {
	plans  []acquisition.ProviderOutcomePlan
	result acquisition.ProviderOutcomeResult
	err    error
}

func (store *outcomeStore) CommitProviderOutcome(_ context.Context, plan acquisition.ProviderOutcomePlan) (acquisition.ProviderOutcomeResult, error) {
	store.plans = append(store.plans, plan)
	if store.err != nil {
		return acquisition.ProviderOutcomeResult{}, store.err
	}
	return store.result, nil
}

func newOutcomeService(t *testing.T) (*acquisition.ProviderOutcomeService, *outcomeStore) {
	t.Helper()
	store := &outcomeStore{}
	service, err := acquisition.NewProviderOutcomeService(store)
	if err != nil {
		t.Fatalf("NewProviderOutcomeService() error = %v", err)
	}
	return service, store
}

func retryAt() *time.Time {
	value := outcomeNow.Add(5 * time.Minute)
	return &value
}

func errorMessage() *string {
	value := "provider stage failed"
	return &value
}

func validOutcomeRequest(outcome acquisition.ProviderOutcome) acquisition.ProviderOutcomeRequest {
	request := acquisition.ProviderOutcomeRequest{
		ManifestID:    executionManifestID,
		JobID:         executionJobID,
		Owner:         executionOwner,
		ExpectedClaim: executionAttempt,
		Outcome:       outcome,
		Now:           outcomeNow,
	}
	transition, err := acquisition.ProviderOutcomeTransitionFor(outcome)
	if err != nil {
		panic(err)
	}
	if transition.RequiresRetryAt {
		request.RetryAt = retryAt()
	}
	if transition.RequiresError {
		request.ErrorMessage = errorMessage()
	}
	return request
}

// TestProviderOutcomeMappingIsFrozen pins the whole D-026 table.
func TestProviderOutcomeMappingIsFrozen(t *testing.T) {
	tests := []struct {
		outcome      acquisition.ProviderOutcome
		wantManifest acquisition.State
		wantJob      jobs.State
		wantRetryAt  bool
		wantError    bool
	}{
		{outcome: acquisition.ProviderOutcomeInProgress, wantManifest: acquisition.StateActive, wantJob: jobs.StateRetryWait, wantRetryAt: true},
		{outcome: acquisition.ProviderOutcomeSucceeded, wantManifest: acquisition.StateAwaitingVisibility, wantJob: jobs.StateRetryWait, wantRetryAt: true},
		{outcome: acquisition.ProviderOutcomeFailed, wantManifest: acquisition.StateFailed, wantJob: jobs.StateFailed, wantError: true},
		{outcome: acquisition.ProviderOutcomeCanceled, wantManifest: acquisition.StateCanceled, wantJob: jobs.StateCanceled},
		{outcome: acquisition.ProviderOutcomeRecovery, wantManifest: acquisition.StateRecoveryRequired, wantJob: jobs.StateRecoveryRequired, wantError: true},
	}
	for _, test := range tests {
		t.Run(string(test.outcome), func(t *testing.T) {
			transition, err := acquisition.ProviderOutcomeTransitionFor(test.outcome)
			if err != nil {
				t.Fatalf("ProviderOutcomeTransitionFor() error = %v", err)
			}
			if transition.ManifestState != test.wantManifest {
				t.Fatalf("manifest state = %q, want %q", transition.ManifestState, test.wantManifest)
			}
			if transition.JobState != test.wantJob {
				t.Fatalf("job state = %q, want %q", transition.JobState, test.wantJob)
			}
			if transition.RequiresRetryAt != test.wantRetryAt {
				t.Fatalf("RequiresRetryAt = %v, want %v", transition.RequiresRetryAt, test.wantRetryAt)
			}
			if transition.RequiresError != test.wantError {
				t.Fatalf("RequiresError = %v, want %v", transition.RequiresError, test.wantError)
			}
		})
	}
}

// TestProviderSuccessDoesNotCompleteTheJobOrManifest is the central D-026 rule:
// provider success queues the next stage instead of finishing the acquisition.
func TestProviderSuccessDoesNotCompleteTheJobOrManifest(t *testing.T) {
	transition, err := acquisition.ProviderOutcomeTransitionFor(acquisition.ProviderOutcomeSucceeded)
	if err != nil {
		t.Fatalf("ProviderOutcomeTransitionFor() error = %v", err)
	}
	if transition.JobState == jobs.StateSucceeded {
		t.Fatal("provider success must not mark the ACQUISITION Job SUCCEEDED")
	}
	if transition.ManifestState == acquisition.StateReady {
		t.Fatal("provider success must not mark the Manifest READY")
	}
	if transition.ManifestState != acquisition.StateAwaitingVisibility {
		t.Fatalf("manifest state = %q, want AWAITING_VISIBILITY", transition.ManifestState)
	}
	if transition.JobState != jobs.StateRetryWait {
		t.Fatalf("job state = %q, want RETRY_WAIT", transition.JobState)
	}
}

func TestUnknownProviderOutcomeRejected(t *testing.T) {
	if _, err := acquisition.ProviderOutcomeTransitionFor(acquisition.ProviderOutcome("PROVIDER_SOMETHING")); !errors.Is(err, acquisition.ErrInvalidProviderOutcome) {
		t.Fatalf("error = %v, want ErrInvalidProviderOutcome", err)
	}
}

func TestProviderOutcomeServiceValidatesRequests(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*acquisition.ProviderOutcomeRequest)
	}{
		{name: "empty Manifest", mutate: func(r *acquisition.ProviderOutcomeRequest) { r.ManifestID = "" }},
		{name: "empty Job", mutate: func(r *acquisition.ProviderOutcomeRequest) { r.JobID = "" }},
		{name: "empty owner", mutate: func(r *acquisition.ProviderOutcomeRequest) { r.Owner = "   " }},
		{name: "zero claim generation", mutate: func(r *acquisition.ProviderOutcomeRequest) { r.ExpectedClaim = 0 }},
		{name: "negative claim generation", mutate: func(r *acquisition.ProviderOutcomeRequest) { r.ExpectedClaim = -1 }},
		{name: "zero Now", mutate: func(r *acquisition.ProviderOutcomeRequest) { r.Now = time.Time{} }},
		{name: "unknown outcome", mutate: func(r *acquisition.ProviderOutcomeRequest) {
			r.Outcome = acquisition.ProviderOutcome("PROVIDER_UNKNOWN")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, store := newOutcomeService(t)
			request := validOutcomeRequest(acquisition.ProviderOutcomeSucceeded)
			test.mutate(&request)
			if _, err := service.Commit(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidProviderOutcome) {
				t.Fatalf("Commit() error = %v, want ErrInvalidProviderOutcome", err)
			}
			if len(store.plans) != 0 {
				t.Fatalf("store plans = %d, want 0 for a rejected request", len(store.plans))
			}
		})
	}
}

func TestProviderOutcomeServiceRequiresRetryAtAndErrorMessage(t *testing.T) {
	requiresRetryAt := []acquisition.ProviderOutcome{
		acquisition.ProviderOutcomeInProgress, acquisition.ProviderOutcomeSucceeded,
	}
	for _, outcome := range requiresRetryAt {
		t.Run(string(outcome)+" without RetryAt", func(t *testing.T) {
			service, store := newOutcomeService(t)
			request := validOutcomeRequest(outcome)
			request.RetryAt = nil
			if _, err := service.Commit(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidProviderOutcome) {
				t.Fatalf("Commit() error = %v, want ErrInvalidProviderOutcome", err)
			}
			request.RetryAt = &time.Time{}
			if _, err := service.Commit(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidProviderOutcome) {
				t.Fatalf("Commit(zero RetryAt) error = %v, want ErrInvalidProviderOutcome", err)
			}
			if len(store.plans) != 0 {
				t.Fatal("store received a plan for a missing RetryAt")
			}
		})
	}

	requiresError := []acquisition.ProviderOutcome{
		acquisition.ProviderOutcomeFailed, acquisition.ProviderOutcomeRecovery,
	}
	for _, outcome := range requiresError {
		t.Run(string(outcome)+" without ErrorMessage", func(t *testing.T) {
			service, store := newOutcomeService(t)
			request := validOutcomeRequest(outcome)
			request.ErrorMessage = nil
			if _, err := service.Commit(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidProviderOutcome) {
				t.Fatalf("Commit() error = %v, want ErrInvalidProviderOutcome", err)
			}
			blank := "   "
			request.ErrorMessage = &blank
			if _, err := service.Commit(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidProviderOutcome) {
				t.Fatalf("Commit(blank error) error = %v, want ErrInvalidProviderOutcome", err)
			}
			if len(store.plans) != 0 {
				t.Fatal("store received a plan for a missing ErrorMessage")
			}
		})
	}
}

func TestProviderOutcomeServiceBuildsExactPlan(t *testing.T) {
	service, store := newOutcomeService(t)
	request := validOutcomeRequest(acquisition.ProviderOutcomeSucceeded)
	// Extra padding is trimmed from the stored error text, and RetryAt is
	// normalized to UTC.
	message := "  provider said no  "
	request.ErrorMessage = &message
	request.RetryAt = retryAt()

	if _, err := service.Commit(context.Background(), request); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if len(store.plans) != 1 {
		t.Fatalf("store plans = %d, want 1", len(store.plans))
	}
	plan := store.plans[0]
	if plan.ManifestID != request.ManifestID || plan.JobID != request.JobID ||
		plan.Owner != request.Owner || plan.ExpectedClaim != request.ExpectedClaim ||
		plan.Outcome != request.Outcome {
		t.Fatalf("plan identity = %#v", plan)
	}
	if plan.ManifestState != acquisition.StateAwaitingVisibility || plan.JobState != jobs.StateRetryWait {
		t.Fatalf("plan states = %q/%q", plan.ManifestState, plan.JobState)
	}
	if plan.Now.Location() != time.UTC {
		t.Fatalf("plan Now location = %v, want UTC", plan.Now.Location())
	}
	if plan.RetryAt == nil || !plan.RetryAt.Equal(*request.RetryAt) {
		t.Fatalf("plan RetryAt = %v, want %v", plan.RetryAt, request.RetryAt)
	}
	if plan.ErrorMessage == nil || *plan.ErrorMessage != strings.TrimSpace(message) {
		t.Fatalf("plan ErrorMessage = %v, want trimmed %q", plan.ErrorMessage, strings.TrimSpace(message))
	}
}

func TestProviderOutcomeServicePropagatesStoreError(t *testing.T) {
	store := &outcomeStore{err: errors.New("injected commit failure")}
	service, err := acquisition.NewProviderOutcomeService(store)
	if err != nil {
		t.Fatalf("NewProviderOutcomeService() error = %v", err)
	}
	if _, err := service.Commit(context.Background(), validOutcomeRequest(acquisition.ProviderOutcomeFailed)); err == nil {
		t.Fatal("Commit() succeeded, want the store error")
	}
}

func TestNewProviderOutcomeServiceRequiresStore(t *testing.T) {
	if _, err := acquisition.NewProviderOutcomeService(nil); !errors.Is(err, acquisition.ErrInvalidProviderOutcome) {
		t.Fatalf("error = %v, want ErrInvalidProviderOutcome", err)
	}
}
