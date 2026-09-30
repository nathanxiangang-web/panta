package acquisition

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nathanxiangang-web/panta/internal/jobs"
)

var (
	ErrInvalidProviderOutcome       = errors.New("invalid acquisition provider outcome request")
	ErrProviderOutcomeManifestState = errors.New("acquisition Manifest state does not accept this provider outcome")
	ErrProviderOutcomeJobMismatch   = errors.New("acquisition provider outcome Job does not match the Manifest")
	ErrProviderOutcomeFence         = errors.New("acquisition provider outcome lease fence did not match")
	ErrProviderOutcomeConflict      = errors.New("acquisition provider outcome conflicts with the committed outcome")
	ErrProviderOutcomePersistence   = errors.New("acquisition provider outcome persistence failure")
)

// ProviderOutcome is the provider-stage result of one bounded execution step. It
// is not a Manifest milestone and not a Job state: the D-026 mapping below is the
// only accepted translation.
type ProviderOutcome string

const (
	ProviderOutcomeInProgress ProviderOutcome = "PROVIDER_IN_PROGRESS"
	ProviderOutcomeSucceeded  ProviderOutcome = "PROVIDER_SUCCEEDED"
	ProviderOutcomeFailed     ProviderOutcome = "PROVIDER_FAILED"
	ProviderOutcomeCanceled   ProviderOutcome = "PROVIDER_CANCELED"
	ProviderOutcomeRecovery   ProviderOutcome = "PROVIDER_RECOVERY_REQUIRED"
)

func (outcome ProviderOutcome) Valid() bool {
	switch outcome {
	case ProviderOutcomeInProgress, ProviderOutcomeSucceeded, ProviderOutcomeFailed,
		ProviderOutcomeCanceled, ProviderOutcomeRecovery:
		return true
	default:
		return false
	}
}

// ProviderOutcomeTransition is the frozen D-026 translation of one provider
// outcome into the Manifest milestone and Job state that must commit together.
type ProviderOutcomeTransition struct {
	// ManifestState is the milestone the Manifest must hold after the handoff.
	ManifestState State
	// JobState is the execution state the linked Job must hold after the handoff.
	JobState jobs.State
	// RequiresRetryAt is set for non-terminal handoffs that queue the next stage.
	RequiresRetryAt bool
	// RequiresError is set when the outcome must record why it did not progress.
	RequiresError bool
}

// ProviderOutcomeTransitionFor returns the frozen D-026 mapping.
//
// PROVIDER_SUCCEEDED is deliberately not Job SUCCEEDED and not Manifest READY: the
// ACQUISITION Job represents the whole acquisition workflow, so provider success
// queues the same Job for the later visibility stage instead of completing it.
func ProviderOutcomeTransitionFor(outcome ProviderOutcome) (ProviderOutcomeTransition, error) {
	switch outcome {
	case ProviderOutcomeInProgress:
		return ProviderOutcomeTransition{
			ManifestState: StateActive, JobState: jobs.StateRetryWait, RequiresRetryAt: true,
		}, nil
	case ProviderOutcomeSucceeded:
		return ProviderOutcomeTransition{
			ManifestState: StateAwaitingVisibility, JobState: jobs.StateRetryWait, RequiresRetryAt: true,
		}, nil
	case ProviderOutcomeFailed:
		return ProviderOutcomeTransition{
			ManifestState: StateFailed, JobState: jobs.StateFailed, RequiresError: true,
		}, nil
	case ProviderOutcomeCanceled:
		return ProviderOutcomeTransition{ManifestState: StateCanceled, JobState: jobs.StateCanceled}, nil
	case ProviderOutcomeRecovery:
		return ProviderOutcomeTransition{
			ManifestState: StateRecoveryRequired, JobState: jobs.StateRecoveryRequired, RequiresError: true,
		}, nil
	default:
		return ProviderOutcomeTransition{}, ErrInvalidProviderOutcome
	}
}

// ProviderOutcomeRequest commits one fenced provider-stage outcome.
//
// Owner and ExpectedClaim fence the currently RUNNING Job; lease validity is
// authorized by database time, consistent with the accepted Job Engine. RetryAt is
// required for the non-terminal handoffs and ErrorMessage for the outcomes that must
// record a cause.
type ProviderOutcomeRequest struct {
	ManifestID    ManifestID
	JobID         jobs.JobID
	Owner         string
	ExpectedClaim int
	Outcome       ProviderOutcome
	Now           time.Time
	RetryAt       *time.Time
	ErrorMessage  *string
	// ProviderResultName is the provider-neutral direct-child name the provider
	// reported for a successful step. It is set only for PROVIDER_SUCCEEDED, and it
	// is never trimmed or normalized.
	ProviderResultName *string
}

// ProviderOutcomeResult is the durable state after a handoff.
type ProviderOutcomeResult struct {
	Manifest Manifest
	Job      jobs.Job
	// Changed is false when the requested outcome was already committed and this
	// call was an exact replay. Replays never rewrite timestamps.
	Changed bool
}

// ProviderOutcomePlan is the complete provider-neutral persistence input. The
// store must commit the Manifest milestone and the Job state together or not at
// all.
type ProviderOutcomePlan struct {
	ManifestID    ManifestID
	JobID         jobs.JobID
	Owner         string
	ExpectedClaim int
	Outcome       ProviderOutcome
	ManifestState State
	JobState      jobs.State
	Now           time.Time
	RetryAt       *time.Time
	ErrorMessage  *string
	// ProviderResultName is the optional provider-reported direct-child name carried
	// into the D-031 expected-name resolution.
	ProviderResultName *string
}

// ProviderOutcomeStore commits the plan atomically.
type ProviderOutcomeStore interface {
	CommitProviderOutcome(context.Context, ProviderOutcomePlan) (ProviderOutcomeResult, error)
}

// ProviderOutcomeService validates one provider-stage handoff and asks the store
// to commit it atomically. It performs no provider, OpenList, or IndexCore call.
type ProviderOutcomeService struct {
	store ProviderOutcomeStore
}

func NewProviderOutcomeService(store ProviderOutcomeStore) (*ProviderOutcomeService, error) {
	if store == nil {
		return nil, ErrInvalidProviderOutcome
	}
	return &ProviderOutcomeService{store: store}, nil
}

// Commit validates the request against the frozen D-026 mapping and commits it.
func (service *ProviderOutcomeService) Commit(ctx context.Context, request ProviderOutcomeRequest) (ProviderOutcomeResult, error) {
	_, plan, err := BuildProviderOutcomePlan(request)
	if err != nil {
		return ProviderOutcomeResult{}, err
	}
	return service.store.CommitProviderOutcome(ctx, plan)
}

// BuildProviderOutcomePlan validates one request against the frozen D-026 mapping
// and produces its persistence plan. It is exported so tests can prove the
// PostgreSQL store rejects a tampered plan rather than trusting its caller.
func BuildProviderOutcomePlan(request ProviderOutcomeRequest) (ProviderOutcomeTransition, ProviderOutcomePlan, error) {
	transition, err := ProviderOutcomeTransitionFor(request.Outcome)
	if err != nil {
		return ProviderOutcomeTransition{}, ProviderOutcomePlan{}, err
	}
	if request.ManifestID == "" || request.JobID == "" || strings.TrimSpace(request.Owner) == "" ||
		request.ExpectedClaim < 1 || request.Now.IsZero() {
		return ProviderOutcomeTransition{}, ProviderOutcomePlan{}, ErrInvalidProviderOutcome
	}
	if transition.RequiresRetryAt {
		if request.RetryAt == nil || request.RetryAt.IsZero() {
			return ProviderOutcomeTransition{}, ProviderOutcomePlan{}, fmt.Errorf("%w: RetryAt is required for %s",
				ErrInvalidProviderOutcome, request.Outcome)
		}
	}
	if transition.RequiresError {
		if request.ErrorMessage == nil || strings.TrimSpace(*request.ErrorMessage) == "" {
			return ProviderOutcomeTransition{}, ProviderOutcomePlan{}, fmt.Errorf("%w: ErrorMessage is required for %s",
				ErrInvalidProviderOutcome, request.Outcome)
		}
	}
	// Only PROVIDER_SUCCEEDED may carry provider-reported result identity. Every other
	// outcome deliberately reports none rather than inventing or forwarding one.
	var providerResultName *string
	if request.Outcome == ProviderOutcomeSucceeded && request.ProviderResultName != nil {
		if err := ValidateExpectedName(*request.ProviderResultName); err != nil {
			return ProviderOutcomeTransition{}, ProviderOutcomePlan{}, fmt.Errorf(
				"%w: provider result name is not a valid direct child", ErrProviderResultName)
		}
		providerResultName = cloneVerbatim(request.ProviderResultName)
	}
	return transition, ProviderOutcomePlan{
		ManifestID:    request.ManifestID,
		JobID:         request.JobID,
		Owner:         request.Owner,
		ExpectedClaim: request.ExpectedClaim,
		Outcome:       request.Outcome,
		ManifestState: transition.ManifestState,
		JobState:      transition.JobState,
		Now:           request.Now.UTC(),
		RetryAt:       cloneTime(request.RetryAt),
		ErrorMessage:  cloneTrimmed(request.ErrorMessage),
		// The provider name is copied verbatim: only a success may carry it, and it
		// must survive to persistence exactly as reported.
		ProviderResultName: providerResultName,
	}, nil
}

// cloneVerbatim copies an optional provider-reported name without trimming,
// cleaning, or normalizing it. A different outcome must not smuggle identity, so a
// non-success outcome is normalized to nil.
func cloneVerbatim(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := value.UTC()
	return &cloned
}

func cloneTrimmed(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := strings.TrimSpace(*value)
	return &cloned
}
