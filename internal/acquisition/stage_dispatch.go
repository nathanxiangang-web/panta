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
	ErrInvalidStageDispatch = errors.New("invalid acquisition stage dispatch request")
	ErrStageDispatchState   = errors.New("acquisition stage and Job state disagree")
	ErrStageDispatchFence   = errors.New("acquisition stage claim fence did not match")
)

// Stage identifies the one persisted milestone handled by an invocation.
type Stage string

const (
	StageProvider   Stage = "PROVIDER"
	StageVisibility Stage = "VISIBILITY"
	StageCanonical  Stage = "CANONICAL"
	StageTerminal   Stage = "TERMINAL"
	StageRecovery   Stage = "RECOVERY"
)

// StageDispatchRequest belongs to an already-claimed ACQUISITION Job. It never
// claims or schedules a Job. Now and RetryAt are policy timestamps; the existing
// stage stores use PostgreSQL time to authorize their durable lease mutations.
type StageDispatchRequest struct {
	JobID              jobs.JobID
	Owner              string
	ExpectedClaim      int
	Now                time.Time
	RetryAt            time.Time
	ProjectorPageLimit int
}

// StageDispatchResult reports the durable pair returned by exactly one stage.
type StageDispatchResult struct {
	Stage    Stage
	Manifest Manifest
	Job      jobs.Job
	Changed  bool
	Pending  bool
	Terminal bool
	// A closed, non-durable diagnostic from the one provider call. No raw
	// upstream error, source, response body or credential crosses this result.
	DiagnosticCategory      string
	DiagnosticElapsedMS     int64
	DiagnosticCauseType     string
	DiagnosticHTTPStatus    int
	DiagnosticStage         string
	DiagnosticResponseShape string
}

type ProviderStage interface {
	Execute(context.Context, StepRequest) (StepResult, error)
}

type ProviderOutcomeCommitter interface {
	Commit(context.Context, ProviderOutcomeRequest) (ProviderOutcomeResult, error)
}

type VisibilityStage interface {
	Submit(context.Context, RefreshRequest) (RefreshResult, error)
}

type CanonicalStage interface {
	Confirm(context.Context, CanonicalRequest) (CanonicalResult, error)
}

// StageDispatcher routes one claimed Job using only its persisted Manifest state.
// Stage services retain sole ownership of provider side effects, Hint submission,
// Copy projection, and database-time-fenced state changes.
type StageDispatcher struct {
	jobs       JobReader
	manifests  ManifestReader
	provider   ProviderStage
	outcomes   ProviderOutcomeCommitter
	visibility VisibilityStage
	canonical  CanonicalStage
}

func NewStageDispatcher(
	jobReader JobReader,
	manifestReader ManifestReader,
	provider ProviderStage,
	outcomes ProviderOutcomeCommitter,
	visibility VisibilityStage,
	canonical CanonicalStage,
) (*StageDispatcher, error) {
	if jobReader == nil || manifestReader == nil || provider == nil || outcomes == nil || visibility == nil || canonical == nil {
		return nil, ErrInvalidStageDispatch
	}
	return &StageDispatcher{jobs: jobReader, manifests: manifestReader, provider: provider,
		outcomes: outcomes, visibility: visibility, canonical: canonical}, nil
}

// Dispatch performs at most one stage and returns immediately after its handoff.
func (dispatcher *StageDispatcher) Dispatch(ctx context.Context, request StageDispatchRequest) (StageDispatchResult, error) {
	if err := ctx.Err(); err != nil {
		return StageDispatchResult{}, err
	}
	if request.JobID == "" || strings.TrimSpace(request.Owner) == "" || request.ExpectedClaim < 1 {
		return StageDispatchResult{}, ErrInvalidStageDispatch
	}
	job, err := dispatcher.jobs.Get(ctx, request.JobID)
	if err != nil {
		return StageDispatchResult{}, fmt.Errorf("load acquisition Job: %w", err)
	}
	if job.ID != request.JobID {
		return StageDispatchResult{}, fmt.Errorf("%w: Job identity", ErrStageDispatchState)
	}
	manifestID, err := linkedManifestID(job)
	if err != nil {
		return StageDispatchResult{}, fmt.Errorf("%w: %w", ErrStageDispatchState, err)
	}
	manifest, err := dispatcher.manifests.GetManifest(ctx, manifestID)
	if err != nil {
		return StageDispatchResult{}, fmt.Errorf("load linked Manifest: %w", err)
	}
	if manifest.ID != manifestID || manifest.JobID == nil || *manifest.JobID != job.ID {
		return StageDispatchResult{}, fmt.Errorf("%w: Manifest/Job reverse link", ErrStageDispatchState)
	}
	if err := ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return StageDispatchResult{}, fmt.Errorf("%w: %w", ErrStageDispatchState, err)
	}

	// Committed terminal pairs have no lease. They replay from durable facts and
	// never resolve a provider session, binding, or IndexCore resource.
	switch manifest.State {
	case StateReady, StateFailed, StateCanceled, StateRecoveryRequired:
		if !validTerminalStagePair(manifest, job) {
			return StageDispatchResult{}, fmt.Errorf("%w: terminal Manifest %s with Job %s", ErrStageDispatchState, manifest.State, job.State)
		}
		stage := StageTerminal
		if manifest.State == StateRecoveryRequired {
			stage = StageRecovery
		}
		return StageDispatchResult{Stage: stage, Manifest: manifest, Job: job, Terminal: true}, nil
	case StateActive, StateAwaitingVisibility, StateAwaitingCanonical:
		// Continue below under the current claimed generation.
	default:
		return StageDispatchResult{}, fmt.Errorf("%w: unsupported Manifest state %s", ErrStageDispatchState, manifest.State)
	}
	if job.State != jobs.StateRunning || job.LeaseOwner == nil || *job.LeaseOwner != request.Owner ||
		job.LeaseExpiresAt == nil || job.ClaimAttempts != request.ExpectedClaim {
		return StageDispatchResult{}, fmt.Errorf("%w: Job %s", ErrStageDispatchFence, job.ID)
	}
	if request.Now.IsZero() || request.RetryAt.IsZero() || !request.RetryAt.After(request.Now) {
		return StageDispatchResult{}, ErrInvalidStageDispatch
	}

	switch manifest.State {
	case StateActive:
		step, stepErr := dispatcher.provider.Execute(ctx, StepRequest{
			JobID: job.ID, Owner: request.Owner, ClaimAttempt: request.ExpectedClaim,
		})
		outcome, err := providerOutcomeForStep(step, stepErr)
		if err != nil {
			return StageDispatchResult{}, err
		}
		handoff := ProviderOutcomeRequest{
			ManifestID: manifest.ID, JobID: job.ID, Owner: request.Owner,
			ExpectedClaim: request.ExpectedClaim, Outcome: outcome, Now: request.Now,
		}
		if outcome == ProviderOutcomeInProgress || outcome == ProviderOutcomeSucceeded {
			retryAt := request.RetryAt
			handoff.RetryAt = &retryAt
		}
		if outcome == ProviderOutcomeSucceeded {
			handoff.ProviderResultName = step.ResultName
		}
		if outcome == ProviderOutcomeFailed || outcome == ProviderOutcomeRecovery {
			message := "provider stage failed"
			if outcome == ProviderOutcomeRecovery {
				message = "provider side effect uncertain; explicit recovery required"
			}
			handoff.ErrorMessage = &message
		}
		committed, err := dispatcher.outcomes.Commit(ctx, handoff)
		if err != nil {
			return StageDispatchResult{}, err
		}
		if err := verifyStageResult(manifest.ID, job.ID, committed.Manifest, committed.Job, outcome); err != nil {
			return StageDispatchResult{}, err
		}
		var category string
		var elapsedMS int64
		var causeType string
		var httpStatus int
		var parserStage, responseShape string
		var diagnostic interface {
			DiagnosticCategory() string
			DiagnosticElapsed() time.Duration
		}
		if errors.As(stepErr, &diagnostic) {
			category = diagnostic.DiagnosticCategory()
			elapsedMS = diagnostic.DiagnosticElapsed().Milliseconds()
		}
		var metadata interface {
			DiagnosticCauseType() string
			DiagnosticHTTPStatus() int
		}
		if errors.As(stepErr, &metadata) {
			causeType, httpStatus = metadata.DiagnosticCauseType(), metadata.DiagnosticHTTPStatus()
		}
		var boundary interface {
			DiagnosticStage() string
			DiagnosticResponseShape() string
		}
		if errors.As(stepErr, &boundary) {
			parserStage, responseShape = boundary.DiagnosticStage(), boundary.DiagnosticResponseShape()
		}
		return StageDispatchResult{Stage: StageProvider, Manifest: committed.Manifest, Job: committed.Job,
			Changed: committed.Changed, Pending: committed.Job.State == jobs.StateRetryWait,
			Terminal:           committed.Job.State != jobs.StateRetryWait,
			DiagnosticCategory: category, DiagnosticElapsedMS: elapsedMS,
			DiagnosticCauseType: causeType, DiagnosticHTTPStatus: httpStatus,
			DiagnosticStage: parserStage, DiagnosticResponseShape: responseShape}, nil
	case StateAwaitingVisibility:
		result, err := dispatcher.visibility.Submit(ctx, RefreshRequest{
			ManifestID: manifest.ID, JobID: job.ID, Owner: request.Owner,
			ExpectedClaim: request.ExpectedClaim, Now: request.Now, RetryAt: request.RetryAt,
		})
		if err != nil {
			return StageDispatchResult{}, err
		}
		if result.Manifest.ID != manifest.ID || result.Job.ID != job.ID ||
			result.Manifest.State != StateAwaitingCanonical || result.Job.State != jobs.StateRetryWait {
			return StageDispatchResult{}, fmt.Errorf("%w: visibility handoff result", ErrStageDispatchState)
		}
		return StageDispatchResult{Stage: StageVisibility, Manifest: result.Manifest, Job: result.Job,
			Changed: result.Changed, Pending: true}, nil
	case StateAwaitingCanonical:
		result, err := dispatcher.canonical.Confirm(ctx, CanonicalRequest{
			ManifestID: manifest.ID, JobID: job.ID, Owner: request.Owner,
			ExpectedClaim: request.ExpectedClaim, Now: request.Now, RetryAt: request.RetryAt,
			ProjectorPageLimit: request.ProjectorPageLimit,
		})
		if err != nil {
			return StageDispatchResult{}, err
		}
		if result.Manifest.ID != manifest.ID || result.Job.ID != job.ID {
			return StageDispatchResult{}, fmt.Errorf("%w: canonical handoff identity", ErrStageDispatchState)
		}
		switch {
		case result.Manifest.State == StateAwaitingCanonical && result.Job.State == jobs.StateRetryWait:
			return StageDispatchResult{Stage: StageCanonical, Manifest: result.Manifest, Job: result.Job,
				Changed: result.Changed, Pending: true}, nil
		case result.Manifest.State == StateReady && result.Job.State == jobs.StateSucceeded && result.Manifest.ResultCopyID != nil:
			return StageDispatchResult{Stage: StageCanonical, Manifest: result.Manifest, Job: result.Job,
				Changed: result.Changed, Terminal: true}, nil
		default:
			return StageDispatchResult{}, fmt.Errorf("%w: canonical handoff result", ErrStageDispatchState)
		}
	}
	return StageDispatchResult{}, ErrStageDispatchState
}

func providerOutcomeForStep(step StepResult, stepErr error) (ProviderOutcome, error) {
	if stepErr != nil {
		if errors.Is(stepErr, ErrExecutionSideEffectUncertain) {
			return ProviderOutcomeRecovery, nil
		}
		return "", stepErr
	}
	switch step.Outcome {
	case OutcomeProviderInProgress:
		return ProviderOutcomeInProgress, nil
	case OutcomeProviderSucceeded:
		return ProviderOutcomeSucceeded, nil
	case OutcomeProviderFailed:
		return ProviderOutcomeFailed, nil
	case OutcomeProviderCanceled:
		return ProviderOutcomeCanceled, nil
	default:
		return "", fmt.Errorf("%w: provider step outcome %q", ErrStageDispatchState, step.Outcome)
	}
}

func verifyStageResult(manifestID ManifestID, jobID jobs.JobID, manifest Manifest, job jobs.Job, outcome ProviderOutcome) error {
	transition, err := ProviderOutcomeTransitionFor(outcome)
	if err != nil || manifest.ID != manifestID || job.ID != jobID ||
		manifest.State != transition.ManifestState || job.State != transition.JobState {
		return fmt.Errorf("%w: provider handoff result", ErrStageDispatchState)
	}
	return nil
}

func validTerminalStagePair(manifest Manifest, job jobs.Job) bool {
	switch manifest.State {
	case StateReady:
		return job.State == jobs.StateSucceeded && manifest.ResultCopyID != nil && *manifest.ResultCopyID != ""
	case StateFailed:
		return job.State == jobs.StateFailed && manifest.ResultCopyID == nil
	case StateCanceled:
		return job.State == jobs.StateCanceled && manifest.ResultCopyID == nil
	case StateRecoveryRequired:
		return job.State == jobs.StateRecoveryRequired && manifest.ResultCopyID == nil
	default:
		return false
	}
}
