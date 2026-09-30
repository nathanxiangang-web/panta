package app

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

var ErrInvalidRunOnce = errors.New("invalid acquisition RunOnce configuration or request")

type RunOnceState string

const (
	RunOnceIdle           RunOnceState = "IDLE"
	RunOnceRecoveryOnly   RunOnceState = "RECOVERY_ONLY"
	RunOnceStageCompleted RunOnceState = "STAGE_COMPLETED"
)

type RunOnceErrorKind string

const (
	RunOnceRecoveryDebt  RunOnceErrorKind = "RECOVERY_DEBT"
	RunOnceRecoveryError RunOnceErrorKind = "RECOVERY_ERROR"
	RunOnceClaimError    RunOnceErrorKind = "CLAIM_ERROR"
	RunOnceLeaseRace     RunOnceErrorKind = "LEASE_RACE"
	RunOnceStageError    RunOnceErrorKind = "STAGE_ERROR"
	RunOnceCanceled      RunOnceErrorKind = "CANCELED"
)

// RunOnceError preserves the original cause and the actual claimed generation
// without guessing a Job-only repair. A zero JobID means no claim was made.
type RunOnceError struct {
	Kind          RunOnceErrorKind
	JobID         jobs.JobID
	ClaimAttempts int
	Cause         error
}

func (err *RunOnceError) Error() string {
	if err.JobID != "" {
		return fmt.Sprintf("acquisition RunOnce %s for Job %s claim %d",
			err.Kind, err.JobID, err.ClaimAttempts)
	}
	return fmt.Sprintf("acquisition RunOnce %s", err.Kind)
}

func (err *RunOnceError) Unwrap() error { return err.Cause }

// RunOnceLimits is explicit deployment-independent policy. The caller must
// choose a lease range that covers its bounded provider/observation operations;
// this Gate does not invent a process cadence or start a timer.
type RunOnceLimits struct {
	MinLeaseDuration      time.Duration
	MaxLeaseDuration      time.Duration
	MaxRecoveryLimit      int
	MaxProjectorPageLimit int
}

type RunOnceRequest struct {
	Owner              string
	Now                time.Time
	LeaseDuration      time.Duration
	RetryAt            time.Time
	ProjectorPageLimit int
	RecoveryLimit      int
}

type RunOnceResult struct {
	State         RunOnceState
	Recovered     int64
	ClaimedJobID  jobs.JobID
	ClaimAttempts int
	Stage         *acquisition.StageDispatchResult
}

// AcquisitionClaimStore is narrower than the generic Job Engine repository.
// The runner has no ClaimNext, Fail, Succeed or RetryAt capability.
type AcquisitionClaimStore interface {
	ClaimNextByType(context.Context, string, jobs.ClaimRequest) (jobs.Job, error)
}

type AcquisitionStageDispatch interface {
	Dispatch(context.Context, acquisition.StageDispatchRequest) (acquisition.StageDispatchResult, error)
}

type AcquisitionRunOnce struct {
	recovery acquisition.ExpiredLeaseRecoveryStore
	claims   AcquisitionClaimStore
	dispatch AcquisitionStageDispatch
	limits   RunOnceLimits
}

func NewAcquisitionRunOnce(
	recovery acquisition.ExpiredLeaseRecoveryStore,
	claims AcquisitionClaimStore,
	dispatch AcquisitionStageDispatch,
	limits RunOnceLimits,
) (*AcquisitionRunOnce, error) {
	if recovery == nil || claims == nil || dispatch == nil ||
		limits.MinLeaseDuration <= 0 || limits.MaxLeaseDuration < limits.MinLeaseDuration ||
		limits.MaxRecoveryLimit < 1 || limits.MaxRecoveryLimit > acquisition.MaxExpiredLeaseRecoveryBatch ||
		limits.MaxProjectorPageLimit < 1 || limits.MaxProjectorPageLimit > acquisition.MaxCanonicalProjectorLimit {
		return nil, ErrInvalidRunOnce
	}
	return &AcquisitionRunOnce{recovery: recovery, claims: claims, dispatch: dispatch, limits: limits}, nil
}

// RunOnce performs one explicit tick: recover, claim once, dispatch once.
// Stage services own every durable handoff; this runner never changes a Job by
// itself after a stage error and never starts a background worker.
func (runner *AcquisitionRunOnce) RunOnce(ctx context.Context, request RunOnceRequest) (RunOnceResult, error) {
	if err := ctx.Err(); err != nil {
		return RunOnceResult{}, &RunOnceError{Kind: RunOnceCanceled, Cause: err}
	}
	if strings.TrimSpace(request.Owner) == "" || request.Owner != strings.TrimSpace(request.Owner) ||
		request.Now.IsZero() || request.RetryAt.IsZero() || !request.RetryAt.After(request.Now) ||
		request.LeaseDuration < runner.limits.MinLeaseDuration || request.LeaseDuration > runner.limits.MaxLeaseDuration ||
		request.RecoveryLimit < 1 || request.RecoveryLimit > runner.limits.MaxRecoveryLimit ||
		request.ProjectorPageLimit < 1 || request.ProjectorPageLimit > runner.limits.MaxProjectorPageLimit {
		return RunOnceResult{}, ErrInvalidRunOnce
	}
	result := RunOnceResult{}
	recovered, err := runner.recovery.MarkExpiredAcquisitionRecoveryRequired(ctx,
		jobs.RecoveryRequest{Now: request.Now, Limit: request.RecoveryLimit})
	result.Recovered = recovered
	if err != nil {
		kind := RunOnceRecoveryError
		if errors.Is(err, acquisition.ErrLeaseRecoveryDebt) {
			kind = RunOnceRecoveryDebt
		}
		if ctx.Err() != nil {
			kind = RunOnceCanceled
		}
		return result, &RunOnceError{Kind: kind, Cause: err}
	}
	if err := ctx.Err(); err != nil {
		return result, &RunOnceError{Kind: RunOnceCanceled, Cause: err}
	}
	claimed, err := runner.claims.ClaimNextByType(ctx, acquisition.JobTypeAcquisition, jobs.ClaimRequest{
		Owner: request.Owner, Now: request.Now, LeaseDuration: request.LeaseDuration,
	})
	if errors.Is(err, jobs.ErrNoClaimableJob) {
		if ctx.Err() != nil {
			return result, &RunOnceError{Kind: RunOnceCanceled, Cause: ctx.Err()}
		}
		result.State = RunOnceIdle
		if recovered > 0 {
			result.State = RunOnceRecoveryOnly
		}
		return result, nil
	}
	if err != nil {
		kind := RunOnceClaimError
		if errors.Is(err, jobs.ErrLeaseConflict) {
			kind = RunOnceLeaseRace
		}
		if ctx.Err() != nil {
			kind = RunOnceCanceled
		}
		return result, &RunOnceError{Kind: kind, Cause: err}
	}
	result.ClaimedJobID = claimed.ID
	result.ClaimAttempts = claimed.ClaimAttempts
	if claimed.ID == "" || claimed.Type != acquisition.JobTypeAcquisition || claimed.State != jobs.StateRunning ||
		claimed.LeaseOwner == nil || *claimed.LeaseOwner != request.Owner || claimed.ClaimAttempts < 1 ||
		claimed.LeaseExpiresAt == nil {
		return result, &RunOnceError{Kind: RunOnceClaimError, JobID: claimed.ID,
			ClaimAttempts: claimed.ClaimAttempts, Cause: errors.New("claimed Job identity or lease is invalid")}
	}
	if err := ctx.Err(); err != nil {
		return result, &RunOnceError{Kind: RunOnceCanceled, JobID: claimed.ID,
			ClaimAttempts: claimed.ClaimAttempts, Cause: err}
	}
	stage, err := runner.dispatch.Dispatch(ctx, acquisition.StageDispatchRequest{
		JobID: claimed.ID, Owner: *claimed.LeaseOwner, ExpectedClaim: claimed.ClaimAttempts,
		Now: request.Now, RetryAt: request.RetryAt, ProjectorPageLimit: request.ProjectorPageLimit,
	})
	if err != nil || ctx.Err() != nil {
		kind := RunOnceStageError
		if isRunOnceLeaseRace(err) {
			kind = RunOnceLeaseRace
		}
		if ctx.Err() != nil {
			kind, err = RunOnceCanceled, ctx.Err()
		}
		return result, &RunOnceError{Kind: kind, JobID: claimed.ID,
			ClaimAttempts: claimed.ClaimAttempts, Cause: err}
	}
	if stage.Stage == "" || stage.Manifest.ID == "" || stage.Job.ID != claimed.ID || stage.Job.ClaimAttempts != claimed.ClaimAttempts ||
		stage.Manifest.JobID == nil || *stage.Manifest.JobID != claimed.ID {
		return result, &RunOnceError{Kind: RunOnceStageError, JobID: claimed.ID,
			ClaimAttempts: claimed.ClaimAttempts, Cause: errors.New("stage returned a mismatched durable pair")}
	}
	result.State = RunOnceStageCompleted
	result.Stage = &stage
	return result, nil
}

func isRunOnceLeaseRace(err error) bool {
	return errors.Is(err, jobs.ErrLeaseConflict) ||
		errors.Is(err, acquisition.ErrStageDispatchFence) ||
		errors.Is(err, acquisition.ErrProviderOutcomeFence) ||
		errors.Is(err, acquisition.ErrRefreshFence) ||
		errors.Is(err, acquisition.ErrCanonicalFence)
}
