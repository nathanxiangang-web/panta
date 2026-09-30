package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

type runOnceRecoveryDouble struct {
	calls  int
	count  int64
	err    error
	last   jobs.RecoveryRequest
	onCall func()
}

func (store *runOnceRecoveryDouble) MarkExpiredAcquisitionRecoveryRequired(_ context.Context, request jobs.RecoveryRequest) (int64, error) {
	store.calls++
	store.last = request
	if store.onCall != nil {
		store.onCall()
	}
	return store.count, store.err
}

type runOnceClaimDouble struct {
	calls   int
	jobType string
	last    jobs.ClaimRequest
	job     jobs.Job
	err     error
}

func (store *runOnceClaimDouble) ClaimNextByType(_ context.Context, jobType string, request jobs.ClaimRequest) (jobs.Job, error) {
	store.calls++
	store.jobType = jobType
	store.last = request
	return store.job, store.err
}

type runOnceDispatchDouble struct {
	calls  int
	last   acquisition.StageDispatchRequest
	result acquisition.StageDispatchResult
	err    error
	onCall func()
}

func (stage *runOnceDispatchDouble) Dispatch(_ context.Context, request acquisition.StageDispatchRequest) (acquisition.StageDispatchResult, error) {
	stage.calls++
	stage.last = request
	if stage.onCall != nil {
		stage.onCall()
	}
	return stage.result, stage.err
}

func runOnceFixture(t *testing.T) (*AcquisitionRunOnce, *runOnceRecoveryDouble, *runOnceClaimDouble, *runOnceDispatchDouble, RunOnceRequest) {
	t.Helper()
	owner := "worker-a"
	leaseEnd := time.Now().UTC().Add(time.Hour)
	jobID := jobs.JobID("50000000-0000-4000-8000-000000000001")
	manifestID := acquisition.ManifestID("50000000-0000-4000-8000-000000000002")
	job := jobs.Job{ID: jobID, Type: acquisition.JobTypeAcquisition, State: jobs.StateRunning,
		ClaimAttempts: 7, FailureCount: 3, LeaseOwner: &owner, LeaseExpiresAt: &leaseEnd}
	recovery := &runOnceRecoveryDouble{}
	claim := &runOnceClaimDouble{job: job}
	dispatch := &runOnceDispatchDouble{result: acquisition.StageDispatchResult{
		Stage: acquisition.StageProvider, Manifest: acquisition.Manifest{ID: manifestID, JobID: &jobID,
			State: acquisition.StateAwaitingVisibility},
		Job: job,
	}}
	runner, err := NewAcquisitionRunOnce(recovery, claim, dispatch, RunOnceLimits{
		MinLeaseDuration: time.Minute, MaxLeaseDuration: time.Hour,
		MaxRecoveryLimit: 10, MaxProjectorPageLimit: acquisition.MaxCanonicalProjectorLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	request := RunOnceRequest{Owner: owner, Now: now, LeaseDuration: 10 * time.Minute,
		RetryAt: now.Add(time.Minute), ProjectorPageLimit: 25, RecoveryLimit: 5}
	return runner, recovery, claim, dispatch, request
}

func TestAcquisitionRunOnceIdleAndRecoveryOnly(t *testing.T) {
	for _, count := range []int64{0, 2} {
		t.Run(time.Duration(count).String(), func(t *testing.T) {
			runner, recovery, claim, dispatch, request := runOnceFixture(t)
			recovery.count = count
			claim.err = jobs.ErrNoClaimableJob
			result, err := runner.RunOnce(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			want := RunOnceIdle
			if count > 0 {
				want = RunOnceRecoveryOnly
			}
			if result.State != want || result.Recovered != count || result.ClaimedJobID != "" || result.Stage != nil ||
				recovery.calls != 1 || claim.calls != 1 || dispatch.calls != 0 || claim.jobType != acquisition.JobTypeAcquisition {
				t.Fatalf("idle result = %#v; calls recovery/claim/dispatch %d/%d/%d", result, recovery.calls, claim.calls, dispatch.calls)
			}
		})
	}
}

func TestAcquisitionRunOnceForwardsActualClaimAndDispatchesOnce(t *testing.T) {
	runner, recovery, claim, dispatch, request := runOnceFixture(t)
	recovery.count = 2
	result, err := runner.RunOnce(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.State != RunOnceStageCompleted || result.Recovered != 2 || result.Stage == nil ||
		result.ClaimedJobID != claim.job.ID || result.ClaimAttempts != claim.job.ClaimAttempts ||
		recovery.calls != 1 || claim.calls != 1 || dispatch.calls != 1 ||
		recovery.last.Limit != request.RecoveryLimit || !recovery.last.Now.Equal(request.Now) ||
		claim.last.Owner != request.Owner || claim.last.LeaseDuration != request.LeaseDuration ||
		dispatch.last.JobID != claim.job.ID || dispatch.last.Owner != *claim.job.LeaseOwner ||
		dispatch.last.ExpectedClaim != claim.job.ClaimAttempts || dispatch.last.ExpectedClaim == claim.job.FailureCount ||
		dispatch.last.ProjectorPageLimit != request.ProjectorPageLimit {
		t.Fatalf("forwarding result = %#v; claim %#v; dispatch %#v", result, claim.last, dispatch.last)
	}
}

func TestAcquisitionRunOnceRecoveryDebtAndStoreFailureStopClaim(t *testing.T) {
	for _, test := range []struct {
		err  error
		want RunOnceErrorKind
	}{
		{acquisition.ErrLeaseRecoveryDebt, RunOnceRecoveryDebt},
		{errors.New("database unavailable"), RunOnceRecoveryError},
	} {
		runner, recovery, claim, dispatch, request := runOnceFixture(t)
		recovery.count, recovery.err = 2, test.err
		result, err := runner.RunOnce(context.Background(), request)
		var typed *RunOnceError
		if !errors.As(err, &typed) || typed.Kind != test.want || !errors.Is(err, test.err) ||
			result.Recovered != 2 || claim.calls != 0 || dispatch.calls != 0 {
			t.Fatalf("recovery error result = %#v, %v", result, err)
		}
	}
}

func TestAcquisitionRunOnceStageErrorNeverRepairsJobOnly(t *testing.T) {
	runner, _, claim, dispatch, request := runOnceFixture(t)
	stageErr := errors.New("stage handoff unavailable")
	dispatch.err = stageErr
	result, err := runner.RunOnce(context.Background(), request)
	var typed *RunOnceError
	if !errors.As(err, &typed) || typed.Kind != RunOnceStageError || !errors.Is(err, stageErr) ||
		typed.JobID != claim.job.ID || typed.ClaimAttempts != claim.job.ClaimAttempts ||
		result.Stage != nil || result.ClaimedJobID != claim.job.ID || dispatch.calls != 1 {
		t.Fatalf("stage error result = %#v, %v", result, err)
	}
}

func TestAcquisitionRunOnceLeaseRaceIsDistinct(t *testing.T) {
	runner, _, claim, dispatch, request := runOnceFixture(t)
	dispatch.err = acquisition.ErrStageDispatchFence
	_, err := runner.RunOnce(context.Background(), request)
	var typed *RunOnceError
	if !errors.As(err, &typed) || typed.Kind != RunOnceLeaseRace || typed.JobID != claim.job.ID ||
		!errors.Is(err, acquisition.ErrStageDispatchFence) {
		t.Fatalf("lease race = %v", err)
	}
}

func TestAcquisitionRunOnceRejectsInvalidClaimAndPolicy(t *testing.T) {
	runner, recovery, claim, dispatch, request := runOnceFixture(t)
	bad := request
	bad.LeaseDuration = 30 * time.Second
	if _, err := runner.RunOnce(context.Background(), bad); !errors.Is(err, ErrInvalidRunOnce) || recovery.calls != 0 {
		t.Fatalf("short lease = %v", err)
	}
	bad = request
	bad.RetryAt = bad.Now
	if _, err := runner.RunOnce(context.Background(), bad); !errors.Is(err, ErrInvalidRunOnce) || recovery.calls != 0 {
		t.Fatalf("past retry = %v", err)
	}
	bad = request
	bad.RecoveryLimit = 11
	if _, err := runner.RunOnce(context.Background(), bad); !errors.Is(err, ErrInvalidRunOnce) || recovery.calls != 0 {
		t.Fatalf("oversized recovery = %v", err)
	}
	claim.job.Type = "OTHER"
	result, err := runner.RunOnce(context.Background(), request)
	var typed *RunOnceError
	if !errors.As(err, &typed) || typed.Kind != RunOnceClaimError || dispatch.calls != 0 || result.ClaimedJobID != claim.job.ID {
		t.Fatalf("wrong-type claim result = %#v, %v", result, err)
	}
}

func TestAcquisitionRunOnceCancellationStopsBeforeClaim(t *testing.T) {
	runner, recovery, claim, dispatch, request := runOnceFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := runner.RunOnce(ctx, request)
	var typed *RunOnceError
	if !errors.As(err, &typed) || typed.Kind != RunOnceCanceled || !errors.Is(err, context.Canceled) ||
		result.State != "" || recovery.calls != 0 || claim.calls != 0 || dispatch.calls != 0 {
		t.Fatalf("cancelled result = %#v, %v", result, err)
	}
}

func TestAcquisitionRunOnceCancellationAfterRecoveryOrClaim(t *testing.T) {
	for _, phase := range []string{"recovery", "stage"} {
		t.Run(phase, func(t *testing.T) {
			runner, recovery, claim, dispatch, request := runOnceFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if phase == "recovery" {
				recovery.onCall = cancel
			} else {
				dispatch.onCall = cancel
			}
			result, err := runner.RunOnce(ctx, request)
			var typed *RunOnceError
			if !errors.As(err, &typed) || typed.Kind != RunOnceCanceled || !errors.Is(err, context.Canceled) ||
				result.State == RunOnceStageCompleted {
				t.Fatalf("cancelled tick = %#v, %v", result, err)
			}
			if phase == "recovery" && (claim.calls != 0 || dispatch.calls != 0) {
				t.Fatal("cancelled recovery continued to claim")
			}
		})
	}
}
