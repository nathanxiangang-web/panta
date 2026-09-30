package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

// claimGenerationNow is the deterministic clock these tests drive.
var claimGenerationNow = time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)

func providerOutcomeInProgressRequest(fixture providerOutcomeFixture, generation int, now time.Time) acquisition.ProviderOutcomeRequest {
	request := acquisition.ProviderOutcomeRequest{
		ManifestID: fixture.manifestID, JobID: fixture.jobID, Owner: fixture.owner,
		ExpectedClaim: generation, Outcome: acquisition.ProviderOutcomeInProgress, Now: now,
	}
	retryAt := now
	request.RetryAt = &retryAt
	return request
}

func providerOutcomeSuccessRequest(fixture providerOutcomeFixture, generation int, now time.Time) acquisition.ProviderOutcomeRequest {
	request := acquisition.ProviderOutcomeRequest{
		ManifestID: fixture.manifestID, JobID: fixture.jobID, Owner: fixture.owner,
		ExpectedClaim: generation, Outcome: acquisition.ProviderOutcomeSucceeded, Now: now,
	}
	retryAt := now
	request.RetryAt = &retryAt
	// D-031: a provider success must supply the acquired direct-child identity when
	// the Manifest has not frozen one yet.
	resultName := "acquired-item.bin"
	request.ProviderResultName = &resultName
	return request
}

// --- Round 1 blocker 1: IN_PROGRESS replay -----------------------------------

// TestPostgresProviderInProgressReplayIsIdempotent shows a committed
// PROVIDER_IN_PROGRESS outcome replays with Changed=false and no timestamp
// rewrite. That outcome leaves the Manifest ACTIVE, so replay must be detected from
// the durable Manifest+Job pairing rather than from "Manifest is not ACTIVE".
func TestPostgresProviderInProgressReplayIsIdempotent(t *testing.T) {
	ctx, pool, outcomes, manifests, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "80", "worker-a", 1, time.Now().UTC().Add(time.Hour))

	first, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, 1, claimGenerationNow))
	if err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}
	if !first.Changed {
		t.Fatal("first Commit() reported Changed=false")
	}
	if first.Manifest.State != acquisition.StateActive || first.Job.State != jobs.StateRetryWait {
		t.Fatalf("first Commit() = %q/%q, want ACTIVE/RETRY_WAIT", first.Manifest.State, first.Job.State)
	}

	// A much later Now must not rewrite anything on replay.
	replay, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, 1, claimGenerationNow.Add(6*time.Hour)))
	if err != nil {
		t.Fatalf("replay Commit() error = %v", err)
	}
	if replay.Changed {
		t.Fatal("replay Commit() reported Changed=true")
	}
	if !replay.Manifest.UpdatedAt.Equal(first.Manifest.UpdatedAt) {
		t.Fatalf("replay rewrote Manifest updated_at: %v -> %v", first.Manifest.UpdatedAt, replay.Manifest.UpdatedAt)
	}
	if !replay.Job.UpdatedAt.Equal(first.Job.UpdatedAt) {
		t.Fatalf("replay rewrote Job updated_at: %v -> %v", first.Job.UpdatedAt, replay.Job.UpdatedAt)
	}

	durableManifest, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if durableManifest.State != acquisition.StateActive {
		t.Fatalf("durable Manifest state = %q, want ACTIVE", durableManifest.State)
	}
	if !durableManifest.UpdatedAt.Equal(first.Manifest.UpdatedAt) {
		t.Fatal("durable Manifest timestamp changed during replay")
	}
	durableJob, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	if durableJob.State != jobs.StateRetryWait || !durableJob.UpdatedAt.Equal(first.Job.UpdatedAt) {
		t.Fatalf("durable Job = %#v, want unchanged RETRY_WAIT", durableJob)
	}
}

// TestPostgresProviderInProgressReplayConvergesUnderConcurrency proves concurrent
// identical IN_PROGRESS replays all report Changed=false.
func TestPostgresProviderInProgressReplayConvergesUnderConcurrency(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "81", "worker-a", 1, time.Now().UTC().Add(time.Hour))

	if _, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, 1, claimGenerationNow)); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	const workers = 6
	changed := make([]bool, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	done := make(chan struct{}, workers)
	for worker := 0; worker < workers; worker++ {
		go func(index int) {
			defer func() { done <- struct{}{} }()
			<-start
			result, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, 1, claimGenerationNow))
			changed[index], errs[index] = result.Changed, err
		}(worker)
	}
	close(start)
	for worker := 0; worker < workers; worker++ {
		<-done
	}
	for index := 0; index < workers; index++ {
		if errs[index] != nil {
			t.Fatalf("worker %d error = %v", index, errs[index])
		}
		if changed[index] {
			t.Fatalf("worker %d reported Changed=true for a replay", index)
		}
	}
}

// --- Round 1 blocker 2: generation vs failure budget -------------------------

// TestPostgresStageTransitionsDoNotExhaustFailureBudget shows repeated
// IN_PROGRESS -> RETRY_WAIT -> Claim cycles never consume the failure budget and
// never strand the Job.
func TestPostgresStageTransitionsDoNotExhaustFailureBudget(t *testing.T) {
	const maxAttempts = 2
	ctx, pool, outcomes, _, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJobWithBudget(t, ctx, pool, bindingID, "82", "worker-a", 1, maxAttempts,
		time.Now().UTC().Add(24*time.Hour))

	// The seeded lease is generation 1 and RUNNING. A ClaimNext can never take a
	// RUNNING Job, so the first stage handoff moves it to RETRY_WAIT and every later
	// cycle then claims and hands back again.
	if _, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, 1, claimGenerationNow)); err != nil {
		t.Fatalf("initial handoff error = %v", err)
	}

	// Many more stage cycles than the entire failure budget.
	const cycles = 6
	for cycle := 0; cycle < cycles; cycle++ {
		now := claimGenerationNow.Add(time.Duration(cycle+1) * time.Minute)
		claimed, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{
			Owner: "worker-a", Now: now, LeaseDuration: time.Hour,
		})
		if err != nil {
			t.Fatalf("cycle %d ClaimNext() error = %v, want the Job to stay schedulable", cycle, err)
		}
		if claimed.ID != fixture.jobID {
			t.Fatalf("cycle %d claimed %s, want %s", cycle, claimed.ID, fixture.jobID)
		}
		if claimed.FailureCount != 0 {
			t.Fatalf("cycle %d failure count = %d, want 0: stage transitions must not consume the budget",
				cycle, claimed.FailureCount)
		}
		// The seed held generation 1, so the first cycle takes generation 2.
		if claimed.ClaimAttempts != cycle+2 {
			t.Fatalf("cycle %d claim generation = %d, want %d", cycle, claimed.ClaimAttempts, cycle+2)
		}

		result, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, claimed.ClaimAttempts, now))
		if err != nil {
			t.Fatalf("cycle %d Commit() error = %v", cycle, err)
		}
		if result.Job.State != jobs.StateRetryWait {
			t.Fatalf("cycle %d Job state = %q, want RETRY_WAIT", cycle, result.Job.State)
		}
		if result.Job.FailureCount != 0 {
			t.Fatalf("cycle %d failure count = %d, want 0", cycle, result.Job.FailureCount)
		}
		if result.Manifest.State != acquisition.StateActive {
			t.Fatalf("cycle %d Manifest state = %q, want ACTIVE", cycle, result.Manifest.State)
		}
	}

	durable, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	if durable.FailureCount != 0 {
		t.Fatalf("durable failure count = %d, want 0", durable.FailureCount)
	}
	if durable.ClaimAttempts != cycles+1 {
		t.Fatalf("durable claim generation = %d, want %d", durable.ClaimAttempts, cycles+1)
	}
	if durable.State != jobs.StateRetryWait {
		t.Fatalf("durable state = %q, want RETRY_WAIT and still schedulable", durable.State)
	}
}

// TestPostgresProviderSuccessReachesVisibilityAfterManyClaims proves a successful
// download still enters the visibility stage after more claims than the failure
// budget allows.
func TestPostgresProviderSuccessReachesVisibilityAfterManyClaims(t *testing.T) {
	const maxAttempts = 2
	ctx, pool, outcomes, _, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJobWithBudget(t, ctx, pool, bindingID, "83", "worker-a", 1, maxAttempts,
		time.Now().UTC().Add(24*time.Hour))

	// The seeded lease is generation 1 and RUNNING; move it to RETRY_WAIT first.
	if _, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, 1, claimGenerationNow)); err != nil {
		t.Fatalf("initial handoff error = %v", err)
	}

	// Poll well past the failure budget.
	const polls = 5
	for poll := 0; poll < polls; poll++ {
		now := claimGenerationNow.Add(time.Duration(poll+1) * time.Minute)
		claimed, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{
			Owner: "worker-a", Now: now, LeaseDuration: time.Hour,
		})
		if err != nil {
			t.Fatalf("poll %d ClaimNext() error = %v", poll, err)
		}
		if _, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, claimed.ClaimAttempts, now)); err != nil {
			t.Fatalf("poll %d Commit() error = %v", poll, err)
		}
	}

	// The download finally succeeds on a later claim.
	successAt := claimGenerationNow.Add(time.Hour)
	claimed, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{
		Owner: "worker-a", Now: successAt, LeaseDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("success ClaimNext() error = %v, want the Job to remain claimable", err)
	}
	success, err := service.Commit(ctx, providerOutcomeSuccessRequest(fixture, claimed.ClaimAttempts, successAt))
	if err != nil {
		t.Fatalf("provider success Commit() error = %v", err)
	}
	if success.Manifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("Manifest state = %q, want AWAITING_VISIBILITY", success.Manifest.State)
	}
	if success.Job.State != jobs.StateRetryWait {
		t.Fatalf("Job state = %q, want RETRY_WAIT", success.Job.State)
	}
	if success.Job.State == jobs.StateSucceeded {
		t.Fatal("provider success marked the Job SUCCEEDED")
	}
	if success.Manifest.State == acquisition.StateReady {
		t.Fatal("provider success marked the Manifest READY")
	}

	// The visibility stage enters by claiming the same Job at a later generation.
	visibility, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{
		Owner: "visibility-worker", Now: successAt.Add(time.Hour), LeaseDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("visibility ClaimNext() error = %v, want the visibility stage to enter", err)
	}
	if visibility.ID != fixture.jobID {
		t.Fatalf("visibility claimed %s, want the same Job %s", visibility.ID, fixture.jobID)
	}
	if visibility.FailureCount != 0 {
		t.Fatalf("failure count = %d, want 0 across the whole workflow", visibility.FailureCount)
	}
}

// TestPostgresFailureRetryStillHonoursMaxAttempts proves the failure budget was not
// weakened: only real failures consume it, and it still terminates the Job.
func TestPostgresFailureRetryStillHonoursMaxAttempts(t *testing.T) {
	const maxAttempts = 2
	ctx, pool, _, _, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	fixture := seedProviderOutcomeJobWithBudget(t, ctx, pool, bindingID, "84", "worker-a", 1, maxAttempts,
		time.Now().UTC().Add(24*time.Hour))

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		now := claimGenerationNow.Add(time.Duration(attempt) * time.Minute)

		// Attempt 1 uses the seeded RUNNING lease at generation 1. Later attempts
		// claim the RETRY_WAIT row the previous failure produced.
		var lease jobs.Job
		if attempt == 1 {
			current, err := jobRepository.Get(ctx, fixture.jobID)
			if err != nil {
				t.Fatalf("attempt 1 Get(job) error = %v", err)
			}
			lease = current
		} else {
			claimed, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{
				Owner: "worker-a", Now: now, LeaseDuration: time.Hour,
			})
			if err != nil {
				t.Fatalf("attempt %d ClaimNext() error = %v", attempt, err)
			}
			lease = claimed
		}
		if lease.ClaimAttempts != attempt {
			t.Fatalf("attempt %d lease generation = %d, want %d", attempt, lease.ClaimAttempts, attempt)
		}

		job, err := jobRepository.RetryAt(ctx, jobs.RetryRequest{
			FailureRequest: jobs.FailureRequest{
				LeaseRequest: jobs.LeaseRequest{
					ID: lease.ID, Owner: "worker-a", ExpectedClaim: lease.ClaimAttempts, Now: now,
				},
				Error: "provider stage failed",
			},
			RetryAt: now.Add(30 * time.Second),
		})
		if err != nil {
			t.Fatalf("attempt %d RetryAt() error = %v", attempt, err)
		}
		if job.FailureCount != attempt {
			t.Fatalf("attempt %d failure count = %d, want %d", attempt, job.FailureCount, attempt)
		}
		if attempt < maxAttempts {
			if job.State != jobs.StateRetryWait {
				t.Fatalf("attempt %d state = %q, want RETRY_WAIT", attempt, job.State)
			}
			continue
		}
		if job.State != jobs.StateFailed {
			t.Fatalf("attempt %d state = %q, want FAILED once the budget is spent", attempt, job.State)
		}
		if job.FinishedAt == nil || job.NextAttemptAt != nil {
			t.Fatalf("terminal failure must set finished_at and clear next_attempt_at: %#v", job)
		}
	}

	// Unlike a stage transition, an exhausted Job is no longer schedulable.
	if _, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{
		Owner: "worker-b", Now: claimGenerationNow.Add(24 * time.Hour), LeaseDuration: time.Hour,
	}); !errors.Is(err, jobs.ErrNoClaimableJob) {
		t.Fatalf("ClaimNext() on an exhausted Job error = %v, want ErrNoClaimableJob", err)
	}

	durable, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	if durable.ClaimAttempts != maxAttempts {
		t.Fatalf("claim generation = %d, want %d (one generation per real attempt)",
			durable.ClaimAttempts, maxAttempts)
	}
	if durable.FailureCount != maxAttempts {
		t.Fatalf("failure count = %d, want %d", durable.FailureCount, maxAttempts)
	}
}

// --- stale generation fencing -------------------------------------------------

// TestPostgresStaleClaimGenerationCannotCommitOutcome proves a SUPERSEDED claim
// generation cannot mutate durable state even when the owner string still matches.
func TestPostgresStaleClaimGenerationCannotCommitOutcome(t *testing.T) {
	ctx, pool, outcomes, manifests, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "85", "worker-a", 1, time.Now().UTC().Add(time.Hour))

	// Generation 1 hands the Job back to the next stage.
	first, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, 1, claimGenerationNow))
	if err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}
	if first.Job.ClaimAttempts != 1 {
		t.Fatalf("first claim generation = %d, want 1", first.Job.ClaimAttempts)
	}

	// A new claimant takes generation 2 under the same owner string, so only the
	// generation distinguishes the two workers.
	second, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{
		Owner: "worker-a", Now: claimGenerationNow.Add(time.Minute), LeaseDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("second ClaimNext() error = %v", err)
	}
	if second.ClaimAttempts != 2 {
		t.Fatalf("second claim generation = %d, want 2", second.ClaimAttempts)
	}

	// The generation-1 worker is stale.
	stale := providerOutcomeSuccessRequest(fixture, 1, claimGenerationNow.Add(2*time.Minute))
	if _, err := service.Commit(ctx, stale); !errors.Is(err, acquisition.ErrProviderOutcomeFence) {
		t.Fatalf("stale generation Commit() error = %v, want ErrProviderOutcomeFence", err)
	}

	durableJob, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	if durableJob.State != jobs.StateRunning || durableJob.ClaimAttempts != 2 {
		t.Fatalf("durable Job = %#v, want RUNNING at generation 2", durableJob)
	}
	durableManifest, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if durableManifest.State != acquisition.StateActive {
		t.Fatalf("durable Manifest state = %q, want ACTIVE after a stale rejection", durableManifest.State)
	}

	// The current generation still commits normally.
	current, err := service.Commit(ctx, providerOutcomeSuccessRequest(fixture, 2, claimGenerationNow.Add(3*time.Minute)))
	if err != nil {
		t.Fatalf("current generation Commit() error = %v", err)
	}
	if current.Manifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("Manifest state = %q, want AWAITING_VISIBILITY", current.Manifest.State)
	}
	if !current.Changed {
		t.Fatal("current generation reported Changed=false for a fresh outcome")
	}
}

// TestPostgresProviderOutcomeDoesNotChangeClaimGenerationOrFailureBudget pins that
// a handoff mutates only workflow state, never the fencing token or the budget.
func TestPostgresProviderOutcomeDoesNotChangeClaimGenerationOrFailureBudget(t *testing.T) {
	ctx, pool, outcomes, _, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJobWithBudget(t, ctx, pool, bindingID, "86", "worker-a", 3, 4,
		time.Now().UTC().Add(time.Hour))

	before, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	if _, err := service.Commit(ctx, providerOutcomeInProgressRequest(fixture, 3, claimGenerationNow)); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	after, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	if after.ClaimAttempts != before.ClaimAttempts {
		t.Fatalf("claim generation changed from %d to %d", before.ClaimAttempts, after.ClaimAttempts)
	}
	if after.FailureCount != before.FailureCount {
		t.Fatalf("failure count changed from %d to %d", before.FailureCount, after.FailureCount)
	}
	if after.MaxAttempts != before.MaxAttempts {
		t.Fatalf("max attempts changed from %d to %d", before.MaxAttempts, after.MaxAttempts)
	}
}
