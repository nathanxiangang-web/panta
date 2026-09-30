package postgres

// Gate 3.8 trusted Mutation Hint handoff.
//
// RefreshRepository.CommitRefresh must commit the Acquisition Manifest milestone
// and the linked ACQUISITION Job stage as one PostgreSQL transaction, fenced by
// the claim generation, without consuming the failure budget. These tests drive
// the real store against real PostgreSQL and reuse this package's integration
// helpers (integrationPool, resetTestSchema, seedStorageBinding) exactly like the
// sibling *_integration_test.go files.

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

// --- fixture ------------------------------------------------------------------

type refreshFixture struct {
	manifestID      acquisition.ManifestID
	jobID           jobs.JobID
	owner           string
	claimGeneration int
	failureBudget   int
	maxAttempts     int
	manifestState   acquisition.State
	leaseEnd        time.Time
}

// seedRefreshHandoff inserts one Manifest in the given milestone linked to a
// fenced RUNNING ACQUISITION Job that satisfies
// acquisition.ValidateLinkedAcquisitionJob.
//
// claimGeneration is the fencing token (jobs.claim_attempts); failureBudget is
// jobs.attempt_count, the budget only a real same-stage failure may consume.
func seedRefreshHandoff(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	suffix string,
	manifestState acquisition.State,
	owner string,
	claimGeneration int,
	failureBudget int,
	maxAttempts int,
	leaseEnd time.Time,
) refreshFixture {
	t.Helper()
	seededAt := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	manifestID := acquisition.ManifestID("38000000-0000-4000-8000-0000000000" + suffix)
	jobID := jobs.JobID("38000000-0000-4000-8000-0000000001" + suffix)
	key := acquisition.AcquisitionJobIdempotencyKey(manifestID)

	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    claim_attempts, attempt_count, max_attempts, lease_owner, lease_expires_at,
    created_at, updated_at, started_at
) VALUES ($1, $2, $3, 'RUNNING', $4, $5, $6, $7, $8, $9, $10, $10, $10)`,
		string(jobID), acquisition.JobTypeAcquisition,
		[]byte(fmt.Sprintf(`{"manifest_id":"%s","schema_version":1}`, manifestID)),
		key, claimGeneration, failureBudget, maxAttempts, owner, leaseEnd, seededAt,
	); err != nil {
		t.Fatalf("seed RUNNING ACQUISITION job: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id, target_path,
    job_id, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, '/downloads/item', $3, $4, $5, $5)`,
		string(manifestID), string(bindingID), string(jobID), string(manifestState), seededAt,
	); err != nil {
		t.Fatalf("seed %s Manifest: %v", manifestState, err)
	}

	return refreshFixture{
		manifestID:      manifestID,
		jobID:           jobID,
		owner:           owner,
		claimGeneration: claimGeneration,
		failureBudget:   failureBudget,
		maxAttempts:     maxAttempts,
		manifestState:   manifestState,
		leaseEnd:        leaseEnd,
	}
}

func newRefreshIntegrationFixture(t *testing.T) (context.Context, *pgxpool.Pool, *RefreshRepository, *AcquisitionManifestRepository, *JobRepository, storage.BindingID) {
	t.Helper()
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 10 {
		t.Fatalf("Apply() = %#v, %v", status, err)
	}
	bindingID := storage.BindingID("38000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "refresh-root")

	repository, err := NewRefreshRepository(pool)
	if err != nil {
		t.Fatalf("NewRefreshRepository() error = %v", err)
	}
	manifests, err := NewAcquisitionManifestRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionManifestRepository() error = %v", err)
	}
	jobRepository, err := NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}
	return ctx, pool, repository, manifests, jobRepository, bindingID
}

func refreshPlanNow() time.Time {
	return time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
}

func refreshPlanFor(fixture refreshFixture, now time.Time) acquisition.RefreshPlan {
	return acquisition.RefreshPlan{
		ManifestID:    fixture.manifestID,
		JobID:         fixture.jobID,
		Owner:         fixture.owner,
		ExpectedClaim: fixture.claimGeneration,
		ManifestState: acquisition.StateAwaitingCanonical,
		JobState:      jobs.StateRetryWait,
		Now:           now,
		RetryAt:       now.Add(90 * time.Second),
	}
}

func refreshDurableState(
	t *testing.T,
	ctx context.Context,
	manifests *AcquisitionManifestRepository,
	jobRepository *JobRepository,
	fixture refreshFixture,
) (acquisition.Manifest, jobs.Job) {
	t.Helper()
	manifest, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	job, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	return manifest, job
}

func refreshLeaseOwner(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return *value
}

// assertRefreshNotMutated proves an error path wrote neither row.
func assertRefreshNotMutated(
	t *testing.T,
	ctx context.Context,
	manifests *AcquisitionManifestRepository,
	jobRepository *JobRepository,
	fixture refreshFixture,
	beforeManifest acquisition.Manifest,
	beforeJob jobs.Job,
) {
	t.Helper()
	afterManifest, afterJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
	if afterManifest.State != beforeManifest.State || !afterManifest.UpdatedAt.Equal(beforeManifest.UpdatedAt) {
		t.Fatalf("Manifest changed: state %q -> %q, updated_at %v -> %v",
			beforeManifest.State, afterManifest.State, beforeManifest.UpdatedAt, afterManifest.UpdatedAt)
	}
	if afterJob.State != beforeJob.State || !afterJob.UpdatedAt.Equal(beforeJob.UpdatedAt) ||
		afterJob.ClaimAttempts != beforeJob.ClaimAttempts || afterJob.FailureCount != beforeJob.FailureCount ||
		refreshLeaseOwner(afterJob.LeaseOwner) != refreshLeaseOwner(beforeJob.LeaseOwner) {
		t.Fatalf("Job changed: state %q -> %q, claim %d -> %d, failure %d -> %d, owner %s -> %s, updated_at %v -> %v",
			beforeJob.State, afterJob.State, beforeJob.ClaimAttempts, afterJob.ClaimAttempts,
			beforeJob.FailureCount, afterJob.FailureCount, refreshLeaseOwner(beforeJob.LeaseOwner),
			refreshLeaseOwner(afterJob.LeaseOwner), beforeJob.UpdatedAt, afterJob.UpdatedAt)
	}
}

// --- coverage 1 and 2: accepted fresh handoff ---------------------------------

func TestPostgresRefreshAcceptedHandoff(t *testing.T) {
	ctx, pool, repository, manifests, jobRepository, bindingID := newRefreshIntegrationFixture(t)
	fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "10", acquisition.StateAwaitingVisibility,
		"worker-a", 3, 1, 5, time.Now().UTC().Add(time.Hour))
	plan := refreshPlanFor(fixture, refreshPlanNow())

	result, err := repository.CommitRefresh(ctx, plan)
	if err != nil {
		t.Fatalf("CommitRefresh() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("CommitRefresh() reported Changed=false for a fresh handoff")
	}
	if result.Manifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("Manifest state = %q, want AWAITING_CANONICAL", result.Manifest.State)
	}
	if result.Job.State != jobs.StateRetryWait {
		t.Fatalf("Job state = %q, want RETRY_WAIT", result.Job.State)
	}

	durableManifest, durableJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
	if durableManifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("durable Manifest state = %q, want AWAITING_CANONICAL", durableManifest.State)
	}
	if durableManifest.JobID == nil || *durableManifest.JobID != fixture.jobID {
		t.Fatalf("durable Manifest JobID = %v, want %s", durableManifest.JobID, fixture.jobID)
	}
	if !durableManifest.UpdatedAt.Equal(plan.Now) {
		t.Fatalf("durable Manifest updated_at = %v, want %v", durableManifest.UpdatedAt, plan.Now)
	}
	if durableJob.State != jobs.StateRetryWait {
		t.Fatalf("durable Job state = %q, want RETRY_WAIT", durableJob.State)
	}
	if durableJob.LeaseOwner != nil || durableJob.LeaseExpiresAt != nil {
		t.Fatalf("lease was not cleared: owner=%v expires=%v", durableJob.LeaseOwner, durableJob.LeaseExpiresAt)
	}
	if durableJob.NextAttemptAt == nil || !durableJob.NextAttemptAt.Equal(plan.RetryAt) {
		t.Fatalf("next_attempt_at = %v, want %v", durableJob.NextAttemptAt, plan.RetryAt)
	}
	if durableJob.FinishedAt != nil {
		t.Fatalf("finished_at = %v, want NULL", durableJob.FinishedAt)
	}
	if durableJob.LastError != nil {
		t.Fatalf("last_error = %v, want NULL", durableJob.LastError)
	}
	if !durableJob.UpdatedAt.Equal(plan.Now) {
		t.Fatalf("durable Job updated_at = %v, want %v", durableJob.UpdatedAt, plan.Now)
	}
	// A stage handoff is not a same-stage retry: neither counter may move.
	if durableJob.ClaimAttempts != 3 || durableJob.FailureCount != 1 || durableJob.MaxAttempts != 5 {
		t.Fatalf("counters = claim %d, failure %d, max %d; want 3, 1, 5",
			durableJob.ClaimAttempts, durableJob.FailureCount, durableJob.MaxAttempts)
	}
}

// --- coverage 3: exact replay rewrites nothing ---------------------------------

func TestPostgresRefreshReplayDoesNotRewriteTimestamps(t *testing.T) {
	ctx, pool, repository, manifests, jobRepository, bindingID := newRefreshIntegrationFixture(t)
	fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "11", acquisition.StateAwaitingVisibility,
		"worker-a", 1, 0, 5, time.Now().UTC().Add(time.Hour))
	plan := refreshPlanFor(fixture, refreshPlanNow())

	first, err := repository.CommitRefresh(ctx, plan)
	if err != nil || !first.Changed {
		t.Fatalf("first CommitRefresh() = %#v, %v", first, err)
	}

	// Exact replay of the same plan.
	exact, err := repository.CommitRefresh(ctx, plan)
	if err != nil {
		t.Fatalf("exact replay CommitRefresh() error = %v", err)
	}
	if exact.Changed {
		t.Fatal("exact replay CommitRefresh() reported Changed=true")
	}
	if !exact.Manifest.UpdatedAt.Equal(first.Manifest.UpdatedAt) || !exact.Job.UpdatedAt.Equal(first.Job.UpdatedAt) {
		t.Fatalf("exact replay rewrote timestamps: Manifest %v -> %v, Job %v -> %v",
			first.Manifest.UpdatedAt, exact.Manifest.UpdatedAt, first.Job.UpdatedAt, exact.Job.UpdatedAt)
	}

	// A later Now must also not rewrite either timestamp.
	later := plan.Now.Add(6 * time.Hour)
	replayed, err := repository.CommitRefresh(ctx, refreshPlanFor(fixture, later))
	if err != nil {
		t.Fatalf("late replay CommitRefresh() error = %v", err)
	}
	if replayed.Changed {
		t.Fatal("late replay CommitRefresh() reported Changed=true")
	}
	if !replayed.Manifest.UpdatedAt.Equal(first.Manifest.UpdatedAt) || !replayed.Job.UpdatedAt.Equal(first.Job.UpdatedAt) {
		t.Fatalf("late replay rewrote timestamps: Manifest %v -> %v, Job %v -> %v",
			first.Manifest.UpdatedAt, replayed.Manifest.UpdatedAt, first.Job.UpdatedAt, replayed.Job.UpdatedAt)
	}
	if replayed.Manifest.UpdatedAt.Equal(later) || replayed.Job.UpdatedAt.Equal(later) {
		t.Fatal("late replay wrote the later plan timestamp")
	}

	durableManifest, durableJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
	if !durableManifest.UpdatedAt.Equal(first.Manifest.UpdatedAt) {
		t.Fatalf("durable Manifest updated_at = %v, want %v", durableManifest.UpdatedAt, first.Manifest.UpdatedAt)
	}
	if !durableJob.UpdatedAt.Equal(first.Job.UpdatedAt) {
		t.Fatalf("durable Job updated_at = %v, want %v", durableJob.UpdatedAt, first.Job.UpdatedAt)
	}
	if durableManifest.State != acquisition.StateAwaitingCanonical || durableJob.State != jobs.StateRetryWait {
		t.Fatalf("replay changed durable states: Manifest %q, Job %q", durableManifest.State, durableJob.State)
	}
}

// --- coverage 4: conflicting durable state fails closed ------------------------

func TestPostgresRefreshConflictingDurableStateFailsClosed(t *testing.T) {
	ctx, pool, repository, manifests, jobRepository, bindingID := newRefreshIntegrationFixture(t)

	t.Run("Manifest already AWAITING_CANONICAL while the Job is still RUNNING", func(t *testing.T) {
		fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "12", acquisition.StateAwaitingCanonical,
			"worker-a", 1, 0, 5, time.Now().UTC().Add(time.Hour))
		beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
		if _, err := repository.CommitRefresh(ctx, refreshPlanFor(fixture, refreshPlanNow())); !errors.Is(err, acquisition.ErrRefreshManifestState) {
			t.Fatalf("CommitRefresh() error = %v, want ErrRefreshManifestState", err)
		}
		assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
	})

	t.Run("Manifest ACTIVE", func(t *testing.T) {
		fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "13", acquisition.StateActive,
			"worker-a", 1, 0, 5, time.Now().UTC().Add(time.Hour))
		beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
		if _, err := repository.CommitRefresh(ctx, refreshPlanFor(fixture, refreshPlanNow())); !errors.Is(err, acquisition.ErrRefreshManifestState) {
			t.Fatalf("CommitRefresh() error = %v, want ErrRefreshManifestState", err)
		}
		assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
	})

	// The Job UPDATE is guarded by state = RUNNING plus the fence. The test-only
	// injection point moves the Job row inside the same transaction to prove the
	// conflict branch fails closed rather than committing half the handoff.
	t.Run("Job state changes under the transaction", func(t *testing.T) {
		fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "14", acquisition.StateAwaitingVisibility,
			"worker-a", 1, 0, 5, time.Now().UTC().Add(time.Hour))
		beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
		repository.afterManifestUpdate = func(ctx context.Context, tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `
UPDATE jobs
SET state = 'SUCCEEDED', lease_owner = NULL, lease_expires_at = NULL, finished_at = CURRENT_TIMESTAMP
WHERE job_id = $1`, string(fixture.jobID))
			return err
		}
		_, err := repository.CommitRefresh(ctx, refreshPlanFor(fixture, refreshPlanNow()))
		repository.afterManifestUpdate = nil
		if !errors.Is(err, acquisition.ErrRefreshConflict) {
			t.Fatalf("CommitRefresh() error = %v, want ErrRefreshConflict", err)
		}
		assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
	})
}

// --- coverage 5: rollback atomicity -------------------------------------------

func TestPostgresRefreshRollbackIsAtomic(t *testing.T) {
	ctx, pool, repository, manifests, jobRepository, bindingID := newRefreshIntegrationFixture(t)
	fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "15", acquisition.StateAwaitingVisibility,
		"worker-a", 3, 1, 5, time.Now().UTC().Add(time.Hour))
	beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)

	injected := errors.New("injected failure after the Manifest update")
	repository.afterManifestUpdate = func(context.Context, pgx.Tx) error { return injected }
	_, err := repository.CommitRefresh(ctx, refreshPlanFor(fixture, refreshPlanNow()))
	repository.afterManifestUpdate = nil
	if !errors.Is(err, acquisition.ErrRefreshPersistence) {
		t.Fatalf("CommitRefresh() error = %v, want ErrRefreshPersistence", err)
	}

	afterManifest, afterJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
	if afterManifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("Manifest state = %q, want AWAITING_VISIBILITY after rollback", afterManifest.State)
	}
	if !afterManifest.UpdatedAt.Equal(beforeManifest.UpdatedAt) {
		t.Fatalf("Manifest updated_at changed despite rollback: %v -> %v", beforeManifest.UpdatedAt, afterManifest.UpdatedAt)
	}
	if afterJob.State != jobs.StateRunning {
		t.Fatalf("Job state = %q, want RUNNING after rollback", afterJob.State)
	}
	if afterJob.LeaseOwner == nil || *afterJob.LeaseOwner != fixture.owner || afterJob.LeaseExpiresAt == nil {
		t.Fatalf("Job lease was not preserved through rollback: owner=%v expires=%v", afterJob.LeaseOwner, afterJob.LeaseExpiresAt)
	}
	if !afterJob.UpdatedAt.Equal(beforeJob.UpdatedAt) {
		t.Fatalf("Job updated_at changed despite rollback: %v -> %v", beforeJob.UpdatedAt, afterJob.UpdatedAt)
	}
	if afterJob.ClaimAttempts != 3 || afterJob.FailureCount != 1 {
		t.Fatalf("counters = claim %d, failure %d; want 3, 1", afterJob.ClaimAttempts, afterJob.FailureCount)
	}
}

// --- coverage 6: lease fence --------------------------------------------------

func TestPostgresRefreshLeaseFenceRejectsAndMutatesNothing(t *testing.T) {
	ctx, pool, repository, manifests, jobRepository, bindingID := newRefreshIntegrationFixture(t)

	t.Run("wrong lease owner", func(t *testing.T) {
		fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "16", acquisition.StateAwaitingVisibility,
			"worker-a", 3, 1, 5, time.Now().UTC().Add(time.Hour))
		beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
		plan := refreshPlanFor(fixture, refreshPlanNow())
		plan.Owner = "worker-b"
		if _, err := repository.CommitRefresh(ctx, plan); !errors.Is(err, acquisition.ErrRefreshFence) {
			t.Fatalf("CommitRefresh() error = %v, want ErrRefreshFence", err)
		}
		assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
	})

	t.Run("stale claim generation", func(t *testing.T) {
		fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "17", acquisition.StateAwaitingVisibility,
			"worker-a", 3, 1, 5, time.Now().UTC().Add(time.Hour))
		beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
		plan := refreshPlanFor(fixture, refreshPlanNow())
		plan.ExpectedClaim = 2
		if _, err := repository.CommitRefresh(ctx, plan); !errors.Is(err, acquisition.ErrRefreshFence) {
			t.Fatalf("CommitRefresh() error = %v, want ErrRefreshFence", err)
		}
		assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
	})

	t.Run("expired lease authorized by database time", func(t *testing.T) {
		fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "18", acquisition.StateAwaitingVisibility,
			"worker-a", 3, 1, 5, time.Now().UTC().Add(-time.Minute))
		beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
		if _, err := repository.CommitRefresh(ctx, refreshPlanFor(fixture, refreshPlanNow())); !errors.Is(err, acquisition.ErrRefreshFence) {
			t.Fatalf("CommitRefresh() error = %v, want ErrRefreshFence", err)
		}
		assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
	})
}

// --- coverage 7: Job linkage --------------------------------------------------

func TestPostgresRefreshJobLinkageMismatch(t *testing.T) {
	ctx, pool, repository, manifests, jobRepository, bindingID := newRefreshIntegrationFixture(t)

	t.Run("Manifest linked to a different Job", func(t *testing.T) {
		fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "19", acquisition.StateAwaitingVisibility,
			"worker-a", 1, 0, 5, time.Now().UTC().Add(time.Hour))
		// acquisition_manifests_job_unique allows one Job per Manifest, so the
		// reachable mismatch is a cross-link: free the second Job by discarding its
		// Manifest, then point the first Manifest at that Job.
		other := seedRefreshHandoff(t, ctx, pool, bindingID, "20", acquisition.StateAwaitingVisibility,
			"worker-a", 1, 0, 5, time.Now().UTC().Add(time.Hour))
		swap, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin cross-link transaction: %v", err)
		}
		if _, err := swap.Exec(ctx, `DELETE FROM acquisition_manifests WHERE manifest_id = $1`, string(other.manifestID)); err != nil {
			_ = swap.Rollback(ctx)
			t.Fatalf("discard second Manifest: %v", err)
		}
		if _, err := swap.Exec(ctx, `UPDATE acquisition_manifests SET job_id = $2 WHERE manifest_id = $1`,
			string(fixture.manifestID), string(other.jobID)); err != nil {
			_ = swap.Rollback(ctx)
			t.Fatalf("cross-link Manifest: %v", err)
		}
		if err := swap.Commit(ctx); err != nil {
			t.Fatalf("commit cross-link: %v", err)
		}

		beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
		if _, err := repository.CommitRefresh(ctx, refreshPlanFor(fixture, refreshPlanNow())); !errors.Is(err, acquisition.ErrRefreshJobMismatch) {
			t.Fatalf("CommitRefresh() error = %v, want ErrRefreshJobMismatch", err)
		}
		assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
	})

	t.Run("non-ACQUISITION Job type", func(t *testing.T) {
		fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "21", acquisition.StateAwaitingVisibility,
			"worker-a", 1, 0, 5, time.Now().UTC().Add(time.Hour))
		if _, err := pool.Exec(ctx, `UPDATE jobs SET job_type = 'INDEXCORE_REFRESH' WHERE job_id = $1`, string(fixture.jobID)); err != nil {
			t.Fatalf("mutate job type: %v", err)
		}
		beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
		if _, err := repository.CommitRefresh(ctx, refreshPlanFor(fixture, refreshPlanNow())); !errors.Is(err, acquisition.ErrRefreshJobMismatch) {
			t.Fatalf("CommitRefresh() error = %v, want ErrRefreshJobMismatch", err)
		}
		assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
	})
}

// --- coverage 8: tampered plan rejection --------------------------------------

func TestPostgresRefreshRejectsTamperedPlansBeforeWriting(t *testing.T) {
	ctx, pool, repository, manifests, jobRepository, bindingID := newRefreshIntegrationFixture(t)
	fixture := seedRefreshHandoff(t, ctx, pool, bindingID, "22", acquisition.StateAwaitingVisibility,
		"worker-a", 1, 0, 5, time.Now().UTC().Add(time.Hour))
	valid := refreshPlanFor(fixture, refreshPlanNow())
	if err := validateRefreshPlan(valid); err != nil {
		t.Fatalf("validateRefreshPlan(valid) error = %v", err)
	}

	unsupportedManifest := valid
	unsupportedManifest.ManifestState = acquisition.StateAwaitingVisibility
	if err := validateRefreshPlan(unsupportedManifest); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("validateRefreshPlan(unsupported Manifest state) error = %v, want ErrInvalidRefreshRequest", err)
	}

	unsupportedJob := valid
	unsupportedJob.JobState = jobs.StateRunning
	if err := validateRefreshPlan(unsupportedJob); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("validateRefreshPlan(unsupported Job state) error = %v, want ErrInvalidRefreshRequest", err)
	}

	retryNow := valid
	retryNow.RetryAt = retryNow.Now
	if err := validateRefreshPlan(retryNow); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("validateRefreshPlan(RetryAt == Now) error = %v, want ErrInvalidRefreshRequest", err)
	}

	retryPast := valid
	retryPast.RetryAt = retryPast.Now.Add(-time.Minute)
	if err := validateRefreshPlan(retryPast); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("validateRefreshPlan(RetryAt before Now) error = %v, want ErrInvalidRefreshRequest", err)
	}

	beforeManifest, beforeJob := refreshDurableState(t, ctx, manifests, jobRepository, fixture)
	if _, err := repository.CommitRefresh(ctx, unsupportedManifest); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
		t.Fatalf("CommitRefresh(tampered plan) error = %v, want ErrInvalidRefreshRequest", err)
	}
	assertRefreshNotMutated(t, ctx, manifests, jobRepository, fixture, beforeManifest, beforeJob)
}
