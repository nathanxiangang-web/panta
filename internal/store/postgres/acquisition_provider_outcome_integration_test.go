package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

// --- seeding ------------------------------------------------------------------

type providerOutcomeFixture struct {
	manifestID acquisition.ManifestID
	jobID      jobs.JobID
	owner      string
	attempt    int
	leaseEnd   time.Time
}

// seedProviderOutcomeJob inserts one ACTIVE Manifest linked to a RUNNING
// ACQUISITION Job with an unexpired lease, matching what a fenced worker holds.
//
// attempt is the claim generation the seeded lease holds, not a failure count: the
// failure budget is seeded at the default 5 and is not what fences this worker.
func seedProviderOutcomeJob(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	suffix string,
	owner string,
	attempt int,
	leaseEnd time.Time,
) providerOutcomeFixture {
	t.Helper()
	return seedProviderOutcomeJobWithBudget(t, ctx, pool, bindingID, suffix, owner, attempt, 5, leaseEnd)
}

// seedProviderOutcomeJobWithBudget is the same fixture with an explicit failure
// budget, so tests can prove stage transitions never consume it.
func seedProviderOutcomeJobWithBudget(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	suffix string,
	owner string,
	claimGeneration int,
	maxAttempts int,
	leaseEnd time.Time,
) providerOutcomeFixture {
	t.Helper()
	now := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	manifestID := acquisition.ManifestID("37000000-0000-4000-8000-0000000000" + suffix)
	jobID := jobs.JobID("37000000-0000-4000-8000-0000000001" + suffix)
	key := acquisition.AcquisitionJobIdempotencyKey(manifestID)
	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    claim_attempts, attempt_count, max_attempts, lease_owner, lease_expires_at,
    created_at, updated_at, started_at
) VALUES ($1, $2, $3, 'RUNNING', $4, $5, 0, $6, $7, $8, $9, $9, $9)`,
		string(jobID), acquisition.JobTypeAcquisition,
		[]byte(fmt.Sprintf(`{"manifest_id":"%s","schema_version":1}`, manifestID)),
		key, claimGeneration, maxAttempts, owner, leaseEnd, now,
	); err != nil {
		t.Fatalf("seed RUNNING ACQUISITION job: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id, target_path,
    job_id, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, '/downloads/item', $3, 'ACTIVE', $4, $4)`,
		string(manifestID), string(bindingID), string(jobID), now,
	); err != nil {
		t.Fatalf("seed ACTIVE Manifest: %v", err)
	}
	return providerOutcomeFixture{
		manifestID: manifestID, jobID: jobID, owner: owner, attempt: claimGeneration, leaseEnd: leaseEnd,
	}
}

func newProviderOutcomeIntegrationFixture(t *testing.T) (context.Context, *pgxpool.Pool, *ProviderOutcomeRepository, *AcquisitionManifestRepository, *JobRepository, storage.BindingID) {
	t.Helper()
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible {
		t.Fatalf("Apply() = %#v, %v", status, err)
	}
	bindingID := storage.BindingID("37000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "provider-outcome-root")

	outcomes, err := NewProviderOutcomeRepository(pool)
	if err != nil {
		t.Fatalf("NewProviderOutcomeRepository() error = %v", err)
	}
	manifests, err := NewAcquisitionManifestRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionManifestRepository() error = %v", err)
	}
	jobRepository, err := NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}
	return ctx, pool, outcomes, manifests, jobRepository, bindingID
}

func providerOutcomeRequest(fixture providerOutcomeFixture, outcome acquisition.ProviderOutcome, now time.Time) acquisition.ProviderOutcomeRequest {
	request := acquisition.ProviderOutcomeRequest{
		ManifestID:    fixture.manifestID,
		JobID:         fixture.jobID,
		Owner:         fixture.owner,
		ExpectedClaim: fixture.attempt,
		Outcome:       outcome,
		Now:           now,
	}
	transition, err := acquisition.ProviderOutcomeTransitionFor(outcome)
	if err != nil {
		panic(err)
	}
	if transition.RequiresRetryAt {
		retry := now.Add(90 * time.Second)
		request.RetryAt = &retry
	}
	if transition.RequiresError {
		message := "provider stage failed"
		request.ErrorMessage = &message
	}
	if outcome == acquisition.ProviderOutcomeSucceeded {
		// D-031: a provider success supplies the acquired direct-child name.
		resultName := providerOutcomeResultName
		request.ProviderResultName = &resultName
	}
	return request
}

// providerOutcomeResultName is the direct-child identity the provider reports in
// these tests. It is deliberately not path-clean or space-normalized, so an
// accidental trim or clean in the handoff would surface as a mismatch.
const providerOutcomeResultName = "acquired-item.bin"

func newOutcomeServiceFor(t *testing.T, repository *ProviderOutcomeRepository) *acquisition.ProviderOutcomeService {
	t.Helper()
	service, err := acquisition.NewProviderOutcomeService(repository)
	if err != nil {
		t.Fatalf("NewProviderOutcomeService() error = %v", err)
	}
	return service
}

// --- 1-11: frozen transitions -------------------------------------------------

func TestPostgresProviderOutcomeTransitionsAtomically(t *testing.T) {
	ctx, pool, outcomes, manifests, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	leaseEnd := time.Now().UTC().Add(time.Hour)

	tests := []struct {
		outcome      acquisition.ProviderOutcome
		wantManifest acquisition.State
		wantJob      jobs.State
		wantFinished bool
	}{
		{outcome: acquisition.ProviderOutcomeInProgress, wantManifest: acquisition.StateActive, wantJob: jobs.StateRetryWait},
		{outcome: acquisition.ProviderOutcomeSucceeded, wantManifest: acquisition.StateAwaitingVisibility, wantJob: jobs.StateRetryWait},
		{outcome: acquisition.ProviderOutcomeFailed, wantManifest: acquisition.StateFailed, wantJob: jobs.StateFailed, wantFinished: true},
		{outcome: acquisition.ProviderOutcomeCanceled, wantManifest: acquisition.StateCanceled, wantJob: jobs.StateCanceled, wantFinished: true},
		{outcome: acquisition.ProviderOutcomeRecovery, wantManifest: acquisition.StateRecoveryRequired, wantJob: jobs.StateRecoveryRequired},
	}
	for index, test := range tests {
		t.Run(string(test.outcome), func(t *testing.T) {
			suffix := fmt.Sprintf("%02d", index+10)
			fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, suffix, "worker-a", 1, leaseEnd)
			now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
			request := providerOutcomeRequest(fixture, test.outcome, now)

			result, err := service.Commit(ctx, request)
			if err != nil {
				t.Fatalf("Commit() error = %v", err)
			}
			if !result.Changed {
				t.Fatal("Commit() reported Changed=false for a fresh outcome")
			}
			if result.Manifest.State != test.wantManifest {
				t.Fatalf("Manifest state = %q, want %q", result.Manifest.State, test.wantManifest)
			}
			if result.Job.State != test.wantJob {
				t.Fatalf("Job state = %q, want %q", result.Job.State, test.wantJob)
			}

			// Read back with independent repositories so the assertions do not
			// depend on the returned structs.
			durableManifest, err := manifests.GetManifest(ctx, fixture.manifestID)
			if err != nil {
				t.Fatalf("GetManifest() error = %v", err)
			}
			if durableManifest.State != test.wantManifest {
				t.Fatalf("durable Manifest state = %q, want %q", durableManifest.State, test.wantManifest)
			}
			durableJob, err := jobRepository.Get(ctx, fixture.jobID)
			if err != nil {
				t.Fatalf("Get(job) error = %v", err)
			}
			if durableJob.State != test.wantJob {
				t.Fatalf("durable Job state = %q, want %q", durableJob.State, test.wantJob)
			}

			// 5: lease fields are cleared on every handoff.
			if durableJob.LeaseOwner != nil || durableJob.LeaseExpiresAt != nil {
				t.Fatalf("lease was not cleared: owner=%v expires=%v", durableJob.LeaseOwner, durableJob.LeaseExpiresAt)
			}
			// 2/3: provider success must never complete the Job or the Manifest.
			if test.outcome == acquisition.ProviderOutcomeSucceeded {
				if durableJob.State == jobs.StateSucceeded {
					t.Fatal("provider success marked the Job SUCCEEDED")
				}
				if durableManifest.State == acquisition.StateReady {
					t.Fatal("provider success marked the Manifest READY")
				}
				// 4: next_attempt_at equals the requested RetryAt.
				if durableJob.NextAttemptAt == nil || !durableJob.NextAttemptAt.Equal(*request.RetryAt) {
					t.Fatalf("next_attempt_at = %v, want %v", durableJob.NextAttemptAt, request.RetryAt)
				}
			}
			if test.wantFinished {
				if durableJob.FinishedAt == nil {
					t.Fatal("terminal Job has no finished_at")
				}
			} else if durableJob.FinishedAt != nil {
				t.Fatalf("non-terminal Job has finished_at = %v", durableJob.FinishedAt)
			}
			// attempt_count is preserved, never incremented here.
			if durableJob.ClaimAttempts != fixture.attempt {
				t.Fatalf("claim generation = %d, want %d (a handoff must not change it)",
					durableJob.ClaimAttempts, fixture.attempt)
			}
		})
	}
}

// TestPostgresProviderInProgressDoesNotRewriteManifestTimestamp covers test 7.
func TestPostgresProviderInProgressDoesNotRewriteManifestTimestamp(t *testing.T) {
	ctx, pool, outcomes, manifests, _, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "20", "worker-a", 1, time.Now().UTC().Add(time.Hour))

	before, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	now := before.UpdatedAt.Add(time.Hour)
	if _, err := service.Commit(ctx, providerOutcomeRequest(fixture, acquisition.ProviderOutcomeInProgress, now)); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	after, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if after.State != acquisition.StateActive {
		t.Fatalf("Manifest state = %q, want ACTIVE", after.State)
	}
	if !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("Manifest updated_at changed from %v to %v while the state stayed ACTIVE",
			before.UpdatedAt, after.UpdatedAt)
	}
}

// TestPostgresRecoveryRequiredJobIsNotClaimable covers test 11.
func TestPostgresRecoveryRequiredJobIsNotClaimable(t *testing.T) {
	ctx, pool, outcomes, _, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "21", "worker-a", 1, time.Now().UTC().Add(time.Hour))

	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	if _, err := service.Commit(ctx, providerOutcomeRequest(fixture, acquisition.ProviderOutcomeRecovery, now)); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	durable, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	if durable.State.Terminal() {
		t.Fatal("RECOVERY_REQUIRED must not be terminal")
	}
	claimNow := now.Add(24 * time.Hour)
	if _, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{
		Owner: "worker-b", Now: claimNow, LeaseDuration: time.Hour,
	}); !errors.Is(err, jobs.ErrNoClaimableJob) {
		t.Fatalf("ClaimNext() error = %v, want ErrNoClaimableJob", err)
	}
}

// --- 12-18: fencing and identity ---------------------------------------------

func TestPostgresProviderOutcomeFencingAndIdentityFailures(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)

	leaseEnd := time.Now().UTC().Add(time.Hour)
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	suffixes := []string{"30", "31", "32", "33", "34", "35", "36", "37"}
	index := -1
	next := func() providerOutcomeFixture {
		index++
		return seedProviderOutcomeJob(t, ctx, pool, bindingID, suffixes[index], "worker-a", 2, leaseEnd)
	}

	t.Run("stale claim generation", func(t *testing.T) {
		fixture := next()
		request := providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)
		request.ExpectedClaim = 1
		if _, err := service.Commit(ctx, request); !errors.Is(err, acquisition.ErrProviderOutcomeFence) {
			t.Fatalf("error = %v, want ErrProviderOutcomeFence", err)
		}
	})

	t.Run("foreign Owner", func(t *testing.T) {
		fixture := next()
		request := providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)
		request.Owner = "worker-b"
		if _, err := service.Commit(ctx, request); !errors.Is(err, acquisition.ErrProviderOutcomeFence) {
			t.Fatalf("error = %v, want ErrProviderOutcomeFence", err)
		}
	})

	t.Run("expired lease authorized by database time", func(t *testing.T) {
		expired := seedProviderOutcomeJob(t, ctx, pool, bindingID, "38", "worker-a", 1, time.Now().UTC().Add(-time.Minute))
		request := providerOutcomeRequest(expired, acquisition.ProviderOutcomeSucceeded, now)
		if _, err := service.Commit(ctx, request); !errors.Is(err, acquisition.ErrProviderOutcomeFence) {
			t.Fatalf("error = %v, want ErrProviderOutcomeFence", err)
		}
	})

	t.Run("wrong Job type", func(t *testing.T) {
		fixture := next()
		if _, err := pool.Exec(ctx, `UPDATE jobs SET job_type = 'INDEXCORE_REFRESH' WHERE job_id = $1`, string(fixture.jobID)); err != nil {
			t.Fatalf("mutate job type: %v", err)
		}
		if _, err := service.Commit(ctx, providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)); !errors.Is(err, acquisition.ErrProviderOutcomeJobMismatch) {
			t.Fatalf("error = %v, want ErrProviderOutcomeJobMismatch", err)
		}
	})

	t.Run("Manifest linked to a different Job", func(t *testing.T) {
		fixture := next()
		// acquisition_manifests_job_unique requires one Job per Manifest, so the
		// reachable mismatch is a cross-link: keep a second Job alive, discard its
		// Manifest, and point the first Manifest at that Job. The Job payload still
		// references the discarded Manifest, which is exactly the linkage
		// inconsistency the fence must reject.
		other := seedProviderOutcomeJob(t, ctx, pool, bindingID, "39", "worker-a", 1, leaseEnd)
		// Delete the second Manifest so its Job becomes free, then cross-link the
		// first Manifest to that Job. Both statements run in one transaction.
		swap, err := pool.Begin(ctx)
		if err != nil {
			t.Fatalf("begin swap transaction: %v", err)
		}
		if _, err := swap.Exec(ctx, `DELETE FROM acquisition_manifests WHERE manifest_id = $1`, string(other.manifestID)); err != nil {
			_ = swap.Rollback(ctx)
			t.Fatalf("discard second manifest: %v", err)
		}
		if _, err := swap.Exec(ctx, `UPDATE acquisition_manifests SET job_id = $2 WHERE manifest_id = $1`,
			string(fixture.manifestID), string(other.jobID)); err != nil {
			_ = swap.Rollback(ctx)
			t.Fatalf("cross-link manifest: %v", err)
		}
		if err := swap.Commit(ctx); err != nil {
			t.Fatalf("commit cross-link: %v", err)
		}

		request := providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)
		if _, err := service.Commit(ctx, request); !errors.Is(err, acquisition.ErrProviderOutcomeJobMismatch) {
			t.Fatalf("error = %v, want ErrProviderOutcomeJobMismatch", err)
		}
	})

	t.Run("missing Manifest", func(t *testing.T) {
		fixture := next()
		request := providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)
		request.ManifestID = acquisition.ManifestID("37000000-0000-4000-8000-00000000ffff")
		if _, err := service.Commit(ctx, request); !errors.Is(err, acquisition.ErrNotFound) {
			t.Fatalf("error = %v, want ErrNotFound", err)
		}
	})

	t.Run("missing Job", func(t *testing.T) {
		fixture := next()
		// The database forbids an ACTIVE Manifest without a linked Job, so the
		// reachable missing-Job case is a Job row that does not exist at all.
		request := providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)
		request.JobID = jobs.JobID("37000000-0000-4000-8000-00000000ffff")
		if _, err := service.Commit(ctx, request); !errors.Is(err, acquisition.ErrProviderOutcomeJobMismatch) {
			t.Fatalf("error = %v, want ErrProviderOutcomeJobMismatch", err)
		}
	})

	t.Run("non-ACTIVE Manifest that is not an exact replay", func(t *testing.T) {
		fixture := next()
		if _, err := pool.Exec(ctx, `UPDATE acquisition_manifests SET state = 'PENDING', job_id = NULL WHERE manifest_id = $1`,
			string(fixture.manifestID)); err != nil {
			t.Fatalf("mutate manifest state: %v", err)
		}
		if _, err := service.Commit(ctx, providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)); !errors.Is(err, acquisition.ErrProviderOutcomeManifestState) {
			t.Fatalf("error = %v, want ErrProviderOutcomeManifestState", err)
		}
	})
}

// --- 19: atomic rollback ------------------------------------------------------

func TestPostgresProviderOutcomeRollsBackBothRecords(t *testing.T) {
	ctx, pool, outcomes, manifests, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "40", "worker-a", 1, time.Now().UTC().Add(time.Hour))
	beforeManifest, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}

	injected := errors.New("injected failure after the first row mutation")
	outcomes.afterManifestUpdate = func(context.Context, pgx.Tx) error { return injected }
	service := newOutcomeServiceFor(t, outcomes)
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	if _, err := service.Commit(ctx, providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)); !errors.Is(err, acquisition.ErrProviderOutcomePersistence) {
		t.Fatalf("Commit() error = %v, want ErrProviderOutcomePersistence", err)
	}
	outcomes.afterManifestUpdate = nil

	afterManifest, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if afterManifest.State != acquisition.StateActive {
		t.Fatalf("Manifest state = %q, want ACTIVE after rollback", afterManifest.State)
	}
	if !afterManifest.UpdatedAt.Equal(beforeManifest.UpdatedAt) {
		t.Fatalf("Manifest updated_at changed despite rollback: %v -> %v", beforeManifest.UpdatedAt, afterManifest.UpdatedAt)
	}
	afterJob, err := jobRepository.Get(ctx, fixture.jobID)
	if err != nil {
		t.Fatalf("Get(job) error = %v", err)
	}
	if afterJob.State != jobs.StateRunning {
		t.Fatalf("Job state = %q, want RUNNING after rollback", afterJob.State)
	}
	if afterJob.LeaseOwner == nil || *afterJob.LeaseOwner != "worker-a" {
		t.Fatalf("Job lease was not preserved through rollback: %v", afterJob.LeaseOwner)
	}
}

// --- 20: idempotent replay ----------------------------------------------------

func TestPostgresProviderOutcomeReplayIsIdempotent(t *testing.T) {
	ctx, pool, outcomes, manifests, jobRepository, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)

	tests := []struct {
		name    string
		outcome acquisition.ProviderOutcome
		suffix  string
	}{
		{name: "succeeded replay", outcome: acquisition.ProviderOutcomeSucceeded, suffix: "50"},
		{name: "failed replay", outcome: acquisition.ProviderOutcomeFailed, suffix: "51"},
		{name: "recovery replay", outcome: acquisition.ProviderOutcomeRecovery, suffix: "52"},
		{name: "canceled replay", outcome: acquisition.ProviderOutcomeCanceled, suffix: "53"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, test.suffix, "worker-a", 1, time.Now().UTC().Add(time.Hour))
			now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
			request := providerOutcomeRequest(fixture, test.outcome, now)

			first, err := service.Commit(ctx, request)
			if err != nil {
				t.Fatalf("first Commit() error = %v", err)
			}
			if !first.Changed {
				t.Fatal("first Commit() reported Changed=false")
			}
			// A later Now must not rewrite timestamps on replay.
			replayRequest := providerOutcomeRequest(fixture, test.outcome, now.Add(6*time.Hour))
			replay, err := service.Commit(ctx, replayRequest)
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

			// Durable state is unchanged by the replay.
			durableManifest, err := manifests.GetManifest(ctx, fixture.manifestID)
			if err != nil {
				t.Fatalf("GetManifest() error = %v", err)
			}
			if !durableManifest.UpdatedAt.Equal(first.Manifest.UpdatedAt) {
				t.Fatal("durable Manifest timestamp changed during replay")
			}
			durableJob, err := jobRepository.Get(ctx, fixture.jobID)
			if err != nil {
				t.Fatalf("Get(job) error = %v", err)
			}
			if durableJob.State != first.Job.State {
				t.Fatalf("durable Job state = %q, want %q", durableJob.State, first.Job.State)
			}
		})
	}
}

// TestPostgresProviderOutcomeConflictingReplayFailsClosed covers the
// different-proposed-outcome rule.
func TestPostgresProviderOutcomeConflictingReplayFailsClosed(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "54", "worker-a", 1, time.Now().UTC().Add(time.Hour))
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)

	if _, err := service.Commit(ctx, providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)); err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}
	if _, err := service.Commit(ctx, providerOutcomeRequest(fixture, acquisition.ProviderOutcomeFailed, now)); !errors.Is(err, acquisition.ErrProviderOutcomeManifestState) {
		t.Fatalf("conflicting Commit() error = %v, want ErrProviderOutcomeManifestState", err)
	}
}

// --- 21-22: concurrency -------------------------------------------------------

func TestPostgresConcurrentProviderOutcomesConverge(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "60", "worker-a", 1, time.Now().UTC().Add(time.Hour))
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)

	const workers = 6
	var waitGroup sync.WaitGroup
	changed := make([]bool, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			<-start
			result, err := service.Commit(ctx, providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now))
			changed[index], errs[index] = result.Changed, err
		}(worker)
	}
	close(start)
	waitGroup.Wait()

	committed := 0
	for index := 0; index < workers; index++ {
		if errs[index] != nil {
			t.Fatalf("worker %d error = %v", index, errs[index])
		}
		if changed[index] {
			committed++
		}
	}
	if committed != 1 {
		t.Fatalf("committed changes = %d, want exactly 1", committed)
	}
}

func TestPostgresConcurrentConflictingProviderOutcomesHaveOneWinner(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newProviderOutcomeIntegrationFixture(t)
	service := newOutcomeServiceFor(t, outcomes)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "61", "worker-a", 1, time.Now().UTC().Add(time.Hour))
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)

	outcomeList := []acquisition.ProviderOutcome{
		acquisition.ProviderOutcomeSucceeded,
		acquisition.ProviderOutcomeFailed,
	}
	var waitGroup sync.WaitGroup
	changed := make([]bool, len(outcomeList))
	errs := make([]error, len(outcomeList))
	start := make(chan struct{})
	for index, outcome := range outcomeList {
		waitGroup.Add(1)
		go func(position int, proposed acquisition.ProviderOutcome) {
			defer waitGroup.Done()
			<-start
			result, err := service.Commit(ctx, providerOutcomeRequest(fixture, proposed, now))
			changed[position], errs[position] = result.Changed, err
		}(index, outcome)
	}
	close(start)
	waitGroup.Wait()

	winners, conflicts := 0, 0
	for index := range outcomeList {
		switch {
		case errs[index] == nil && changed[index]:
			winners++
		case errs[index] != nil && isProviderOutcomeConflict(errs[index]):
			conflicts++
		default:
			t.Fatalf("outcome %d produced neither a win nor a typed conflict: changed=%v err=%v",
				index, changed[index], errs[index])
		}
	}
	if winners != 1 || conflicts != 1 {
		t.Fatalf("winners=%d conflicts=%d, want exactly one of each", winners, conflicts)
	}
}

// isProviderOutcomeConflict accepts the typed conflict or the state error that
// reports the loser's competing proposal.
func isProviderOutcomeConflict(err error) bool {
	return errors.Is(err, acquisition.ErrProviderOutcomeConflict) ||
		errors.Is(err, acquisition.ErrProviderOutcomeManifestState)
}

// --- service-level validation against the real store -------------------------

func TestPostgresProviderOutcomeRejectsInvalidPlansBeforeWriting(t *testing.T) {
	ctx, pool, outcomes, manifests, _, bindingID := newProviderOutcomeIntegrationFixture(t)
	fixture := seedProviderOutcomeJob(t, ctx, pool, bindingID, "70", "worker-a", 1, time.Now().UTC().Add(time.Hour))
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)

	// A plan whose states disagree with the frozen mapping must never write.
	valid := providerOutcomeRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)
	_, plan, err := acquisition.BuildProviderOutcomePlan(valid)
	if err != nil {
		t.Fatalf("BuildProviderOutcomePlan() error = %v", err)
	}
	plan.ManifestState = acquisition.StateFailed
	if _, err := outcomes.CommitProviderOutcome(ctx, plan); !errors.Is(err, acquisition.ErrInvalidProviderOutcome) {
		t.Fatalf("CommitProviderOutcome(tampered plan) error = %v, want ErrInvalidProviderOutcome", err)
	}
	durable, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if durable.State != acquisition.StateActive {
		t.Fatalf("Manifest state = %q, want ACTIVE", durable.State)
	}
}
