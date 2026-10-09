package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/migrations"
)

// seedLegacyV9Job inserts one Job row directly under the pre-version-10 schema,
// where attempt_count still means the claim generation.
func seedLegacyV9Job(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	jobID jobs.JobID,
	attemptCount int,
	maxAttempts int,
	state string,
	leaseOwner string,
	leaseExpiresAt *time.Time,
	nextAttemptAt *time.Time,
	now time.Time,
) {
	t.Helper()
	key := "legacy:" + string(jobID)
	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    attempt_count, max_attempts, next_attempt_at, lease_owner, lease_expires_at,
    created_at, updated_at, started_at
) VALUES ($1, 'ACQUISITION', '{"schema_version":1,"manifest_id":"37000000-0000-4000-8000-000000000090"}', $2, $3,
          $4, $5, $6, $7, $8, $9, $9, $9)`,
		string(jobID), state, key, attemptCount, maxAttempts, nextAttemptAt,
		sql.NullString{String: leaseOwner, Valid: leaseOwner != ""}, leaseExpiresAt, now,
	); err != nil {
		t.Fatalf("seed legacy v9 job %s: %v", jobID, err)
	}
}

// TestPostgresJobClaimGenerationUpgradeMigratesLegacyAttemptCount is the required
// v9 -> v10 upgrade proof.
//
// Before version 10, attempt_count was the claim generation: every claim
// incremented it. Version 10 introduces claim_attempts as the claim generation and
// redefines attempt_count as the failure retry budget. The migration must MOVE the
// legacy value, not drop it: rewinding a legacy generation to 0 would let an
// already superseded worker pass the fence, and reinterpreting N historical claims
// as N failures would let the next real failure terminate the Job early.
func TestPostgresJobClaimGenerationUpgradeMigratesLegacyAttemptCount(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	history, err := migrations.All()
	if err != nil {
		t.Fatalf("migrations.All() error = %v", err)
	}
	if len(history) != 12 {
		t.Fatalf("migration history length = %d, want 10", len(history))
	}
	// Apply the pre-split schema only: versions 1 through 9.
	legacy := &Migrator{pool: pool, migrations: history[:9]}
	if _, err := legacy.Apply(ctx); err != nil {
		t.Fatalf("apply through version 9: %v", err)
	}

	now := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	const legacyGeneration = 4
	const maxAttempts = 5

	// A legacy Job waiting for its next stage is a RETRY_WAIT row, which is exactly
	// the shape most Jobs have in production and the shape ClaimNext can pick up.
	// Its attempt_count of 4 means "generation 4", not "four failures".
	legacyJobID := jobs.JobID("37000000-0000-4000-8000-000000000191")
	legacyDue := now.Add(-time.Minute)
	seedLegacyV9Job(t, ctx, pool, legacyJobID, legacyGeneration, maxAttempts,
		"RETRY_WAIT", "", nil, &legacyDue, now)

	// Sanity: the legacy schema has no claim_attempts column yet, and the legacy
	// column really does hold the generation.
	var hasClaimAttempts bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'jobs' AND column_name = 'claim_attempts'
)`).Scan(&hasClaimAttempts); err != nil {
		t.Fatalf("inspect pre-upgrade columns: %v", err)
	}
	if hasClaimAttempts {
		t.Fatal("claim_attempts already exists before the version 10 upgrade")
	}
	var legacyAttemptCount int
	if err := pool.QueryRow(ctx, `SELECT attempt_count FROM jobs WHERE job_id = $1`,
		string(legacyJobID)).Scan(&legacyAttemptCount); err != nil {
		t.Fatalf("read legacy attempt_count: %v", err)
	}
	if legacyAttemptCount != legacyGeneration {
		t.Fatalf("legacy attempt_count = %d, want %d", legacyAttemptCount, legacyGeneration)
	}

	// Upgrade to version 10.
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("upgrade to version 10: %v", err)
	}
	if !status.Compatible || status.CurrentVersion != 12 || status.LatestVersion != 12 {
		t.Fatalf("upgrade status = %#v", status)
	}
	repository, err := NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}

	// The legacy generation moved across; the failure budget restarted at zero.
	upgraded, err := repository.Get(ctx, legacyJobID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if upgraded.ClaimAttempts != legacyGeneration {
		t.Fatalf("claim generation = %d, want the legacy value %d (never rewound to 0)",
			upgraded.ClaimAttempts, legacyGeneration)
	}
	if upgraded.FailureCount != 0 {
		t.Fatalf("failure count = %d, want 0: historical claims are not failures", upgraded.FailureCount)
	}
	if upgraded.MaxAttempts != maxAttempts {
		t.Fatalf("max attempts = %d, want %d", upgraded.MaxAttempts, maxAttempts)
	}

	// The next claim advances the generation to N+1 and still spends no budget.
	claimed, err := repository.ClaimNext(ctx, jobs.ClaimRequest{
		Owner: "worker-b", Now: now.Add(2 * time.Minute), LeaseDuration: time.Hour,
	})
	if err != nil {
		t.Fatalf("ClaimNext() after upgrade error = %v", err)
	}
	if claimed.ID != legacyJobID {
		t.Fatalf("ClaimNext() took %s, want %s", claimed.ID, legacyJobID)
	}
	if claimed.ClaimAttempts != legacyGeneration+1 {
		t.Fatalf("post-upgrade claim generation = %d, want %d", claimed.ClaimAttempts, legacyGeneration+1)
	}
	if claimed.FailureCount != 0 {
		t.Fatalf("post-upgrade failure count = %d, want 0", claimed.FailureCount)
	}

	// The claim superseded generation N, so that generation can no longer mutate
	// state even for the worker that legitimately held it before the upgrade.
	if _, err := repository.Succeed(ctx, jobs.LeaseRequest{
		ID: legacyJobID, Owner: "worker-a", ExpectedClaim: legacyGeneration, Now: now.Add(4 * time.Minute),
	}); err == nil {
		t.Fatal("the superseded legacy generation mutated state after a newer claim")
	}
	stillRunning, err := repository.Get(ctx, legacyJobID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if stillRunning.State != jobs.StateRunning || stillRunning.ClaimAttempts != legacyGeneration+1 {
		t.Fatalf("job = %#v, want RUNNING at generation %d", stillRunning, legacyGeneration+1)
	}
	// The current generation can still mutate state normally.
	if _, err := repository.Succeed(ctx, jobs.LeaseRequest{
		ID: legacyJobID, Owner: "worker-b", ExpectedClaim: legacyGeneration + 1, Now: now.Add(4 * time.Minute),
	}); err != nil {
		t.Fatalf("current generation Succeed() error = %v", err)
	}
}

// TestPostgresJobClaimGenerationUpgradeKeepsLiveLeaseFence covers the other legacy
// shape: a Job that is RUNNING with an unexpired lease when the upgrade happens. Its
// live worker must keep working at the migrated generation, and the failure budget
// must not inherit the legacy claim count.
func TestPostgresJobClaimGenerationUpgradeKeepsLiveLeaseFence(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	history, err := migrations.All()
	if err != nil {
		t.Fatalf("migrations.All() error = %v", err)
	}
	legacy := &Migrator{pool: pool, migrations: history[:9]}
	if _, err := legacy.Apply(ctx); err != nil {
		t.Fatalf("apply through version 9: %v", err)
	}

	now := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	const legacyGeneration = 3
	const maxAttempts = 4
	// The lease must be live according to PostgreSQL time on the runner; the
	// fixed historical migration timestamp is not a durable future deadline.
	leaseEnd := time.Now().UTC().Add(24 * time.Hour)
	jobID := jobs.JobID("37000000-0000-4000-8000-000000000194")
	seedLegacyV9Job(t, ctx, pool, jobID, legacyGeneration, maxAttempts,
		"RUNNING", "worker-a", &leaseEnd, nil, now)

	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	if _, err := migrator.Apply(ctx); err != nil {
		t.Fatalf("upgrade to version 10: %v", err)
	}
	repository, err := NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}

	// The live worker still holds its migrated generation and can mutate state.
	if _, err := repository.RenewLease(ctx, jobs.RenewLeaseRequest{
		LeaseRequest: jobs.LeaseRequest{
			ID: jobID, Owner: "worker-a", ExpectedClaim: legacyGeneration, Now: now.Add(time.Minute),
		},
		LeaseDuration: time.Hour,
	}); err != nil {
		t.Fatalf("RenewLease() at the migrated generation error = %v", err)
	}

	job, err := repository.Get(ctx, jobID)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if job.ClaimAttempts != legacyGeneration {
		t.Fatalf("claim generation = %d, want %d", job.ClaimAttempts, legacyGeneration)
	}
	if job.FailureCount != 0 {
		t.Fatalf("failure count = %d, want 0 on upgrade", job.FailureCount)
	}

	// A superseded generation is still fenced.
	if _, err := repository.RenewLease(ctx, jobs.RenewLeaseRequest{
		LeaseRequest: jobs.LeaseRequest{
			ID: jobID, Owner: "worker-a", ExpectedClaim: legacyGeneration - 1, Now: now.Add(time.Minute),
		},
		LeaseDuration: time.Hour,
	}); err == nil {
		t.Fatal("a superseded generation renewed the lease, want a fence rejection")
	}

	// The migrated generation is nowhere near the failure budget, so a real failure
	// retry remains available.
	failure, err := repository.RetryAt(ctx, jobs.RetryRequest{
		FailureRequest: jobs.FailureRequest{
			LeaseRequest: jobs.LeaseRequest{
				ID: jobID, Owner: "worker-a", ExpectedClaim: legacyGeneration, Now: now.Add(2 * time.Minute),
			},
			Error: "genuine provider failure",
		},
		RetryAt: now.Add(3 * time.Minute),
	})
	if err != nil {
		t.Fatalf("RetryAt() error = %v", err)
	}
	if failure.FailureCount != 1 {
		t.Fatalf("failure count = %d, want 1: the legacy claim count must not preload the budget",
			failure.FailureCount)
	}
	if failure.State != jobs.StateRetryWait {
		t.Fatalf("state = %q, want RETRY_WAIT with budget still available", failure.State)
	}
}
