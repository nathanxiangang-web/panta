package postgres

// Gate 3.9 (D-031) requires the acquired-result identity to be captured
// atomically with the provider-success handoff, and requires migration 0011 to
// fence every new write while leaving pre-Gate-3.9 rows untouched. These tests
// drive acquisition.ProviderOutcomeService and the real PostgreSQL repository
// against a real database and read every result back with real SQL.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
	"github.com/nathanxiangang-web/panta/migrations"
)

// expectedNameFrozen is the direct-child identity already frozen on the Manifest
// in the identity tests. It is deliberately not path-cleaned or normalized.
const expectedNameFrozen = "gate39-frozen.bin"

// expectedNameValidWrite is a valid direct-child identity used to prove the
// database accepts a well-formed new write and rejects malformed ones.
const expectedNameValidWrite = "gate39-valid.bin"

// expectedNameLaterStateConstraint is the migration 0011 constraint that fences
// AWAITING_VISIBILITY / AWAITING_CANONICAL / READY without a durable identity.
const expectedNameLaterStateConstraint = "acquisition_manifests_later_state_requires_expected_name"

// --- seeding ------------------------------------------------------------------

type expectedNameFixture struct {
	manifestID acquisition.ManifestID
	jobID      jobs.JobID
	owner      string
	attempt    int
}

// expectedNameSuffix renders the two-character suffix the fixture IDs expect.
func expectedNameSuffix(index int) string {
	return fmt.Sprintf("%02d", index)
}

// seedExpectedNameJob inserts one RUNNING ACQUISITION Job with an unexpired lease
// and its linked Manifest in the requested state, carrying the requested durable
// identity. claimGeneration is the fenced claim generation, not a failure count:
// the failure budget is seeded at the default 5 and never fences this worker.
func seedExpectedNameJob(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	suffix string,
	manifestState acquisition.State,
	expectedName *string,
	owner string,
	claimGeneration int,
	leaseEnd time.Time,
) expectedNameFixture {
	t.Helper()
	now := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	manifestID := acquisition.ManifestID("39000000-0000-4000-8000-0000000000" + suffix)
	jobID := jobs.JobID("39000000-0000-4000-8000-0000000001" + suffix)
	key := acquisition.AcquisitionJobIdempotencyKey(manifestID)
	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    claim_attempts, attempt_count, max_attempts, lease_owner, lease_expires_at,
    created_at, updated_at, started_at
) VALUES ($1, $2, $3, 'RUNNING', $4, $5, 0, 5, $6, $7, $8, $8, $8)`,
		string(jobID), acquisition.JobTypeAcquisition,
		[]byte(fmt.Sprintf(`{"manifest_id":"%s","schema_version":1}`, manifestID)),
		key, claimGeneration, owner, leaseEnd, now,
	); err != nil {
		t.Fatalf("seed RUNNING ACQUISITION job: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, expected_name, target_storage_binding_id,
    target_path, job_id, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, '/downloads/item', $4, $5, $6, $6)`,
		string(manifestID), expectedName, string(bindingID), string(jobID), string(manifestState), now,
	); err != nil {
		t.Fatalf("seed %s Manifest: %v", manifestState, err)
	}
	return expectedNameFixture{
		manifestID: manifestID, jobID: jobID, owner: owner, attempt: claimGeneration,
	}
}

// newExpectedNameFixture migrates a fresh schema and returns the real
// repositories plus the storage binding the seeded Manifests point at.
func newExpectedNameFixture(t *testing.T) (context.Context, *pgxpool.Pool, *ProviderOutcomeRepository, *AcquisitionManifestRepository, *JobRepository, storage.BindingID) {
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
	bindingID := storage.BindingID("39000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "expected-name-root")

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

func expectedNameServiceFor(t *testing.T, repository *ProviderOutcomeRepository) *acquisition.ProviderOutcomeService {
	t.Helper()
	service, err := acquisition.NewProviderOutcomeService(repository)
	if err != nil {
		t.Fatalf("NewProviderOutcomeService() error = %v", err)
	}
	return service
}

// expectedNameRequest builds a fenced handoff request for any provider outcome,
// filling the retry timestamp and error message the frozen mapping requires.
func expectedNameRequest(fixture expectedNameFixture, outcome acquisition.ProviderOutcome, now time.Time) acquisition.ProviderOutcomeRequest {
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
	return request
}

func expectedNameSuccessRequest(fixture expectedNameFixture, now time.Time, providerResultName *string) acquisition.ProviderOutcomeRequest {
	request := expectedNameRequest(fixture, acquisition.ProviderOutcomeSucceeded, now)
	request.ProviderResultName = providerResultName
	return request
}

// --- real SQL read-back -------------------------------------------------------

type expectedNameManifestRow struct {
	state        string
	expectedName *string
	updatedAt    time.Time
}

func readExpectedNameManifestRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, manifestID string) expectedNameManifestRow {
	t.Helper()
	var row expectedNameManifestRow
	var expectedName sql.NullString
	if err := pool.QueryRow(ctx, `
SELECT state, expected_name, updated_at
FROM acquisition_manifests
WHERE manifest_id = $1`, manifestID).Scan(&row.state, &expectedName, &row.updatedAt); err != nil {
		t.Fatalf("read durable Manifest %s: %v", manifestID, err)
	}
	row.expectedName = stringPointer(expectedName)
	return row
}

type expectedNameJobRow struct {
	state      string
	leaseOwner *string
	updatedAt  time.Time
}

func readExpectedNameJobRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, jobID string) expectedNameJobRow {
	t.Helper()
	var row expectedNameJobRow
	var leaseOwner sql.NullString
	if err := pool.QueryRow(ctx, `
SELECT state, lease_owner, updated_at
FROM jobs
WHERE job_id = $1`, jobID).Scan(&row.state, &leaseOwner, &row.updatedAt); err != nil {
		t.Fatalf("read durable Job %s: %v", jobID, err)
	}
	row.leaseOwner = stringPointer(leaseOwner)
	return row
}

func expectedNameDisplay(value *string) string {
	if value == nil {
		return "NULL"
	}
	return fmt.Sprintf("%q", *value)
}

func expectedNameIdentityEqual(t *testing.T, got, want *string, context string) {
	t.Helper()
	switch {
	case got == nil && want == nil:
		return
	case got == nil || want == nil:
		t.Fatalf("%s: expected_name = %s, want %s", context, expectedNameDisplay(got), expectedNameDisplay(want))
	case *got != *want:
		t.Fatalf("%s: expected_name = %q, want %q byte-identical", context, *got, *want)
	}
}

func expectedNameIsConflict(err error) bool {
	return errors.Is(err, acquisition.ErrProviderOutcomeConflict) ||
		errors.Is(err, acquisition.ErrProviderOutcomeManifestState)
}

// expectedNameRequireCheckViolation proves the database, not the Go domain
// validator, rejected the write.
func expectedNameRequireCheckViolation(t *testing.T, err error) *pgconn.PgError {
	t.Helper()
	if err == nil {
		t.Fatal("database accepted a write the identity constraint must reject")
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		t.Fatalf("error = %v, want a PostgreSQL error", err)
	}
	if postgresError.Code != "23514" {
		t.Fatalf("SQLSTATE = %s (%s), want 23514 check_violation", postgresError.Code, postgresError.Message)
	}
	return postgresError
}

// --- test 12: atomic identity capture on provider success ---------------------

func TestPostgresExpectedNameStoresProviderIdentityAtomicallyOnSuccess(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	service := expectedNameServiceFor(t, outcomes)
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(10),
		acquisition.StateActive, nil, "worker-a", 1, time.Now().UTC().Add(time.Hour))

	before := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	if before.state != string(acquisition.StateActive) || before.expectedName != nil {
		t.Fatalf("seeded Manifest = %#v, want ACTIVE with NULL expected_name", before)
	}

	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	providerName := "gate39-provider-result.bin"
	request := expectedNameSuccessRequest(fixture, now, &providerName)
	result, err := service.Commit(ctx, request)
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Commit() reported Changed=false for a fresh success handoff")
	}
	if result.Manifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("returned Manifest state = %q, want AWAITING_VISIBILITY", result.Manifest.State)
	}
	expectedNameIdentityEqual(t, result.Manifest.ExpectedName, &providerName, "returned Manifest")
	if result.Job.State != jobs.StateRetryWait {
		t.Fatalf("returned Job state = %q, want RETRY_WAIT", result.Job.State)
	}

	// Read all three durable facts back with real SQL, not the returned structs.
	var (
		manifestState string
		expectedName  sql.NullString
		jobState      string
		leaseOwner    sql.NullString
		nextAttemptAt sql.NullTime
	)
	if err := pool.QueryRow(ctx, `
SELECT manifest.state, manifest.expected_name, job.state, job.lease_owner, job.next_attempt_at
FROM acquisition_manifests AS manifest
JOIN jobs AS job ON job.job_id = manifest.job_id
WHERE manifest.manifest_id = $1`, string(fixture.manifestID)).Scan(
		&manifestState, &expectedName, &jobState, &leaseOwner, &nextAttemptAt); err != nil {
		t.Fatalf("read back durable handoff: %v", err)
	}
	if manifestState != string(acquisition.StateAwaitingVisibility) {
		t.Fatalf("durable Manifest state = %q, want AWAITING_VISIBILITY", manifestState)
	}
	if !expectedName.Valid || expectedName.String != providerName {
		t.Fatalf("durable expected_name = %v, want %q", expectedName, providerName)
	}
	if jobState != string(jobs.StateRetryWait) {
		t.Fatalf("durable Job state = %q, want RETRY_WAIT", jobState)
	}
	if leaseOwner.Valid {
		t.Fatalf("durable Job lease_owner = %q, want NULL", leaseOwner.String)
	}
	if !nextAttemptAt.Valid || !nextAttemptAt.Time.Equal(*request.RetryAt) {
		t.Fatalf("durable next_attempt_at = %v, want %v", nextAttemptAt, request.RetryAt)
	}
}

// --- test 13: same-name success is idempotent ---------------------------------

func TestPostgresExpectedNameSameProviderNameIsIdempotent(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	service := expectedNameServiceFor(t, outcomes)
	frozen := expectedNameFrozen
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(11),
		acquisition.StateActive, &frozen, "worker-a", 1, time.Now().UTC().Add(time.Hour))

	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	result, err := service.Commit(ctx, expectedNameSuccessRequest(fixture, now, &frozen))
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Commit() reported Changed=false for a fresh success handoff")
	}
	if result.Manifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("returned Manifest state = %q, want AWAITING_VISIBILITY", result.Manifest.State)
	}
	// The success branch advances ACTIVE -> AWAITING_VISIBILITY, so the accepted
	// behavior is that the milestone's updated_at is stamped with the plan Now while
	// the already-correct identity is preserved.
	if !result.Manifest.UpdatedAt.Equal(now) {
		t.Fatalf("Manifest updated_at = %v, want the plan Now %v", result.Manifest.UpdatedAt, now)
	}

	firstManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	expectedNameIdentityEqual(t, firstManifest.expectedName, &frozen, "same-name success")
	if firstManifest.state != string(acquisition.StateAwaitingVisibility) {
		t.Fatalf("durable Manifest state = %q, want AWAITING_VISIBILITY", firstManifest.state)
	}
	if !firstManifest.updatedAt.Equal(now) {
		t.Fatalf("durable Manifest updated_at = %v, want %v", firstManifest.updatedAt, now)
	}
	firstJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))
	if firstJob.state != string(jobs.StateRetryWait) {
		t.Fatalf("durable Job state = %q, want RETRY_WAIT", firstJob.state)
	}

	// The exact replay must succeed without rewriting anything.
	replay, err := service.Commit(ctx, expectedNameSuccessRequest(fixture, now.Add(6*time.Hour), &frozen))
	if err != nil {
		t.Fatalf("replay Commit() error = %v", err)
	}
	if replay.Changed {
		t.Fatal("replay Commit() reported Changed=true")
	}
	secondManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	expectedNameIdentityEqual(t, secondManifest.expectedName, firstManifest.expectedName, "same-name replay")
	if !secondManifest.updatedAt.Equal(firstManifest.updatedAt) {
		t.Fatalf("replay rewrote Manifest updated_at: %v -> %v", firstManifest.updatedAt, secondManifest.updatedAt)
	}
	secondJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))
	if !secondJob.updatedAt.Equal(firstJob.updatedAt) {
		t.Fatalf("replay rewrote Job updated_at: %v -> %v", firstJob.updatedAt, secondJob.updatedAt)
	}
}

// --- test 14: provider omission uses the frozen identity ----------------------

func TestPostgresExpectedNameUsesFrozenIdentityWhenProviderOmitsName(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	service := expectedNameServiceFor(t, outcomes)
	frozen := expectedNameFrozen
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(12),
		acquisition.StateActive, &frozen, "worker-a", 1, time.Now().UTC().Add(time.Hour))

	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	result, err := service.Commit(ctx, expectedNameSuccessRequest(fixture, now, nil))
	if err != nil {
		t.Fatalf("Commit() with no provider result name error = %v", err)
	}
	if !result.Changed {
		t.Fatal("Commit() reported Changed=false for a fresh success handoff")
	}
	if result.Manifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("returned Manifest state = %q, want AWAITING_VISIBILITY", result.Manifest.State)
	}
	expectedNameIdentityEqual(t, result.Manifest.ExpectedName, &frozen, "returned Manifest")

	durableManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	expectedNameIdentityEqual(t, durableManifest.expectedName, &frozen, "omitted provider result name")
	if durableManifest.state != string(acquisition.StateAwaitingVisibility) {
		t.Fatalf("durable Manifest state = %q, want AWAITING_VISIBILITY", durableManifest.state)
	}
	durableJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))
	if durableJob.state != string(jobs.StateRetryWait) {
		t.Fatalf("durable Job state = %q, want RETRY_WAIT", durableJob.state)
	}
}

// --- test 15: a different provider name fails closed --------------------------

func TestPostgresExpectedNameDifferentProviderNameFailsClosed(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	service := expectedNameServiceFor(t, outcomes)
	frozen := expectedNameFrozen
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(13),
		acquisition.StateActive, &frozen, "worker-a", 1, time.Now().UTC().Add(time.Hour))

	beforeManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	beforeJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))

	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	different := "gate39-different-result.bin"
	if _, err := service.Commit(ctx, expectedNameSuccessRequest(fixture, now, &different)); !errors.Is(err, acquisition.ErrExpectedNameMismatch) {
		t.Fatalf("Commit() error = %v, want ErrExpectedNameMismatch", err)
	}

	afterManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	if afterManifest.state != string(acquisition.StateActive) {
		t.Fatalf("Manifest state = %q, want ACTIVE after the rejected handoff", afterManifest.state)
	}
	expectedNameIdentityEqual(t, afterManifest.expectedName, &frozen, "rejected mismatch")
	if !afterManifest.updatedAt.Equal(beforeManifest.updatedAt) {
		t.Fatalf("Manifest updated_at changed despite failing closed: %v -> %v",
			beforeManifest.updatedAt, afterManifest.updatedAt)
	}
	afterJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))
	if afterJob.state != string(jobs.StateRunning) {
		t.Fatalf("Job state = %q, want RUNNING after the rejected handoff", afterJob.state)
	}
	if afterJob.leaseOwner == nil || *afterJob.leaseOwner != "worker-a" {
		t.Fatalf("Job lease_owner = %v, want worker-a", afterJob.leaseOwner)
	}
	if !afterJob.updatedAt.Equal(beforeJob.updatedAt) {
		t.Fatalf("Job updated_at changed despite failing closed: %v -> %v", beforeJob.updatedAt, afterJob.updatedAt)
	}
}

// --- test 16: success with no usable identity fails closed --------------------

func TestPostgresExpectedNameMissingProviderNameFailsClosed(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	service := expectedNameServiceFor(t, outcomes)
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(14),
		acquisition.StateActive, nil, "worker-a", 1, time.Now().UTC().Add(time.Hour))

	beforeManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	beforeJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))

	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	if _, err := service.Commit(ctx, expectedNameSuccessRequest(fixture, now, nil)); !errors.Is(err, acquisition.ErrExpectedNameMissing) {
		t.Fatalf("Commit() error = %v, want ErrExpectedNameMissing", err)
	}

	afterManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	if afterManifest.state != string(acquisition.StateActive) {
		t.Fatalf("Manifest state = %q, want ACTIVE after the rejected handoff", afterManifest.state)
	}
	if afterManifest.expectedName != nil {
		t.Fatalf("expected_name = %q, want NULL after the rejected handoff", *afterManifest.expectedName)
	}
	if !afterManifest.updatedAt.Equal(beforeManifest.updatedAt) {
		t.Fatalf("Manifest updated_at changed despite failing closed: %v -> %v",
			beforeManifest.updatedAt, afterManifest.updatedAt)
	}
	afterJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))
	if afterJob.state != string(jobs.StateRunning) {
		t.Fatalf("Job state = %q, want RUNNING after the rejected handoff", afterJob.state)
	}
	if !afterJob.updatedAt.Equal(beforeJob.updatedAt) {
		t.Fatalf("Job updated_at changed despite failing closed: %v -> %v", beforeJob.updatedAt, afterJob.updatedAt)
	}
}

// --- test 17: non-success outcomes never touch identity -----------------------

func TestPostgresExpectedNameUntouchedForNonSuccessOutcomes(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	service := expectedNameServiceFor(t, outcomes)
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	frozen := expectedNameFrozen

	outcomesUnderTest := []acquisition.ProviderOutcome{
		acquisition.ProviderOutcomeInProgress,
		acquisition.ProviderOutcomeFailed,
		acquisition.ProviderOutcomeCanceled,
		acquisition.ProviderOutcomeRecovery,
	}
	identities := []struct {
		name  string
		value *string
	}{
		{name: "frozen-identity", value: &frozen},
		{name: "no-identity", value: nil},
	}

	index := 20
	for _, outcome := range outcomesUnderTest {
		transition, err := acquisition.ProviderOutcomeTransitionFor(outcome)
		if err != nil {
			t.Fatalf("ProviderOutcomeTransitionFor(%s) error = %v", outcome, err)
		}
		for _, identity := range identities {
			suffix := expectedNameSuffix(index)
			index++
			seedIdentity := identity.value
			t.Run(string(outcome)+"/"+identity.name, func(t *testing.T) {
				fixture := seedExpectedNameJob(t, ctx, pool, bindingID, suffix,
					acquisition.StateActive, seedIdentity, "worker-a", 1, time.Now().UTC().Add(time.Hour))
				before := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))

				result, err := service.Commit(ctx, expectedNameRequest(fixture, outcome, now))
				if err != nil {
					t.Fatalf("Commit(%s) error = %v", outcome, err)
				}
				if !result.Changed {
					t.Fatalf("Commit(%s) reported Changed=false", outcome)
				}

				after := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
				expectedNameIdentityEqual(t, after.expectedName, before.expectedName, string(outcome)+" identity")
				if after.state != string(transition.ManifestState) {
					t.Fatalf("%s Manifest state = %q, want %q", outcome, after.state, transition.ManifestState)
				}
				durableJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))
				if durableJob.state != string(transition.JobState) {
					t.Fatalf("%s Job state = %q, want %q", outcome, durableJob.state, transition.JobState)
				}
			})
		}
	}
}

// --- test 18: exact success replay preserves identity and timestamps ----------

func TestPostgresExpectedNameExactSuccessReplayPreservesIdentityAndTimestamps(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	service := expectedNameServiceFor(t, outcomes)
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	replayFrozen := "gate39-replay-frozen.bin"

	cases := []struct {
		name     string
		suffix   string
		frozen   *string
		provider string
	}{
		{name: "captured-then-replayed", suffix: expectedNameSuffix(30), frozen: nil, provider: "gate39-replay-captured.bin"},
		{name: "confirmed-then-replayed", suffix: expectedNameSuffix(31), frozen: &replayFrozen, provider: replayFrozen},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := seedExpectedNameJob(t, ctx, pool, bindingID, test.suffix,
				acquisition.StateActive, test.frozen, "worker-a", 1, time.Now().UTC().Add(time.Hour))
			providerName := test.provider

			first, err := service.Commit(ctx, expectedNameSuccessRequest(fixture, now, &providerName))
			if err != nil {
				t.Fatalf("first Commit() error = %v", err)
			}
			if !first.Changed {
				t.Fatal("first Commit() reported Changed=false")
			}
			firstManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
			expectedNameIdentityEqual(t, firstManifest.expectedName, &providerName, "first commit")
			firstJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))

			replay, err := service.Commit(ctx, expectedNameSuccessRequest(fixture, now.Add(6*time.Hour), &providerName))
			if err != nil {
				t.Fatalf("replay Commit() error = %v", err)
			}
			if replay.Changed {
				t.Fatal("replay Commit() reported Changed=true")
			}
			expectedNameIdentityEqual(t, replay.Manifest.ExpectedName, &providerName, "replay returned Manifest")
			if !replay.Manifest.UpdatedAt.Equal(firstManifest.updatedAt) {
				t.Fatalf("replay returned Manifest updated_at %v, want %v", replay.Manifest.UpdatedAt, firstManifest.updatedAt)
			}
			if !replay.Job.UpdatedAt.Equal(firstJob.updatedAt) {
				t.Fatalf("replay returned Job updated_at %v, want %v", replay.Job.UpdatedAt, firstJob.updatedAt)
			}

			secondManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
			expectedNameIdentityEqual(t, secondManifest.expectedName, firstManifest.expectedName, "replayed identity")
			expectedNameIdentityEqual(t, secondManifest.expectedName, &providerName, "replayed identity")
			if !secondManifest.updatedAt.Equal(firstManifest.updatedAt) {
				t.Fatalf("replay rewrote Manifest updated_at: %v -> %v", firstManifest.updatedAt, secondManifest.updatedAt)
			}
			secondJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))
			if !secondJob.updatedAt.Equal(firstJob.updatedAt) {
				t.Fatalf("replay rewrote Job updated_at: %v -> %v", firstJob.updatedAt, secondJob.updatedAt)
			}
		})
	}
}

// --- test 19: concurrent competing proposals have exactly one winner ----------

func TestPostgresExpectedNameConcurrentSuccessHandoffsHaveOneWinner(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	service := expectedNameServiceFor(t, outcomes)
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(32),
		acquisition.StateActive, nil, "worker-a", 1, time.Now().UTC().Add(time.Hour))
	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)

	const contenders = 8
	proposals := make([]string, contenders)
	for index := range proposals {
		proposals[index] = fmt.Sprintf("gate39-race-%02d.bin", index)
	}

	results := make([]acquisition.ProviderOutcomeResult, contenders)
	errs := make([]error, contenders)
	var waitGroup sync.WaitGroup
	start := make(chan struct{})
	for index := 0; index < contenders; index++ {
		waitGroup.Add(1)
		go func(position int) {
			defer waitGroup.Done()
			<-start
			name := proposals[position]
			results[position], errs[position] = service.Commit(ctx, expectedNameSuccessRequest(fixture, now, &name))
		}(index)
	}
	close(start)
	waitGroup.Wait()

	winners := make([]int, 0, 1)
	for index := range errs {
		if errs[index] == nil && results[index].Changed {
			winners = append(winners, index)
		}
	}
	if len(winners) != 1 {
		t.Fatalf("durable winners = %d, want exactly 1 (errors = %v)", len(winners), errs)
	}
	winnerIndex := winners[0]
	if results[winnerIndex].Manifest.ExpectedName == nil {
		t.Fatal("winner returned no durable expected_name")
	}
	winnerName := *results[winnerIndex].Manifest.ExpectedName
	if winnerName != proposals[winnerIndex] {
		t.Fatalf("winning durable name = %q, want its own proposal %q", winnerName, proposals[winnerIndex])
	}
	if results[winnerIndex].Manifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("winner Manifest state = %q, want AWAITING_VISIBILITY", results[winnerIndex].Manifest.State)
	}
	if results[winnerIndex].Job.State != jobs.StateRetryWait {
		t.Fatalf("winner Job state = %q, want RETRY_WAIT", results[winnerIndex].Job.State)
	}

	for index := range errs {
		if index == winnerIndex {
			continue
		}
		if errs[index] != nil {
			if !expectedNameIsConflict(errs[index]) {
				t.Fatalf("loser %d error = %v, want a typed conflict", index, errs[index])
			}
			continue
		}
		if results[index].Changed {
			t.Fatalf("loser %d also reported Changed=true", index)
		}
		// The replay path must hand back the winner's durable identity, never the
		// loser's competing proposal.
		if results[index].Manifest.ExpectedName == nil || *results[index].Manifest.ExpectedName != winnerName {
			t.Fatalf("loser %d returned expected_name %v, want the winner %q",
				index, expectedNameDisplay(results[index].Manifest.ExpectedName), winnerName)
		}
	}

	durable := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	if durable.expectedName == nil || *durable.expectedName != winnerName {
		t.Fatalf("durable expected_name = %v, want the winner %q", expectedNameDisplay(durable.expectedName), winnerName)
	}
	for index, proposal := range proposals {
		if index == winnerIndex {
			continue
		}
		if *durable.expectedName == proposal {
			t.Fatalf("losing proposal %q became the durable identity", proposal)
		}
	}
}

// --- test 20: a failure between the mutations rolls back both rows ------------

func TestPostgresExpectedNameRollbackRestoresIdentityAndBothStates(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newExpectedNameFixture(t)
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(33),
		acquisition.StateActive, nil, "worker-a", 1, time.Now().UTC().Add(time.Hour))
	beforeManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	beforeJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))

	injected := errors.New("injected failure between the Manifest and Job mutations")
	outcomes.afterManifestUpdate = func(context.Context, pgx.Tx) error { return injected }
	service := expectedNameServiceFor(t, outcomes)

	now := time.Date(2026, 9, 30, 9, 30, 0, 0, time.UTC)
	providerName := "gate39-rolled-back.bin"
	if _, err := service.Commit(ctx, expectedNameSuccessRequest(fixture, now, &providerName)); !errors.Is(err, acquisition.ErrProviderOutcomePersistence) {
		t.Fatalf("Commit() error = %v, want ErrProviderOutcomePersistence", err)
	}
	outcomes.afterManifestUpdate = nil

	afterManifest := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
	if afterManifest.state != string(acquisition.StateActive) {
		t.Fatalf("Manifest state = %q, want ACTIVE after rollback", afterManifest.state)
	}
	if afterManifest.expectedName != nil {
		t.Fatalf("expected_name = %q, want NULL after rollback", *afterManifest.expectedName)
	}
	if !afterManifest.updatedAt.Equal(beforeManifest.updatedAt) {
		t.Fatalf("Manifest updated_at changed despite rollback: %v -> %v",
			beforeManifest.updatedAt, afterManifest.updatedAt)
	}
	afterJob := readExpectedNameJobRow(t, ctx, pool, string(fixture.jobID))
	if afterJob.state != string(jobs.StateRunning) {
		t.Fatalf("Job state = %q, want RUNNING after rollback", afterJob.state)
	}
	if afterJob.leaseOwner == nil || *afterJob.leaseOwner != "worker-a" {
		t.Fatalf("Job lease_owner = %v, want worker-a after rollback", afterJob.leaseOwner)
	}
	if !afterJob.updatedAt.Equal(beforeJob.updatedAt) {
		t.Fatalf("Job updated_at changed despite rollback: %v -> %v", beforeJob.updatedAt, afterJob.updatedAt)
	}
}

// --- test 22: migration 0011 rejects malformed identities ---------------------

func TestPostgresExpectedNameRejectsInvalidIdentityAtDatabase(t *testing.T) {
	ctx, pool, _, _, _, bindingID := newExpectedNameFixture(t)
	valid := expectedNameValidWrite
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(40),
		acquisition.StateActive, &valid, "worker-a", 1, time.Now().UTC().Add(time.Hour))
	now := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)

	invalid := []string{"a/b", `a\b`, ".", "..", "   "}
	for index, value := range invalid {
		t.Run(fmt.Sprintf("UPDATE rejects %q", value), func(t *testing.T) {
			_, err := pool.Exec(ctx, `
UPDATE acquisition_manifests SET expected_name = $2 WHERE manifest_id = $1`,
				string(fixture.manifestID), value)
			postgresError := expectedNameRequireCheckViolation(t, err)
			if !strings.Contains(postgresError.ConstraintName, "expected_name") {
				t.Fatalf("constraint = %q, want an expected_name constraint", postgresError.ConstraintName)
			}
			durable := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
			expectedNameIdentityEqual(t, durable.expectedName, &valid, "rejected UPDATE")
		})

		t.Run(fmt.Sprintf("INSERT rejects %q", value), func(t *testing.T) {
			manifestID := fmt.Sprintf("39000000-0000-4000-8000-0000000002%02d", index)
			_, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, expected_name,
    target_storage_binding_id, target_path, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, '/downloads/item', 'FAILED', $4, $4)`,
				manifestID, value, string(bindingID), now)
			postgresError := expectedNameRequireCheckViolation(t, err)
			if !strings.Contains(postgresError.ConstraintName, "expected_name") {
				t.Fatalf("constraint = %q, want an expected_name constraint", postgresError.ConstraintName)
			}
			var present bool
			if err := pool.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM acquisition_manifests WHERE manifest_id = $1)`,
				manifestID).Scan(&present); err != nil {
				t.Fatalf("inspect rejected INSERT: %v", err)
			}
			if present {
				t.Fatal("rejected INSERT left a durable row behind")
			}
		})
	}
}

// --- test 23: later states require a durable identity on new writes -----------

func TestPostgresExpectedNameLaterStatesRequireIdentityAtDatabase(t *testing.T) {
	ctx, pool, _, _, _, bindingID := newExpectedNameFixture(t)
	fixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(41),
		acquisition.StateActive, nil, "worker-a", 1, time.Now().UTC().Add(time.Hour))

	laterStates := []acquisition.State{
		acquisition.StateAwaitingVisibility,
		acquisition.StateAwaitingCanonical,
		acquisition.StateReady,
	}
	for _, state := range laterStates {
		t.Run("rejects "+string(state)+" without identity", func(t *testing.T) {
			_, err := pool.Exec(ctx, `
UPDATE acquisition_manifests SET state = $2 WHERE manifest_id = $1`,
				string(fixture.manifestID), string(state))
			postgresError := expectedNameRequireCheckViolation(t, err)
			if postgresError.ConstraintName != expectedNameLaterStateConstraint {
				t.Fatalf("constraint = %q, want %q", postgresError.ConstraintName, expectedNameLaterStateConstraint)
			}
			durable := readExpectedNameManifestRow(t, ctx, pool, string(fixture.manifestID))
			if durable.state != string(acquisition.StateActive) || durable.expectedName != nil {
				t.Fatalf("rejected write changed the row: state=%q expected_name=%s",
					durable.state, expectedNameDisplay(durable.expectedName))
			}
		})
	}

	valid := expectedNameValidWrite
	for index, state := range laterStates {
		t.Run("accepts "+string(state)+" with identity", func(t *testing.T) {
			stateFixture := seedExpectedNameJob(t, ctx, pool, bindingID, expectedNameSuffix(42+index),
				acquisition.StateActive, nil, "worker-a", 1, time.Now().UTC().Add(time.Hour))
			if _, err := pool.Exec(ctx, `
UPDATE acquisition_manifests SET state = $2, expected_name = $3 WHERE manifest_id = $1`,
				string(stateFixture.manifestID), string(state), valid); err != nil {
				t.Fatalf("database rejected a valid %s write: %v", state, err)
			}
			durable := readExpectedNameManifestRow(t, ctx, pool, string(stateFixture.manifestID))
			if durable.state != string(state) {
				t.Fatalf("Manifest state = %q, want %q", durable.state, state)
			}
			expectedNameIdentityEqual(t, durable.expectedName, &valid, string(state)+" write")
		})
	}
}

// --- tests 24-25: the v10 -> v11 upgrade --------------------------------------

// applyExpectedNameSchemaThroughV10 applies migrations 1-10 only, exactly as the
// accepted claim-generation upgrade test constructs a partial Migrator.
func applyExpectedNameSchemaThroughV10(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	history, err := migrations.All()
	if err != nil {
		t.Fatalf("migrations.All() error = %v", err)
	}
	if len(history) != 11 {
		t.Fatalf("migration history length = %d, want 11", len(history))
	}
	if history[9].Version != 10 || history[10].Version != 11 {
		t.Fatalf("migration order = %d then %d, want 10 then 11", history[9].Version, history[10].Version)
	}
	legacy := &Migrator{pool: pool, migrations: history[:10]}
	status, err := legacy.Apply(ctx)
	if err != nil {
		t.Fatalf("apply migrations through version 10: %v", err)
	}
	if !status.Compatible || status.CurrentVersion != 10 || status.LatestVersion != 10 {
		t.Fatalf("pre-upgrade status = %#v", status)
	}
}

// applyExpectedNameMigration11 applies the single pending version 11 migration.
func applyExpectedNameMigration11(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("apply migration 11: %v", err)
	}
	if !status.Compatible || status.CurrentVersion != 11 || status.LatestVersion != 11 {
		t.Fatalf("post-upgrade status = %#v", status)
	}
}

// seedExpectedNameHistoricalManifest inserts one AWAITING_CANONICAL row directly
// under the pre-version-11 schema, where the identity constraint does not exist yet.
func seedExpectedNameHistoricalManifest(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	manifestID string,
	expectedName *string,
	now time.Time,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, expected_name,
    target_storage_binding_id, target_path, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, '/downloads/item', 'AWAITING_CANONICAL', $4, $4)`,
		manifestID, expectedName, string(bindingID), now); err != nil {
		t.Fatalf("seed historical AWAITING_CANONICAL Manifest %s: %v", manifestID, err)
	}
}

// laterStateConstraintState reports whether the migration 0011 later-state
// constraint exists and whether PostgreSQL validated it.
func laterStateConstraintState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (bool, bool) {
	t.Helper()
	var validated bool
	err := pool.QueryRow(ctx, `
SELECT convalidated
FROM pg_constraint
WHERE conname = $1 AND conrelid = 'acquisition_manifests'::regclass`,
		expectedNameLaterStateConstraint).Scan(&validated)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false
	}
	if err != nil {
		t.Fatalf("inspect %s: %v", expectedNameLaterStateConstraint, err)
	}
	return true, validated
}

// TestPostgresExpectedNameUpgradeFromV10KeepsUnidentifiableHistoricalRow proves
// the migration is compatible with a pre-Gate-3.9 row that has no identity.
func TestPostgresExpectedNameUpgradeFromV10KeepsUnidentifiableHistoricalRow(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	applyExpectedNameSchemaThroughV10(t, ctx, pool)

	exists, _ := laterStateConstraintState(t, ctx, pool)
	if exists {
		t.Fatal("the later-state identity constraint already exists before version 11")
	}

	bindingID := storage.BindingID("39000000-0000-4000-8000-0000000000a1")
	seedStorageBinding(t, ctx, pool, bindingID, "expected-name-upgrade-debt")
	historicalID := "39000000-0000-4000-8000-000000000300"
	now := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	seedExpectedNameHistoricalManifest(t, ctx, pool, bindingID, historicalID, nil, now)

	applyExpectedNameMigration11(t, ctx, pool)

	exists, validated := laterStateConstraintState(t, ctx, pool)
	if !exists {
		t.Fatal("version 11 did not add the later-state identity constraint")
	}
	if validated {
		t.Fatal("the later-state constraint is VALID; the v10 upgrade cannot rely on NOT VALID")
	}

	durable := readExpectedNameManifestRow(t, ctx, pool, historicalID)
	if durable.state != string(acquisition.StateAwaitingCanonical) {
		t.Fatalf("historical Manifest state = %q, want AWAITING_CANONICAL", durable.state)
	}
	if durable.expectedName != nil {
		t.Fatalf("historical expected_name = %q, want NULL (explicit recovery debt)", *durable.expectedName)
	}
}

// TestPostgresExpectedNameUpgradeFromV10KeepsValidHistoricalIdentity proves the
// migration leaves an already-valid historical identity byte-identical.
func TestPostgresExpectedNameUpgradeFromV10KeepsValidHistoricalIdentity(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	applyExpectedNameSchemaThroughV10(t, ctx, pool)

	bindingID := storage.BindingID("39000000-0000-4000-8000-0000000000a2")
	seedStorageBinding(t, ctx, pool, bindingID, "expected-name-upgrade-valid")
	historicalID := "39000000-0000-4000-8000-000000000302"
	historicalName := "gate39-historical-valid.bin"
	now := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	seedExpectedNameHistoricalManifest(t, ctx, pool, bindingID, historicalID, &historicalName, now)

	applyExpectedNameMigration11(t, ctx, pool)

	durable := readExpectedNameManifestRow(t, ctx, pool, historicalID)
	if durable.state != string(acquisition.StateAwaitingCanonical) {
		t.Fatalf("historical Manifest state = %q, want AWAITING_CANONICAL", durable.state)
	}
	expectedNameIdentityEqual(t, durable.expectedName, &historicalName, "historical valid identity")
}
