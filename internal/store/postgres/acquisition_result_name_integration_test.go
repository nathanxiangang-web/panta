package postgres

// Gate 3.9 D-032: the durable acquired-result locator, proven against real
// PostgreSQL.
//
// The frozen separation this file tests is:
//
//	expected_name  request-time intent / fallback, never rewritten by provider
//	               execution, and never the acquired-result locator;
//	result_name    durable provider-stage acquired-result locator, resolved once
//	               from the provider name (intent is only the fallback), frozen
//	               when persisted, immutable afterwards.
//
// Two deliberate test techniques are used throughout:
//
//   - identity is read back with raw SQL, so the assertions do not depend on the
//     production reader agreeing with the writer;
//   - the success plan is built and then has ProviderResultName set directly on
//     the plan, so a malformed provider name reaches persistence exactly as a
//     defect would, instead of being rejected by request-level validation.

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

// resultNamePgConstraint is the named CHECK constraint migration 0011 adds.
const resultNamePgConstraint = "acquisition_manifests_result_name_direct_child"

// resultNamePgFixture is one seeded Manifest plus its linked RUNNING Job.
type resultNamePgFixture struct {
	manifestID acquisition.ManifestID
	jobID      jobs.JobID
	owner      string
	claim      int
	leaseEnd   time.Time
}

// --- unique-prefix helpers ----------------------------------------------------

func resultNamePgString(value string) *string {
	return &value
}

// newResultNamePgFixture migrates a fresh schema and returns the real
// repositories under test. It reuses the package's integration pool, schema reset,
// and StorageBinding seed helpers rather than redeclaring them.
func newResultNamePgFixture(t *testing.T) (context.Context, *pgxpool.Pool, *ProviderOutcomeRepository, *AcquisitionManifestRepository, *JobRepository, storage.BindingID) {
	t.Helper()
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 12 {
		t.Fatalf("Apply() = %#v, %v", status, err)
	}
	bindingID := storage.BindingID("39000000-0000-4000-8000-000000000000")
	seedStorageBinding(t, ctx, pool, bindingID, "result-name-root")

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

// seedResultNamePgFixture inserts one Manifest and its linked RUNNING ACQUISITION
// Job, so a fenced provider handoff can run against the real schema.
//
// state, expectedName, and resultName are written exactly as given (NULL when nil),
// so each case starts from the precise durable row D-032 talks about. suffix must
// be a unique two-character digit pair; it only shapes the two UUIDs.
func seedResultNamePgFixture(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	suffix string,
	owner string,
	claim int,
	state acquisition.State,
	expectedName *string,
	resultName *string,
	leaseEnd time.Time,
) resultNamePgFixture {
	t.Helper()
	now := time.Date(2026, 9, 30, 13, 0, 0, 0, time.UTC)
	manifestID := acquisition.ManifestID("39000000-0000-4000-8000-0000000000" + suffix)
	jobID := jobs.JobID("39000000-0000-4000-8000-0000000001" + suffix)
	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    claim_attempts, attempt_count, max_attempts, lease_owner, lease_expires_at,
    created_at, updated_at, started_at
) VALUES ($1, $2, $3, 'RUNNING', $4, $5, 0, 5, $6, $7, $8, $8, $8)`,
		string(jobID), acquisition.JobTypeAcquisition,
		fmt.Sprintf(`{"manifest_id":"%s","schema_version":1}`, manifestID),
		acquisition.AcquisitionJobIdempotencyKey(manifestID),
		claim, owner, leaseEnd, now,
	); err != nil {
		t.Fatalf("seed RUNNING ACQUISITION job: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, expected_name, result_name,
    target_storage_binding_id, target_path, job_id, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, $4, '/downloads/item', $5, $6, $7, $7)`,
		string(manifestID), expectedName, resultName, string(bindingID),
		string(jobID), string(state), now,
	); err != nil {
		t.Fatalf("seed %s Manifest: %v", state, err)
	}
	return resultNamePgFixture{
		manifestID: manifestID,
		jobID:      jobID,
		owner:      owner,
		claim:      claim,
		leaseEnd:   leaseEnd,
	}
}

// resultNamePgPlan builds the plan the frozen D-026 mapping produces for one
// outcome, with no provider-reported name.
func resultNamePgPlan(t *testing.T, fixture resultNamePgFixture, outcome acquisition.ProviderOutcome, now time.Time) acquisition.ProviderOutcomePlan {
	t.Helper()
	request := acquisition.ProviderOutcomeRequest{
		ManifestID:    fixture.manifestID,
		JobID:         fixture.jobID,
		Owner:         fixture.owner,
		ExpectedClaim: fixture.claim,
		Outcome:       outcome,
		Now:           now,
	}
	transition, err := acquisition.ProviderOutcomeTransitionFor(outcome)
	if err != nil {
		t.Fatalf("ProviderOutcomeTransitionFor(%s) error = %v", outcome, err)
	}
	if transition.RequiresRetryAt {
		retry := now.Add(90 * time.Second)
		request.RetryAt = &retry
	}
	if transition.RequiresError {
		message := "provider stage did not progress"
		request.ErrorMessage = &message
	}
	_, plan, err := acquisition.BuildProviderOutcomePlan(request)
	if err != nil {
		t.Fatalf("BuildProviderOutcomePlan(%s) error = %v", outcome, err)
	}
	return plan
}

// resultNamePgSuccessPlan is resultNamePgPlan for PROVIDER_SUCCEEDED with the
// provider-reported name placed on the plan itself.
func resultNamePgSuccessPlan(t *testing.T, fixture resultNamePgFixture, now time.Time, providerResultName *string) acquisition.ProviderOutcomePlan {
	t.Helper()
	plan := resultNamePgPlan(t, fixture, acquisition.ProviderOutcomeSucceeded, now)
	plan.ProviderResultName = providerResultName
	return plan
}

// resultNamePgReArm returns the same Manifest and Job to the exact shape a later
// provider attempt would find: the Manifest ACTIVE again and the Job RUNNING under
// a fresh, higher claim generation. It is only used to reach a fresh (non-replay)
// success handoff while a durable result_name is already persisted.
func resultNamePgReArm(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fixture resultNamePgFixture, leaseEnd time.Time) resultNamePgFixture {
	t.Helper()
	if _, err := pool.Exec(ctx, `
UPDATE acquisition_manifests SET state = 'ACTIVE' WHERE manifest_id = $1`,
		string(fixture.manifestID)); err != nil {
		t.Fatalf("re-arm Manifest: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE jobs
SET state = 'RUNNING',
    lease_owner = $2,
    lease_expires_at = $3,
    next_attempt_at = NULL,
    claim_attempts = claim_attempts + 1
WHERE job_id = $1`,
		string(fixture.jobID), fixture.owner, leaseEnd); err != nil {
		t.Fatalf("re-arm Job: %v", err)
	}
	fixture.claim++
	fixture.leaseEnd = leaseEnd
	return fixture
}

// --- raw-SQL durable readers --------------------------------------------------

type resultNamePgDurableManifest struct {
	ResultName   *string
	ExpectedName *string
	State        acquisition.State
	UpdatedAt    time.Time
}

func readResultNamePgManifest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id acquisition.ManifestID) resultNamePgDurableManifest {
	t.Helper()
	var resultName, expectedName sql.NullString
	var state string
	var updatedAt time.Time
	if err := pool.QueryRow(ctx, `
SELECT result_name, expected_name, state, updated_at
FROM acquisition_manifests
WHERE manifest_id = $1`, string(id)).Scan(&resultName, &expectedName, &state, &updatedAt); err != nil {
		t.Fatalf("read durable Manifest %s: %v", id, err)
	}
	row := resultNamePgDurableManifest{State: acquisition.State(state), UpdatedAt: updatedAt}
	if resultName.Valid {
		value := resultName.String
		row.ResultName = &value
	}
	if expectedName.Valid {
		value := expectedName.String
		row.ExpectedName = &value
	}
	return row
}

type resultNamePgDurableJob struct {
	State          jobs.State
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	UpdatedAt      time.Time
}

func readResultNamePgJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id jobs.JobID) resultNamePgDurableJob {
	t.Helper()
	var state string
	var leaseOwner sql.NullString
	var leaseExpiresAt sql.NullTime
	var updatedAt time.Time
	if err := pool.QueryRow(ctx, `
SELECT state, lease_owner, lease_expires_at, updated_at
FROM jobs
WHERE job_id = $1`, string(id)).Scan(&state, &leaseOwner, &leaseExpiresAt, &updatedAt); err != nil {
		t.Fatalf("read durable Job %s: %v", id, err)
	}
	row := resultNamePgDurableJob{State: jobs.State(state), UpdatedAt: updatedAt}
	if leaseOwner.Valid {
		value := leaseOwner.String
		row.LeaseOwner = &value
	}
	if leaseExpiresAt.Valid {
		value := leaseExpiresAt.Time
		row.LeaseExpiresAt = &value
	}
	return row
}

// --- comparison helpers -------------------------------------------------------

func resultNamePgOptionalStringEqual(first, second *string) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	return *first == *second
}

func resultNamePgFormatOptionalString(value *string) string {
	if value == nil {
		return "<NULL>"
	}
	return fmt.Sprintf("%q", *value)
}

func resultNamePgOptionalTimeEqual(first, second *time.Time) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	return first.Equal(*second)
}

func resultNamePgFormatOptionalTime(value *time.Time) string {
	if value == nil {
		return "<NULL>"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func resultNamePgAssertManifestUnchanged(t *testing.T, id acquisition.ManifestID, before, after resultNamePgDurableManifest) {
	t.Helper()
	if !resultNamePgOptionalStringEqual(before.ResultName, after.ResultName) {
		t.Fatalf("Manifest %s result_name changed from %s to %s",
			id, resultNamePgFormatOptionalString(before.ResultName), resultNamePgFormatOptionalString(after.ResultName))
	}
	if !resultNamePgOptionalStringEqual(before.ExpectedName, after.ExpectedName) {
		t.Fatalf("Manifest %s expected_name changed from %s to %s",
			id, resultNamePgFormatOptionalString(before.ExpectedName), resultNamePgFormatOptionalString(after.ExpectedName))
	}
	if before.State != after.State {
		t.Fatalf("Manifest %s state changed from %q to %q", id, before.State, after.State)
	}
	if !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatalf("Manifest %s updated_at changed from %s to %s", id, before.UpdatedAt, after.UpdatedAt)
	}
}

func resultNamePgAssertJobUnchanged(t *testing.T, id jobs.JobID, before, after resultNamePgDurableJob) {
	t.Helper()
	if before.State != after.State {
		t.Fatalf("Job %s state changed from %q to %q", id, before.State, after.State)
	}
	if !resultNamePgOptionalStringEqual(before.LeaseOwner, after.LeaseOwner) {
		t.Fatalf("Job %s lease_owner changed from %s to %s",
			id, resultNamePgFormatOptionalString(before.LeaseOwner), resultNamePgFormatOptionalString(after.LeaseOwner))
	}
	if !resultNamePgOptionalTimeEqual(before.LeaseExpiresAt, after.LeaseExpiresAt) {
		t.Fatalf("Job %s lease_expires_at changed from %s to %s",
			id, resultNamePgFormatOptionalTime(before.LeaseExpiresAt), resultNamePgFormatOptionalTime(after.LeaseExpiresAt))
	}
	if !before.UpdatedAt.Equal(after.UpdatedAt) {
		t.Fatalf("Job %s updated_at changed from %s to %s", id, before.UpdatedAt, after.UpdatedAt)
	}
}

// resultNamePgIsTypedConflict accepts the errors a losing concurrent handoff may
// legitimately surface.
func resultNamePgIsTypedConflict(err error) bool {
	return errors.Is(err, acquisition.ErrProviderOutcomeConflict) ||
		errors.Is(err, acquisition.ErrResultNameImmutable) ||
		errors.Is(err, acquisition.ErrProviderOutcomeManifestState)
}

// resultNamePgAssertCheckViolation proves the database rejected a value through the
// named CHECK constraint (SQLSTATE 23514).
func resultNamePgAssertCheckViolation(t *testing.T, err error, operation string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s was accepted, want SQLSTATE 23514", operation)
	}
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) {
		t.Fatalf("%s error = %v, want *pgconn.PgError", operation, err)
	}
	if postgresError.Code != "23514" {
		t.Fatalf("%s SQLSTATE = %q, want 23514 (%v)", operation, postgresError.Code, err)
	}
	if postgresError.ConstraintName != resultNamePgConstraint {
		t.Fatalf("%s violated %q, want %q", operation, postgresError.ConstraintName, resultNamePgConstraint)
	}
}

// --- 1: provider success freezes the locator and advances both records ---------

func TestPostgresResultNameSuccessPersistsDurableLocatorAtomically(t *testing.T) {
	ctx, pool, outcomes, manifests, _, bindingID := newResultNamePgFixture(t)
	fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, "01", "result-name-worker", 1,
		acquisition.StateActive, nil, nil, time.Now().UTC().Add(time.Hour))

	now := time.Date(2026, 9, 30, 13, 30, 0, 0, time.UTC)
	const providerName = "Acquired  Result.MKV"

	result, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, resultNamePgString(providerName)))
	if err != nil {
		t.Fatalf("CommitProviderOutcome() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("CommitProviderOutcome() reported Changed=false for a fresh provider success")
	}

	durableManifest := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
	if durableManifest.ResultName == nil || *durableManifest.ResultName != providerName {
		t.Fatalf("durable result_name = %s, want %q verbatim",
			resultNamePgFormatOptionalString(durableManifest.ResultName), providerName)
	}
	if durableManifest.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("durable Manifest state = %q, want %q", durableManifest.State, acquisition.StateAwaitingVisibility)
	}
	if durableManifest.ExpectedName != nil {
		t.Fatalf("expected_name = %s, want NULL: intent is never written by provider execution",
			resultNamePgFormatOptionalString(durableManifest.ExpectedName))
	}
	if !durableManifest.UpdatedAt.Equal(now) {
		t.Fatalf("durable Manifest updated_at = %s, want %s", durableManifest.UpdatedAt, now)
	}

	durableJob := readResultNamePgJob(t, ctx, pool, fixture.jobID)
	if durableJob.State != jobs.StateRetryWait {
		t.Fatalf("durable Job state = %q, want %q", durableJob.State, jobs.StateRetryWait)
	}
	if durableJob.LeaseOwner != nil || durableJob.LeaseExpiresAt != nil {
		t.Fatalf("durable Job lease was not cleared: owner=%s expires=%s",
			resultNamePgFormatOptionalString(durableJob.LeaseOwner), resultNamePgFormatOptionalTime(durableJob.LeaseExpiresAt))
	}
	if !durableJob.UpdatedAt.Equal(now) {
		t.Fatalf("durable Job updated_at = %s, want %s", durableJob.UpdatedAt, now)
	}

	// The production reader must expose the same durable locator as raw SQL.
	readBack, err := manifests.GetManifest(ctx, fixture.manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if readBack.ResultName == nil || *readBack.ResultName != providerName {
		t.Fatalf("GetManifest() result_name = %s, want %q",
			resultNamePgFormatOptionalString(readBack.ResultName), providerName)
	}
}

// --- 2: the provider name wins over expected_name, which stays intent ----------

func TestPostgresResultNameProviderNameWinsOverExpectedName(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	const expectedName = "A.mkv"
	const providerName = "B.mkv"
	fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, "02", "result-name-worker", 1,
		acquisition.StateActive, resultNamePgString(expectedName), nil, time.Now().UTC().Add(time.Hour))

	now := time.Date(2026, 9, 30, 13, 31, 0, 0, time.UTC)
	result, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, resultNamePgString(providerName)))
	if err != nil {
		t.Fatalf("CommitProviderOutcome() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("CommitProviderOutcome() reported Changed=false for a fresh provider success")
	}

	durable := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
	if durable.ResultName == nil || *durable.ResultName != providerName {
		t.Fatalf("durable result_name = %s, want the observed provider name %q",
			resultNamePgFormatOptionalString(durable.ResultName), providerName)
	}
	if durable.ExpectedName == nil || *durable.ExpectedName != expectedName {
		t.Fatalf("expected_name = %s, want the untouched request intent %q",
			resultNamePgFormatOptionalString(durable.ExpectedName), expectedName)
	}
}

// --- 3: without a provider name, expected_name is the fallback -----------------

func TestPostgresResultNameFallsBackToExpectedName(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	now := time.Date(2026, 9, 30, 13, 32, 0, 0, time.UTC)

	cases := []struct {
		name         string
		suffix       string
		providerName *string
	}{
		{name: "absent provider name", suffix: "03", providerName: nil},
		{name: "empty provider name", suffix: "04", providerName: resultNamePgString("")},
		{name: "blank provider name", suffix: "05", providerName: resultNamePgString("   ")},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			const expectedName = "A.mkv"
			fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, test.suffix, "result-name-worker", 1,
				acquisition.StateActive, resultNamePgString(expectedName), nil, time.Now().UTC().Add(time.Hour))

			result, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, test.providerName))
			if err != nil {
				t.Fatalf("CommitProviderOutcome() error = %v", err)
			}
			if !result.Changed {
				t.Fatal("CommitProviderOutcome() reported Changed=false for a fresh provider success")
			}

			durable := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
			if durable.ResultName == nil || *durable.ResultName != expectedName {
				t.Fatalf("durable result_name = %s, want the intent fallback %q",
					resultNamePgFormatOptionalString(durable.ResultName), expectedName)
			}
			if durable.ExpectedName == nil || *durable.ExpectedName != expectedName {
				t.Fatalf("expected_name = %s, want %q preserved",
					resultNamePgFormatOptionalString(durable.ExpectedName), expectedName)
			}
			if durable.State != acquisition.StateAwaitingVisibility {
				t.Fatalf("durable Manifest state = %q, want %q", durable.State, acquisition.StateAwaitingVisibility)
			}
		})
	}
}

// TestPostgresResultNameUnpromotableIntentFailsClosed is the Round 2 fix.
//
// expected_name is permissive because it is request intent, but promoting an intent to
// the durable locator must satisfy the direct-child rule. A permissive intent such as
// "a/b" must therefore fail closed as a typed missing identity rather than being
// written as result_name and only rejected later by the database CHECK, which would
// surface as a persistence error instead of an identity decision.
func TestPostgresResultNameUnpromotableIntentFailsClosed(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	now := time.Date(2026, 9, 30, 13, 33, 0, 0, time.UTC)

	// Each is a valid bounded intention that cannot become a direct-child locator.
	intents := []string{"a/b", `a\b`, ".", "..", "a/../b", "/absolute"}
	for index, intent := range intents {
		t.Run(intent, func(t *testing.T) {
			suffix := fmt.Sprintf("%02d", 40+index)
			fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, suffix, "result-name-worker", 1,
				acquisition.StateActive, resultNamePgString(intent), nil, time.Now().UTC().Add(time.Hour))

			before := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)

			// The provider reports no name, so the intent would be the only candidate.
			_, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, nil))
			if !errors.Is(err, acquisition.ErrResultNameMissing) {
				t.Fatalf("CommitProviderOutcome() error = %v, want ErrResultNameMissing", err)
			}
			// It must be a typed identity failure, never a database constraint error.
			if errors.Is(err, acquisition.ErrProviderOutcomePersistence) {
				t.Fatalf("error = %v, want a typed identity decision rather than a persistence error", err)
			}

			after := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
			if after.ResultName != nil {
				t.Fatalf("result_name = %s, want NULL when the intent is not promotable",
					resultNamePgFormatOptionalString(after.ResultName))
			}
			if after.State != before.State {
				t.Fatalf("Manifest state = %q, want %q unchanged", after.State, before.State)
			}
			if !after.UpdatedAt.Equal(before.UpdatedAt) {
				t.Fatalf("Manifest updated_at = %v, want %v unchanged", after.UpdatedAt, before.UpdatedAt)
			}
			if after.ExpectedName == nil || *after.ExpectedName != intent {
				t.Fatalf("expected_name = %s, want the intent %q preserved",
					resultNamePgFormatOptionalString(after.ExpectedName), intent)
			}
		})
	}
}

// --- 4: no provider name and no usable intent fails closed, changing nothing ---

func TestPostgresResultNameMissingLocatorFailsClosedWithoutMutation(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	now := time.Date(2026, 9, 30, 13, 33, 0, 0, time.UTC)

	cases := []struct {
		name         string
		suffix       string
		providerName *string
	}{
		{name: "absent provider name", suffix: "06", providerName: nil},
		{name: "blank provider name", suffix: "07", providerName: resultNamePgString("  ")},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, test.suffix, "result-name-worker", 1,
				acquisition.StateActive, nil, nil, time.Now().UTC().Add(time.Hour))
			beforeManifest := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
			beforeJob := readResultNamePgJob(t, ctx, pool, fixture.jobID)

			_, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, test.providerName))
			if !errors.Is(err, acquisition.ErrResultNameMissing) {
				t.Fatalf("CommitProviderOutcome() error = %v, want ErrResultNameMissing", err)
			}
			if errors.Is(err, acquisition.ErrProviderResultName) {
				t.Fatalf("CommitProviderOutcome() error = %v, want the missing-locator error, not a malformed-name error", err)
			}

			resultNamePgAssertManifestUnchanged(t, fixture.manifestID, beforeManifest,
				readResultNamePgManifest(t, ctx, pool, fixture.manifestID))
			resultNamePgAssertJobUnchanged(t, fixture.jobID, beforeJob,
				readResultNamePgJob(t, ctx, pool, fixture.jobID))
		})
	}
}

// --- 5: a malformed provider name never silently falls back --------------------

func TestPostgresResultNameMalformedProviderNameFailsClosed(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	now := time.Date(2026, 9, 30, 13, 34, 0, 0, time.UTC)

	cases := []struct {
		name         string
		suffix       string
		providerName string
	}{
		{name: "forward slash separator", suffix: "08", providerName: "a/b"},
		{name: "backslash separator", suffix: "09", providerName: `a\b`},
		{name: "dot component", suffix: "10", providerName: "."},
		{name: "dot dot component", suffix: "11", providerName: ".."},
		{name: "over the rune bound", suffix: "12", providerName: strings.Repeat("x", acquisition.MaxResultNameLength+1)},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			// A perfectly valid fallback is present: a malformed provider name must
			// still fail closed rather than quietly use it.
			const expectedName = "A.mkv"
			fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, test.suffix, "result-name-worker", 1,
				acquisition.StateActive, resultNamePgString(expectedName), nil, time.Now().UTC().Add(time.Hour))
			beforeManifest := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
			beforeJob := readResultNamePgJob(t, ctx, pool, fixture.jobID)

			_, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, resultNamePgString(test.providerName)))
			if !errors.Is(err, acquisition.ErrProviderResultName) {
				t.Fatalf("CommitProviderOutcome() error = %v, want ErrProviderResultName", err)
			}
			if errors.Is(err, acquisition.ErrResultNameMissing) {
				t.Fatalf("CommitProviderOutcome() error = %v, a malformed name is not a missing name", err)
			}

			resultNamePgAssertManifestUnchanged(t, fixture.manifestID, beforeManifest,
				readResultNamePgManifest(t, ctx, pool, fixture.manifestID))
			resultNamePgAssertJobUnchanged(t, fixture.jobID, beforeJob,
				readResultNamePgJob(t, ctx, pool, fixture.jobID))
		})
	}
}

// --- 6: a persisted locator is immutable --------------------------------------

func TestPostgresResultNameIsImmutableOncePersisted(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	now := time.Date(2026, 9, 30, 13, 35, 0, 0, time.UTC)

	t.Run("committed pair replays identically and rejects a different name", func(t *testing.T) {
		const expectedName = "A.mkv"
		const committedName = "B.mkv"
		const conflictingName = "C.mkv"
		fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, "13", "result-name-worker", 1,
			acquisition.StateActive, resultNamePgString(expectedName), nil, time.Now().UTC().Add(time.Hour))

		first, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, resultNamePgString(committedName)))
		if err != nil {
			t.Fatalf("first CommitProviderOutcome() error = %v", err)
		}
		if !first.Changed {
			t.Fatal("first CommitProviderOutcome() reported Changed=false")
		}
		committedManifest := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
		committedJob := readResultNamePgJob(t, ctx, pool, fixture.jobID)
		if committedManifest.ResultName == nil || *committedManifest.ResultName != committedName {
			t.Fatalf("durable result_name = %s, want %q",
				resultNamePgFormatOptionalString(committedManifest.ResultName), committedName)
		}

		// The same already-committed outcome proposed again with the same name is an
		// idempotent replay: Changed=false and neither timestamp is rewritten.
		replay, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now.Add(time.Hour), resultNamePgString(committedName)))
		if err != nil {
			t.Fatalf("same-name replay CommitProviderOutcome() error = %v", err)
		}
		if replay.Changed {
			t.Fatal("same-name replay CommitProviderOutcome() reported Changed=true")
		}
		if replay.Manifest.ResultName == nil || *replay.Manifest.ResultName != committedName {
			t.Fatalf("same-name replay Manifest result_name = %s, want %q",
				resultNamePgFormatOptionalString(replay.Manifest.ResultName), committedName)
		}
		if !replay.Manifest.UpdatedAt.Equal(committedManifest.UpdatedAt) {
			t.Fatalf("same-name replay rewrote Manifest updated_at: %s -> %s",
				committedManifest.UpdatedAt, replay.Manifest.UpdatedAt)
		}
		if !replay.Job.UpdatedAt.Equal(committedJob.UpdatedAt) {
			t.Fatalf("same-name replay rewrote Job updated_at: %s -> %s", committedJob.UpdatedAt, replay.Job.UpdatedAt)
		}
		resultNamePgAssertManifestUnchanged(t, fixture.manifestID, committedManifest,
			readResultNamePgManifest(t, ctx, pool, fixture.manifestID))
		resultNamePgAssertJobUnchanged(t, fixture.jobID, committedJob,
			readResultNamePgJob(t, ctx, pool, fixture.jobID))

		// The SAME already-committed outcome proposed with a DIFFERENT provider name is
		// a different-value replay, so it must fail closed on the committed pair itself
		// instead of being silently reported as an idempotent no-op.
		conflicting, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now.Add(2*time.Hour), resultNamePgString(conflictingName)))
		if !errors.Is(err, acquisition.ErrResultNameImmutable) {
			t.Fatalf("different-name replay CommitProviderOutcome() error = %v, want ErrResultNameImmutable", err)
		}
		if conflicting.Changed {
			t.Fatal("different-name replay CommitProviderOutcome() reported Changed=true")
		}
		preserved := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
		if preserved.ResultName == nil || *preserved.ResultName != committedName {
			t.Fatalf("durable result_name = %s, want the committed %q",
				resultNamePgFormatOptionalString(preserved.ResultName), committedName)
		}
		resultNamePgAssertManifestUnchanged(t, fixture.manifestID, committedManifest, preserved)
		resultNamePgAssertJobUnchanged(t, fixture.jobID, committedJob,
			readResultNamePgJob(t, ctx, pool, fixture.jobID))
	})

	// A blank or absent provider name carries no new evidence, so it must stay an
	// idempotent replay rather than becoming an immutability conflict.
	t.Run("blank or absent provider name on a committed pair is no new evidence", func(t *testing.T) {
		const expectedName = "A.mkv"
		const committedName = "B.mkv"
		fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, "26", "result-name-worker", 1,
			acquisition.StateActive, resultNamePgString(expectedName), nil, time.Now().UTC().Add(time.Hour))

		if _, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, resultNamePgString(committedName))); err != nil {
			t.Fatalf("first CommitProviderOutcome() error = %v", err)
		}
		committedManifest := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
		committedJob := readResultNamePgJob(t, ctx, pool, fixture.jobID)

		replays := []struct {
			name         string
			providerName *string
		}{
			{name: "absent provider name", providerName: nil},
			{name: "blank provider name", providerName: resultNamePgString("   ")},
		}
		for index := range replays {
			test := replays[index]
			t.Run(test.name, func(t *testing.T) {
				replay, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now.Add(time.Duration(index+2)*time.Hour), test.providerName))
				if err != nil {
					t.Fatalf("replay CommitProviderOutcome() error = %v, want the committed locator to stand", err)
				}
				if replay.Changed {
					t.Fatal("replay CommitProviderOutcome() reported Changed=true")
				}
				if replay.Manifest.ResultName == nil || *replay.Manifest.ResultName != committedName {
					t.Fatalf("replay Manifest result_name = %s, want %q",
						resultNamePgFormatOptionalString(replay.Manifest.ResultName), committedName)
				}
				resultNamePgAssertManifestUnchanged(t, fixture.manifestID, committedManifest,
					readResultNamePgManifest(t, ctx, pool, fixture.manifestID))
				resultNamePgAssertJobUnchanged(t, fixture.jobID, committedJob,
					readResultNamePgJob(t, ctx, pool, fixture.jobID))
			})
		}
	})

	t.Run("different provider name on a fresh handoff fails closed", func(t *testing.T) {
		const expectedName = "A.mkv"
		const committedName = "B.mkv"
		const conflictingName = "C.mkv"
		fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, "27", "result-name-worker", 1,
			acquisition.StateActive, resultNamePgString(expectedName), nil, time.Now().UTC().Add(time.Hour))

		if _, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, resultNamePgString(committedName))); err != nil {
			t.Fatalf("first CommitProviderOutcome() error = %v", err)
		}

		// Re-open the provider stage the way a later attempt would find it: the
		// Manifest ACTIVE again under a fresh RUNNING lease, while the locator the
		// first success committed is still persisted. The fresh path must apply the
		// same immutability rule the committed-pair replay applies.
		rearmed := resultNamePgReArm(t, ctx, pool, fixture, time.Now().UTC().Add(time.Hour))
		beforeManifest := readResultNamePgManifest(t, ctx, pool, rearmed.manifestID)
		beforeJob := readResultNamePgJob(t, ctx, pool, rearmed.jobID)
		if beforeManifest.ResultName == nil || *beforeManifest.ResultName != committedName {
			t.Fatalf("precondition: durable result_name = %s, want %q",
				resultNamePgFormatOptionalString(beforeManifest.ResultName), committedName)
		}

		_, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, rearmed, now.Add(2*time.Hour), resultNamePgString(conflictingName)))
		if !errors.Is(err, acquisition.ErrResultNameImmutable) {
			t.Fatalf("CommitProviderOutcome() error = %v, want ErrResultNameImmutable", err)
		}

		preserved := readResultNamePgManifest(t, ctx, pool, rearmed.manifestID)
		if preserved.ResultName == nil || *preserved.ResultName != committedName {
			t.Fatalf("durable result_name = %s, want the committed %q",
				resultNamePgFormatOptionalString(preserved.ResultName), committedName)
		}
		resultNamePgAssertManifestUnchanged(t, rearmed.manifestID, beforeManifest, preserved)
		resultNamePgAssertJobUnchanged(t, rearmed.jobID, beforeJob,
			readResultNamePgJob(t, ctx, pool, rearmed.jobID))
	})
}

// --- 7: non-success outcomes never rewrite either identity column --------------

func TestPostgresResultNameNonSuccessOutcomesNeverRewriteIdentity(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	now := time.Date(2026, 9, 30, 13, 36, 0, 0, time.UTC)

	outcomeList := []acquisition.ProviderOutcome{
		acquisition.ProviderOutcomeInProgress,
		acquisition.ProviderOutcomeFailed,
		acquisition.ProviderOutcomeCanceled,
		acquisition.ProviderOutcomeRecovery,
	}
	seedList := []struct {
		name         string
		resultName   *string
		expectedName string
	}{
		{name: "without a persisted locator", resultName: nil, expectedName: "Intent Kept.mkv"},
		{name: "with a persisted locator", resultName: resultNamePgString("seeded-existing.bin"), expectedName: "intent/kept  exactly.mkv"},
	}

	for seedIndex := range seedList {
		seed := seedList[seedIndex]
		for outcomeIndex := range outcomeList {
			outcome := outcomeList[outcomeIndex]
			t.Run(string(outcome)+" "+seed.name, func(t *testing.T) {
				suffix := fmt.Sprintf("%02d", 15+seedIndex*len(outcomeList)+outcomeIndex)
				fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, suffix, "result-name-worker", 1,
					acquisition.StateActive, resultNamePgString(seed.expectedName), seed.resultName, time.Now().UTC().Add(time.Hour))
				beforeManifest := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)

				transition, err := acquisition.ProviderOutcomeTransitionFor(outcome)
				if err != nil {
					t.Fatalf("ProviderOutcomeTransitionFor(%s) error = %v", outcome, err)
				}
				result, err := outcomes.CommitProviderOutcome(ctx, resultNamePgPlan(t, fixture, outcome, now))
				if err != nil {
					t.Fatalf("CommitProviderOutcome(%s) error = %v", outcome, err)
				}
				if !result.Changed {
					t.Fatalf("CommitProviderOutcome(%s) reported Changed=false", outcome)
				}

				afterManifest := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
				afterJob := readResultNamePgJob(t, ctx, pool, fixture.jobID)
				if !resultNamePgOptionalStringEqual(beforeManifest.ResultName, afterManifest.ResultName) {
					t.Fatalf("%s changed result_name from %s to %s",
						outcome, resultNamePgFormatOptionalString(beforeManifest.ResultName),
						resultNamePgFormatOptionalString(afterManifest.ResultName))
				}
				if afterManifest.ExpectedName == nil || *afterManifest.ExpectedName != seed.expectedName {
					t.Fatalf("%s changed expected_name to %s, want %q byte for byte",
						outcome, resultNamePgFormatOptionalString(afterManifest.ExpectedName), seed.expectedName)
				}
				if afterManifest.State != transition.ManifestState {
					t.Fatalf("durable Manifest state = %q, want %q", afterManifest.State, transition.ManifestState)
				}
				if afterJob.State != transition.JobState {
					t.Fatalf("durable Job state = %q, want %q", afterJob.State, transition.JobState)
				}
				if outcome == acquisition.ProviderOutcomeInProgress {
					// The Manifest stays ACTIVE and nothing about it changed, so the
					// handoff deliberately preserves its timestamp.
					if !afterManifest.UpdatedAt.Equal(beforeManifest.UpdatedAt) {
						t.Fatalf("ACTIVE milestone rewrote updated_at: %s -> %s",
							beforeManifest.UpdatedAt, afterManifest.UpdatedAt)
					}
				} else if !afterManifest.UpdatedAt.Equal(now) {
					t.Fatalf("durable Manifest updated_at = %s, want %s", afterManifest.UpdatedAt, now)
				}
			})
		}
	}
}

// --- 8: concurrent different proposals leave exactly one durable locator -------

func TestPostgresResultNameConcurrentProvidersHaveOneDurableWinner(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, "23", "result-name-racer", 1,
		acquisition.StateActive, nil, nil, time.Now().UTC().Add(time.Hour))
	now := time.Date(2026, 9, 30, 13, 37, 0, 0, time.UTC)

	const workers = 5
	proposals := make([]string, workers)
	plans := make([]acquisition.ProviderOutcomePlan, workers)
	for index := 0; index < workers; index++ {
		proposals[index] = fmt.Sprintf("concurrent-%02d.bin", index+1)
		plans[index] = resultNamePgSuccessPlan(t, fixture, now, resultNamePgString(proposals[index]))
	}

	results := make([]acquisition.ProviderOutcomeResult, workers)
	errs := make([]error, workers)
	var waitGroup sync.WaitGroup
	start := make(chan struct{})
	for index := 0; index < workers; index++ {
		waitGroup.Add(1)
		go func(position int) {
			defer waitGroup.Done()
			<-start
			results[position], errs[position] = outcomes.CommitProviderOutcome(ctx, plans[position])
		}(index)
	}
	close(start)
	waitGroup.Wait()

	final := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
	if final.ResultName == nil {
		t.Fatal("no durable result_name after concurrent provider successes")
	}
	proposalMatched := false
	for _, proposal := range proposals {
		if proposal == *final.ResultName {
			proposalMatched = true
			break
		}
	}
	if !proposalMatched {
		t.Fatalf("durable result_name = %q, which is not exactly one of the %d proposals",
			*final.ResultName, workers)
	}

	winners := 0
	for index := 0; index < workers; index++ {
		if errs[index] != nil {
			if !resultNamePgIsTypedConflict(errs[index]) {
				t.Fatalf("worker %d error = %v, want a typed conflict", index, errs[index])
			}
			continue
		}
		if results[index].Manifest.ResultName == nil || *results[index].Manifest.ResultName != *final.ResultName {
			t.Fatalf("worker %d reported %s, want the winner's durable %q",
				index, resultNamePgFormatOptionalString(results[index].Manifest.ResultName), *final.ResultName)
		}
		if results[index].Changed {
			if proposals[index] != *final.ResultName {
				t.Fatalf("worker %d reported a win with %q, but the durable winner is %q",
					index, proposals[index], *final.ResultName)
			}
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
	if final.ExpectedName != nil {
		t.Fatalf("expected_name = %s, want NULL: intent is never written by provider execution",
			resultNamePgFormatOptionalString(final.ExpectedName))
	}
	if final.State != acquisition.StateAwaitingVisibility {
		t.Fatalf("durable Manifest state = %q, want %q", final.State, acquisition.StateAwaitingVisibility)
	}
	if durableJob := readResultNamePgJob(t, ctx, pool, fixture.jobID); durableJob.State != jobs.StateRetryWait {
		t.Fatalf("durable Job state = %q, want %q", durableJob.State, jobs.StateRetryWait)
	}
}

// --- 9: a failure between the two mutations rolls the locator back -------------

func TestPostgresResultNameForcedFailureRollsBackLocatorAndBothRecords(t *testing.T) {
	ctx, pool, outcomes, _, _, bindingID := newResultNamePgFixture(t)
	const expectedName = "A.mkv"
	fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, "24", "result-name-worker", 1,
		acquisition.StateActive, resultNamePgString(expectedName), nil, time.Now().UTC().Add(time.Hour))
	beforeManifest := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
	beforeJob := readResultNamePgJob(t, ctx, pool, fixture.jobID)

	injected := errors.New("injected failure between the Manifest and Job mutations")
	outcomes.afterManifestUpdate = func(context.Context, pgx.Tx) error { return injected }
	now := time.Date(2026, 9, 30, 13, 38, 0, 0, time.UTC)
	_, err := outcomes.CommitProviderOutcome(ctx, resultNamePgSuccessPlan(t, fixture, now, resultNamePgString("B.mkv")))
	outcomes.afterManifestUpdate = nil
	if !errors.Is(err, acquisition.ErrProviderOutcomePersistence) {
		t.Fatalf("CommitProviderOutcome() error = %v, want ErrProviderOutcomePersistence", err)
	}

	resultNamePgAssertManifestUnchanged(t, fixture.manifestID, beforeManifest,
		readResultNamePgManifest(t, ctx, pool, fixture.manifestID))
	resultNamePgAssertJobUnchanged(t, fixture.jobID, beforeJob,
		readResultNamePgJob(t, ctx, pool, fixture.jobID))
}

// --- 10: migration 0011's direct-child CHECK constraint ------------------------

func TestPostgresResultNameDatabaseConstraintRejectsNonDirectChild(t *testing.T) {
	ctx, pool, _, _, _, bindingID := newResultNamePgFixture(t)
	fixture := seedResultNamePgFixture(t, ctx, pool, bindingID, "25", "result-name-worker", 1,
		acquisition.StateActive, nil, nil, time.Now().UTC().Add(time.Hour))
	now := time.Date(2026, 9, 30, 13, 39, 0, 0, time.UTC)

	invalid := []struct {
		name  string
		value string
	}{
		{name: "forward slash", value: "a/b"},
		{name: "backslash", value: `a\b`},
		{name: "dot", value: "."},
		{name: "dot dot", value: ".."},
		{name: "blank", value: "   "},
		{name: "over the rune bound", value: strings.Repeat("x", acquisition.MaxResultNameLength+1)},
	}

	for _, test := range invalid {
		_, err := pool.Exec(ctx, `
UPDATE acquisition_manifests SET result_name = $1 WHERE manifest_id = $2`,
			test.value, string(fixture.manifestID))
		resultNamePgAssertCheckViolation(t, err, "UPDATE result_name = "+test.name)
	}

	for index, test := range invalid {
		// A PENDING row needs no Job, so the only constraint in play is the
		// result_name direct-child rule this case is about.
		_, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, result_name, target_storage_binding_id,
    target_path, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, '/downloads/inserted', 'PENDING', $4, $4)`,
			fmt.Sprintf("39000000-0000-4000-8000-0000000003%02d", index),
			test.value, string(bindingID), now)
		resultNamePgAssertCheckViolation(t, err, "INSERT result_name = "+test.name)
	}

	// A valid direct child is accepted verbatim.
	const validName = "Direct  Child.bin"
	if _, err := pool.Exec(ctx, `
UPDATE acquisition_manifests SET result_name = $1 WHERE manifest_id = $2`,
		validName, string(fixture.manifestID)); err != nil {
		t.Fatalf("UPDATE with a valid direct-child result_name was rejected: %v", err)
	}
	if row := readResultNamePgManifest(t, ctx, pool, fixture.manifestID); row.ResultName == nil || *row.ResultName != validName {
		t.Fatalf("durable result_name = %s, want %q",
			resultNamePgFormatOptionalString(row.ResultName), validName)
	}

	// NULL stays legal for a Manifest that has not reached a provider success.
	if _, err := pool.Exec(ctx, `
UPDATE acquisition_manifests SET result_name = NULL WHERE manifest_id = $1`,
		string(fixture.manifestID)); err != nil {
		t.Fatalf("UPDATE result_name = NULL was rejected: %v", err)
	}
	row := readResultNamePgManifest(t, ctx, pool, fixture.manifestID)
	if row.ResultName != nil {
		t.Fatalf("durable result_name = %s, want NULL", resultNamePgFormatOptionalString(row.ResultName))
	}
	if row.State != acquisition.StateActive {
		t.Fatalf("durable Manifest state = %q, want ACTIVE: NULL is legal before a provider success", row.State)
	}
}

// --- 11: result_name has no database default and is nullable -------------------

func TestPostgresResultNameColumnHasNoDefaultAndIsNullable(t *testing.T) {
	ctx, pool, _, _, _, bindingID := newResultNamePgFixture(t)

	var columnDefault sql.NullString
	var isNullable string
	var dataType string
	if err := pool.QueryRow(ctx, `
SELECT column_default, is_nullable, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'acquisition_manifests'
  AND column_name = 'result_name'`).Scan(&columnDefault, &isNullable, &dataType); err != nil {
		t.Fatalf("read information_schema for acquisition_manifests.result_name: %v", err)
	}
	if columnDefault.Valid {
		t.Fatalf("result_name has column default %q; D-032 forbids fabricating a locator", columnDefault.String)
	}
	if isNullable != "YES" {
		t.Fatalf("result_name is_nullable = %q, want YES", isNullable)
	}
	if dataType != "text" {
		t.Fatalf("result_name data_type = %q, want text", dataType)
	}

	// Behavioural proof of "no default": a row inserted without result_name keeps
	// NULL instead of acquiring a fabricated locator. PENDING needs no Job, so the
	// row proves the column default rather than any linkage rule.
	now := time.Date(2026, 9, 30, 13, 40, 0, 0, time.UTC)
	const manifestID = "39000000-0000-4000-8000-000000000400"
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id,
    target_path, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, '/downloads/no-default', 'PENDING', $3, $3)`,
		manifestID, string(bindingID), now); err != nil {
		t.Fatalf("INSERT without result_name: %v", err)
	}
	if row := readResultNamePgManifest(t, ctx, pool, manifestID); row.ResultName != nil {
		t.Fatalf("result_name = %s after an INSERT that omitted it, want NULL",
			resultNamePgFormatOptionalString(row.ResultName))
	}
}

// --- 12: the v10 -> v11 upgrade preserves history without guessing -------------

func TestPostgresResultNameMigrationV11ToV12PreservesHistory(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	history, err := migrations.All()
	if err != nil {
		t.Fatalf("migrations.All() error = %v", err)
	}
	legacy := make([]migrations.Migration, 0, 11)
	for _, migration := range history {
		if migration.Version <= 11 {
			legacy = append(legacy, migration)
		}
	}
	if len(legacy) != 11 {
		t.Fatalf("legacy history has %d migrations, want 11", len(legacy))
	}
	if last := legacy[len(legacy)-1].Version; last != 11 {
		t.Fatalf("legacy history ends at version %d, want 10", last)
	}
	legacyMigrator := &Migrator{pool: pool, migrations: legacy}
	legacyStatus, err := legacyMigrator.Apply(ctx)
	if err != nil || !legacyStatus.Compatible || legacyStatus.CurrentVersion != 11 {
		t.Fatalf("apply through version 11 = %#v, %v", legacyStatus, err)
	}

	// Gate 3.10 advances v11 -> v12 by adding result_copy_id. result_name already
	// exists at v11, and result_copy_id must not.
	var resultCopyColumnExists bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'acquisition_manifests' AND column_name = 'result_copy_id'
)`).Scan(&resultCopyColumnExists); err != nil {
		t.Fatalf("inspect pre-upgrade columns: %v", err)
	}
	if resultCopyColumnExists {
		t.Fatal("result_copy_id already exists before the version 12 upgrade")
	}

	bindingID := storage.BindingID("39000000-0000-4000-8000-000000000000")
	seedStorageBinding(t, ctx, pool, bindingID, "result-name-upgrade-root")

	const manifestID = "39000000-0000-4000-8000-000000000200"
	const legacyExpectedName = "legacy/expected  name.mkv"
	now := time.Date(2026, 9, 30, 13, 41, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, expected_name, target_storage_binding_id,
    target_path, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, '/downloads/legacy', 'AWAITING_CANONICAL', $4, $4)`,
		manifestID, legacyExpectedName, string(bindingID), now); err != nil {
		t.Fatalf("seed version 11 Manifest: %v", err)
	}

	var beforeExpectedName, beforeState string
	var beforeUpdatedAt time.Time
	if err := pool.QueryRow(ctx, `
SELECT expected_name, state, updated_at FROM acquisition_manifests WHERE manifest_id = $1`,
		manifestID).Scan(&beforeExpectedName, &beforeState, &beforeUpdatedAt); err != nil {
		t.Fatalf("read version 11 Manifest: %v", err)
	}
	if beforeExpectedName != legacyExpectedName {
		t.Fatalf("version 11 expected_name = %q, want %q", beforeExpectedName, legacyExpectedName)
	}
	if beforeState != string(acquisition.StateAwaitingCanonical) {
		t.Fatalf("version 11 state = %q, want %q", beforeState, acquisition.StateAwaitingCanonical)
	}

	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	upgraded, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("apply version 12: %v", err)
	}
	if !upgraded.Compatible || upgraded.CurrentVersion != 12 || upgraded.LatestVersion != 12 {
		t.Fatalf("upgrade status = %#v", upgraded)
	}

	var afterResultCopyID sql.NullString
	var afterExpectedName, afterState string
	var afterUpdatedAt time.Time
	if err := pool.QueryRow(ctx, `
SELECT result_copy_id, expected_name, state, updated_at
FROM acquisition_manifests
WHERE manifest_id = $1`, manifestID).Scan(&afterResultCopyID, &afterExpectedName, &afterState, &afterUpdatedAt); err != nil {
		t.Fatalf("read upgraded Manifest: %v", err)
	}
	if afterResultCopyID.Valid {
		t.Fatalf("result_copy_id = %q after the upgrade, want NULL: migration 0012 backfills and guesses nothing",
			afterResultCopyID.String)
	}
	if afterExpectedName != legacyExpectedName {
		t.Fatalf("expected_name = %q after the upgrade, want %q byte for byte", afterExpectedName, legacyExpectedName)
	}
	if afterState != string(acquisition.StateAwaitingCanonical) {
		t.Fatalf("state = %q after the upgrade, want %q", afterState, acquisition.StateAwaitingCanonical)
	}
	if !afterUpdatedAt.Equal(beforeUpdatedAt) {
		t.Fatalf("updated_at changed during the upgrade: %s -> %s", beforeUpdatedAt, afterUpdatedAt)
	}

	var backfilled int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM acquisition_manifests WHERE result_copy_id IS NOT NULL`).Scan(&backfilled); err != nil {
		t.Fatalf("count backfilled result links: %v", err)
	}
	if backfilled != 0 {
		t.Fatalf("migration 0012 backfilled %d row(s), want 0", backfilled)
	}
}
