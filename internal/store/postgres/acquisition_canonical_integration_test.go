package postgres

// Gate 3.10 (D-033): READY is anchored to one canonical projected Copy.
//
// CanonicalStageRepository.CommitCanonical must commit the Acquisition Manifest's
// canonical milestone and its linked ACQUISITION Job stage as one PostgreSQL
// transaction, fenced by the claim generation, and it must revalidate the exact
// confirming Copy under its row lock so a stale application-layer read can never
// authorize READY. These tests drive the real repository against real PostgreSQL 16
// and read every durable fact back with raw SQL, so no assertion depends on the
// production reader agreeing with the production writer.
//
// Two deliberate techniques are used throughout:
//
//   - the RUNNING fixture Job is seeded with non-NULL last_error and finished_at,
//     so "this transition cleared it" is a real observation instead of a NULL that
//     was never written in the first place;
//   - the pre-Gate-3.10 READY Manifest in the replay-conflict coverage is seeded
//     through the frozen version 11 schema, which is exactly the recovery debt
//     migration 0012 tolerates by adding its CHECK as NOT VALID.
//
// Every test function is named TestPostgresCanonical* so
// `-run TestPostgresCanonical` isolates this file.

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
	"github.com/nathanxiangang-web/panta/migrations"
)

const (
	// canonicalPgCopyRoot is the ACTIVE StorageBinding's IndexCore root for every
	// fixture in this file. The binding is seeded through the package's shared
	// seedStorageBinding helper, which already enforces root uniqueness.
	canonicalPgCopyRoot = "canonical-root"
	// canonicalPgReadyConstraint is the named CHECK migration 0012 adds.
	canonicalPgReadyConstraint = "acquisition_manifests_ready_requires_result_copy"
	// canonicalPgBindingUUID is the StorageBinding every fixture Job and Manifest
	// points at.
	canonicalPgBindingUUID = "3c000000-0000-4000-8000-0000000000f0"
)

// --- fixture -------------------------------------------------------------------

type canonicalPgIntegration struct {
	ctx        context.Context
	pool       *pgxpool.Pool
	repository *CanonicalStageRepository
	manifests  *AcquisitionManifestRepository
	bindingID  storage.BindingID
}

// newCanonicalPgFixture migrates a fresh schema and returns the real repository
// under test. It reuses the package's integration pool, schema reset, migration
// status, and StorageBinding seed helpers rather than redeclaring them.
func newCanonicalPgFixture(t *testing.T) canonicalPgIntegration {
	t.Helper()
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 12 || status.LatestVersion != 12 {
		t.Fatalf("Apply() = %#v, %v; want a compatible schema at version 12", status, err)
	}

	bindingID := storage.BindingID(canonicalPgBindingUUID)
	seedStorageBinding(t, ctx, pool, bindingID, canonicalPgCopyRoot)

	repository, err := NewCanonicalStageRepository(pool)
	if err != nil {
		t.Fatalf("NewCanonicalStageRepository() error = %v", err)
	}
	manifests, err := NewAcquisitionManifestRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionManifestRepository() error = %v", err)
	}
	return canonicalPgIntegration{
		ctx: ctx, pool: pool, repository: repository, manifests: manifests, bindingID: bindingID,
	}
}

// --- unique-prefix identity helpers --------------------------------------------

func canonicalPgManifestID(suffix string) acquisition.ManifestID {
	return acquisition.ManifestID("3c000000-0000-4000-8000-0000000000" + suffix)
}

func canonicalPgJobID(suffix string) jobs.JobID {
	return jobs.JobID("3c000000-0000-4000-8000-0000000001" + suffix)
}

func canonicalPgCopyID(suffix string) catalog.CopyID {
	return catalog.CopyID("3c000000-0000-4000-8000-0000000002" + suffix)
}

// canonicalPgNow is the plan time every fresh commit uses. It is deliberately
// later than the seeded row timestamps so "unchanged" is a meaningful assertion.
func canonicalPgNow() time.Time {
	return time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
}

// canonicalPgLeaseEnd returns a live lease deadline. It is truncated to a whole
// microsecond because PostgreSQL stores timestamptz at microsecond resolution and
// an untruncated time.Time would never compare Equal after a round trip.
func canonicalPgLeaseEnd() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond).Add(time.Hour)
}

func canonicalPgExpiredLeaseEnd() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond).Add(-time.Minute)
}

func canonicalPgString(value string) *string {
	return &value
}

// --- seeding -------------------------------------------------------------------

// canonicalPgSeeded is one seeded Manifest plus its linked fenced RUNNING
// ACQUISITION Job, carrying the exact durable values a rejected commit must leave
// untouched.
type canonicalPgSeeded struct {
	manifestID        acquisition.ManifestID
	jobID             jobs.JobID
	owner             string
	claim             int
	failureBudget     int
	maxAttempts       int
	leaseEnd          time.Time
	manifestUpdatedAt time.Time
	jobUpdatedAt      time.Time
	lastError         *string
	finishedAt        *time.Time
}

// seedCanonicalPgManifestJob inserts one Manifest in the given milestone and its
// linked RUNNING ACQUISITION Job that satisfies acquisition.ValidateLinkedAcquisitionJob.
//
// last_error and finished_at are seeded non-NULL on purpose: the finalization and
// pending transitions both clear them, and starting from NULL could not prove it.
func seedCanonicalPgManifestJob(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	suffix string,
	state acquisition.State,
	owner string,
	claim int,
	failureBudget int,
	maxAttempts int,
	leaseEnd time.Time,
	variantID *catalog.VariantID,
) canonicalPgSeeded {
	t.Helper()
	manifestID := canonicalPgManifestID(suffix)
	jobID := canonicalPgJobID(suffix)
	seededAt := time.Date(2026, 9, 30, 13, 0, 0, 0, time.UTC)
	jobUpdatedAt := seededAt.Add(5 * time.Minute)
	manifestUpdatedAt := seededAt.Add(10 * time.Minute)
	scratchError := "canonical stage scratch"
	finishedAt := seededAt.Add(11 * time.Minute)

	// acquisition_manifests_lineage_shape requires variant -> release -> asset to be
	// present together, so a Manifest that declares a Variant resolves its lineage
	// from the seeded Variant instead of inventing it.
	var assetIDValue, releaseIDValue, variantIDValue *string
	if variantID != nil {
		var releaseID, assetID string
		if err := pool.QueryRow(ctx, `
SELECT variant_row.release_id::text, release_row.asset_id::text
FROM variants variant_row
JOIN releases release_row ON release_row.release_id = variant_row.release_id
WHERE variant_row.variant_id = $1`, string(*variantID)).Scan(&releaseID, &assetID); err != nil {
			t.Fatalf("resolve Variant %s lineage: %v", *variantID, err)
		}
		assetIDValue = &assetID
		releaseIDValue = &releaseID
		variantIDValue = optionalID(variantID)
	}

	payload := `{"schema_version":1,"manifest_id":"` + string(manifestID) + `"}`
	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    claim_attempts, attempt_count, max_attempts, lease_owner, lease_expires_at,
    last_error, created_at, updated_at, started_at, finished_at
) VALUES ($1, $2, $3, 'RUNNING', $4, $5, $6, $7, $8, $9, $10, $11, $12, $11, $13)`,
		string(jobID), acquisition.JobTypeAcquisition, payload,
		acquisition.AcquisitionJobIdempotencyKey(manifestID),
		claim, failureBudget, maxAttempts, owner, leaseEnd,
		scratchError, seededAt, jobUpdatedAt, finishedAt,
	); err != nil {
		t.Fatalf("seed RUNNING ACQUISITION Job %s: %v", jobID, err)
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, expected_name, result_name,
    target_storage_binding_id, target_path, asset_id, release_id, variant_id,
    job_id, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, $4, '/downloads/item', $5, $6, $7, $8, $9, $10, $11)`,
		string(manifestID), "acquired-item.bin", "acquired-result.bin", string(bindingID),
		assetIDValue, releaseIDValue, variantIDValue, string(jobID), string(state), seededAt, manifestUpdatedAt,
	); err != nil {
		t.Fatalf("seed %s Manifest %s: %v", state, manifestID, err)
	}

	return canonicalPgSeeded{
		manifestID: manifestID, jobID: jobID, owner: owner, claim: claim,
		failureBudget: failureBudget, maxAttempts: maxAttempts, leaseEnd: leaseEnd,
		manifestUpdatedAt: manifestUpdatedAt, jobUpdatedAt: jobUpdatedAt,
		lastError: &scratchError, finishedAt: &finishedAt,
	}
}

// seedCanonicalPgCopy inserts one projected Copy for the fixture root and returns
// it, so plans can carry exactly the durable identity under test.
func seedCanonicalPgCopy(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	bindingID storage.BindingID,
	suffix string,
	resourceID string,
	variantID *catalog.VariantID,
	availability string,
) catalog.Copy {
	t.Helper()
	seededAt := time.Date(2026, 9, 30, 13, 0, 0, 0, time.UTC)
	copyRecord := catalog.Copy{
		ID:                  canonicalPgCopyID(suffix),
		VariantID:           variantID,
		IndexCoreRootID:     canonicalPgCopyRoot,
		IndexCoreResourceID: resourceID,
		StorageBindingID:    catalog.StorageBindingID(bindingID),
		Availability:        availability,
		CreatedAt:           seededAt,
		UpdatedAt:           seededAt,
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO copies (
    copy_id, variant_id, indexcore_root_id, indexcore_resource_id,
    storage_binding_id, availability, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
		string(copyRecord.ID), optionalID(variantID), copyRecord.IndexCoreRootID,
		copyRecord.IndexCoreResourceID, string(bindingID), availability, seededAt,
	); err != nil {
		t.Fatalf("seed Copy %s: %v", copyRecord.ID, err)
	}
	return copyRecord
}

// --- plan builders -------------------------------------------------------------

// canonicalPgReadyPlan is the plan the frozen D-033 ready path produces: the
// manifest and Job milestones are the post-finalization states, and the Copy facts
// are revalidated under the row lock at commit time.
func canonicalPgReadyPlan(
	seeded canonicalPgSeeded,
	bindingID storage.BindingID,
	copyRecord catalog.Copy,
	manifestVariantID *catalog.VariantID,
	now time.Time,
) acquisition.CanonicalPlan {
	return acquisition.CanonicalPlan{
		ManifestID:               seeded.manifestID,
		JobID:                    seeded.jobID,
		Owner:                    seeded.owner,
		ExpectedClaim:            seeded.claim,
		Ready:                    true,
		ResultCopyID:             copyRecord.ID,
		ExpectedCopyRootID:       copyRecord.IndexCoreRootID,
		ExpectedCopyResourceID:   copyRecord.IndexCoreResourceID,
		ExpectedCopyBindingID:    bindingID,
		ExpectedCopyAvailability: catalog.CopyAvailabilityPresent,
		ExpectedCopyVariantID:    copyRecord.VariantID,
		ManifestVariantID:        manifestVariantID,
		ManifestState:            acquisition.StateReady,
		JobState:                 jobs.StateSucceeded,
		Now:                      now,
	}
}

// canonicalPgPendingPlan is the plan the D-033 pending path produces: the Manifest
// stays awaiting canonical and the same Job is rescheduled without consuming the
// failure budget.
func canonicalPgPendingPlan(
	seeded canonicalPgSeeded,
	bindingID storage.BindingID,
	manifestVariantID *catalog.VariantID,
	now time.Time,
) acquisition.CanonicalPlan {
	retryAt := now.Add(2 * time.Minute)
	return acquisition.CanonicalPlan{
		ManifestID:            seeded.manifestID,
		JobID:                 seeded.jobID,
		Owner:                 seeded.owner,
		ExpectedClaim:         seeded.claim,
		Ready:                 false,
		ManifestState:         acquisition.StateAwaitingCanonical,
		JobState:              jobs.StateRetryWait,
		ExpectedCopyBindingID: bindingID,
		ManifestVariantID:     manifestVariantID,
		Now:                   now,
		RetryAt:               &retryAt,
	}
}

// canonicalPgMutatePlan returns a copy of plan with one field tampered, so a
// concrete plan can be rejected exactly as a defective caller would send it.
func canonicalPgMutatePlan(plan acquisition.CanonicalPlan, mutate func(*acquisition.CanonicalPlan)) acquisition.CanonicalPlan {
	mutate(&plan)
	return plan
}

// --- raw-SQL readers -----------------------------------------------------------

type canonicalPgDurableManifest struct {
	State        acquisition.State
	ResultCopyID *catalog.CopyID
	JobID        *jobs.JobID
	VariantID    *catalog.VariantID
	UpdatedAt    time.Time
}

func readCanonicalPgManifest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id acquisition.ManifestID) canonicalPgDurableManifest {
	t.Helper()
	var state string
	var resultCopyID, jobID, variantID sql.NullString
	var updatedAt time.Time
	if err := pool.QueryRow(ctx, `
SELECT state, result_copy_id::text, job_id::text, variant_id::text, updated_at
FROM acquisition_manifests
WHERE manifest_id = $1`, string(id)).Scan(&state, &resultCopyID, &jobID, &variantID, &updatedAt); err != nil {
		t.Fatalf("read durable Manifest %s: %v", id, err)
	}
	row := canonicalPgDurableManifest{State: acquisition.State(state), UpdatedAt: updatedAt}
	if resultCopyID.Valid {
		value := catalog.CopyID(resultCopyID.String)
		row.ResultCopyID = &value
	}
	if jobID.Valid {
		value := jobs.JobID(jobID.String)
		row.JobID = &value
	}
	if variantID.Valid {
		value := catalog.VariantID(variantID.String)
		row.VariantID = &value
	}
	return row
}

type canonicalPgDurableJob struct {
	State          jobs.State
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	NextAttemptAt  *time.Time
	LastError      *string
	FinishedAt     *time.Time
	ClaimAttempts  int
	FailureCount   int
	MaxAttempts    int
	UpdatedAt      time.Time
}

func readCanonicalPgJob(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id jobs.JobID) canonicalPgDurableJob {
	t.Helper()
	row := canonicalPgDurableJob{}
	var state string
	var leaseOwner, lastError sql.NullString
	var leaseExpiresAt, nextAttemptAt, finishedAt sql.NullTime
	if err := pool.QueryRow(ctx, `
SELECT state, lease_owner, lease_expires_at, next_attempt_at, last_error, finished_at,
       claim_attempts, attempt_count, max_attempts, updated_at
FROM jobs
WHERE job_id = $1`, string(id)).Scan(
		&state, &leaseOwner, &leaseExpiresAt, &nextAttemptAt, &lastError, &finishedAt,
		&row.ClaimAttempts, &row.FailureCount, &row.MaxAttempts, &row.UpdatedAt,
	); err != nil {
		t.Fatalf("read durable Job %s: %v", id, err)
	}
	row.State = jobs.State(state)
	row.LeaseOwner = stringPointer(leaseOwner)
	row.LeaseExpiresAt = timePointer(leaseExpiresAt)
	row.NextAttemptAt = timePointer(nextAttemptAt)
	row.LastError = stringPointer(lastError)
	row.FinishedAt = timePointer(finishedAt)
	return row
}

// --- comparison helpers --------------------------------------------------------

func canonicalPgOptionalStringEqual(first, second *string) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	return *first == *second
}

func canonicalPgFormatOptionalString(value *string) string {
	if value == nil {
		return "<NULL>"
	}
	return *value
}

func canonicalPgOptionalTimeEqual(first, second *time.Time) bool {
	if first == nil || second == nil {
		return first == nil && second == nil
	}
	return first.Equal(*second)
}

func canonicalPgFormatOptionalTime(value *time.Time) string {
	if value == nil {
		return "<NULL>"
	}
	return value.UTC().Format(time.RFC3339Nano)
}

// canonicalPgAssertUntouched proves a rejected commit wrote neither the Manifest
// nor the Job: both states, both timestamps, the lease, the schedule, and both
// counters must be byte-for-byte the seeded values.
func canonicalPgAssertUntouched(
	t *testing.T,
	fixture canonicalPgIntegration,
	seeded canonicalPgSeeded,
	wantState acquisition.State,
) {
	t.Helper()
	manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	if manifest.State != wantState {
		t.Fatalf("Manifest state = %q, want %q after a rejected commit", manifest.State, wantState)
	}
	if manifest.ResultCopyID != nil {
		t.Fatalf("Manifest result_copy_id = %q, want NULL after a rejected commit", *manifest.ResultCopyID)
	}
	if !manifest.UpdatedAt.Equal(seeded.manifestUpdatedAt) {
		t.Fatalf("Manifest updated_at = %s, want %s (unchanged)",
			manifest.UpdatedAt.UTC().Format(time.RFC3339Nano),
			seeded.manifestUpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	if manifest.JobID == nil || *manifest.JobID != seeded.jobID {
		t.Fatalf("Manifest job_id = %v, want %s", manifest.JobID, seeded.jobID)
	}

	job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
	if job.State != jobs.StateRunning {
		t.Fatalf("Job state = %q, want RUNNING after a rejected commit", job.State)
	}
	if !canonicalPgOptionalStringEqual(job.LeaseOwner, canonicalPgString(seeded.owner)) {
		t.Fatalf("Job lease_owner = %s, want %q after a rejected commit",
			canonicalPgFormatOptionalString(job.LeaseOwner), seeded.owner)
	}
	if job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.Equal(seeded.leaseEnd) {
		t.Fatalf("Job lease_expires_at = %s, want %s after a rejected commit",
			canonicalPgFormatOptionalTime(job.LeaseExpiresAt),
			seeded.leaseEnd.UTC().Format(time.RFC3339Nano))
	}
	if job.NextAttemptAt != nil {
		t.Fatalf("Job next_attempt_at = %s, want NULL after a rejected commit",
			canonicalPgFormatOptionalTime(job.NextAttemptAt))
	}
	if !canonicalPgOptionalStringEqual(job.LastError, seeded.lastError) {
		t.Fatalf("Job last_error = %s, want %s after a rejected commit",
			canonicalPgFormatOptionalString(job.LastError),
			canonicalPgFormatOptionalString(seeded.lastError))
	}
	if !canonicalPgOptionalTimeEqual(job.FinishedAt, seeded.finishedAt) {
		t.Fatalf("Job finished_at = %s, want %s after a rejected commit",
			canonicalPgFormatOptionalTime(job.FinishedAt),
			canonicalPgFormatOptionalTime(seeded.finishedAt))
	}
	if job.ClaimAttempts != seeded.claim || job.FailureCount != seeded.failureBudget || job.MaxAttempts != seeded.maxAttempts {
		t.Fatalf("Job counters = claim %d, failure %d, max %d; want %d, %d, %d after a rejected commit",
			job.ClaimAttempts, job.FailureCount, job.MaxAttempts,
			seeded.claim, seeded.failureBudget, seeded.maxAttempts)
	}
	if !job.UpdatedAt.Equal(seeded.jobUpdatedAt) {
		t.Fatalf("Job updated_at = %s, want %s (unchanged)",
			job.UpdatedAt.UTC().Format(time.RFC3339Nano),
			seeded.jobUpdatedAt.UTC().Format(time.RFC3339Nano))
	}
}

// canonicalPgAssertCheckViolation proves the database rejected a value through the
// named CHECK constraint (SQLSTATE 23514).
func canonicalPgAssertCheckViolation(t *testing.T, err error, constraint, operation string) {
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
	if postgresError.ConstraintName != constraint {
		t.Fatalf("%s violated %q, want %q", operation, postgresError.ConstraintName, constraint)
	}
}

// --- 1: READY finalization -----------------------------------------------------

func TestPostgresCanonicalReadyFinalizesManifestAndJob(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "01",
		acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "01",
		"resource-ready-01", nil, catalog.CopyAvailabilityPresent)
	plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())

	result, err := fixture.repository.CommitCanonical(fixture.ctx, plan)
	if err != nil {
		t.Fatalf("CommitCanonical() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("CommitCanonical() reported Changed=false for a fresh finalization")
	}
	if result.Manifest.State != acquisition.StateReady {
		t.Fatalf("result Manifest state = %q, want READY", result.Manifest.State)
	}
	if result.Manifest.ResultCopyID == nil || *result.Manifest.ResultCopyID != copyRecord.ID {
		t.Fatalf("result Manifest result_copy_id = %v, want %s", result.Manifest.ResultCopyID, copyRecord.ID)
	}
	if result.Job.State != jobs.StateSucceeded {
		t.Fatalf("result Job state = %q, want SUCCEEDED", result.Job.State)
	}

	manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	if manifest.State != acquisition.StateReady {
		t.Fatalf("durable Manifest state = %q, want READY", manifest.State)
	}
	if manifest.ResultCopyID == nil || *manifest.ResultCopyID != copyRecord.ID {
		t.Fatalf("durable Manifest result_copy_id = %v, want %s", manifest.ResultCopyID, copyRecord.ID)
	}
	if !manifest.UpdatedAt.Equal(plan.Now) {
		t.Fatalf("durable Manifest updated_at = %s, want plan.Now %s",
			manifest.UpdatedAt.UTC().Format(time.RFC3339Nano), plan.Now.UTC().Format(time.RFC3339Nano))
	}
	if manifest.JobID == nil || *manifest.JobID != seeded.jobID {
		t.Fatalf("durable Manifest job_id = %v, want %s", manifest.JobID, seeded.jobID)
	}

	job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
	if job.State != jobs.StateSucceeded {
		t.Fatalf("durable Job state = %q, want SUCCEEDED", job.State)
	}
	if job.LeaseOwner != nil || job.LeaseExpiresAt != nil {
		t.Fatalf("lease was not cleared: owner=%s expires=%s",
			canonicalPgFormatOptionalString(job.LeaseOwner), canonicalPgFormatOptionalTime(job.LeaseExpiresAt))
	}
	if job.NextAttemptAt != nil {
		t.Fatalf("Job next_attempt_at = %s, want NULL", canonicalPgFormatOptionalTime(job.NextAttemptAt))
	}
	if job.LastError != nil {
		t.Fatalf("Job last_error = %s, want NULL", canonicalPgFormatOptionalString(job.LastError))
	}
	if job.FinishedAt == nil || !job.FinishedAt.Equal(plan.Now) {
		t.Fatalf("Job finished_at = %s, want plan.Now %s",
			canonicalPgFormatOptionalTime(job.FinishedAt), plan.Now.UTC().Format(time.RFC3339Nano))
	}
	if !job.UpdatedAt.Equal(plan.Now) {
		t.Fatalf("Job updated_at = %s, want plan.Now %s",
			job.UpdatedAt.UTC().Format(time.RFC3339Nano), plan.Now.UTC().Format(time.RFC3339Nano))
	}
	if job.ClaimAttempts != 1 || job.FailureCount != 1 || job.MaxAttempts != 5 {
		t.Fatalf("Job counters = claim %d, failure %d, max %d; want 1, 1, 5 (finalization is not a failure)",
			job.ClaimAttempts, job.FailureCount, job.MaxAttempts)
	}
}

// --- 2: pending reschedule -----------------------------------------------------

func TestPostgresCanonicalPendingRescheduleLeavesManifestAwaiting(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "02",
		acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	plan := canonicalPgPendingPlan(seeded, fixture.bindingID, nil, canonicalPgNow())

	result, err := fixture.repository.CommitCanonical(fixture.ctx, plan)
	if err != nil {
		t.Fatalf("CommitCanonical() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("CommitCanonical() reported Changed=false for a fresh pending reschedule")
	}
	if result.Manifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("result Manifest state = %q, want AWAITING_CANONICAL", result.Manifest.State)
	}
	if result.Job.State != jobs.StateRetryWait {
		t.Fatalf("result Job state = %q, want RETRY_WAIT", result.Job.State)
	}

	manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	if manifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("durable Manifest state = %q, want AWAITING_CANONICAL", manifest.State)
	}
	if manifest.ResultCopyID != nil {
		t.Fatalf("durable Manifest result_copy_id = %q, want NULL: pending work never links a result",
			*manifest.ResultCopyID)
	}
	if !manifest.UpdatedAt.Equal(seeded.manifestUpdatedAt) {
		t.Fatalf("durable Manifest updated_at = %s, want the untouched seed value %s",
			manifest.UpdatedAt.UTC().Format(time.RFC3339Nano),
			seeded.manifestUpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	if manifest.UpdatedAt.Equal(plan.Now) {
		t.Fatal("pending reschedule wrote plan.Now into the Manifest timestamp")
	}

	job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
	if job.State != jobs.StateRetryWait {
		t.Fatalf("durable Job state = %q, want RETRY_WAIT", job.State)
	}
	if job.LeaseOwner != nil || job.LeaseExpiresAt != nil {
		t.Fatalf("lease was not cleared: owner=%s expires=%s",
			canonicalPgFormatOptionalString(job.LeaseOwner), canonicalPgFormatOptionalTime(job.LeaseExpiresAt))
	}
	if job.NextAttemptAt == nil || !job.NextAttemptAt.Equal(*plan.RetryAt) {
		t.Fatalf("Job next_attempt_at = %s, want RetryAt %s",
			canonicalPgFormatOptionalTime(job.NextAttemptAt), plan.RetryAt.UTC().Format(time.RFC3339Nano))
	}
	if job.FinishedAt != nil {
		t.Fatalf("Job finished_at = %s, want NULL", canonicalPgFormatOptionalTime(job.FinishedAt))
	}
	if job.LastError != nil {
		t.Fatalf("Job last_error = %s, want NULL", canonicalPgFormatOptionalString(job.LastError))
	}
	if !job.UpdatedAt.Equal(plan.Now) {
		t.Fatalf("Job updated_at = %s, want plan.Now %s",
			job.UpdatedAt.UTC().Format(time.RFC3339Nano), plan.Now.UTC().Format(time.RFC3339Nano))
	}
	if job.ClaimAttempts != 1 || job.FailureCount != 1 || job.MaxAttempts != 5 {
		t.Fatalf("Job counters = claim %d, failure %d, max %d; want 1, 1, 5 (pending work is not a failure)",
			job.ClaimAttempts, job.FailureCount, job.MaxAttempts)
	}
}

// --- 3: exact replay rewrites nothing ------------------------------------------

func TestPostgresCanonicalExactReplayRewritesNothing(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "03",
		acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "03",
		"resource-replay-03", nil, catalog.CopyAvailabilityPresent)
	plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())

	first, err := fixture.repository.CommitCanonical(fixture.ctx, plan)
	if err != nil || !first.Changed {
		t.Fatalf("first CommitCanonical() = %#v, %v", first, err)
	}
	firstManifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	firstJob := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)

	// The exact same plan, replayed later: the durable outcome must not move.
	later := plan
	later.Now = plan.Now.Add(3 * time.Hour)
	replayed, err := fixture.repository.CommitCanonical(fixture.ctx, later)
	if err != nil {
		t.Fatalf("exact replay CommitCanonical() error = %v", err)
	}
	if replayed.Changed {
		t.Fatal("exact replay CommitCanonical() reported Changed=true")
	}
	if replayed.Manifest.ResultCopyID == nil || *replayed.Manifest.ResultCopyID != copyRecord.ID {
		t.Fatalf("replay result_copy_id = %v, want %s", replayed.Manifest.ResultCopyID, copyRecord.ID)
	}
	if !replayed.Manifest.UpdatedAt.Equal(firstManifest.UpdatedAt) || !replayed.Job.UpdatedAt.Equal(firstJob.UpdatedAt) {
		t.Fatalf("replay returned rewritten timestamps: Manifest %s -> %s, Job %s -> %s",
			canonicalPgFormatOptionalTime(&firstManifest.UpdatedAt), canonicalPgFormatOptionalTime(&replayed.Manifest.UpdatedAt),
			canonicalPgFormatOptionalTime(&firstJob.UpdatedAt), canonicalPgFormatOptionalTime(&replayed.Job.UpdatedAt))
	}

	manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
	if manifest.State != acquisition.StateReady || job.State != jobs.StateSucceeded {
		t.Fatalf("replay changed durable states: Manifest %q, Job %q", manifest.State, job.State)
	}
	if manifest.ResultCopyID == nil || *manifest.ResultCopyID != copyRecord.ID {
		t.Fatalf("durable result_copy_id = %v, want %s", manifest.ResultCopyID, copyRecord.ID)
	}
	if !manifest.UpdatedAt.Equal(firstManifest.UpdatedAt) {
		t.Fatalf("durable Manifest updated_at = %s, want %s",
			manifest.UpdatedAt.UTC().Format(time.RFC3339Nano), firstManifest.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	if !job.UpdatedAt.Equal(firstJob.UpdatedAt) {
		t.Fatalf("durable Job updated_at = %s, want %s",
			job.UpdatedAt.UTC().Format(time.RFC3339Nano), firstJob.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	if manifest.UpdatedAt.Equal(later.Now) || job.UpdatedAt.Equal(later.Now) {
		t.Fatal("replay wrote the later plan timestamp")
	}
	if !canonicalPgOptionalTimeEqual(job.FinishedAt, firstJob.FinishedAt) {
		t.Fatalf("durable Job finished_at = %s, want %s",
			canonicalPgFormatOptionalTime(job.FinishedAt), canonicalPgFormatOptionalTime(firstJob.FinishedAt))
	}
}

// --- 4: conflicting replay -----------------------------------------------------

func TestPostgresCanonicalReplayWithDifferentResultCopyFailsClosed(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "04",
		acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	confirmed := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "04",
		"resource-replay-confirmed-04", nil, catalog.CopyAvailabilityPresent)
	other := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "05",
		"resource-replay-other-05", nil, catalog.CopyAvailabilityPresent)

	plan := canonicalPgReadyPlan(seeded, fixture.bindingID, confirmed, nil, canonicalPgNow())
	if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); err != nil {
		t.Fatalf("first CommitCanonical() error = %v", err)
	}
	beforeManifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	beforeJob := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)

	conflicting := canonicalPgReadyPlan(seeded, fixture.bindingID, other, nil, canonicalPgNow().Add(time.Hour))
	if _, err := fixture.repository.CommitCanonical(fixture.ctx, conflicting); !errors.Is(err, acquisition.ErrCanonicalCopyConflict) {
		t.Fatalf("conflicting replay CommitCanonical() error = %v, want ErrCanonicalCopyConflict", err)
	}

	afterManifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
	afterJob := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
	if afterManifest.State != acquisition.StateReady {
		t.Fatalf("durable Manifest state = %q, want READY", afterManifest.State)
	}
	if afterManifest.ResultCopyID == nil || *afterManifest.ResultCopyID != confirmed.ID {
		t.Fatalf("durable result_copy_id = %v, want the confirmed Copy %s", afterManifest.ResultCopyID, confirmed.ID)
	}
	if !afterManifest.UpdatedAt.Equal(beforeManifest.UpdatedAt) {
		t.Fatalf("durable Manifest updated_at = %s, want %s",
			afterManifest.UpdatedAt.UTC().Format(time.RFC3339Nano),
			beforeManifest.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	if !afterJob.UpdatedAt.Equal(beforeJob.UpdatedAt) {
		t.Fatalf("durable Job updated_at = %s, want %s",
			afterJob.UpdatedAt.UTC().Format(time.RFC3339Nano), beforeJob.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	if !canonicalPgOptionalTimeEqual(afterJob.FinishedAt, beforeJob.FinishedAt) {
		t.Fatalf("durable Job finished_at = %s, want %s",
			canonicalPgFormatOptionalTime(afterJob.FinishedAt), canonicalPgFormatOptionalTime(beforeJob.FinishedAt))
	}
}

// --- 5: READY without a result link --------------------------------------------

// TestPostgresCanonicalReadyWithoutResultCopyFailsClosed seeds the exact recovery
// debt migration 0012 documents: a READY row that predates the column. Migration
// 0012 adds its CHECK as NOT VALID precisely so such a historical row survives, and
// the store must then fail closed instead of finalizing against it.
func TestPostgresCanonicalReadyWithoutResultCopyFailsClosed(t *testing.T) {
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
	legacyMigrator := &Migrator{pool: pool, migrations: legacy}
	legacyStatus, err := legacyMigrator.Apply(ctx)
	if err != nil || legacyStatus.CurrentVersion != 11 {
		t.Fatalf("apply through version 11 = %#v, %v", legacyStatus, err)
	}

	bindingID := storage.BindingID(canonicalPgBindingUUID)
	seedStorageBinding(t, ctx, pool, bindingID, canonicalPgCopyRoot)

	manifestID := canonicalPgManifestID("06")
	jobID := canonicalPgJobID("06")
	seededAt := time.Date(2026, 9, 30, 13, 0, 0, 0, time.UTC)
	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    claim_attempts, attempt_count, max_attempts, created_at, updated_at, started_at, finished_at
) VALUES ($1, $2, $3, 'SUCCEEDED', $4, 1, 1, 5, $5, $5, $5, $5)`,
		string(jobID), acquisition.JobTypeAcquisition,
		`{"schema_version":1,"manifest_id":"`+string(manifestID)+`"}`,
		acquisition.AcquisitionJobIdempotencyKey(manifestID), seededAt,
	); err != nil {
		t.Fatalf("seed SUCCEEDED ACQUISITION Job: %v", err)
	}
	// result_copy_id does not exist yet, so a READY row is legal and honest here:
	// this is exactly the pre-Gate-3.10 database the migration must tolerate.
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, expected_name, result_name,
    target_storage_binding_id, target_path, job_id, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, $4, '/downloads/item', $5, 'READY', $6, $6)`,
		string(manifestID), "acquired-item.bin", "acquired-result.bin",
		string(bindingID), string(jobID), seededAt,
	); err != nil {
		t.Fatalf("seed version 11 READY Manifest: %v", err)
	}

	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	upgraded, err := migrator.Apply(ctx)
	if err != nil || !upgraded.Compatible || upgraded.CurrentVersion != 12 {
		t.Fatalf("apply version 12 = %#v, %v", upgraded, err)
	}

	historical := readCanonicalPgManifest(t, ctx, pool, manifestID)
	if historical.State != acquisition.StateReady || historical.ResultCopyID != nil {
		t.Fatalf("upgraded historical row = %#v, want READY with a NULL result link", historical)
	}

	manifests, err := NewAcquisitionManifestRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionManifestRepository() error = %v", err)
	}
	stored, err := manifests.GetManifest(ctx, manifestID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if stored.State != acquisition.StateReady || stored.ResultCopyID != nil {
		t.Fatalf("production reader returned = %#v, want READY with a NULL result link", stored)
	}
	// The port itself refuses the shape the historical row carries.
	if err := acquisition.ValidateManifest(stored); !errors.Is(err, acquisition.ErrInvalidArgument) {
		t.Fatalf("ValidateManifest(READY without result_copy_id) error = %v, want ErrInvalidArgument", err)
	}

	repository, err := NewCanonicalStageRepository(pool)
	if err != nil {
		t.Fatalf("NewCanonicalStageRepository() error = %v", err)
	}
	// The plan is a well-formed Ready plan: the defect is entirely in the durable row.
	proposed := catalog.Copy{
		ID:                  canonicalPgCopyID("06"),
		IndexCoreRootID:     canonicalPgCopyRoot,
		IndexCoreResourceID: "resource-null-result-06",
		StorageBindingID:    catalog.StorageBindingID(bindingID),
		Availability:        catalog.CopyAvailabilityPresent,
	}
	plan := canonicalPgReadyPlan(canonicalPgSeeded{
		manifestID: manifestID, jobID: jobID, owner: "canonical-worker-a", claim: 1,
	}, bindingID, proposed, nil, canonicalPgNow())
	if _, err := repository.CommitCanonical(ctx, plan); !errors.Is(err, acquisition.ErrCanonicalCopyConflict) {
		t.Fatalf("CommitCanonical() against a READY row without a result link error = %v, want ErrCanonicalCopyConflict", err)
	}

	after := readCanonicalPgManifest(t, ctx, pool, manifestID)
	if after.State != acquisition.StateReady || after.ResultCopyID != nil {
		t.Fatalf("historical row changed to %#v, want READY with a NULL result link", after)
	}
	if !after.UpdatedAt.Equal(seededAt) {
		t.Fatalf("historical row updated_at = %s, want %s",
			after.UpdatedAt.UTC().Format(time.RFC3339Nano), seededAt.UTC().Format(time.RFC3339Nano))
	}
	afterJob := readCanonicalPgJob(t, ctx, pool, jobID)
	if afterJob.State != jobs.StateSucceeded {
		t.Fatalf("historical Job state = %q, want SUCCEEDED", afterJob.State)
	}
	if !afterJob.UpdatedAt.Equal(seededAt) {
		t.Fatalf("historical Job updated_at = %s, want %s",
			afterJob.UpdatedAt.UTC().Format(time.RFC3339Nano), seededAt.UTC().Format(time.RFC3339Nano))
	}
}

// --- 6: a REMOVED Copy can never authorize READY --------------------------------

func TestPostgresCanonicalRemovedCopyFailsClosed(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "07",
		acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	removed := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "07",
		"resource-removed-07", nil, catalog.CopyAvailabilityRemoved)
	plan := canonicalPgReadyPlan(seeded, fixture.bindingID, removed, nil, canonicalPgNow())
	if plan.ExpectedCopyAvailability != catalog.CopyAvailabilityPresent {
		t.Fatalf("plan expects %q, want PRESENT: a stale application read still asserts PRESENT",
			plan.ExpectedCopyAvailability)
	}

	if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalCopyRemoved) {
		t.Fatalf("CommitCanonical() against a REMOVED Copy error = %v, want ErrCanonicalCopyRemoved", err)
	}
	canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
}

// TestPostgresCanonicalCopyRemovedUnderLockFailsClosed proves the exact race the
// row lock exists for: the application read the Copy as PRESENT, then the Copy
// became REMOVED before the finalization transaction committed. The blocker holds
// an uncommitted UPDATE on the Copy row, so the repository's `FOR SHARE` re-read
// must block and then observe the committed REMOVED version.
func TestPostgresCanonicalCopyRemovedUnderLockFailsClosed(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "08",
		acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "08",
		"resource-removed-under-lock-08", nil, catalog.CopyAvailabilityPresent)
	plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())

	blocker, err := fixture.pool.Begin(fixture.ctx)
	if err != nil {
		t.Fatalf("begin blocker transaction: %v", err)
	}
	if _, err := blocker.Exec(fixture.ctx,
		`UPDATE copies SET availability = $2 WHERE copy_id = $1`,
		string(copyRecord.ID), catalog.CopyAvailabilityRemoved,
	); err != nil {
		_ = blocker.Rollback(fixture.ctx)
		t.Fatalf("mark Copy REMOVED under lock: %v", err)
	}

	type canonicalPgCommitOutcome struct {
		result acquisition.CanonicalResult
		err    error
	}
	done := make(chan canonicalPgCommitOutcome, 1)
	go func() {
		result, err := fixture.repository.CommitCanonical(fixture.ctx, plan)
		done <- canonicalPgCommitOutcome{result: result, err: err}
	}()

	// While the blocker holds the exclusive row lock the commit cannot have read the
	// Copy, so it cannot have completed.
	select {
	case outcome := <-done:
		_ = blocker.Rollback(fixture.ctx)
		t.Fatalf("CommitCanonical() completed while the Copy row was exclusively locked: %#v, %v",
			outcome.result, outcome.err)
	case <-time.After(250 * time.Millisecond):
	}

	if err := blocker.Commit(fixture.ctx); err != nil {
		t.Fatalf("commit blocked Copy removal: %v", err)
	}

	select {
	case outcome := <-done:
		if !errors.Is(outcome.err, acquisition.ErrCanonicalCopyRemoved) {
			t.Fatalf("CommitCanonical() error = %v, want ErrCanonicalCopyRemoved", outcome.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("CommitCanonical() did not return after the Copy row lock was released")
	}

	canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
}

// --- 7: locked Copy identity ---------------------------------------------------

func TestPostgresCanonicalCopyIdentityMismatchFailsClosed(t *testing.T) {
	fixture := newCanonicalPgFixture(t)

	t.Run("wrong indexcore_root_id", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "09",
			acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
		copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "09",
			"resource-identity-09", nil, catalog.CopyAvailabilityPresent)
		plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())
		plan.ExpectedCopyRootID = "canonical-other-root"

		if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalIdentity) {
			t.Fatalf("CommitCanonical() with a wrong root error = %v, want ErrCanonicalIdentity", err)
		}
		canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
	})

	t.Run("wrong indexcore_resource_id", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "0a",
			acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
		copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "0a",
			"resource-identity-0a", nil, catalog.CopyAvailabilityPresent)
		plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())
		plan.ExpectedCopyResourceID = "resource-identity-other"

		if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalIdentity) {
			t.Fatalf("CommitCanonical() with a wrong resource error = %v, want ErrCanonicalIdentity", err)
		}
		canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
	})

	t.Run("wrong storage_binding_id", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "0b",
			acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
		copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "0b",
			"resource-identity-0b", nil, catalog.CopyAvailabilityPresent)
		plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())
		plan.ExpectedCopyBindingID = storage.BindingID("3f000000-0000-4000-8000-000000000099")

		if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalCopyBinding) {
			t.Fatalf("CommitCanonical() with a wrong StorageBinding error = %v, want ErrCanonicalCopyBinding", err)
		}
		canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
	})
}

// --- 8: Variant classification -------------------------------------------------

func TestPostgresCanonicalVariantBindingFailsClosed(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	variantID := catalog.VariantID(seedVariant(t, fixture.ctx, fixture.pool))

	t.Run("Copy not bound to the Manifest Variant", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "0c",
			acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), &variantID)
		copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "0c",
			"resource-variant-0c", nil, catalog.CopyAvailabilityPresent)
		plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, &variantID, canonicalPgNow())

		if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalCopyClassified) {
			t.Fatalf("CommitCanonical() with an unbound Copy error = %v, want ErrCanonicalCopyClassified", err)
		}
		canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
	})

	t.Run("Copy already bound to the Manifest Variant finalizes", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "0d",
			acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), &variantID)
		copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "0d",
			"resource-variant-0d", &variantID, catalog.CopyAvailabilityPresent)
		plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, &variantID, canonicalPgNow())

		result, err := fixture.repository.CommitCanonical(fixture.ctx, plan)
		if err != nil {
			t.Fatalf("CommitCanonical() with a bound Copy error = %v", err)
		}
		if !result.Changed {
			t.Fatal("CommitCanonical() reported Changed=false for a Variant-bound finalization")
		}
		manifest := readCanonicalPgManifest(t, fixture.ctx, fixture.pool, seeded.manifestID)
		if manifest.State != acquisition.StateReady {
			t.Fatalf("durable Manifest state = %q, want READY", manifest.State)
		}
		if manifest.ResultCopyID == nil || *manifest.ResultCopyID != copyRecord.ID {
			t.Fatalf("durable result_copy_id = %v, want %s", manifest.ResultCopyID, copyRecord.ID)
		}
		if manifest.VariantID == nil || *manifest.VariantID != variantID {
			t.Fatalf("durable Manifest variant_id = %v, want %s", manifest.VariantID, variantID)
		}
	})
}

// --- 9: claim generation and failure budget ------------------------------------

func TestPostgresCanonicalPreservesClaimGenerationAndFailureBudget(t *testing.T) {
	fixture := newCanonicalPgFixture(t)

	t.Run("pending reschedule", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "0e",
			acquisition.StateAwaitingCanonical, "canonical-worker-budget", 3, 1, 5, canonicalPgLeaseEnd(), nil)
		plan := canonicalPgPendingPlan(seeded, fixture.bindingID, nil, canonicalPgNow())
		if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); err != nil {
			t.Fatalf("CommitCanonical() error = %v", err)
		}
		job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
		if job.State != jobs.StateRetryWait {
			t.Fatalf("Job state = %q, want RETRY_WAIT", job.State)
		}
		if job.ClaimAttempts != 3 || job.FailureCount != 1 || job.MaxAttempts != 5 {
			t.Fatalf("Job counters = claim %d, failure %d, max %d; want 3, 1, 5",
				job.ClaimAttempts, job.FailureCount, job.MaxAttempts)
		}
	})

	t.Run("READY finalization", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "0f",
			acquisition.StateAwaitingCanonical, "canonical-worker-budget", 3, 1, 5, canonicalPgLeaseEnd(), nil)
		copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "0f",
			"resource-budget-0f", nil, catalog.CopyAvailabilityPresent)
		plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())
		if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); err != nil {
			t.Fatalf("CommitCanonical() error = %v", err)
		}
		job := readCanonicalPgJob(t, fixture.ctx, fixture.pool, seeded.jobID)
		if job.State != jobs.StateSucceeded {
			t.Fatalf("Job state = %q, want SUCCEEDED", job.State)
		}
		if job.ClaimAttempts != 3 || job.FailureCount != 1 || job.MaxAttempts != 5 {
			t.Fatalf("Job counters = claim %d, failure %d, max %d; want 3, 1, 5",
				job.ClaimAttempts, job.FailureCount, job.MaxAttempts)
		}
	})
}

// --- 10: lease fence -----------------------------------------------------------

func TestPostgresCanonicalLeaseFenceRejectsAndMutatesNothing(t *testing.T) {
	fixture := newCanonicalPgFixture(t)

	t.Run("wrong lease owner", func(t *testing.T) {
		t.Run("pending plan", func(t *testing.T) {
			seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "10",
				acquisition.StateAwaitingCanonical, "canonical-worker-a", 3, 1, 5, canonicalPgLeaseEnd(), nil)
			plan := canonicalPgPendingPlan(seeded, fixture.bindingID, nil, canonicalPgNow())
			plan.Owner = "canonical-worker-b"
			if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalFence) {
				t.Fatalf("CommitCanonical() error = %v, want ErrCanonicalFence", err)
			}
			canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
		})

		t.Run("Ready plan", func(t *testing.T) {
			seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "11",
				acquisition.StateAwaitingCanonical, "canonical-worker-a", 3, 1, 5, canonicalPgLeaseEnd(), nil)
			copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "11",
				"resource-fence-11", nil, catalog.CopyAvailabilityPresent)
			plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())
			plan.Owner = "canonical-worker-b"
			if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalFence) {
				t.Fatalf("CommitCanonical() error = %v, want ErrCanonicalFence", err)
			}
			canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
		})
	})

	t.Run("stale claim generation", func(t *testing.T) {
		t.Run("pending plan", func(t *testing.T) {
			seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "12",
				acquisition.StateAwaitingCanonical, "canonical-worker-a", 3, 1, 5, canonicalPgLeaseEnd(), nil)
			plan := canonicalPgPendingPlan(seeded, fixture.bindingID, nil, canonicalPgNow())
			plan.ExpectedClaim = 2
			if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalFence) {
				t.Fatalf("CommitCanonical() error = %v, want ErrCanonicalFence", err)
			}
			canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
		})

		t.Run("Ready plan", func(t *testing.T) {
			seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "13",
				acquisition.StateAwaitingCanonical, "canonical-worker-a", 3, 1, 5, canonicalPgLeaseEnd(), nil)
			copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "13",
				"resource-fence-13", nil, catalog.CopyAvailabilityPresent)
			plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())
			plan.ExpectedClaim = 2
			if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalFence) {
				t.Fatalf("CommitCanonical() error = %v, want ErrCanonicalFence", err)
			}
			canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
		})
	})

	t.Run("expired lease", func(t *testing.T) {
		t.Run("pending plan", func(t *testing.T) {
			seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "14",
				acquisition.StateAwaitingCanonical, "canonical-worker-a", 3, 1, 5, canonicalPgExpiredLeaseEnd(), nil)
			plan := canonicalPgPendingPlan(seeded, fixture.bindingID, nil, canonicalPgNow())
			if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalFence) {
				t.Fatalf("CommitCanonical() error = %v, want ErrCanonicalFence", err)
			}
			canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
		})

		t.Run("Ready plan", func(t *testing.T) {
			seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "15",
				acquisition.StateAwaitingCanonical, "canonical-worker-a", 3, 1, 5, canonicalPgExpiredLeaseEnd(), nil)
			copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "15",
				"resource-fence-15", nil, catalog.CopyAvailabilityPresent)
			plan := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())
			if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalFence) {
				t.Fatalf("CommitCanonical() error = %v, want ErrCanonicalFence", err)
			}
			canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
		})
	})
}

// --- 11: Manifest linkage ------------------------------------------------------

func TestPostgresCanonicalManifestLinkageMismatchFailsClosed(t *testing.T) {
	fixture := newCanonicalPgFixture(t)

	t.Run("Manifest linked to a different Job", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "16",
			acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
		other := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "17",
			acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)

		plan := canonicalPgPendingPlan(seeded, fixture.bindingID, nil, canonicalPgNow())
		plan.JobID = other.jobID
		if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalFence) {
			t.Fatalf("CommitCanonical() with a cross-linked Job error = %v, want ErrCanonicalFence", err)
		}
		canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
		canonicalPgAssertUntouched(t, fixture, other, acquisition.StateAwaitingCanonical)
	})

	t.Run("non-ACQUISITION Job type", func(t *testing.T) {
		seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "18",
			acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
		if _, err := fixture.pool.Exec(fixture.ctx,
			`UPDATE jobs SET job_type = 'INDEXCORE_REFRESH' WHERE job_id = $1`, string(seeded.jobID),
		); err != nil {
			t.Fatalf("mutate Job type: %v", err)
		}

		plan := canonicalPgPendingPlan(seeded, fixture.bindingID, nil, canonicalPgNow())
		if _, err := fixture.repository.CommitCanonical(fixture.ctx, plan); !errors.Is(err, acquisition.ErrCanonicalFence) {
			t.Fatalf("CommitCanonical() with a non-ACQUISITION Job error = %v, want ErrCanonicalFence", err)
		}
		canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
	})
}

// --- 12: tampered plans --------------------------------------------------------

func TestPostgresCanonicalRejectsTamperedPlansBeforeWriting(t *testing.T) {
	fixture := newCanonicalPgFixture(t)
	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "19",
		acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "19",
		"resource-tampered-19", nil, catalog.CopyAvailabilityPresent)

	validReady := canonicalPgReadyPlan(seeded, fixture.bindingID, copyRecord, nil, canonicalPgNow())
	validPending := canonicalPgPendingPlan(seeded, fixture.bindingID, nil, canonicalPgNow())
	if err := validateCanonicalPlan(validReady); err != nil {
		t.Fatalf("validateCanonicalPlan(valid Ready plan) error = %v", err)
	}
	if err := validateCanonicalPlan(validPending); err != nil {
		t.Fatalf("validateCanonicalPlan(valid pending plan) error = %v", err)
	}

	cases := []struct {
		name string
		plan acquisition.CanonicalPlan
	}{
		{"Ready plan with an empty result Copy", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.ResultCopyID = "" })},
		{"Ready plan with ManifestState AWAITING_CANONICAL", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.ManifestState = acquisition.StateAwaitingCanonical })},
		{"Ready plan with JobState RUNNING", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.JobState = jobs.StateRunning })},
		{"Ready plan without a copied root identity", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.ExpectedCopyRootID = "" })},
		{"Ready plan without a copied resource identity", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.ExpectedCopyResourceID = "" })},
		{"Ready plan without a copied StorageBinding", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.ExpectedCopyBindingID = "" })},
		{"Ready plan expecting a REMOVED Copy", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.ExpectedCopyAvailability = catalog.CopyAvailabilityRemoved })},
		{"Ready plan with an empty owner", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.Owner = "" })},
		{"Ready plan without a claim generation", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.ExpectedClaim = 0 })},
		{"Ready plan without a Manifest identity", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.ManifestID = "" })},
		{"Ready plan without a Job identity", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.JobID = "" })},
		{"Ready plan without a plan time", canonicalPgMutatePlan(validReady,
			func(plan *acquisition.CanonicalPlan) { plan.Now = time.Time{} })},
		{"pending plan carrying a result Copy", canonicalPgMutatePlan(validPending,
			func(plan *acquisition.CanonicalPlan) { plan.ResultCopyID = copyRecord.ID })},
		{"pending plan with RetryAt equal to Now", canonicalPgMutatePlan(validPending,
			func(plan *acquisition.CanonicalPlan) { plan.RetryAt = &plan.Now })},
		{"pending plan with RetryAt before Now", canonicalPgMutatePlan(validPending,
			func(plan *acquisition.CanonicalPlan) {
				retryAt := plan.Now.Add(-time.Minute)
				plan.RetryAt = &retryAt
			})},
		{"pending plan without RetryAt", canonicalPgMutatePlan(validPending,
			func(plan *acquisition.CanonicalPlan) { plan.RetryAt = nil })},
		{"pending plan with ManifestState READY", canonicalPgMutatePlan(validPending,
			func(plan *acquisition.CanonicalPlan) { plan.ManifestState = acquisition.StateReady })},
		{"pending plan with JobState SUCCEEDED", canonicalPgMutatePlan(validPending,
			func(plan *acquisition.CanonicalPlan) { plan.JobState = jobs.StateSucceeded })},
		{"pending plan with an empty owner", canonicalPgMutatePlan(validPending,
			func(plan *acquisition.CanonicalPlan) { plan.Owner = "" })},
		{"pending plan without a claim generation", canonicalPgMutatePlan(validPending,
			func(plan *acquisition.CanonicalPlan) { plan.ExpectedClaim = 0 })},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := fixture.repository.CommitCanonical(fixture.ctx, testCase.plan); !errors.Is(err, acquisition.ErrInvalidCanonicalRequest) {
				t.Fatalf("CommitCanonical(%s) error = %v, want ErrInvalidCanonicalRequest", testCase.name, err)
			}
			canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
		})
	}
}

// --- 13: migration 0012 schema and constraint behaviour ------------------------

func TestPostgresCanonicalMigration0012SchemaAndConstraints(t *testing.T) {
	fixture := newCanonicalPgFixture(t)

	var columnDefault sql.NullString
	var isNullable, dataType string
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT column_default, is_nullable, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'acquisition_manifests'
  AND column_name = 'result_copy_id'`).Scan(&columnDefault, &isNullable, &dataType); err != nil {
		t.Fatalf("read information_schema for acquisition_manifests.result_copy_id: %v", err)
	}
	if columnDefault.Valid {
		t.Fatalf("result_copy_id has column default %q; migration 0012 forbids fabricating a result link", columnDefault.String)
	}
	if isNullable != "YES" {
		t.Fatalf("result_copy_id is_nullable = %q, want YES", isNullable)
	}
	if dataType != "uuid" {
		t.Fatalf("result_copy_id data_type = %q, want uuid", dataType)
	}

	var constraintType, constraintDefinition string
	var constraintValidated bool
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT constraint_row.contype, constraint_row.convalidated, pg_get_constraintdef(constraint_row.oid)
FROM pg_constraint constraint_row
JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
WHERE namespace_row.nspname = 'public'
  AND table_row.relname = 'acquisition_manifests'
  AND constraint_row.conname = $1`, canonicalPgReadyConstraint).Scan(
		&constraintType, &constraintValidated, &constraintDefinition,
	); err != nil {
		t.Fatalf("read constraint %s: %v", canonicalPgReadyConstraint, err)
	}
	if constraintType != "c" {
		t.Fatalf("constraint %s contype = %q, want c (CHECK)", canonicalPgReadyConstraint, constraintType)
	}
	if !strings.Contains(constraintDefinition, "READY") || !strings.Contains(constraintDefinition, "result_copy_id") {
		t.Fatalf("constraint %s definition = %q, want the READY/result_copy_id rule", canonicalPgReadyConstraint, constraintDefinition)
	}
	// NOT VALID is the documented shape (it tolerates the pre-Gate-3.10 recovery
	// debt), but a validated constraint is equally correct for a fresh schema, so the
	// flag is recorded rather than asserted.
	t.Logf("migration 0012 constraint %s: convalidated=%v, definition=%s",
		canonicalPgReadyConstraint, constraintValidated, constraintDefinition)

	var foreignKeyCount int
	if err := fixture.pool.QueryRow(fixture.ctx, `
SELECT count(*)
FROM pg_constraint constraint_row
JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
JOIN pg_namespace namespace_row ON namespace_row.oid = table_row.relnamespace
WHERE namespace_row.nspname = 'public'
  AND table_row.relname = 'acquisition_manifests'
  AND constraint_row.contype = 'f'
  AND pg_get_constraintdef(constraint_row.oid) LIKE '%result_copy_id%copies%'`).Scan(&foreignKeyCount); err != nil {
		t.Fatalf("inspect the result_copy_id foreign key: %v", err)
	}
	if foreignKeyCount != 1 {
		t.Fatalf("result_copy_id foreign key count = %d, want 1", foreignKeyCount)
	}

	seeded := seedCanonicalPgManifestJob(t, fixture.ctx, fixture.pool, fixture.bindingID, "1a",
		acquisition.StateAwaitingCanonical, "canonical-worker-a", 1, 1, 5, canonicalPgLeaseEnd(), nil)
	copyRecord := seedCanonicalPgCopy(t, fixture.ctx, fixture.pool, fixture.bindingID, "1a",
		"resource-schema-1a", nil, catalog.CopyAvailabilityPresent)

	// A non-READY row may not be promoted to READY without its result link.
	if _, err := fixture.pool.Exec(fixture.ctx,
		`UPDATE acquisition_manifests SET state = 'READY' WHERE manifest_id = $1`, string(seeded.manifestID),
	); err != nil {
		canonicalPgAssertCheckViolation(t, err, canonicalPgReadyConstraint,
			"UPDATE state = READY with a NULL result_copy_id")
	} else {
		t.Fatal("UPDATE state = READY with a NULL result_copy_id was accepted")
	}

	// A READY row may not be inserted without its result link.
	if _, err := fixture.pool.Exec(fixture.ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id,
    target_path, state, result_copy_id, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, '/downloads/schema-check', 'READY', NULL, $3, $3)`,
		"3c000000-0000-4000-8000-0000000000ff", string(fixture.bindingID), canonicalPgNow(),
	); err != nil {
		canonicalPgAssertCheckViolation(t, err, canonicalPgReadyConstraint,
			"INSERT state = READY with a NULL result_copy_id")
	} else {
		t.Fatal("INSERT state = READY with a NULL result_copy_id was accepted")
	}

	// A non-READY row may not carry a result link.
	if _, err := fixture.pool.Exec(fixture.ctx,
		`UPDATE acquisition_manifests SET result_copy_id = $2 WHERE manifest_id = $1`,
		string(seeded.manifestID), string(copyRecord.ID),
	); err != nil {
		canonicalPgAssertCheckViolation(t, err, canonicalPgReadyConstraint,
			"UPDATE result_copy_id on a non-READY Manifest")
	} else {
		t.Fatal("UPDATE result_copy_id on a non-READY Manifest was accepted")
	}

	if _, err := fixture.pool.Exec(fixture.ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id,
    target_path, state, result_copy_id, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, '/downloads/schema-check', 'AWAITING_CANONICAL', $3, $4, $4)`,
		"3c000000-0000-4000-8000-0000000000fe", string(fixture.bindingID), string(copyRecord.ID), canonicalPgNow(),
	); err != nil {
		canonicalPgAssertCheckViolation(t, err, canonicalPgReadyConstraint,
			"INSERT result_copy_id on a non-READY Manifest")
	} else {
		t.Fatal("INSERT result_copy_id on a non-READY Manifest was accepted")
	}

	// Every rejected statement above must have left the healthy row exactly as seeded.
	canonicalPgAssertUntouched(t, fixture, seeded, acquisition.StateAwaitingCanonical)
}
