package acquisition

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var (
	ErrInvalidCanonicalRequest = errors.New("invalid acquisition canonical confirmation request")
	ErrCanonicalManifestState  = errors.New("acquisition Manifest is not awaiting canonical")
	ErrCanonicalManifestNotFnd = errors.New("acquisition canonical Manifest not found")
	ErrCanonicalResultName     = errors.New("acquisition canonical candidate path is invalid")
	ErrCanonicalBindingMissing = errors.New("acquisition canonical StorageBinding not found")
	ErrCanonicalBindingState   = errors.New("acquisition canonical StorageBinding is not active")
	ErrCanonicalRootID         = errors.New("acquisition canonical IndexCore root ID is invalid")
	ErrCanonicalFence          = errors.New("acquisition canonical lease fence did not match")
	ErrCanonicalAmbiguous      = errors.New("acquisition canonical resolution is ambiguous")
	ErrCanonicalIdentity       = errors.New("acquisition canonical resource identity mismatch")
	ErrCanonicalCopyMissing    = errors.New("acquisition canonical Copy is not projected yet")
	ErrCanonicalCopyRemoved    = errors.New("acquisition canonical Copy is REMOVED")
	ErrCanonicalCopyBinding    = errors.New("acquisition canonical Copy belongs to another StorageBinding")
	ErrCanonicalCopyClassified = errors.New("acquisition canonical Copy is classified to a different Variant")
	ErrCanonicalCopyConflict   = errors.New("acquisition canonical result Copy conflicts with a finalized Manifest")
	ErrCanonicalProjector      = errors.New("acquisition canonical projector failure")
	ErrCanonicalConflict       = errors.New("acquisition canonical handoff conflicts with the committed state")
	ErrCanonicalPersistence    = errors.New("acquisition canonical persistence failure")
)

// CanonicalPresence mirrors the provider-neutral physical presence a canonical
// resolution reports. It is acquisition-owned so this package never imports a
// concrete integration.
type CanonicalPresence string

const (
	CanonicalPresencePresent CanonicalPresence = "PRESENT"
	CanonicalPresenceRemoved CanonicalPresence = "REMOVED"
)

// CanonicalResolveRequest is one exact known-path resolution.
//
// Visibility is deliberately absent: D-033 requires the default PRESENT-only
// visibility, so there is nothing for a caller to widen.
type CanonicalResolveRequest struct {
	RootID string
	Path   string
}

// CanonicalResource is the narrow physical identity of one resolved resource.
// It carries no provider file ID, OpenList path, or IndexCore-internal type.
type CanonicalResource struct {
	RootID        string
	ResourceID    string
	CanonicalPath string
	Presence      CanonicalPresence
}

// CanonicalResolution retains every match and the ambiguity decision, so this
// package never silently chooses a winner.
type CanonicalResolution struct {
	Matches   []CanonicalResource
	Ambiguous bool
}

// CanonicalResolvePort is the narrow Q5 boundary.
type CanonicalResolvePort interface {
	ResolveCanonical(context.Context, CanonicalResolveRequest) (CanonicalResolution, error)
}

// CanonicalProjectorPort advances the existing per-binding Journal projection by at
// most one bounded page. It is deliberately expressed as a single bounded call:
// this package cannot loop, and it cannot reach Copy creation.
type CanonicalProjectorPort interface {
	ProjectOnce(context.Context, storage.BindingID, int) (CanonicalProjection, error)
}

// CanonicalProjection reports what one bounded projector invocation did.
type CanonicalProjection struct {
	EventsRead int
	Mutations  int
}

// CanonicalCopyReader resolves Panta's Copy for one exact physical identity.
type CanonicalCopyReader interface {
	GetCopyByPhysicalIdentity(context.Context, string, string) (catalog.Copy, error)
}

// CanonicalClassifier performs the accepted monotonic Copy -> Variant binding and
// returns the resulting Copy.
type CanonicalClassifier interface {
	Bind(context.Context, catalog.CopyID, catalog.VariantID) (catalog.Copy, error)
}

// CanonicalPlan is the complete persistence input for one canonical confirmation
// invocation. The store commits it atomically or not at all.
type CanonicalPlan struct {
	ManifestID    ManifestID
	JobID         jobs.JobID
	Owner         string
	ExpectedClaim int
	// Ready selects the outcome: a finalized READY + SUCCEEDED pair, or a normal
	// pending reschedule that leaves the Manifest awaiting canonical.
	Ready bool
	// ResultCopyID is required when Ready is true and must be empty otherwise.
	ResultCopyID catalog.CopyID
	// ExpectedCopyAvailability and ExpectedCopyVariantID are the locked revalidation
	// facts. The finalization transaction re-reads the exact Copy and proves them
	// again under the row lock, so a stale application-layer read cannot authorize
	// READY.
	ExpectedCopyRootID       string
	ExpectedCopyResourceID   string
	ExpectedCopyBindingID    storage.BindingID
	ExpectedCopyAvailability string
	ExpectedCopyVariantID    *catalog.VariantID
	ManifestVariantID        *catalog.VariantID
	// ClassifiedVariantID requests the monotonic Copy -> Variant binding INSIDE the
	// same database-time lease-fenced transaction that finalizes the acquisition.
	// The binding must not be a separate write: a stale worker whose lease expired
	// after the in-memory check could otherwise mutate Copy classification and only
	// be rejected later by the finalization fence, leaving a permanently changed
	// Copy behind a failed acquisition. D-019 makes that binding monotonic, so such a
	// stale write could even win a legitimate classification race.
	ClassifiedVariantID *catalog.VariantID
	Now                 time.Time
	RetryAt             *time.Time
	ManifestState       State
	JobState            jobs.State
}

// CanonicalResult is the durable state after one canonical confirmation invocation.
type CanonicalResult struct {
	Manifest Manifest
	Job      jobs.Job
	// Changed is false when the exact outcome was already committed.
	Changed bool
}

// CanonicalStageStore owns the database-time-fenced atomic finalization.
type CanonicalStageStore interface {
	CommitCanonical(context.Context, CanonicalPlan) (CanonicalResult, error)
}

// CanonicalRequest identifies one fenced currently RUNNING ACQUISITION Job and
// bounds what this single invocation may do.
type CanonicalRequest struct {
	ManifestID    ManifestID
	JobID         jobs.JobID
	Owner         string
	ExpectedClaim int
	// ProjectorPageLimit bounds the single projector invocation. Zero selects a
	// conservative default.
	ProjectorPageLimit int
	// RetryAt queues the same Job for the next canonical attempt.
	RetryAt time.Time
	Now     time.Time
}

// CanonicalConfirmation performs exactly one bounded canonical-confirmation
// attempt: one Q5 resolution, at most one bounded projector invocation, one exact
// Copy read, an optional monotonic Variant binding, and one atomic finalization.
//
// It performs no Copy creation, no OpenList call, no IndexCore database access, and
// no search. It never loops internally.
type CanonicalConfirmation struct {
	manifests ManifestReader
	jobs      JobReader
	bindings  RefreshBindingReader
	resolve   CanonicalResolvePort
	projector CanonicalProjectorPort
	copies    CanonicalCopyReader
	classify  CanonicalClassifier
	store     CanonicalStageStore
}

const (
	defaultCanonicalProjectorLimit = 100
	// MaxCanonicalProjectorLimit bounds the caller-supplied projector page size so
	// one invocation can never become unbounded.
	MaxCanonicalProjectorLimit = 1000
)

func NewCanonicalConfirmation(
	manifests ManifestReader,
	jobs JobReader,
	bindings RefreshBindingReader,
	resolve CanonicalResolvePort,
	projector CanonicalProjectorPort,
	copies CanonicalCopyReader,
	classify CanonicalClassifier,
	store CanonicalStageStore,
) (*CanonicalConfirmation, error) {
	if manifests == nil || jobs == nil || bindings == nil || resolve == nil ||
		projector == nil || copies == nil || classify == nil || store == nil {
		return nil, ErrInvalidCanonicalRequest
	}
	return &CanonicalConfirmation{
		manifests: manifests, jobs: jobs, bindings: bindings, resolve: resolve,
		projector: projector, copies: copies, classify: classify, store: store,
	}, nil
}

// Confirm performs one bounded canonical-confirmation invocation.
func (confirmation *CanonicalConfirmation) Confirm(ctx context.Context, request CanonicalRequest) (CanonicalResult, error) {
	if err := ctx.Err(); err != nil {
		return CanonicalResult{}, err
	}
	if request.ManifestID == "" || request.JobID == "" || strings.TrimSpace(request.Owner) == "" ||
		request.ExpectedClaim < 1 || request.Now.IsZero() {
		return CanonicalResult{}, ErrInvalidCanonicalRequest
	}

	manifest, job, err := confirmation.loadManifest(ctx, request)
	if err != nil {
		return CanonicalResult{}, err
	}

	// An already-finalized acquisition is resolved from the Manifest and Job alone.
	//
	// result_copy_id is the historical result link, so a later disabled StorageBinding,
	// changed root configuration, or Copy availability must never invalidate the
	// replay. Nothing below this branch - binding, root, Q5, projector, Copy read,
	// classification - is consulted, and RetryAt is not required because a finalized
	// acquisition never retries.
	if manifest.State == StateReady {
		if job.State != jobs.StateSucceeded {
			return CanonicalResult{}, fmt.Errorf("%w: Manifest %s is READY but Job %s is %s",
				ErrCanonicalConflict, manifest.ID, job.ID, job.State)
		}
		return confirmation.store.CommitCanonical(ctx, canonicalReplayPlan(request, manifest))
	}
	if manifest.State != StateAwaitingCanonical {
		return CanonicalResult{}, fmt.Errorf("%w: %s", ErrCanonicalManifestState, manifest.State)
	}
	if request.RetryAt.IsZero() || !request.RetryAt.After(request.Now) {
		return CanonicalResult{}, fmt.Errorf("%w: RetryAt must be after Now", ErrInvalidCanonicalRequest)
	}
	limit := request.ProjectorPageLimit
	if limit <= 0 {
		limit = defaultCanonicalProjectorLimit
	}
	if limit > MaxCanonicalProjectorLimit {
		return CanonicalResult{}, fmt.Errorf("%w: projector page limit %d exceeds %d",
			ErrInvalidCanonicalRequest, limit, MaxCanonicalProjectorLimit)
	}
	if err := validateCanonicalJob(manifest, job, request); err != nil {
		return CanonicalResult{}, err
	}

	binding, err := confirmation.loadBinding(ctx, manifest)
	if err != nil {
		return CanonicalResult{}, err
	}

	candidatePath, err := CanonicalCandidatePath(manifest.TargetPath, manifest.ResultName)
	if err != nil {
		return CanonicalResult{}, err
	}

	// Step 1: exactly one Q5 resolution for the deterministic candidate path.
	resolution, err := confirmation.resolve.ResolveCanonical(ctx, CanonicalResolveRequest{
		RootID: binding.IndexCoreRootID,
		Path:   candidatePath,
	})
	if err != nil {
		return CanonicalResult{}, err
	}

	resource, pending, err := classifyCanonicalResolution(resolution, binding.IndexCoreRootID, candidatePath)
	if err != nil {
		return CanonicalResult{}, err
	}
	if pending {
		// Zero matches is normal pending work, not a failure: reschedule the same Job
		// without touching the Manifest and without consuming the failure budget.
		return confirmation.store.CommitCanonical(ctx, canonicalPendingPlan(request, manifest))
	}

	// Step 2: advance the existing projector by at most one bounded page.
	projection, err := confirmation.projector.ProjectOnce(ctx, binding.ID, limit)
	if err != nil {
		return CanonicalResult{}, fmt.Errorf("%w: %v", ErrCanonicalProjector, err)
	}
	_ = projection

	// Step 3: load the exact Copy by physical identity. Acquisition never creates it.
	copyRecord, err := confirmation.copies.GetCopyByPhysicalIdentity(ctx, resource.RootID, resource.ResourceID)
	if errors.Is(err, catalog.ErrNotFound) {
		return confirmation.store.CommitCanonical(ctx, canonicalPendingPlan(request, manifest))
	}
	if err != nil {
		return CanonicalResult{}, err
	}
	if err := validateCanonicalCopy(copyRecord, resource, binding, manifest); err != nil {
		if errors.Is(err, ErrCanonicalCopyRemoved) {
			// A REMOVED Copy is still pending: later Journal/canonical evidence may
			// change it, so this is not a failure and not READY.
			return confirmation.store.CommitCanonical(ctx, canonicalPendingPlan(request, manifest))
		}
		return CanonicalResult{}, err
	}

	// Step 4: the optional monotonic Copy -> Variant binding travels INSIDE the plan.
	// It is applied by the finalization transaction under the same database-time lease
	// fence, so an unauthorized worker can never mutate Copy classification.
	return confirmation.store.CommitCanonical(ctx, canonicalReadyPlan(request, manifest, binding, resource, copyRecord))
}

// loadManifest reads the Manifest and its Job. These two are the only facts a
// finalized replay needs, so the binding is deliberately fetched separately.
func (confirmation *CanonicalConfirmation) loadManifest(ctx context.Context, request CanonicalRequest) (Manifest, jobs.Job, error) {
	manifest, err := confirmation.manifests.GetManifest(ctx, request.ManifestID)
	if errors.Is(err, ErrNotFound) {
		return Manifest{}, jobs.Job{},
			fmt.Errorf("%w: %s", ErrCanonicalManifestNotFnd, request.ManifestID)
	}
	if err != nil {
		return Manifest{}, jobs.Job{}, fmt.Errorf("load acquisition Manifest: %w", err)
	}
	if manifest.ID != request.ManifestID {
		return Manifest{}, jobs.Job{}, fmt.Errorf("%w: requested %s, read %s",
			ErrExecutionIdentityMismatch, request.ManifestID, manifest.ID)
	}
	job, err := confirmation.jobs.Get(ctx, request.JobID)
	if errors.Is(err, jobs.ErrNotFound) {
		return Manifest{}, jobs.Job{}, fmt.Errorf("%w: Job %s", ErrCanonicalFence, request.JobID)
	}
	if err != nil {
		return Manifest{}, jobs.Job{}, fmt.Errorf("load Job: %w", err)
	}
	return manifest, job, nil
}

// loadBinding reads the ACTIVE StorageBinding and its IndexCore root. It is only
// consulted on the path that still has to resolve canonical truth.
func (confirmation *CanonicalConfirmation) loadBinding(ctx context.Context, manifest Manifest) (storage.Binding, error) {
	binding, err := confirmation.bindings.GetBinding(ctx, manifest.TargetStorageBindingID)
	if errors.Is(err, storage.ErrNotFound) {
		return storage.Binding{},
			fmt.Errorf("%w: %s", ErrCanonicalBindingMissing, manifest.TargetStorageBindingID)
	}
	if err != nil {
		return storage.Binding{}, fmt.Errorf("load StorageBinding: %w", err)
	}
	if binding.ID != manifest.TargetStorageBindingID {
		return storage.Binding{}, fmt.Errorf("%w: requested binding %s, read %s",
			ErrStorageTopologyMismatch, manifest.TargetStorageBindingID, binding.ID)
	}
	if binding.Status != storage.BindingStatusActive {
		return storage.Binding{}, fmt.Errorf("%w: %s", ErrCanonicalBindingState, binding.ID)
	}
	if strings.TrimSpace(binding.IndexCoreRootID) == "" || binding.IndexCoreRootID != strings.TrimSpace(binding.IndexCoreRootID) {
		return storage.Binding{}, fmt.Errorf("%w: binding %s", ErrCanonicalRootID, binding.ID)
	}
	return binding, nil
}

// validateCanonicalJob proves the Job is the linked ACQUISITION Job holding the
// fenced RUNNING lease for the expected claim generation.
//
// The Manifest -> Job direction is checked explicitly, mirroring
// isCommittedRefresh. The Job-side link alone is not sufficient: a Manifest whose
// JobID points at a different Job is corrupt, and finalizing it would finalize an
// acquisition against a Job it is not durably attached to.
func validateCanonicalJob(manifest Manifest, job jobs.Job, request CanonicalRequest) error {
	if job.ID != request.JobID {
		return fmt.Errorf("%w: requested %s, read %s", ErrCanonicalFence, request.JobID, job.ID)
	}
	if manifest.JobID == nil || *manifest.JobID != request.JobID {
		return fmt.Errorf("%w: Manifest %s is not linked to Job %s",
			ErrCanonicalFence, manifest.ID, request.JobID)
	}
	if err := ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return fmt.Errorf("%w: %w", ErrCanonicalFence, err)
	}
	if job.State != jobs.StateRunning {
		return fmt.Errorf("%w: Job %s is %s", ErrCanonicalFence, job.ID, job.State)
	}
	if job.LeaseOwner == nil || *job.LeaseOwner != request.Owner {
		return fmt.Errorf("%w: Job %s is not leased by %s", ErrCanonicalFence, job.ID, request.Owner)
	}
	if job.ClaimAttempts != request.ExpectedClaim {
		return fmt.Errorf("%w: Job %s claim generation %d does not match %d",
			ErrCanonicalFence, job.ID, job.ClaimAttempts, request.ExpectedClaim)
	}
	return nil
}

// CanonicalCandidatePath builds the one deterministic root-absolute candidate from
// the frozen target directory and the validated direct-child locator.
func CanonicalCandidatePath(targetPath string, resultName *string) (string, error) {
	normalizedTarget, err := NormalizeTargetPath(targetPath)
	if err != nil || normalizedTarget != targetPath {
		return "", fmt.Errorf("%w: target path %q", ErrCanonicalResultName, targetPath)
	}
	if resultName == nil {
		return "", fmt.Errorf("%w: result_name is required", ErrCanonicalResultName)
	}
	if err := ValidateResultName(*resultName); err != nil {
		return "", fmt.Errorf("%w: result_name %q", ErrCanonicalResultName, *resultName)
	}
	candidate := path.Join(normalizedTarget, *resultName)
	if candidate == "" || path.Clean(candidate) != candidate || !strings.HasPrefix(candidate, "/") {
		return "", fmt.Errorf("%w: candidate %q is not canonical", ErrCanonicalResultName, candidate)
	}
	// The candidate must remain a direct child of the target directory, so no
	// separator inside result_name can move it.
	if path.Dir(candidate) != normalizedTarget {
		return "", fmt.Errorf("%w: candidate %q is not a direct child of %q",
			ErrCanonicalResultName, candidate, normalizedTarget)
	}
	return candidate, nil
}

// classifyCanonicalResolution applies the frozen D-033 classification.
func classifyCanonicalResolution(
	resolution CanonicalResolution,
	requestedRoot string,
	candidatePath string,
) (CanonicalResource, bool, error) {
	// Ambiguity is never guessed, even when it carries a single match.
	if resolution.Ambiguous {
		return CanonicalResource{}, false, fmt.Errorf("%w: IndexCore reported ambiguity", ErrCanonicalAmbiguous)
	}
	if len(resolution.Matches) == 0 {
		// Normal pending work.
		return CanonicalResource{}, true, nil
	}
	if len(resolution.Matches) > 1 {
		return CanonicalResource{}, false, fmt.Errorf("%w: %d matches", ErrCanonicalAmbiguous, len(resolution.Matches))
	}
	match := resolution.Matches[0]
	if match.RootID != requestedRoot {
		return CanonicalResource{}, false, fmt.Errorf("%w: root %q, want %q",
			ErrCanonicalIdentity, match.RootID, requestedRoot)
	}
	if match.CanonicalPath == "" || match.CanonicalPath != candidatePath {
		return CanonicalResource{}, false, fmt.Errorf("%w: path %q, want %q",
			ErrCanonicalIdentity, match.CanonicalPath, candidatePath)
	}
	if match.Presence != CanonicalPresencePresent {
		return CanonicalResource{}, false, fmt.Errorf("%w: presence %q, want PRESENT",
			ErrCanonicalIdentity, match.Presence)
	}
	if strings.TrimSpace(match.ResourceID) == "" {
		return CanonicalResource{}, false, fmt.Errorf("%w: resource ID is blank", ErrCanonicalIdentity)
	}
	return match, false, nil
}

// validateCanonicalCopy proves the projected Copy is the accepted physical result.
func validateCanonicalCopy(copyRecord catalog.Copy, resource CanonicalResource, binding storage.Binding, manifest Manifest) error {
	if copyRecord.IndexCoreRootID != resource.RootID || copyRecord.IndexCoreResourceID != resource.ResourceID {
		return fmt.Errorf("%w: Copy %s is %s/%s, want %s/%s", ErrCanonicalIdentity,
			copyRecord.ID, copyRecord.IndexCoreRootID, copyRecord.IndexCoreResourceID,
			resource.RootID, resource.ResourceID)
	}
	if string(copyRecord.StorageBindingID) != string(binding.ID) {
		return fmt.Errorf("%w: Copy %s belongs to %s, want %s",
			ErrCanonicalCopyBinding, copyRecord.ID, copyRecord.StorageBindingID, binding.ID)
	}
	if copyRecord.Availability == catalog.CopyAvailabilityRemoved {
		return fmt.Errorf("%w: Copy %s", ErrCanonicalCopyRemoved, copyRecord.ID)
	}
	if copyRecord.Availability != catalog.CopyAvailabilityPresent {
		return fmt.Errorf("%w: Copy %s availability is %q, want PRESENT",
			ErrCanonicalIdentity, copyRecord.ID, copyRecord.Availability)
	}
	// A Copy already classified to a different Variant than the Manifest intends must
	// never be silently reclassified.
	if manifest.VariantID != nil && copyRecord.VariantID != nil && *copyRecord.VariantID != *manifest.VariantID {
		return fmt.Errorf("%w: Copy %s is bound to %s, want %s",
			ErrCanonicalCopyClassified, copyRecord.ID, *copyRecord.VariantID, *manifest.VariantID)
	}
	return nil
}

func canonicalPendingPlan(request CanonicalRequest, manifest Manifest) CanonicalPlan {
	retryAt := request.RetryAt.UTC()
	return CanonicalPlan{
		ManifestID:        request.ManifestID,
		JobID:             request.JobID,
		Owner:             request.Owner,
		ExpectedClaim:     request.ExpectedClaim,
		Ready:             false,
		ManifestState:     StateAwaitingCanonical,
		JobState:          jobs.StateRetryWait,
		ManifestVariantID: manifest.VariantID,
		Now:               request.Now.UTC(),
		RetryAt:           &retryAt,
	}
}

func canonicalReadyPlan(
	request CanonicalRequest,
	manifest Manifest,
	binding storage.Binding,
	resource CanonicalResource,
	copyRecord catalog.Copy,
) CanonicalPlan {
	return CanonicalPlan{
		ManifestID:               request.ManifestID,
		JobID:                    request.JobID,
		Owner:                    request.Owner,
		ExpectedClaim:            request.ExpectedClaim,
		Ready:                    true,
		ResultCopyID:             copyRecord.ID,
		ExpectedCopyRootID:       resource.RootID,
		ExpectedCopyResourceID:   resource.ResourceID,
		ExpectedCopyBindingID:    binding.ID,
		ExpectedCopyAvailability: catalog.CopyAvailabilityPresent,
		ExpectedCopyVariantID:    copyRecord.VariantID,
		ManifestVariantID:        manifest.VariantID,
		ClassifiedVariantID:      manifest.VariantID,
		ManifestState:            StateReady,
		JobState:                 jobs.StateSucceeded,
		Now:                      request.Now.UTC(),
	}
}

// canonicalReplayPlan is the replay of an already-finalized acquisition. It carries
// only the durable facts a result link needs - the Manifest, the Job, and the committed
// result Copy - so replay stays independent of the current StorageBinding status, root
// configuration, Q5, projector, and Copy availability.
//
// Replay also needs no RetryAt, because a finalized acquisition never retries.
func canonicalReplayPlan(request CanonicalRequest, manifest Manifest) CanonicalPlan {
	return CanonicalPlan{
		ManifestID:        request.ManifestID,
		JobID:             request.JobID,
		Owner:             request.Owner,
		ExpectedClaim:     request.ExpectedClaim,
		Ready:             true,
		ResultCopyID:      copyIDOrEmpty(manifest.ResultCopyID),
		ManifestVariantID: manifest.VariantID,
		ManifestState:     StateReady,
		JobState:          jobs.StateSucceeded,
		Now:               request.Now.UTC(),
	}
}

func copyIDOrEmpty(value *catalog.CopyID) catalog.CopyID {
	if value == nil {
		return ""
	}
	return *value
}
