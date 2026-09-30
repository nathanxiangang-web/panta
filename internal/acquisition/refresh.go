package acquisition

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var (
	ErrInvalidRefreshRequest   = errors.New("invalid acquisition refresh request")
	ErrRefreshManifestState    = errors.New("acquisition Manifest is not awaiting visibility")
	ErrRefreshManifestNotFound = errors.New("acquisition refresh Manifest not found")
	ErrRefreshJobMismatch      = errors.New("acquisition refresh Job does not match the Manifest")
	ErrRefreshFence            = errors.New("acquisition refresh lease fence did not match")
	ErrRefreshBindingMissing   = errors.New("acquisition refresh StorageBinding not found")
	ErrRefreshBindingState     = errors.New("acquisition refresh StorageBinding is not active")
	ErrRefreshRootID           = errors.New("acquisition refresh IndexCore root ID is invalid")
	ErrRefreshScopeKey         = errors.New("acquisition refresh scope key is invalid")
	ErrRefreshHintRejected     = errors.New("acquisition refresh trusted Hint was not accepted")
	ErrRefreshConflict         = errors.New("acquisition refresh conflicts with the committed handoff")
	ErrRefreshPersistence      = errors.New("acquisition refresh persistence failure")
)

// MutationHintReason is the Panta-owned reason vocabulary for a trusted observation
// Hint. Gate 3.8 uses only MutationHintPossibleChange.
type MutationHintReason string

const (
	MutationHintPossibleChange MutationHintReason = "POSSIBLE_CHANGE"
)

func (reason MutationHintReason) valid() bool { return reason == MutationHintPossibleChange }

// MutationHint is one trusted Panta-owned observation request.
//
// RootID is IndexCore's canonical root identity and ScopeKey is the root-absolute
// directory scope to re-verify. Neither is derived from provider_scope or from an
// OpenList mount, and neither is an OpenList path.
type MutationHint struct {
	RootID   string
	ScopeKey string
	Reason   MutationHintReason
}

// MutationHintReceipt confirms IndexCore durably ingested observation work.
//
// It is only ingest proof. It is never canonical truth and never authorizes READY.
type MutationHintReceipt struct {
	Status    string
	RootID    string
	ScopeKey  string
	WorkState string
	// SignalSeq is IndexCore observation-work metadata. It is not a canonical
	// generation and not a resource identity.
	SignalSeq int64
}

// MutationHintPort submits one trusted observation Hint. It grants no IndexCore
// read, query, journal, or database capability.
type MutationHintPort interface {
	SubmitMutationHint(context.Context, MutationHint) (MutationHintReceipt, error)
}

// RefreshBindingReader is the narrow storage read port this step needs. It
// deliberately does not read a provider scope or a connection: handing an
// observation scope to IndexCore never selects a provider session.
type RefreshBindingReader interface {
	GetBinding(context.Context, storage.BindingID) (storage.Binding, error)
}

// RefreshRequest identifies one fenced currently RUNNING ACQUISITION Job.
type RefreshRequest struct {
	ManifestID    ManifestID
	JobID         jobs.JobID
	Owner         string
	ExpectedClaim int
	Now           time.Time
	// RetryAt queues the same Job for the later canonical-confirmation stage.
	RetryAt time.Time
}

// RefreshPlan is the complete persistence input for the refresh handoff. The store
// must move the Manifest and the Job together or not at all.
type RefreshPlan struct {
	ManifestID    ManifestID
	JobID         jobs.JobID
	Owner         string
	ExpectedClaim int
	// ManifestState is the milestone the Manifest must hold after the handoff.
	ManifestState State
	// JobState is the state the linked Job must hold after the handoff.
	JobState jobs.State
	Now      time.Time
	RetryAt  time.Time
}

// RefreshResult is the durable state after a handoff.
type RefreshResult struct {
	Manifest Manifest
	Job      jobs.Job
	// Changed is false when the exact handoff was already committed and this call
	// was a replay. A replay never sends a second Hint and never rewrites timestamps.
	Changed bool
}

// RefreshStore commits the plan atomically.
type RefreshStore interface {
	CommitRefresh(context.Context, RefreshPlan) (RefreshResult, error)
}

// RefreshStep hands one exact root/scope to the IndexCore-owned observation
// pipeline and then atomically advances Panta's own state.
//
// It performs no OpenList call, no IndexCore read, and no provider call. It sends at
// most one Hint per invocation.
type RefreshStep struct {
	manifests ManifestReader
	jobs      JobReader
	bindings  RefreshBindingReader
	hints     MutationHintPort
	store     RefreshStore
}

func NewRefreshStep(
	manifests ManifestReader,
	jobs JobReader,
	bindings RefreshBindingReader,
	hints MutationHintPort,
	store RefreshStore,
) (*RefreshStep, error) {
	if manifests == nil || jobs == nil || bindings == nil || hints == nil || store == nil {
		return nil, ErrInvalidRefreshRequest
	}
	return &RefreshStep{manifests: manifests, jobs: jobs, bindings: bindings, hints: hints, store: store}, nil
}

// Submit validates one refresh step, submits exactly one trusted Hint, and commits
// the fenced handoff.
func (step *RefreshStep) Submit(ctx context.Context, request RefreshRequest) (RefreshResult, error) {
	if err := ctx.Err(); err != nil {
		return RefreshResult{}, err
	}
	_, plan, err := BuildRefreshPlan(request)
	if err != nil {
		return RefreshResult{}, err
	}

	manifest, job, binding, err := step.load(ctx, request)
	if err != nil {
		return RefreshResult{}, err
	}

	// An already-committed handoff is an exact replay: return the durable pair
	// without submitting another Hint. This is what makes the at-least-once window
	// safe once Panta has durably advanced.
	committed, err := isCommittedRefresh(manifest, job, plan)
	if err != nil {
		return RefreshResult{}, err
	}
	if committed {
		return RefreshResult{Manifest: manifest, Job: job, Changed: false}, nil
	}

	if manifest.State != StateAwaitingVisibility {
		return RefreshResult{}, fmt.Errorf("%w: %s", ErrRefreshManifestState, manifest.State)
	}
	// D-031: an acquisition that has no durable direct-child identity must not be
	// handed to the observation pipeline at all. Advancing it would create an
	// AWAITING_CANONICAL Manifest that a later canonical-confirmation gate could
	// never resolve without guessing.
	if manifest.ExpectedName == nil {
		return RefreshResult{}, fmt.Errorf("%w: Manifest %s", ErrExpectedNameRequired, manifest.ID)
	}
	if err := ValidateExpectedName(*manifest.ExpectedName); err != nil {
		return RefreshResult{}, fmt.Errorf("%w: Manifest %s expected name is not a valid direct child",
			ErrExpectedNameRequired, manifest.ID)
	}
	if err := validateRefreshJob(manifest, job, plan); err != nil {
		return RefreshResult{}, err
	}
	hint, err := buildMutationHint(binding, manifest)
	if err != nil {
		return RefreshResult{}, err
	}

	// Exactly one Hint per invocation. IndexCore Mutation Hints are at-least-once and
	// coalescing, so a replay from a lost response or a failed Panta commit below is
	// safe: it may advance IndexCore signal metadata but cannot duplicate a provider
	// download or create canonical truth.
	receipt, err := step.hints.SubmitMutationHint(ctx, hint)
	if err != nil {
		return RefreshResult{}, err
	}
	if err := verifyMutationHintReceipt(hint, receipt); err != nil {
		return RefreshResult{}, err
	}

	// The durable handoff happens after acceptance. If it fails, Panta stays
	// AWAITING_VISIBILITY and a retry may resend the same Hint.
	return step.store.CommitRefresh(ctx, plan)
}

// load reads the Manifest, the fenced Job, and the target StorageBinding.
func (step *RefreshStep) load(ctx context.Context, request RefreshRequest) (Manifest, jobs.Job, storage.Binding, error) {
	manifest, err := step.manifests.GetManifest(ctx, request.ManifestID)
	if errors.Is(err, ErrNotFound) {
		return Manifest{}, jobs.Job{}, storage.Binding{},
			fmt.Errorf("%w: %s", ErrRefreshManifestNotFound, request.ManifestID)
	}
	if err != nil {
		return Manifest{}, jobs.Job{}, storage.Binding{}, fmt.Errorf("load acquisition Manifest: %w", err)
	}
	if manifest.ID != request.ManifestID {
		return Manifest{}, jobs.Job{}, storage.Binding{}, fmt.Errorf("%w: requested %s, read %s",
			ErrExecutionIdentityMismatch, request.ManifestID, manifest.ID)
	}

	job, err := step.jobs.Get(ctx, request.JobID)
	if errors.Is(err, jobs.ErrNotFound) {
		return Manifest{}, jobs.Job{}, storage.Binding{}, fmt.Errorf("%w: Job %s",
			ErrRefreshJobMismatch, request.JobID)
	}
	if err != nil {
		return Manifest{}, jobs.Job{}, storage.Binding{}, fmt.Errorf("load Job: %w", err)
	}

	binding, err := step.bindings.GetBinding(ctx, manifest.TargetStorageBindingID)
	if errors.Is(err, storage.ErrNotFound) {
		return Manifest{}, jobs.Job{}, storage.Binding{},
			fmt.Errorf("%w: %s", ErrRefreshBindingMissing, manifest.TargetStorageBindingID)
	}
	if err != nil {
		return Manifest{}, jobs.Job{}, storage.Binding{}, fmt.Errorf("load StorageBinding: %w", err)
	}
	if binding.ID != manifest.TargetStorageBindingID {
		return Manifest{}, jobs.Job{}, storage.Binding{}, fmt.Errorf("%w: requested binding %s, read %s",
			ErrStorageTopologyMismatch, manifest.TargetStorageBindingID, binding.ID)
	}
	return manifest, job, binding, nil
}

// buildMutationHint freezes the D-029 mapping:
//
//	root_id   = StorageBinding.indexcore_root_id
//	scope_key = Manifest.target_path
//	reason    = POSSIBLE_CHANGE
//
// provider_scope and openlist_mount_path deliberately do not participate.
func buildMutationHint(binding storage.Binding, manifest Manifest) (MutationHint, error) {
	if binding.Status != storage.BindingStatusActive {
		return MutationHint{}, fmt.Errorf("%w: %s", ErrRefreshBindingState, binding.ID)
	}
	rootID := strings.TrimSpace(binding.IndexCoreRootID)
	if rootID == "" || rootID != binding.IndexCoreRootID {
		return MutationHint{}, fmt.Errorf("%w: binding %s", ErrRefreshRootID, binding.ID)
	}
	normalizedTarget, err := NormalizeTargetPath(manifest.TargetPath)
	if err != nil || normalizedTarget != manifest.TargetPath {
		return MutationHint{}, fmt.Errorf("%w: Manifest %s target %q",
			ErrRefreshScopeKey, manifest.ID, manifest.TargetPath)
	}
	return MutationHint{
		RootID:   rootID,
		ScopeKey: manifest.TargetPath,
		Reason:   MutationHintPossibleChange,
	}, nil
}

// verifyMutationHintReceipt requires an accepted receipt that describes exactly the
// hint that was submitted.
func verifyMutationHintReceipt(hint MutationHint, receipt MutationHintReceipt) error {
	if !strings.EqualFold(strings.TrimSpace(receipt.Status), "accepted") {
		return fmt.Errorf("%w: status %q", ErrRefreshHintRejected, receipt.Status)
	}
	if receipt.RootID != hint.RootID || receipt.ScopeKey != hint.ScopeKey {
		return fmt.Errorf("%w: receipt root=%q scope=%q does not match the submitted hint",
			ErrRefreshHintRejected, receipt.RootID, receipt.ScopeKey)
	}
	return nil
}

// validateRefreshJob proves the Job is the linked ACQUISITION Job holding the
// fenced RUNNING lease for the expected claim generation. Lease validity is
// authorized by the durable store with database time, not by a local clock.
func validateRefreshJob(manifest Manifest, job jobs.Job, plan RefreshPlan) error {
	if job.ID != plan.JobID {
		return fmt.Errorf("%w: requested %s, read %s", ErrRefreshJobMismatch, plan.JobID, job.ID)
	}
	if err := ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return fmt.Errorf("%w: %w", ErrRefreshJobMismatch, err)
	}
	if job.State != jobs.StateRunning {
		return fmt.Errorf("%w: Job %s is %s", ErrRefreshFence, job.ID, job.State)
	}
	if job.LeaseOwner == nil || *job.LeaseOwner != plan.Owner {
		return fmt.Errorf("%w: Job %s is not leased by %s", ErrRefreshFence, job.ID, plan.Owner)
	}
	if job.ClaimAttempts != plan.ExpectedClaim {
		return fmt.Errorf("%w: Job %s claim generation %d does not match %d",
			ErrRefreshFence, job.ID, job.ClaimAttempts, plan.ExpectedClaim)
	}
	return nil
}

// isCommittedRefresh reports whether the durable pair already holds exactly the
// handoff this plan would commit.
func isCommittedRefresh(manifest Manifest, job jobs.Job, plan RefreshPlan) (bool, error) {
	// The linkage is validated before the state shortcut, so a broken or tampered
	// link fails closed even when the states happen to look committed.
	if manifest.ID != plan.ManifestID {
		return false, fmt.Errorf("%w: requested %s, read %s",
			ErrRefreshJobMismatch, plan.ManifestID, manifest.ID)
	}
	if job.ID != plan.JobID {
		return false, fmt.Errorf("%w: requested %s, read %s", ErrRefreshJobMismatch, plan.JobID, job.ID)
	}
	if manifest.JobID == nil || *manifest.JobID != plan.JobID {
		return false, fmt.Errorf("%w: Manifest %s is not linked to Job %s",
			ErrRefreshJobMismatch, manifest.ID, plan.JobID)
	}
	if err := ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return false, fmt.Errorf("%w: %w", ErrRefreshJobMismatch, err)
	}
	if manifest.State != plan.ManifestState {
		return false, nil
	}
	return job.State == plan.JobState, nil
}

// BuildRefreshPlan validates one request and produces its persistence plan. It is
// exported so tests can prove the store rejects a tampered plan rather than
// trusting its caller.
func BuildRefreshPlan(request RefreshRequest) (MutationHintReason, RefreshPlan, error) {
	if request.ManifestID == "" || request.JobID == "" || strings.TrimSpace(request.Owner) == "" ||
		request.ExpectedClaim < 1 || request.Now.IsZero() || request.RetryAt.IsZero() {
		return "", RefreshPlan{}, ErrInvalidRefreshRequest
	}
	if !request.RetryAt.After(request.Now) {
		return "", RefreshPlan{}, fmt.Errorf("%w: RetryAt must be after Now", ErrInvalidRefreshRequest)
	}
	return MutationHintPossibleChange, RefreshPlan{
		ManifestID:    request.ManifestID,
		JobID:         request.JobID,
		Owner:         request.Owner,
		ExpectedClaim: request.ExpectedClaim,
		ManifestState: StateAwaitingCanonical,
		JobState:      jobs.StateRetryWait,
		Now:           request.Now.UTC(),
		RetryAt:       request.RetryAt.UTC(),
	}, nil
}
