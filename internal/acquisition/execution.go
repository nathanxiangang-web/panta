package acquisition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var (
	ErrInvalidExecutionRequest      = errors.New("invalid acquisition execution request")
	ErrProviderNotDownloader        = errors.New("acquisition provider has no downloader capability")
	ErrProviderIdentityMismatch     = errors.New("acquisition provider identity mismatch")
	ErrProviderTaskReference        = errors.New("acquisition provider returned an invalid task reference")
	ErrProviderTaskState            = errors.New("acquisition provider returned an unknown task state")
	ErrExecutionJobMismatch         = errors.New("acquisition execution Job does not match the Manifest")
	ErrDownloaderSessionUnresolved  = errors.New("acquisition downloader session could not be resolved")
	ErrDownloaderSessionIdentitySet = errors.New("acquisition downloader session identity is incomplete")
	// ErrExecutionSideEffectUncertain reports that an external side effect may
	// have happened but its opaque task reference was not durably recorded.
	// Callers must treat this as recovery-required state: a later execution may
	// not start another provider task on the strength of this outcome.
	ErrExecutionSideEffectUncertain = errors.New("acquisition external side effect is uncertain")
)

// StepRequest proves that this execution runs on behalf of one fenced RUNNING
// ACQUISITION Job. Owner and ClaimAttempt come from the Job Engine lease context and
// are compared against durable Job state, so a foreign, stale, or unleased actor
// fails closed before any provider task is started or polled.
type StepRequest struct {
	JobID        jobs.JobID
	Owner        string
	ClaimAttempt int
}

// StepOutcome is the provider-neutral result of one bounded execution step. It
// never reports Manifest READY: provider success is not canonical confirmation.
type StepOutcome string

const (
	OutcomeProviderInProgress StepOutcome = "PROVIDER_IN_PROGRESS"
	OutcomeProviderSucceeded  StepOutcome = "PROVIDER_SUCCEEDED"
	OutcomeProviderFailed     StepOutcome = "PROVIDER_FAILED"
	OutcomeProviderCanceled   StepOutcome = "PROVIDER_CANCELED"
)

// JobReader is the narrow Job Engine read port used to prove the fenced lease and
// the frozen Gate 3.2 Manifest linkage. It carries no persistence or lease
// mutation responsibility: the Job Engine still owns execution state.
type JobReader interface {
	Get(context.Context, jobs.JobID) (jobs.Job, error)
}

// DownloaderSessionRequest is the complete accepted Gate 3.3 execution identity
// needed to select authenticated provider state.
//
// ProviderID alone must never select provider state: a real adapter executes under
// the credential session belonging to one configured StorageConnection. The
// CredentialRef is opaque and never carries secret material.
type DownloaderSessionRequest struct {
	ProviderID    contracts.ProviderID
	ConnectionID  storage.ConnectionID
	CredentialRef *string
}

// DownloaderSessionResolver resolves the downloader port for an exact
// provider/connection/credential identity.
//
// Both this port and its implementations depend only on the provider-neutral
// contracts package for the result type, and the composition package
// internal/providers/session satisfies it directly with no adapter.
type DownloaderSessionResolver interface {
	ResolveDownloader(context.Context, DownloaderSessionRequest) (contracts.DownloaderBinding, error)
}

// ExecutionOption configures the execution step service.
type ExecutionOption func(*ExecutionStepService) error

// WithExecutionClock injects a deterministic clock for tests.
func WithExecutionClock(clock Clock) ExecutionOption {
	return func(service *ExecutionStepService) error {
		if clock == nil {
			return ErrInvalidExecutionRequest
		}
		service.now = clock
		return nil
	}
}

// ExecutionStepService performs one bounded, restart-safe downloader step. It
// crosses the external side-effect boundary at most once per execution and never
// blindly recreates an existing provider task.
type ExecutionStepService struct {
	inputs    *ExecutionInputResolver
	manifests ManifestReader
	jobs      JobReader
	sessions  DownloaderSessionResolver
	tasks     ProviderTaskStore
	now       Clock
}

func NewExecutionStepService(
	inputs *ExecutionInputResolver,
	manifests ManifestReader,
	jobs JobReader,
	sessions DownloaderSessionResolver,
	tasks ProviderTaskStore,
	options ...ExecutionOption,
) (*ExecutionStepService, error) {
	if inputs == nil || manifests == nil || jobs == nil || sessions == nil || tasks == nil {
		return nil, ErrInvalidExecutionRequest
	}
	service := &ExecutionStepService{
		inputs: inputs, manifests: manifests, jobs: jobs, sessions: sessions, tasks: tasks, now: time.Now,
	}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(service); err != nil {
			return nil, err
		}
	}
	return service, nil
}

// Execute validates the fenced Job, resolves accepted Gate 3.3 execution input,
// resolves a downloader session for the exact connection and credential
// identity, and performs exactly one bounded provider step: either StartDownload
// once followed by durable linkage, or DownloadStatus for an already-linked task.
// It performs no OpenList, IndexCore, canonical confirmation, or Copy mutation.
func (service *ExecutionStepService) Execute(ctx context.Context, request StepRequest) (StepOutcome, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if request.JobID == "" || request.Owner == "" || request.ClaimAttempt < 1 {
		return "", ErrInvalidExecutionRequest
	}

	manifest, job, err := service.loadLinkedJob(ctx, request)
	if err != nil {
		return "", err
	}
	input, err := service.inputs.Resolve(ctx, manifest.ID)
	if err != nil {
		return "", err
	}
	if input.ManifestID != manifest.ID {
		return "", fmt.Errorf("%w: requested Manifest %s, got %s", ErrExecutionIdentityMismatch, manifest.ID, input.ManifestID)
	}

	// Provider identity, connection identity, and the opaque credential reference
	// together select the downloader. ProviderID alone never does.
	downloader, err := service.resolveDownloaderSession(ctx, input)
	if err != nil {
		return "", err
	}

	task, err := service.tasks.GetProviderTask(ctx, manifest.ID)
	if errors.Is(err, ErrProviderTaskNotFound) {
		return service.claimAndStartProviderTask(ctx, request, input, downloader)
	}
	if err != nil {
		return "", fmt.Errorf("%w: load provider task: %v", ErrProviderTaskPersistence, err)
	}
	if task.ProviderID != input.ProviderID || task.JobID != job.ID {
		return "", fmt.Errorf("%w: durable provider task for Manifest %s", ErrExecutionIdentityMismatch, manifest.ID)
	}
	if !task.ReferenceKnown() {
		// A previous execution reserved the start but never durably recorded a
		// reference. The external side effect state is unknown, so a later
		// execution is forbidden from starting another task.
		return "", fmt.Errorf("%w: Manifest %s holds a durable start reservation without a task reference",
			ErrExecutionSideEffectUncertain, manifest.ID)
	}
	return service.pollProviderTask(ctx, task, downloader)
}

// resolveDownloaderSession fails closed unless the resolved session matches the
// complete execution identity and still advertises a downloader port whose
// descriptor identity equals the resolved ProviderID.
func (service *ExecutionStepService) resolveDownloaderSession(
	ctx context.Context,
	input ExecutionInput,
) (contracts.DownloaderProvider, error) {
	if !input.ProviderID.Valid() {
		return nil, fmt.Errorf("%w: provider identity %q", ErrDownloaderSessionIdentitySet, input.ProviderID)
	}
	if input.ConnectionID == "" {
		return nil, fmt.Errorf("%w: Manifest %s has no StorageConnection", ErrDownloaderSessionIdentitySet, input.ManifestID)
	}
	binding, err := service.sessions.ResolveDownloader(ctx, DownloaderSessionRequest{
		ProviderID:    input.ProviderID,
		ConnectionID:  input.ConnectionID,
		CredentialRef: cloneString(input.CredentialRef),
	})
	if err != nil {
		return nil, fmt.Errorf("%w: provider %s connection %s: %w",
			ErrDownloaderSessionUnresolved, input.ProviderID, input.ConnectionID, err)
	}
	if binding.Descriptor.ID != input.ProviderID {
		return nil, fmt.Errorf("%w: requested %s, session returned %s",
			ErrProviderIdentityMismatch, input.ProviderID, binding.Descriptor.ID)
	}
	if binding.Downloader == nil || !binding.Descriptor.Capabilities.Downloader {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotDownloader, input.ProviderID)
	}
	return binding.Downloader, nil
}

// loadLinkedJob reads the ACQUISITION Job first, derives its Manifest from the
// frozen payload linkage, and proves the linkage holds in both directions before
// accepting the fenced RUNNING lease identity.
func (service *ExecutionStepService) loadLinkedJob(ctx context.Context, request StepRequest) (Manifest, jobs.Job, error) {
	job, err := service.jobs.Get(ctx, request.JobID)
	if errors.Is(err, jobs.ErrNotFound) {
		return Manifest{}, jobs.Job{}, fmt.Errorf("%w: %s", ErrExecutionJobMismatch, request.JobID)
	}
	if err != nil {
		return Manifest{}, jobs.Job{}, fmt.Errorf("load ACQUISITION Job: %w", err)
	}
	manifestID, err := linkedManifestID(job)
	if err != nil {
		return Manifest{}, jobs.Job{}, err
	}
	manifest, err := service.manifests.GetManifest(ctx, manifestID)
	if errors.Is(err, ErrNotFound) {
		return Manifest{}, jobs.Job{}, fmt.Errorf("%w: %s", ErrExecutionManifestNotFound, manifestID)
	}
	if err != nil {
		return Manifest{}, jobs.Job{}, fmt.Errorf("load acquisition Manifest: %w", err)
	}
	if manifest.JobID == nil || *manifest.JobID != job.ID {
		return Manifest{}, jobs.Job{}, fmt.Errorf("%w: Manifest %s is not linked to Job %s", ErrExecutionJobMismatch, manifest.ID, job.ID)
	}
	if err := ValidateLinkedAcquisitionJob(manifest.ID, job); err != nil {
		return Manifest{}, jobs.Job{}, err
	}
	if err := service.validateFencedLease(job, request); err != nil {
		return Manifest{}, jobs.Job{}, err
	}
	return manifest, job, nil
}

// linkedManifestID extracts the Manifest identity from a valid ACQUISITION job
// payload without interpreting provider-specific data.
func linkedManifestID(job jobs.Job) (ManifestID, error) {
	if job.Type != JobTypeAcquisition || job.IdempotencyKey == nil {
		return "", fmt.Errorf("%w: Job %s is not an ACQUISITION Job", ErrExecutionJobMismatch, job.ID)
	}
	payload, err := decodeAcquisitionJobPayload(job.Payload)
	if err != nil {
		return "", err
	}
	if err := ValidateLinkedAcquisitionJob(payload.ManifestID, job); err != nil {
		return "", err
	}
	return payload.ManifestID, nil
}

// validateFencedLease proves that the durable Job is RUNNING, unexpired, owned by
// the requesting lease holder, and still on the requesting claim generation.
func (service *ExecutionStepService) validateFencedLease(job jobs.Job, request StepRequest) error {
	mismatch := func(reason string) error {
		return fmt.Errorf("%w: %s", ErrExecutionJobMismatch, reason)
	}
	if job.State != jobs.StateRunning {
		return mismatch("Job " + string(job.ID) + " is not RUNNING")
	}
	if job.LeaseOwner == nil || *job.LeaseOwner != request.Owner {
		return mismatch("Job " + string(job.ID) + " is not leased by " + request.Owner)
	}
	if job.LeaseExpiresAt == nil || !job.LeaseExpiresAt.After(service.now().UTC()) {
		return mismatch("Job " + string(job.ID) + " lease has expired")
	}
	if job.ClaimAttempts != request.ClaimAttempt {
		return mismatch(fmt.Sprintf("Job %s claim generation %d does not match %d", job.ID, job.ClaimAttempts, request.ClaimAttempt))
	}
	return nil
}

// claimAndStartProviderTask durably fences the external side effect before
// attempting it. Only the single execution that wins the START_RESERVED claim may
// call StartDownload; every other caller either polls a known reference or fails
// closed on the reservation.
func (service *ExecutionStepService) claimAndStartProviderTask(
	ctx context.Context,
	request StepRequest,
	input ExecutionInput,
	downloader contracts.DownloaderProvider,
) (StepOutcome, error) {
	claim, claimErr := service.tasks.ClaimProviderTask(ctx, ProviderTaskClaimRequest{
		ManifestID: input.ManifestID, JobID: request.JobID, ProviderID: input.ProviderID,
		Now: service.now().UTC(),
	})
	if claimErr != nil {
		return "", fmt.Errorf("%w: claim provider start for Manifest %s: %w",
			ErrProviderTaskPersistence, input.ManifestID, claimErr)
	}
	if claim.Task.ProviderID != input.ProviderID || claim.Task.JobID != request.JobID {
		return "", fmt.Errorf("%w: claimed provider task for Manifest %s", ErrExecutionIdentityMismatch, input.ManifestID)
	}
	if !claim.ClaimedStart {
		if !claim.Task.ReferenceKnown() {
			return "", fmt.Errorf("%w: Manifest %s holds a durable start reservation without a task reference",
				ErrExecutionSideEffectUncertain, input.ManifestID)
		}
		return service.pollProviderTask(ctx, claim.Task, downloader)
	}

	reference, startErr := downloader.StartDownload(ctx, input.Download)
	if startErr != nil {
		return "", fmt.Errorf("provider %s StartDownload: %w", input.ProviderID, startErr)
	}
	if !ValidProviderTaskRef(reference.Value) {
		return "", fmt.Errorf("%w: provider %s", ErrProviderTaskReference, input.ProviderID)
	}

	commit, commitErr := service.tasks.ClaimProviderTask(ctx, ProviderTaskClaimRequest{
		ManifestID: input.ManifestID, JobID: request.JobID, ProviderID: input.ProviderID,
		Reference: reference.Value, Now: service.now().UTC(),
	})
	if commitErr != nil {
		// The external task may exist while its reference is not durably known.
		// The START_RESERVED row persists, so later executions fail closed. The
		// cause stays wrapped and readable so callers can still classify a lost
		// fence race, while this error stays an uncertain side effect.
		return "", fmt.Errorf("%w: Manifest %s: %w", ErrExecutionSideEffectUncertain, input.ManifestID, commitErr)
	}
	if !commit.CommittedReference || !commit.Task.ReferenceKnown() {
		return "", fmt.Errorf("%w: Manifest %s did not durably commit a task reference",
			ErrExecutionSideEffectUncertain, input.ManifestID)
	}
	return service.pollProviderTask(ctx, commit.Task, downloader)
}

// pollProviderTask reads provider task status for an already-linked task. It
// never starts a provider task.
func (service *ExecutionStepService) pollProviderTask(
	ctx context.Context,
	task ProviderTask,
	downloader contracts.DownloaderProvider,
) (StepOutcome, error) {
	status, err := downloader.DownloadStatus(ctx, contracts.TaskReference{Value: task.ProviderTaskRef})
	if err != nil {
		return "", fmt.Errorf("provider %s DownloadStatus: %w", task.ProviderID, err)
	}
	return mapTaskStateToOutcome(status.State)
}

// mapTaskStateToOutcome maps only the externally observable provider task
// lifecycle. Provider success is never translated into Manifest READY.
func mapTaskStateToOutcome(state contracts.TaskState) (StepOutcome, error) {
	switch state {
	case contracts.TaskStatePending, contracts.TaskStateRunning:
		return OutcomeProviderInProgress, nil
	case contracts.TaskStateSucceeded:
		return OutcomeProviderSucceeded, nil
	case contracts.TaskStateFailed:
		return OutcomeProviderFailed, nil
	case contracts.TaskStateCanceled:
		return OutcomeProviderCanceled, nil
	default:
		return "", fmt.Errorf("%w: %q", ErrProviderTaskState, state)
	}
}
