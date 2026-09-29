package acquisition

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

var (
	ErrInvalidExecutionRequest  = errors.New("invalid acquisition execution request")
	ErrProviderNotRegistered    = errors.New("acquisition provider is not registered")
	ErrProviderNotDownloader    = errors.New("acquisition provider has no downloader capability")
	ErrProviderIdentityMismatch = errors.New("acquisition provider identity mismatch")
	ErrProviderTaskReference    = errors.New("acquisition provider returned an invalid task reference")
	ErrProviderTaskState        = errors.New("acquisition provider returned an unknown task state")
	ErrExecutionJobMismatch     = errors.New("acquisition execution Job does not match the Manifest")
	// ErrExecutionSideEffectUncertain reports that an external side effect may
	// have happened but its opaque task reference was not durably recorded.
	// Callers must treat this as recovery-required state: a later execution may
	// not start another provider task on the strength of this outcome.
	ErrExecutionSideEffectUncertain = errors.New("acquisition external side effect is uncertain")
)

// StepRequest proves that this execution runs on behalf of one fenced RUNNING
// ACQUISITION Job. Owner and Attempt come from the Job Engine lease context and
// are compared against durable Job state, so a foreign, stale, or unleased actor
// fails closed before any provider task is started or polled.
type StepRequest struct {
	JobID   jobs.JobID
	Owner   string
	Attempt int
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

// ProviderCatalog is the narrow read port this service needs from the provider
// registry. Package registry.Registry satisfies it directly, which keeps this
// package free of any dependency on the registry or adapters.
type ProviderCatalog interface {
	Lookup(contracts.ProviderID) (DownloaderBinding, error)
}

// DownloaderBinding is the provider-neutral projection of one registered
// provider: its descriptor identity plus its Downloader port when implemented.
type DownloaderBinding struct {
	Descriptor contracts.Descriptor
	Downloader contracts.DownloaderProvider
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
	providers ProviderCatalog
	tasks     ProviderTaskStore
	now       Clock
}

func NewExecutionStepService(
	inputs *ExecutionInputResolver,
	manifests ManifestReader,
	jobs JobReader,
	providers ProviderCatalog,
	tasks ProviderTaskStore,
	options ...ExecutionOption,
) (*ExecutionStepService, error) {
	if inputs == nil || manifests == nil || jobs == nil || providers == nil || tasks == nil {
		return nil, ErrInvalidExecutionRequest
	}
	service := &ExecutionStepService{
		inputs: inputs, manifests: manifests, jobs: jobs, providers: providers, tasks: tasks, now: time.Now,
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
// and performs exactly one bounded provider step: either StartDownload once
// followed by durable linkage, or DownloadStatus for an already-linked task.
// It performs no OpenList, IndexCore, canonical confirmation, or Copy mutation.
func (service *ExecutionStepService) Execute(ctx context.Context, request StepRequest) (StepOutcome, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if request.JobID == "" || request.Owner == "" || request.Attempt < 1 {
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

	downloader, err := service.resolveDownloader(input.ProviderID)
	if err != nil {
		return "", err
	}

	task, err := service.tasks.GetProviderTask(ctx, manifest.ID)
	if errors.Is(err, ErrProviderTaskNotFound) {
		return service.startProviderTask(ctx, request, input, downloader)
	}
	if err != nil {
		return "", fmt.Errorf("%w: load provider task: %v", ErrProviderTaskPersistence, err)
	}
	if task.ProviderID != input.ProviderID || task.JobID != job.ID || !ValidProviderTaskRef(task.ProviderTaskRef) {
		return "", fmt.Errorf("%w: durable provider task for Manifest %s", ErrExecutionIdentityMismatch, manifest.ID)
	}
	return service.pollProviderTask(ctx, task, downloader)
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
// the requesting lease holder, and still on the requesting attempt.
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
	if job.AttemptCount != request.Attempt {
		return mismatch(fmt.Sprintf("Job %s attempt %d does not match %d", job.ID, job.AttemptCount, request.Attempt))
	}
	return nil
}

// resolveDownloader fails closed when the resolved provider identity is not
// registered, has no Downloader port, or reports a different descriptor identity
// than the StorageConnection provider identity.
func (service *ExecutionStepService) resolveDownloader(providerID contracts.ProviderID) (contracts.DownloaderProvider, error) {
	if !providerID.Valid() {
		return nil, fmt.Errorf("%w: %q", ErrProviderIdentityMismatch, providerID)
	}
	binding, err := service.providers.Lookup(providerID)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrProviderNotRegistered, providerID, err)
	}
	if binding.Descriptor.ID != providerID {
		return nil, fmt.Errorf("%w: requested %s, registry returned %s", ErrProviderIdentityMismatch, providerID, binding.Descriptor.ID)
	}
	if binding.Downloader == nil || !binding.Descriptor.Capabilities.Downloader {
		return nil, fmt.Errorf("%w: %s", ErrProviderNotDownloader, providerID)
	}
	return binding.Downloader, nil
}

// startProviderTask performs the single StartDownload call for this execution and
// records the returned reference durably. When the side effect succeeds but the
// linkage cannot be persisted, the outcome is explicitly uncertain and no second
// StartDownload is attempted on this path.
func (service *ExecutionStepService) startProviderTask(
	ctx context.Context,
	request StepRequest,
	input ExecutionInput,
	downloader contracts.DownloaderProvider,
) (StepOutcome, error) {
	reference, err := downloader.StartDownload(ctx, input.Download)
	if err != nil {
		return "", fmt.Errorf("provider %s StartDownload: %w", input.ProviderID, err)
	}
	if !ValidProviderTaskRef(reference.Value) {
		return "", fmt.Errorf("%w: provider %s", ErrProviderTaskReference, input.ProviderID)
	}
	now := service.now().UTC()
	task := ProviderTask{
		ManifestID: input.ManifestID, JobID: request.JobID, ProviderID: input.ProviderID,
		ProviderTaskRef: reference.Value, CreatedAt: now, UpdatedAt: now,
	}
	if err := service.tasks.StoreProviderTask(ctx, task); err != nil {
		if errors.Is(err, ErrProviderTaskIdentityChange) {
			return "", fmt.Errorf("%w: Manifest %s", ErrExecutionIdentityMismatch, input.ManifestID)
		}
		return "", fmt.Errorf("%w: Manifest %s: %v", ErrExecutionSideEffectUncertain, input.ManifestID, err)
	}
	return service.pollProviderTask(ctx, task, downloader)
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
