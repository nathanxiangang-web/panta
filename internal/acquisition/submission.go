package acquisition

import (
	"context"
	"errors"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var (
	ErrInvalidSubmission   = errors.New("invalid acquisition submission")
	ErrSubmissionConflict  = errors.New("conflicting acquisition submission")
	ErrActivationPending   = errors.New("acquisition intent pending activation")
	ErrSubmissionUncertain = errors.New("acquisition submission state uncertain")
)

type SubmissionStatus string

const (
	SubmissionCreated           SubmissionStatus = "CREATED"
	SubmissionReplay            SubmissionStatus = "REPLAY"
	SubmissionPendingActivation SubmissionStatus = "PENDING_ACTIVATION"
)

type SubmitRequest struct {
	ManifestID             ManifestID
	JobID                  jobs.JobID
	MaxAttempts            int
	Source                 string
	UserID                 *UserID
	ExpectedName           *string
	TargetStorageBindingID storage.BindingID
	TargetPath             string
	AssetID                *catalog.AssetID
	ReleaseID              *catalog.ReleaseID
	VariantID              *catalog.VariantID
}

type SubmitResult struct {
	ManifestID ManifestID
	JobID      jobs.JobID
	Status     SubmissionStatus
}

type ManifestCreator interface {
	CreateManifest(context.Context, CreateManifestRequest) (Manifest, error)
}

type ManifestLookup interface {
	GetManifest(context.Context, ManifestID) (Manifest, error)
}

type ManifestActivator interface {
	Activate(context.Context, ActivateRequest) (ActivateResult, error)
}

type SubmissionJobLookup interface {
	Get(context.Context, jobs.JobID) (jobs.Job, error)
}

type SubmissionService struct {
	creator   ManifestCreator
	lookup    ManifestLookup
	activator ManifestActivator
	jobs      SubmissionJobLookup
}

func NewSubmissionService(creator ManifestCreator, lookup ManifestLookup, activator ManifestActivator, jobLookup SubmissionJobLookup) (*SubmissionService, error) {
	if creator == nil || lookup == nil || activator == nil || jobLookup == nil {
		return nil, ErrInvalidSubmission
	}
	return &SubmissionService{creator: creator, lookup: lookup, activator: activator, jobs: jobLookup}, nil
}

// Submit creates durable PENDING intent, then activates it through D-024/025.
// The two steps are intentionally not hidden in a transaction: activation
// failure leaves a visible, retryable PENDING Manifest with the stable IDs.
func (service *SubmissionService) Submit(ctx context.Context, request SubmitRequest) (SubmitResult, error) {
	if service == nil || !validSubmissionID(string(request.ManifestID)) ||
		!validSubmissionID(string(request.JobID)) || request.MaxAttempts < 1 {
		return SubmitResult{}, ErrInvalidSubmission
	}
	source, err := ResolveSource(request.Source)
	if err != nil {
		return SubmitResult{}, ErrInvalidSubmission
	}
	create := CreateManifestRequest{
		ID: request.ManifestID, UserID: request.UserID,
		SourceType: source.Scheme, SourceRef: source.Value, ExpectedName: request.ExpectedName,
		TargetStorageBindingID: request.TargetStorageBindingID, TargetPath: request.TargetPath,
		AssetID: request.AssetID, ReleaseID: request.ReleaseID, VariantID: request.VariantID,
	}
	result := SubmitResult{ManifestID: request.ManifestID, JobID: request.JobID}
	created, err := service.lookup.GetManifest(ctx, request.ManifestID)
	newManifest := false
	if errors.Is(err, ErrNotFound) {
		created, err = service.creator.CreateManifest(ctx, create)
		newManifest = err == nil
	}
	if errors.Is(err, ErrConflict) {
		created, err = service.lookup.GetManifest(ctx, request.ManifestID)
		if errors.Is(err, ErrNotFound) {
			return SubmitResult{}, ErrSubmissionConflict
		}
	}
	if err != nil {
		return SubmitResult{}, submissionError(err)
	}
	if !sameSubmissionIntent(created, create) || !created.State.Valid() ||
		created.State == StatePending && created.JobID != nil ||
		created.State != StatePending && created.JobID == nil ||
		created.JobID != nil && *created.JobID != request.JobID {
		return SubmitResult{}, ErrSubmissionConflict
	}
	if created.State != StatePending && created.State != StateActive {
		if created.JobID == nil {
			return SubmitResult{}, ErrSubmissionConflict
		}
		job, err := service.jobs.Get(ctx, request.JobID)
		if err != nil || job.MaxAttempts != request.MaxAttempts ||
			ValidateLinkedAcquisitionJob(request.ManifestID, job) != nil {
			return SubmitResult{}, ErrSubmissionConflict
		}
		result.Status = SubmissionReplay
		return result, nil
	}
	activated, err := service.activator.Activate(ctx, ActivateRequest{
		ManifestID: request.ManifestID, JobID: request.JobID, MaxAttempts: request.MaxAttempts,
	})
	if err != nil {
		// A failed/uncertain commit is not proof of PENDING. Read back the
		// durable pair before describing it to an operator.
		observed, readErr := service.lookup.GetManifest(ctx, request.ManifestID)
		if readErr != nil || !sameSubmissionIntent(observed, create) {
			return SubmitResult{}, ErrSubmissionUncertain
		}
		if observed.State == StatePending && observed.JobID == nil {
			result.Status = SubmissionPendingActivation
			return result, ErrActivationPending
		}
		if observed.State == StateActive && observed.JobID != nil && *observed.JobID == request.JobID {
			job, jobErr := service.jobs.Get(ctx, request.JobID)
			if jobErr == nil && job.MaxAttempts == request.MaxAttempts &&
				ValidateLinkedAcquisitionJob(request.ManifestID, job) == nil {
				result.Status = SubmissionReplay
				return result, nil
			}
		}
		return SubmitResult{}, ErrSubmissionUncertain
	}
	if activated.Manifest.ID != request.ManifestID || activated.Job.ID != request.JobID ||
		activated.Job.MaxAttempts != request.MaxAttempts ||
		activated.Manifest.JobID == nil || *activated.Manifest.JobID != request.JobID ||
		activated.Manifest.State != StateActive ||
		ValidateLinkedAcquisitionJob(request.ManifestID, activated.Job) != nil {
		return SubmitResult{}, ErrSubmissionConflict
	}
	if newManifest {
		result.Status = SubmissionCreated
	} else {
		result.Status = SubmissionReplay
	}
	return result, nil
}

func validSubmissionID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if char != '-' {
				return false
			}
		} else if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') ||
			(char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}

func sameSubmissionIntent(stored Manifest, request CreateManifestRequest) bool {
	targetPath, err := NormalizeTargetPath(request.TargetPath)
	return err == nil && stored.ID == request.ID && stored.SourceType == request.SourceType &&
		stored.SourceRef == request.SourceRef && stored.TargetStorageBindingID == request.TargetStorageBindingID &&
		stored.TargetPath == targetPath && equalOptional(stored.UserID, request.UserID) &&
		equalOptional(stored.ExpectedName, request.ExpectedName) &&
		equalOptional(stored.AssetID, request.AssetID) && equalOptional(stored.ReleaseID, request.ReleaseID) &&
		equalOptional(stored.VariantID, request.VariantID)
}

func equalOptional[T comparable](left, right *T) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func submissionError(err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, ErrInvalidArgument) || errors.Is(err, ErrInvalidTargetPath) ||
		errors.Is(err, ErrInvalidReference) || errors.Is(err, ErrStorageBindingNotFound) ||
		errors.Is(err, ErrStorageBindingDisabled) || errors.Is(err, ErrStorageBindingNotAcquisitionCapable) ||
		errors.Is(err, ErrStorageConnectionNotFound) || errors.Is(err, ErrStorageConnectionDisabled) ||
		errors.Is(err, ErrStorageTopologyMismatch) || errors.Is(err, ErrInvalidProviderIdentity) ||
		errors.Is(err, ErrAssetNotFound) || errors.Is(err, ErrReleaseNotFound) ||
		errors.Is(err, ErrVariantNotFound) || errors.Is(err, ErrReleaseOwnershipMismatch) ||
		errors.Is(err, ErrVariantOwnershipMismatch) {
		return ErrInvalidSubmission
	}
	return ErrSubmissionUncertain
}
