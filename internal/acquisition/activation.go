package acquisition

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/nathanxiangang-web/panta/internal/jobs"
)

const (
	JobTypeAcquisition              = "ACQUISITION"
	AcquisitionPayloadSchemaVersion = 1
)

var (
	ErrInvalidActivation          = errors.New("invalid acquisition activation")
	ErrActivationManifestNotFound = errors.New("acquisition activation Manifest not found")
	ErrInvalidManifestState       = errors.New("invalid acquisition Manifest state for activation")
	ErrCorruptActivation          = errors.New("corrupt acquisition activation linkage")
	ErrActivationPersistence      = errors.New("acquisition activation persistence failure")
)

type ActivateRequest struct {
	ManifestID  ManifestID
	JobID       jobs.JobID
	MaxAttempts int
}

type ActivateResult struct {
	Manifest Manifest
	Job      jobs.Job
	Changed  bool
}

// ActivationPlan is the complete provider-neutral persistence input. The store
// must commit the Job and Manifest transition atomically.
type ActivationPlan struct {
	ManifestID ManifestID
	Job        jobs.CreateRequest
}

type ActivationStore interface {
	ActivateManifest(context.Context, ActivationPlan) (ActivateResult, error)
}

type ActivationService struct {
	store ActivationStore
}

func NewActivationService(store ActivationStore) (*ActivationService, error) {
	if store == nil {
		return nil, ErrInvalidActivation
	}
	return &ActivationService{store: store}, nil
}

// Activate makes a validated Manifest executable without performing any
// provider, OpenList, IndexCore, or network operation.
func (service *ActivationService) Activate(ctx context.Context, request ActivateRequest) (ActivateResult, error) {
	if request.ManifestID == "" || request.JobID == "" || request.MaxAttempts < 1 {
		return ActivateResult{}, ErrInvalidActivation
	}
	payload, err := json.Marshal(acquisitionJobPayload{
		SchemaVersion: AcquisitionPayloadSchemaVersion,
		ManifestID:    request.ManifestID,
	})
	if err != nil {
		return ActivateResult{}, fmt.Errorf("%w: encode Job payload: %v", ErrInvalidActivation, err)
	}
	key := AcquisitionJobIdempotencyKey(request.ManifestID)
	return service.store.ActivateManifest(ctx, ActivationPlan{
		ManifestID: request.ManifestID,
		Job: jobs.CreateRequest{
			ID: request.JobID, Type: JobTypeAcquisition, Payload: payload,
			IdempotencyKey: &key, MaxAttempts: request.MaxAttempts,
		},
	})
}

func AcquisitionJobIdempotencyKey(manifestID ManifestID) string {
	return "acquisition:" + string(manifestID)
}

type acquisitionJobPayload struct {
	SchemaVersion int        `json:"schema_version"`
	ManifestID    ManifestID `json:"manifest_id"`
}

// ValidateLinkedAcquisitionJob fails closed when an ACTIVE Manifest's durable
// Job does not exactly match the frozen Gate 3.2 linkage contract.
func ValidateLinkedAcquisitionJob(manifestID ManifestID, job jobs.Job) error {
	if manifestID == "" || job.ID == "" || job.Type != JobTypeAcquisition || job.IdempotencyKey == nil ||
		*job.IdempotencyKey != AcquisitionJobIdempotencyKey(manifestID) {
		return ErrCorruptActivation
	}
	decoder := json.NewDecoder(bytes.NewReader(job.Payload))
	decoder.DisallowUnknownFields()
	var payload acquisitionJobPayload
	if err := decoder.Decode(&payload); err != nil {
		return fmt.Errorf("%w: invalid Job payload", ErrCorruptActivation)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: invalid trailing Job payload", ErrCorruptActivation)
	}
	if payload.SchemaVersion != AcquisitionPayloadSchemaVersion || payload.ManifestID != manifestID {
		return ErrCorruptActivation
	}
	return nil
}
