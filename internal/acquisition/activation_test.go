package acquisition_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
)

func TestActivateRejectsInvalidRequestBeforePersistence(t *testing.T) {
	store := &activationStore{}
	service, err := acquisition.NewActivationService(store)
	if err != nil {
		t.Fatalf("NewActivationService() error = %v", err)
	}
	valid := acquisition.ActivateRequest{ManifestID: "manifest-a", JobID: "job-a", MaxAttempts: 1}
	tests := []struct {
		name   string
		mutate func(*acquisition.ActivateRequest)
	}{
		{name: "Manifest ID", mutate: func(value *acquisition.ActivateRequest) { value.ManifestID = "" }},
		{name: "Job ID", mutate: func(value *acquisition.ActivateRequest) { value.JobID = "" }},
		{name: "MaxAttempts zero", mutate: func(value *acquisition.ActivateRequest) { value.MaxAttempts = 0 }},
		{name: "MaxAttempts negative", mutate: func(value *acquisition.ActivateRequest) { value.MaxAttempts = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			if _, err := service.Activate(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidActivation) {
				t.Fatalf("Activate() error = %v", err)
			}
		})
	}
	if store.calls != 0 {
		t.Fatalf("invalid activation reached persistence %d times", store.calls)
	}
}

func TestActivateBuildsMinimalAcquisitionJobContract(t *testing.T) {
	store := &activationStore{}
	service, _ := acquisition.NewActivationService(store)
	request := acquisition.ActivateRequest{ManifestID: "manifest-a", JobID: "job-a", MaxAttempts: 4}
	store.result = acquisition.ActivateResult{Changed: true}

	result, err := service.Activate(context.Background(), request)
	if err != nil || !result.Changed || store.calls != 1 {
		t.Fatalf("Activate() = %#v, %v; calls=%d", result, err, store.calls)
	}
	job := store.plan.Job
	if store.plan.ManifestID != request.ManifestID || job.ID != request.JobID || job.Type != acquisition.JobTypeAcquisition ||
		job.MaxAttempts != request.MaxAttempts || job.IdempotencyKey == nil ||
		*job.IdempotencyKey != acquisition.AcquisitionJobIdempotencyKey(request.ManifestID) {
		t.Fatalf("activation plan = %#v", store.plan)
	}
	var payload map[string]any
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(payload) != 2 || payload["schema_version"] != float64(1) || payload["manifest_id"] != string(request.ManifestID) {
		t.Fatalf("payload = %#v", payload)
	}
	for _, forbidden := range []string{"source_ref", "target_path", "credentials", "provider", "provider_data"} {
		if _, exists := payload[forbidden]; exists {
			t.Fatalf("payload contains forbidden field %q", forbidden)
		}
	}
}

func TestValidateLinkedAcquisitionJobFailsClosed(t *testing.T) {
	manifestID := acquisition.ManifestID("manifest-a")
	key := acquisition.AcquisitionJobIdempotencyKey(manifestID)
	valid := jobs.Job{
		ID: "job-a", Type: acquisition.JobTypeAcquisition,
		Payload: json.RawMessage(`{"schema_version":1,"manifest_id":"manifest-a"}`), IdempotencyKey: &key,
	}
	if err := acquisition.ValidateLinkedAcquisitionJob(manifestID, valid); err != nil {
		t.Fatalf("valid Job error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*jobs.Job)
	}{
		{name: "wrong type", mutate: func(value *jobs.Job) { value.Type = "OTHER" }},
		{name: "missing key", mutate: func(value *jobs.Job) { value.IdempotencyKey = nil }},
		{name: "wrong key", mutate: func(value *jobs.Job) { wrong := "acquisition:other"; value.IdempotencyKey = &wrong }},
		{name: "wrong manifest", mutate: func(value *jobs.Job) { value.Payload = json.RawMessage(`{"schema_version":1,"manifest_id":"other"}`) }},
		{name: "wrong schema", mutate: func(value *jobs.Job) {
			value.Payload = json.RawMessage(`{"schema_version":2,"manifest_id":"manifest-a"}`)
		}},
		{name: "provider data", mutate: func(value *jobs.Job) {
			value.Payload = json.RawMessage(`{"schema_version":1,"manifest_id":"manifest-a","provider":"115"}`)
		}},
		{name: "invalid JSON", mutate: func(value *jobs.Job) { value.Payload = json.RawMessage(`{`) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			test.mutate(&candidate)
			if err := acquisition.ValidateLinkedAcquisitionJob(manifestID, candidate); !errors.Is(err, acquisition.ErrCorruptActivation) {
				t.Fatalf("ValidateLinkedAcquisitionJob() error = %v", err)
			}
		})
	}
}

type activationStore struct {
	calls  int
	plan   acquisition.ActivationPlan
	result acquisition.ActivateResult
	err    error
}

func (store *activationStore) ActivateManifest(_ context.Context, plan acquisition.ActivationPlan) (acquisition.ActivateResult, error) {
	store.calls++
	store.plan = plan
	return store.result, store.err
}
