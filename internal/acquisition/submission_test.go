package acquisition_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

type submissionManifestStore struct {
	manifest *acquisition.Manifest
	job      *jobs.Job
}

func (store *submissionManifestStore) CreateManifest(_ context.Context, request acquisition.CreateManifestRequest) (acquisition.Manifest, error) {
	if store.manifest != nil {
		return acquisition.Manifest{}, acquisition.ErrConflict
	}
	manifest := acquisition.Manifest{
		ID: request.ID, UserID: request.UserID, SourceType: request.SourceType, SourceRef: request.SourceRef,
		ExpectedName: request.ExpectedName, TargetStorageBindingID: request.TargetStorageBindingID,
		TargetPath: request.TargetPath, AssetID: request.AssetID, ReleaseID: request.ReleaseID,
		VariantID: request.VariantID, State: acquisition.StatePending,
	}
	store.manifest = &manifest
	return manifest, nil
}

func (store *submissionManifestStore) GetManifest(_ context.Context, id acquisition.ManifestID) (acquisition.Manifest, error) {
	if store.manifest == nil || store.manifest.ID != id {
		return acquisition.Manifest{}, acquisition.ErrNotFound
	}
	return *store.manifest, nil
}

func (store *submissionManifestStore) Get(_ context.Context, id jobs.JobID) (jobs.Job, error) {
	if store.job == nil || store.job.ID != id {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return *store.job, nil
}

type submissionActivator struct {
	store           *submissionManifestStore
	failOnce        bool
	commitThenError bool
	calls           int
}

func (adapter *submissionActivator) Activate(_ context.Context, request acquisition.ActivateRequest) (acquisition.ActivateResult, error) {
	adapter.calls++
	if adapter.failOnce {
		adapter.failOnce = false
		return acquisition.ActivateResult{}, errors.New("injected activation failure")
	}
	manifest := adapter.store.manifest
	if manifest.State == acquisition.StatePending {
		manifest.State = acquisition.StateActive
		manifest.JobID = &request.JobID
	}
	key := acquisition.AcquisitionJobIdempotencyKey(request.ManifestID)
	job := jobs.Job{ID: *manifest.JobID, Type: acquisition.JobTypeAcquisition,
		Payload:        json.RawMessage(`{"schema_version":1,"manifest_id":"` + string(request.ManifestID) + `"}`),
		IdempotencyKey: &key, MaxAttempts: request.MaxAttempts}
	adapter.store.job = &job
	if adapter.commitThenError {
		adapter.commitThenError = false
		return acquisition.ActivateResult{}, errors.New("commit outcome unavailable")
	}
	return acquisition.ActivateResult{Manifest: *manifest, Job: job}, nil
}

func TestSubmissionPendingRetryReplayAndConflict(t *testing.T) {
	store := &submissionManifestStore{}
	activator := &submissionActivator{store: store, failOnce: true}
	service, err := acquisition.NewSubmissionService(store, store, activator, store)
	if err != nil {
		t.Fatal(err)
	}
	request := acquisition.SubmitRequest{
		ManifestID: "25000000-0000-4000-8000-000000000101",
		JobID:      "25000000-0000-4000-8000-000000000102", MaxAttempts: 3,
		Source:                 `MAGNET:?xt=urn:btih:ABC&dn=A%20B&x=tail`,
		TargetStorageBindingID: storage.BindingID("25000000-0000-4000-8000-000000000103"),
		TargetPath:             "/incoming",
	}
	first, err := service.Submit(context.Background(), request)
	if !errors.Is(err, acquisition.ErrActivationPending) || first.Status != acquisition.SubmissionPendingActivation ||
		store.manifest.State != acquisition.StatePending || store.manifest.JobID != nil ||
		store.manifest.SourceRef != request.Source || store.manifest.SourceType != "magnet" {
		t.Fatalf("pending submission = %#v, %v", first, err)
	}
	second, err := service.Submit(context.Background(), request)
	if err != nil || second.Status != acquisition.SubmissionReplay || store.manifest.State != acquisition.StateActive ||
		store.manifest.JobID == nil || *store.manifest.JobID != request.JobID {
		t.Fatalf("retry = %#v, %v", second, err)
	}
	third, err := service.Submit(context.Background(), request)
	if err != nil || third.Status != acquisition.SubmissionReplay || activator.calls != 3 {
		t.Fatalf("replay = %#v, %v calls=%d", third, err, activator.calls)
	}
	store.manifest.State = acquisition.StateAwaitingCanonical
	progressed, err := service.Submit(context.Background(), request)
	if err != nil || progressed.Status != acquisition.SubmissionReplay || activator.calls != 3 {
		t.Fatalf("progressed replay = %#v, %v calls=%d", progressed, err, activator.calls)
	}
	for _, mutate := range []func(*acquisition.SubmitRequest){
		func(value *acquisition.SubmitRequest) { value.Source += "&unexpected=true" },
		func(value *acquisition.SubmitRequest) { value.TargetPath = "/elsewhere" },
		func(value *acquisition.SubmitRequest) { value.JobID = "25000000-0000-4000-8000-000000000199" },
	} {
		conflict := request
		mutate(&conflict)
		if _, err := service.Submit(context.Background(), conflict); !errors.Is(err, acquisition.ErrSubmissionConflict) {
			t.Fatalf("conflict error = %v", err)
		}
	}
	if activator.calls != 3 {
		t.Fatalf("conflict unexpectedly activated: %d", activator.calls)
	}
}

func TestSubmissionRejectsSourceAndUnsafeIdentityBeforePersistence(t *testing.T) {
	store := &submissionManifestStore{}
	service, _ := acquisition.NewSubmissionService(store, store, &submissionActivator{store: store}, store)
	request := acquisition.SubmitRequest{ManifestID: "25000000-0000-4000-8000-000000000101",
		JobID: "25000000-0000-4000-8000-000000000102", MaxAttempts: 1, Source: "http://user:pass@example.invalid/a"}
	if _, err := service.Submit(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidSubmission) || store.manifest != nil {
		t.Fatalf("userinfo request = %v", err)
	}
	request.Source = "magnet:?xt=x"
	request.ManifestID = "magnet:?xt=source-text"
	if _, err := service.Submit(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidSubmission) || store.manifest != nil {
		t.Fatalf("unsafe ID request = %v", err)
	}
}

func TestSubmissionUncertainActivationReadbackUsesDurablePair(t *testing.T) {
	store := &submissionManifestStore{}
	activator := &submissionActivator{store: store, commitThenError: true}
	service, _ := acquisition.NewSubmissionService(store, store, activator, store)
	request := acquisition.SubmitRequest{
		ManifestID: "25000000-0000-4000-8000-000000000111",
		JobID:      "25000000-0000-4000-8000-000000000112", MaxAttempts: 2,
		Source: "ed2k://|file|x|1|A|/", TargetStorageBindingID: "25000000-0000-4000-8000-000000000113",
		TargetPath: "/incoming",
	}
	result, err := service.Submit(context.Background(), request)
	if err != nil || result.Status != acquisition.SubmissionReplay || store.manifest.State != acquisition.StateActive {
		t.Fatalf("uncertain commit readback = %#v, %v", result, err)
	}
}
