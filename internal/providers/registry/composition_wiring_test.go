package registry_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/providers/registry"
	"github.com/nathanxiangang-web/panta/internal/providers/testprovider"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

const (
	compositionManifestID = acquisition.ManifestID("c0000000-0000-4000-8000-000000000001")
	compositionJobID      = jobs.JobID("c0000000-0000-4000-8000-000000000002")
	compositionBindingID  = storage.BindingID("c0000000-0000-4000-8000-000000000003")
	compositionConnID     = storage.ConnectionID("c0000000-0000-4000-8000-000000000004")
	compositionOwner      = "composition-worker"
)

var compositionNow = time.Date(2026, 9, 30, 6, 0, 0, 0, time.UTC)

// --- minimal durable doubles (the registry and provider are the real ones) ----

type compositionManifestReader struct {
	values map[acquisition.ManifestID]acquisition.Manifest
}

func (reader *compositionManifestReader) GetManifest(_ context.Context, id acquisition.ManifestID) (acquisition.Manifest, error) {
	manifest, exists := reader.values[id]
	if !exists {
		return acquisition.Manifest{}, acquisition.ErrNotFound
	}
	return manifest, nil
}

type compositionJobReader struct {
	values map[jobs.JobID]jobs.Job
}

func (reader *compositionJobReader) Get(_ context.Context, id jobs.JobID) (jobs.Job, error) {
	job, exists := reader.values[id]
	if !exists {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return job, nil
}

type compositionTopologyReader struct {
	binding    storage.Binding
	connection storage.Connection
}

func (reader *compositionTopologyReader) GetBinding(_ context.Context, _ storage.BindingID) (storage.Binding, error) {
	return reader.binding, nil
}

func (reader *compositionTopologyReader) GetConnection(_ context.Context, _ storage.ConnectionID) (storage.Connection, error) {
	return reader.connection, nil
}

// compositionFence is an in-memory implementation of the durable fence with the
// same claim semantics as the PostgreSQL store.
type compositionFence struct {
	mu      sync.Mutex
	values  map[acquisition.ManifestID]acquisition.ProviderTask
	claimed int
}

func (fence *compositionFence) GetProviderTask(_ context.Context, id acquisition.ManifestID) (acquisition.ProviderTask, error) {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	task, exists := fence.values[id]
	if !exists {
		return acquisition.ProviderTask{}, acquisition.ErrProviderTaskNotFound
	}
	return task, nil
}

func (fence *compositionFence) ClaimProviderTask(
	_ context.Context,
	request acquisition.ProviderTaskClaimRequest,
) (acquisition.ProviderTaskClaimResult, error) {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	existing, exists := fence.values[request.ManifestID]
	if !exists {
		if request.Reference != "" {
			return acquisition.ProviderTaskClaimResult{}, acquisition.ErrInvalidProviderTask
		}
		reserved := acquisition.ProviderTask{
			ManifestID: request.ManifestID, JobID: request.JobID, ProviderID: request.ProviderID,
			State: acquisition.ProviderTaskStartReserved, CreatedAt: request.Now, UpdatedAt: request.Now,
		}
		fence.values[request.ManifestID] = reserved
		fence.claimed++
		return acquisition.ProviderTaskClaimResult{Task: reserved, ClaimedStart: true}, nil
	}
	if existing.JobID != request.JobID || existing.ProviderID != request.ProviderID {
		return acquisition.ProviderTaskClaimResult{}, acquisition.ErrProviderTaskIdentityChange
	}
	if existing.ReferenceKnown() {
		if request.Reference == "" {
			return acquisition.ProviderTaskClaimResult{Task: existing}, nil
		}
		if existing.ProviderTaskRef != request.Reference {
			return acquisition.ProviderTaskClaimResult{}, acquisition.ErrProviderTaskIdentityChange
		}
		return acquisition.ProviderTaskClaimResult{Task: existing, CommittedReference: true}, nil
	}
	if request.Reference == "" {
		return acquisition.ProviderTaskClaimResult{Task: existing}, nil
	}
	committed := existing
	committed.ProviderTaskRef = request.Reference
	committed.State = acquisition.ProviderTaskReferenceKnown
	committed.UpdatedAt = request.Now
	fence.values[request.ManifestID] = committed
	return acquisition.ProviderTaskClaimResult{Task: committed, CommittedReference: true}, nil
}

// --- real composition fixture -------------------------------------------------

type compositionFixture struct {
	service  *acquisition.ExecutionStepService
	registry *registry.Registry
	provider *testprovider.Provider
	fence    *compositionFence
}

// newCompositionFixture wires the real registry, the real test provider, and the
// real execution service. The only doubles are the durable readers and the fence.
func newCompositionFixture(t *testing.T) *compositionFixture {
	t.Helper()
	provider := testprovider.New()
	registered := registry.New()
	if err := registered.Register(registry.Entry{
		Descriptor: provider.Descriptor(),
		Storage:    provider,
		Downloader: provider,
		Share:      provider,
	}); err != nil {
		t.Fatalf("Register(real testprovider) error = %v", err)
	}

	scope := testprovider.KnownTarget().Scope
	manifests := &compositionManifestReader{values: map[acquisition.ManifestID]acquisition.Manifest{
		compositionManifestID: {
			ID: compositionManifestID, SourceType: "opaque-source", SourceRef: "opaque-ref",
			TargetStorageBindingID: compositionBindingID,
			TargetPath:             testprovider.KnownTarget().Path,
			JobID:                  jobIDPointer(compositionJobID), State: acquisition.StateActive,
			CreatedAt: compositionNow, UpdatedAt: compositionNow,
		},
	}}
	key := acquisition.AcquisitionJobIdempotencyKey(compositionManifestID)
	leaseExpiry := compositionNow.Add(time.Hour)
	jobsvc := &compositionJobReader{values: map[jobs.JobID]jobs.Job{
		compositionJobID: {
			ID: compositionJobID, Type: acquisition.JobTypeAcquisition,
			Payload:        []byte(`{"schema_version":1,"manifest_id":"` + string(compositionManifestID) + `"}`),
			State:          jobs.StateRunning,
			IdempotencyKey: &key, AttemptCount: 1, MaxAttempts: 3,
			LeaseOwner: stringPointer(compositionOwner), LeaseExpiresAt: &leaseExpiry,
			CreatedAt: compositionNow, UpdatedAt: compositionNow,
		},
	}}
	topology := &compositionTopologyReader{
		binding: storage.Binding{
			ID: compositionBindingID, ConnectionID: compositionConnID, ProviderScope: &scope,
			OpenListMountPath: "/openlist/mount", IndexCoreRootID: "root-1", Status: storage.BindingStatusActive,
		},
		connection: storage.Connection{
			ID: compositionConnID, ProviderType: string(testprovider.ID), Status: storage.ConnectionStatusActive,
		},
	}
	resolver, err := acquisition.NewExecutionInputResolver(manifests, topology)
	if err != nil {
		t.Fatalf("NewExecutionInputResolver() error = %v", err)
	}
	fence := &compositionFence{values: map[acquisition.ManifestID]acquisition.ProviderTask{}}
	service, err := acquisition.NewExecutionStepService(
		resolver, manifests, jobsvc, registered, fence,
		acquisition.WithExecutionClock(func() time.Time { return compositionNow }),
	)
	if err != nil {
		t.Fatalf("NewExecutionStepService(real registry) error = %v", err)
	}
	return &compositionFixture{service: service, registry: registered, provider: provider, fence: fence}
}

func (fixture *compositionFixture) request() acquisition.StepRequest {
	return acquisition.StepRequest{JobID: compositionJobID, Owner: compositionOwner, Attempt: 1}
}

func jobIDPointer(id jobs.JobID) *jobs.JobID { return &id }

func stringPointer(value string) *string { return &value }

// --- tests --------------------------------------------------------------------

func TestRegistrySatisfiesAcquisitionProviderCatalogAtRuntime(t *testing.T) {
	var catalog acquisition.ProviderCatalog = registry.New()
	if catalog == nil {
		t.Fatal("registry.New() did not satisfy acquisition.ProviderCatalog")
	}
}

// TestRealRegistryWiresIntoExecutionService is the end-to-end wiring proof: real
// registry + real provider + real execution service, with no adapter.
func TestRealRegistryWiresIntoExecutionService(t *testing.T) {
	fixture := newCompositionFixture(t)
	ctx := context.Background()

	binding, err := fixture.registry.LookupDownloader(testprovider.ID)
	if err != nil {
		t.Fatalf("LookupDownloader() error = %v", err)
	}
	if binding.Descriptor.ID != testprovider.ID || binding.Downloader == nil {
		t.Fatalf("LookupDownloader() = %#v", binding)
	}

	outcome, err := fixture.service.Execute(ctx, fixture.request())
	if err != nil {
		t.Fatalf("Execute() with the real registry error = %v", err)
	}
	// testprovider starts tasks in PENDING, which maps to in-progress.
	if outcome != acquisition.OutcomeProviderInProgress {
		t.Fatalf("Execute() outcome = %q, want %q", outcome, acquisition.OutcomeProviderInProgress)
	}
	if fixture.fence.claimed != 1 {
		t.Fatalf("durable start claims = %d, want exactly 1", fixture.fence.claimed)
	}
	task, err := fixture.fence.GetProviderTask(ctx, compositionManifestID)
	if err != nil {
		t.Fatalf("GetProviderTask() error = %v", err)
	}
	if !task.ReferenceKnown() {
		t.Fatalf("durable task = %#v, want a known reference from the real provider", task)
	}
	// The stored reference is the one the real provider actually issued.
	status, err := fixture.provider.DownloadStatus(ctx, contracts.TaskReference{Value: task.ProviderTaskRef})
	if err != nil {
		t.Fatalf("testprovider DownloadStatus(stored ref) error = %v", err)
	}
	if status.Reference.Value != task.ProviderTaskRef {
		t.Fatalf("provider status reference = %q, want %q", status.Reference.Value, task.ProviderTaskRef)
	}
}

// TestRealRegistryReplayDoesNotStartAgain proves the real provider is invoked at
// most once for a Manifest even across repeated executions.
func TestRealRegistryReplayDoesNotStartAgain(t *testing.T) {
	fixture := newCompositionFixture(t)
	ctx := context.Background()
	if _, err := fixture.service.Execute(ctx, fixture.request()); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := fixture.service.Execute(ctx, fixture.request()); err != nil {
			t.Fatalf("replay %d Execute() error = %v", attempt, err)
		}
	}
	if fixture.fence.claimed != 1 {
		t.Fatalf("durable start claims = %d, want exactly 1 across replays", fixture.fence.claimed)
	}
}

func TestRealRegistryReportsUnknownProvider(t *testing.T) {
	registered := registry.New()
	if _, err := registered.LookupDownloader(contracts.ProviderID("not-registered")); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("LookupDownloader(unknown) error = %v, want registry.ErrNotFound", err)
	}
}
