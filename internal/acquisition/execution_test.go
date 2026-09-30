package acquisition_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var executionNow = time.Date(2026, 9, 30, 4, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------------------
// doubles
// ---------------------------------------------------------------------------

type manifestReaderDouble struct {
	values map[acquisition.ManifestID]acquisition.Manifest
}

func (reader *manifestReaderDouble) GetManifest(_ context.Context, id acquisition.ManifestID) (acquisition.Manifest, error) {
	manifest, exists := reader.values[id]
	if !exists {
		return acquisition.Manifest{}, acquisition.ErrNotFound
	}
	return manifest, nil
}

type jobReaderDouble struct {
	values map[jobs.JobID]jobs.Job
}

func (reader *jobReaderDouble) Get(_ context.Context, id jobs.JobID) (jobs.Job, error) {
	job, exists := reader.values[id]
	if !exists {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return job, nil
}

type topologyDouble struct {
	bindings    map[storage.BindingID]storage.Binding
	connections map[storage.ConnectionID]storage.Connection
}

func (topology *topologyDouble) GetBinding(_ context.Context, id storage.BindingID) (storage.Binding, error) {
	binding, exists := topology.bindings[id]
	if !exists {
		return storage.Binding{}, storage.ErrNotFound
	}
	return binding, nil
}

func (topology *topologyDouble) GetConnection(_ context.Context, id storage.ConnectionID) (storage.Connection, error) {
	connection, exists := topology.connections[id]
	if !exists {
		return storage.Connection{}, storage.ErrNotFound
	}
	return connection, nil
}

// taskStoreDouble is an in-memory implementation of the durable provider-task
// fence with the same atomic claim semantics as the PostgreSQL store. A single
// instance spans several service instances so tests can prove restart safety.
type taskStoreDouble struct {
	mu              sync.Mutex
	values          map[acquisition.ManifestID]acquisition.ProviderTask
	claimCalls      int
	failClaimStart  error
	failClaimCommit error
}

func newTaskStoreDouble() *taskStoreDouble {
	return &taskStoreDouble{values: map[acquisition.ManifestID]acquisition.ProviderTask{}}
}

func (store *taskStoreDouble) GetProviderTask(_ context.Context, id acquisition.ManifestID) (acquisition.ProviderTask, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	task, exists := store.values[id]
	if !exists {
		return acquisition.ProviderTask{}, acquisition.ErrProviderTaskNotFound
	}
	return task, nil
}

func (store *taskStoreDouble) ClaimProviderTask(
	_ context.Context,
	request acquisition.ProviderTaskClaimRequest,
) (acquisition.ProviderTaskClaimResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.claimCalls++

	existing, exists := store.values[request.ManifestID]
	if !exists {
		if request.Reference != "" {
			return acquisition.ProviderTaskClaimResult{}, acquisition.ErrInvalidProviderTask
		}
		if store.failClaimStart != nil {
			return acquisition.ProviderTaskClaimResult{}, store.failClaimStart
		}
		reserved := acquisition.ProviderTask{
			ManifestID: request.ManifestID, JobID: request.JobID, ProviderID: request.ProviderID,
			State: acquisition.ProviderTaskStartReserved, CreatedAt: request.Now, UpdatedAt: request.Now,
		}
		store.values[request.ManifestID] = reserved
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
		// A reservation without a reference may not authorize a second start.
		return acquisition.ProviderTaskClaimResult{Task: existing}, nil
	}
	if store.failClaimCommit != nil {
		return acquisition.ProviderTaskClaimResult{}, store.failClaimCommit
	}
	committed := existing
	committed.ProviderTaskRef = request.Reference
	committed.State = acquisition.ProviderTaskReferenceKnown
	committed.UpdatedAt = request.Now
	store.values[request.ManifestID] = committed
	return acquisition.ProviderTaskClaimResult{Task: committed, CommittedReference: true}, nil
}

// scriptedProvider is a provider-neutral Downloader double that records exactly
// which provider operations were requested and with which opaque reference.
type scriptedProvider struct {
	descriptor contracts.Descriptor

	mu             sync.Mutex
	startCalls     int
	statusCalls    int
	statusRefs     []string
	startRequests  []contracts.DownloadRequest
	startReference contracts.TaskReference
	startError     error
	statuses       map[string]contracts.TaskStatus
	statusError    error
}

func newScriptedProvider(id contracts.ProviderID) *scriptedProvider {
	return &scriptedProvider{
		descriptor: contracts.Descriptor{
			ID:           id,
			DisplayName:  "scripted downloader",
			Capabilities: contracts.CapabilitySet{Downloader: true},
		},
		startReference: contracts.TaskReference{Value: "provider-task-0001"},
		statuses:       map[string]contracts.TaskStatus{},
	}
}

func (provider *scriptedProvider) Descriptor() contracts.Descriptor { return provider.descriptor }

func (provider *scriptedProvider) StartDownload(_ context.Context, request contracts.DownloadRequest) (contracts.TaskReference, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.startCalls++
	provider.startRequests = append(provider.startRequests, request)
	if provider.startError != nil {
		return contracts.TaskReference{}, provider.startError
	}
	// A provider owns its task as soon as it returns a reference, so the double
	// must be immediately pollable for that reference.
	provider.statuses[provider.startReference.Value] = contracts.TaskStatus{
		Reference: provider.startReference, State: contracts.TaskStatePending,
	}
	return provider.startReference, nil
}

func (provider *scriptedProvider) DownloadStatus(_ context.Context, reference contracts.TaskReference) (contracts.TaskStatus, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.statusCalls++
	provider.statusRefs = append(provider.statusRefs, reference.Value)
	if provider.statusError != nil {
		return contracts.TaskStatus{}, provider.statusError
	}
	status, exists := provider.statuses[reference.Value]
	if !exists {
		return contracts.TaskStatus{}, errors.New("scripted provider: unknown task reference")
	}
	return status, nil
}

func (provider *scriptedProvider) CancelDownload(context.Context, contracts.TaskReference) error {
	return nil
}

func (provider *scriptedProvider) setState(reference string, state contracts.TaskState) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.statuses[reference] = contracts.TaskStatus{Reference: contracts.TaskReference{Value: reference}, State: state}
}

// adoptTask teaches the double about a task reference created before a simulated
// process restart, without recording a StartDownload call.
func (provider *scriptedProvider) adoptTask(reference string, state contracts.TaskState) {
	provider.setState(reference, state)
}

func (provider *scriptedProvider) counts() (start int, status int, refs []string) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.startCalls, provider.statusCalls, append([]string(nil), provider.statusRefs...)
}

// sessionDouble resolves downloader sessions from an explicit map so session
// problems can be injected independently of the real composition package. It
// implements the exact same port internal/providers/session satisfies, records the
// exact identity it was asked to resolve, and is safe for concurrent executions.
type sessionDouble struct {
	mu          sync.Mutex
	bindings    map[sessionKey]contracts.DownloaderBinding
	err         error
	calls       int
	lastRequest acquisition.DownloaderSessionRequest
}

type sessionKey struct {
	providerID   contracts.ProviderID
	connectionID string
}

func newSessionKey(providerID contracts.ProviderID, connectionID string) sessionKey {
	return sessionKey{providerID: providerID, connectionID: connectionID}
}

func (session *sessionDouble) ResolveDownloader(
	_ context.Context,
	request acquisition.DownloaderSessionRequest,
) (contracts.DownloaderBinding, error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.calls++
	session.lastRequest = request
	if session.err != nil {
		return contracts.DownloaderBinding{}, session.err
	}
	binding, exists := session.bindings[newSessionKey(request.ProviderID, string(request.ConnectionID))]
	if !exists {
		return contracts.DownloaderBinding{}, fmt.Errorf("no session for provider %s connection %s",
			request.ProviderID, request.ConnectionID)
	}
	return binding, nil
}

// setBinding, clearBindings, and setError mutate the double under the same lock so
// tests can drive it while executions may be in flight.
func (session *sessionDouble) setBinding(key sessionKey, binding contracts.DownloaderBinding) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.bindings == nil {
		session.bindings = map[sessionKey]contracts.DownloaderBinding{}
	}
	session.bindings[key] = binding
}

func (session *sessionDouble) clearBindings() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.bindings = map[sessionKey]contracts.DownloaderBinding{}
}

func (session *sessionDouble) setError(err error) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.err = err
}

func (session *sessionDouble) resolutionCount() int {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.calls
}

func (session *sessionDouble) recordedRequest() acquisition.DownloaderSessionRequest {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.lastRequest
}

// The doubles below must satisfy the same ports the real implementations do.
var (
	_ acquisition.DownloaderSessionResolver = (*sessionDouble)(nil)
	_ acquisition.JobReader                 = (*jobReaderDouble)(nil)
	_ acquisition.ManifestReader            = (*manifestReaderDouble)(nil)
	_ acquisition.StorageTopologyReader     = (*topologyDouble)(nil)
	_ acquisition.ProviderTaskStore         = (*taskStoreDouble)(nil)
)

// ---------------------------------------------------------------------------
// fixtures
// ---------------------------------------------------------------------------

const (
	executionProviderID    contracts.ProviderID = "gate0-memory"
	executionBindingID                          = storage.BindingID("b0000000-0000-4000-8000-000000000001")
	executionConnID                             = storage.ConnectionID("c0000000-0000-4000-8000-000000000001")
	executionManifestID                         = acquisition.ManifestID("b0000000-0000-4000-8000-000000000010")
	executionJobID                              = jobs.JobID("b0000000-0000-4000-8000-000000000020")
	executionOwner                              = "worker-1"
	executionAttempt                            = 2
	executionCredentialRef                      = "secret-ref://connections/gate0"
)

func jobIDPointer(id jobs.JobID) *jobs.JobID { return &id }

func stringPointer(value string) *string { return &value }

func executionTopology(scope string) *topologyDouble {
	return &topologyDouble{
		bindings: map[storage.BindingID]storage.Binding{
			executionBindingID: {
				ID: executionBindingID, ConnectionID: executionConnID, ProviderScope: &scope,
				OpenListMountPath: "/openlist/mount", IndexCoreRootID: "root-1", Status: storage.BindingStatusActive,
			},
		},
		connections: map[storage.ConnectionID]storage.Connection{
			executionConnID: {
				ID: executionConnID, ProviderType: string(executionProviderID), Status: storage.ConnectionStatusActive,
				CredentialRef: stringPointer(executionCredentialRef),
			},
		},
	}
}

func executionManifest() acquisition.Manifest {
	return acquisition.Manifest{
		ID: executionManifestID, SourceType: "opaque-source", SourceRef: "opaque-ref",
		TargetStorageBindingID: executionBindingID, TargetPath: "/known/artifact.bin",
		JobID: jobIDPointer(executionJobID), State: acquisition.StateActive,
		CreatedAt: executionNow, UpdatedAt: executionNow,
	}
}

func executionPayload(manifestID acquisition.ManifestID) []byte {
	payload, err := json.Marshal(struct {
		SchemaVersion int                    `json:"schema_version"`
		ManifestID    acquisition.ManifestID `json:"manifest_id"`
	}{SchemaVersion: acquisition.AcquisitionPayloadSchemaVersion, ManifestID: manifestID})
	if err != nil {
		panic(err)
	}
	return payload
}

// executionJob is a durable RUNNING ACQUISITION Job whose lease matches the
// default execution request.
func executionJob() jobs.Job {
	key := acquisition.AcquisitionJobIdempotencyKey(executionManifestID)
	leaseExpiry := executionNow.Add(time.Hour)
	return jobs.Job{
		ID: executionJobID, Type: acquisition.JobTypeAcquisition, Payload: executionPayload(executionManifestID),
		State: jobs.StateRunning, IdempotencyKey: &key, ClaimAttempts: executionAttempt, FailureCount: 0, MaxAttempts: 5,
		LeaseOwner: stringPointer(executionOwner), LeaseExpiresAt: &leaseExpiry,
		CreatedAt: executionNow, UpdatedAt: executionNow,
	}
}

func executionRequest() acquisition.StepRequest {
	return acquisition.StepRequest{JobID: executionJobID, Owner: executionOwner, ClaimAttempt: executionAttempt}
}

type executionFixture struct {
	service   *acquisition.ExecutionStepService
	manifests *manifestReaderDouble
	jobsvc    *jobReaderDouble
	tasks     *taskStoreDouble
	provider  *scriptedProvider
	sessions  *sessionDouble
	request   acquisition.StepRequest
}

// newExecutionFixture builds a fully valid RUNNING ACQUISITION execution
// fixture. Individual tests mutate one dimension to prove fail-closed behavior.
func newExecutionFixture(t *testing.T) *executionFixture {
	t.Helper()
	manifests := &manifestReaderDouble{values: map[acquisition.ManifestID]acquisition.Manifest{
		executionManifestID: executionManifest(),
	}}
	jobsvc := &jobReaderDouble{values: map[jobs.JobID]jobs.Job{executionJobID: executionJob()}}
	tasks := newTaskStoreDouble()
	provider := newScriptedProvider(executionProviderID)
	sessions := &sessionDouble{bindings: map[sessionKey]contracts.DownloaderBinding{
		newSessionKey(executionProviderID, string(executionConnID)): {
			Descriptor: provider.Descriptor(), Downloader: provider,
		},
	}}

	resolver, err := acquisition.NewExecutionInputResolver(manifests, executionTopology("library"))
	if err != nil {
		t.Fatalf("NewExecutionInputResolver() error = %v", err)
	}
	service := newExecutionService(t, resolver, manifests, jobsvc, sessions, tasks)
	return &executionFixture{
		service: service, manifests: manifests, jobsvc: jobsvc, tasks: tasks, provider: provider, sessions: sessions,
		request: executionRequest(),
	}
}

func newExecutionService(
	t *testing.T,
	resolver *acquisition.ExecutionInputResolver,
	manifests *manifestReaderDouble,
	jobsvc *jobReaderDouble,
	sessions acquisition.DownloaderSessionResolver,
	tasks acquisition.ProviderTaskStore,
) *acquisition.ExecutionStepService {
	t.Helper()
	service, err := acquisition.NewExecutionStepService(
		resolver, manifests, jobsvc, sessions, tasks,
		acquisition.WithExecutionClock(func() time.Time { return executionNow }),
	)
	if err != nil {
		t.Fatalf("NewExecutionStepService() error = %v", err)
	}
	return service
}

func (fixture *executionFixture) job() jobs.Job { return fixture.jobsvc.values[executionJobID] }

func (fixture *executionFixture) durableTask(t *testing.T) acquisition.ProviderTask {
	t.Helper()
	task, err := fixture.tasks.GetProviderTask(context.Background(), executionManifestID)
	if err != nil {
		t.Fatalf("durable provider task = %v", err)
	}
	return task
}

// ---------------------------------------------------------------------------
// tests 1-2: Job and Manifest linkage
// ---------------------------------------------------------------------------

func TestExecutionStepRejectsNonAcquisitionJob(t *testing.T) {
	for _, jobType := range []string{"INDEXCORE_REFRESH", ""} {
		t.Run("type="+jobType, func(t *testing.T) {
			fixture := newExecutionFixture(t)
			job := fixture.job()
			job.Type = jobType
			fixture.jobsvc.values[executionJobID] = job

			if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrExecutionJobMismatch) {
				t.Fatalf("Execute() error = %v, want ErrExecutionJobMismatch", err)
			}
			if start, status, _ := fixture.provider.counts(); start != 0 || status != 0 {
				t.Fatalf("provider calls start=%d status=%d, want 0/0", start, status)
			}
			if fixture.tasks.claimCalls != 0 {
				t.Fatalf("claim calls = %d, want 0", fixture.tasks.claimCalls)
			}
		})
	}
}

func TestExecutionStepRejectsJobManifestLinkageMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*executionFixture)
	}{
		{name: "payload points at another Manifest", mutate: func(fixture *executionFixture) {
			job := fixture.job()
			job.Payload = executionPayload(acquisition.ManifestID("b0000000-0000-4000-8000-0000000000ff"))
			fixture.jobsvc.values[executionJobID] = job
		}},
		{name: "Manifest is not linked to the Job", mutate: func(fixture *executionFixture) {
			manifest := fixture.manifests.values[executionManifestID]
			other := jobs.JobID("b0000000-0000-4000-8000-0000000000ff")
			manifest.JobID = &other
			fixture.manifests.values[executionManifestID] = manifest
		}},
		{name: "Manifest has no Job link", mutate: func(fixture *executionFixture) {
			manifest := fixture.manifests.values[executionManifestID]
			manifest.JobID = nil
			fixture.manifests.values[executionManifestID] = manifest
		}},
		{name: "idempotency key does not match Manifest", mutate: func(fixture *executionFixture) {
			job := fixture.job()
			other := acquisition.AcquisitionJobIdempotencyKey("b0000000-0000-4000-8000-0000000000ff")
			job.IdempotencyKey = &other
			fixture.jobsvc.values[executionJobID] = job
		}},
		{name: "payload schema version drift", mutate: func(fixture *executionFixture) {
			payload, err := json.Marshal(struct {
				SchemaVersion int                    `json:"schema_version"`
				ManifestID    acquisition.ManifestID `json:"manifest_id"`
			}{SchemaVersion: 99, ManifestID: executionManifestID})
			if err != nil {
				t.Fatalf("encode payload: %v", err)
			}
			job := fixture.job()
			job.Payload = payload
			fixture.jobsvc.values[executionJobID] = job
		}},
		{name: "payload rejects unknown fields", mutate: func(fixture *executionFixture) {
			job := fixture.job()
			job.Payload = []byte(`{"schema_version":1,"manifest_id":"` + string(executionManifestID) + `","extra":true}`)
			fixture.jobsvc.values[executionJobID] = job
		}},
		{name: "duplicate payload document", mutate: func(fixture *executionFixture) {
			job := fixture.job()
			job.Payload = []byte(`{"schema_version":1,"manifest_id":"` + string(executionManifestID) + `"}{}`)
			fixture.jobsvc.values[executionJobID] = job
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutionFixture(t)
			test.mutate(fixture)
			if _, err := fixture.service.Execute(context.Background(), fixture.request); err == nil {
				t.Fatal("Execute() succeeded, want fail-closed linkage error")
			}
			if start, status, _ := fixture.provider.counts(); start != 0 || status != 0 {
				t.Fatalf("provider calls start=%d status=%d, want 0/0", start, status)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// fenced Job identity
// ---------------------------------------------------------------------------

func TestExecutionStepRequiresFencedRunningLease(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*executionFixture) *acquisition.StepRequest
	}{
		{name: "Job is QUEUED", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			job := fixture.job()
			job.State = jobs.StateQueued
			job.LeaseOwner = nil
			job.LeaseExpiresAt = nil
			fixture.jobsvc.values[executionJobID] = job
			return &fixture.request
		}},
		{name: "Job is SUCCEEDED", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			job := fixture.job()
			job.State = jobs.StateSucceeded
			job.LeaseOwner = nil
			job.LeaseExpiresAt = nil
			fixture.jobsvc.values[executionJobID] = job
			return &fixture.request
		}},
		{name: "Job is RECOVERY_REQUIRED", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			job := fixture.job()
			job.State = jobs.StateRecoveryRequired
			job.LeaseOwner = nil
			job.LeaseExpiresAt = nil
			fixture.jobsvc.values[executionJobID] = job
			return &fixture.request
		}},
		{name: "foreign lease owner", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			request := fixture.request
			request.Owner = "worker-2"
			return &request
		}},
		{name: "stale attempt", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			request := fixture.request
			request.ClaimAttempt = executionAttempt + 1
			return &request
		}},
		{name: "expired lease", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			job := fixture.job()
			expired := executionNow.Add(-time.Minute)
			job.LeaseExpiresAt = &expired
			fixture.jobsvc.values[executionJobID] = job
			return &fixture.request
		}},
		{name: "missing lease expiry", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			job := fixture.job()
			job.LeaseExpiresAt = nil
			fixture.jobsvc.values[executionJobID] = job
			return &fixture.request
		}},
		{name: "missing owner", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			request := fixture.request
			request.Owner = ""
			return &request
		}},
		{name: "zero attempt", mutate: func(fixture *executionFixture) *acquisition.StepRequest {
			request := fixture.request
			request.ClaimAttempt = 0
			return &request
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutionFixture(t)
			request := test.mutate(fixture)
			if _, err := fixture.service.Execute(context.Background(), *request); err == nil {
				t.Fatal("Execute() succeeded, want fail-closed lease error")
			}
			if start, status, _ := fixture.provider.counts(); start != 0 || status != 0 {
				t.Fatalf("provider calls start=%d status=%d, want 0/0", start, status)
			}
			if fixture.tasks.claimCalls != 0 {
				t.Fatalf("claim calls = %d, want 0", fixture.tasks.claimCalls)
			}
		})
	}
}

func TestExecutionStepRejectsMissingJobOrManifest(t *testing.T) {
	t.Run("missing Job", func(t *testing.T) {
		fixture := newExecutionFixture(t)
		request := fixture.request
		request.JobID = jobs.JobID("b0000000-0000-4000-8000-00000000ffff")
		if _, err := fixture.service.Execute(context.Background(), request); !errors.Is(err, acquisition.ErrExecutionJobMismatch) {
			t.Fatalf("Execute() error = %v, want ErrExecutionJobMismatch", err)
		}
	})
	t.Run("missing Manifest", func(t *testing.T) {
		fixture := newExecutionFixture(t)
		delete(fixture.manifests.values, executionManifestID)
		if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrExecutionManifestNotFound) {
			t.Fatalf("Execute() error = %v, want ErrExecutionManifestNotFound", err)
		}
	})
}

// ---------------------------------------------------------------------------
// tests 1, 7-8: connection-scoped downloader session and capability
// ---------------------------------------------------------------------------

func TestExecutionStepRejectsProviderWithoutDownloaderCapability(t *testing.T) {
	fixture := newExecutionFixture(t)
	storageOnly := contracts.Descriptor{
		ID: executionProviderID, DisplayName: "storage only", Capabilities: contracts.CapabilitySet{Storage: true},
	}
	fixture.sessions.setBinding(newSessionKey(executionProviderID, string(executionConnID)),
		contracts.DownloaderBinding{Descriptor: storageOnly})

	if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrProviderNotDownloader) {
		t.Fatalf("Execute() error = %v, want ErrProviderNotDownloader", err)
	}
	if start, _, _ := fixture.provider.counts(); start != 0 {
		t.Fatalf("StartDownload calls = %d, want 0", start)
	}
}

func TestExecutionStepRejectsDescriptorIdentityMismatch(t *testing.T) {
	fixture := newExecutionFixture(t)
	mismatched := newScriptedProvider("other-provider")
	fixture.sessions.setBinding(newSessionKey(executionProviderID, string(executionConnID)),
		contracts.DownloaderBinding{Descriptor: mismatched.Descriptor(), Downloader: mismatched})

	if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrProviderIdentityMismatch) {
		t.Fatalf("Execute() error = %v, want ErrProviderIdentityMismatch", err)
	}
	if start, _, _ := mismatched.counts(); start != 0 {
		t.Fatalf("StartDownload calls = %d, want 0", start)
	}
}

// ---------------------------------------------------------------------------
// tests 5-7: start once, persist, then poll exactly
// ---------------------------------------------------------------------------

func TestExecutionStepStartsDownloadOnceAndPersistsOpaqueReference(t *testing.T) {
	fixture := newExecutionFixture(t)
	opaque := "  provider://task//opaque?token=%2F  "
	fixture.provider.startReference = contracts.TaskReference{Value: opaque}
	fixture.provider.setState(opaque, contracts.TaskStateRunning)

	outcome, err := fixture.service.Execute(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if outcome != acquisition.OutcomeProviderInProgress {
		t.Fatalf("Execute() outcome = %q, want %q", outcome, acquisition.OutcomeProviderInProgress)
	}
	start, status, refs := fixture.provider.counts()
	if start != 1 {
		t.Fatalf("StartDownload calls = %d, want exactly 1", start)
	}
	if status != 1 || len(refs) != 1 || refs[0] != opaque {
		t.Fatalf("DownloadStatus calls = %d refs = %q, want exactly the stored opaque reference %q", status, refs, opaque)
	}
	stored := fixture.durableTask(t)
	if !stored.ReferenceKnown() || stored.ProviderTaskRef != opaque {
		t.Fatalf("durable provider task = %#v, want REFERENCE_KNOWN with exact opaque %q", stored, opaque)
	}
	if stored.JobID != executionJobID || stored.ProviderID != executionProviderID {
		t.Fatalf("stored provider task = %#v", stored)
	}
	fixture.provider.mu.Lock()
	requests := append([]contracts.DownloadRequest(nil), fixture.provider.startRequests...)
	fixture.provider.mu.Unlock()
	if len(requests) != 1 || requests[0].Source.Value != "opaque-ref" || requests[0].Target.Path != "/known/artifact.bin" ||
		requests[0].Target.Scope != "library" {
		t.Fatalf("StartDownload request = %#v", requests)
	}
}

func TestExecutionStepReplayNeverStartsDownloadAgain(t *testing.T) {
	fixture := newExecutionFixture(t)
	if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	fixture.provider.setState("provider-task-0001", contracts.TaskStateRunning)

	for attempt := 0; attempt < 3; attempt++ {
		if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
			t.Fatalf("replay Execute() error = %v", err)
		}
	}
	start, status, refs := fixture.provider.counts()
	if start != 1 {
		t.Fatalf("StartDownload calls = %d, want exactly 1 across replays", start)
	}
	if status != 4 {
		t.Fatalf("DownloadStatus calls = %d, want 4", status)
	}
	for index, ref := range refs {
		if ref != "provider-task-0001" {
			t.Fatalf("DownloadStatus ref[%d] = %q, want the exact stored reference", index, ref)
		}
	}
}

// ---------------------------------------------------------------------------
// tests 8-12: outcome mapping and provider state validation
// ---------------------------------------------------------------------------

func TestExecutionStepMapsProviderTaskStates(t *testing.T) {
	tests := []struct {
		state contracts.TaskState
		want  acquisition.StepOutcome
	}{
		{state: contracts.TaskStatePending, want: acquisition.OutcomeProviderInProgress},
		{state: contracts.TaskStateRunning, want: acquisition.OutcomeProviderInProgress},
		{state: contracts.TaskStateSucceeded, want: acquisition.OutcomeProviderSucceeded},
		{state: contracts.TaskStateFailed, want: acquisition.OutcomeProviderFailed},
		{state: contracts.TaskStateCanceled, want: acquisition.OutcomeProviderCanceled},
	}
	for _, test := range tests {
		t.Run(string(test.state), func(t *testing.T) {
			fixture := newExecutionFixture(t)
			if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
				t.Fatalf("first Execute() error = %v", err)
			}
			fixture.provider.setState("provider-task-0001", test.state)

			outcome, err := fixture.service.Execute(context.Background(), fixture.request)
			if err != nil {
				t.Fatalf("replay Execute() error = %v", err)
			}
			if outcome != test.want {
				t.Fatalf("outcome = %q, want %q", outcome, test.want)
			}
			if string(outcome) == string(acquisition.StateReady) {
				t.Fatal("provider success must never map to Manifest READY")
			}
		})
	}
}

func TestExecutionStepRejectsInvalidProviderTaskReference(t *testing.T) {
	for _, reference := range []string{"", "   ", "task\x00ref", strings.Repeat("x", acquisition.MaxProviderTaskRefLength+1)} {
		t.Run("ref="+reference, func(t *testing.T) {
			fixture := newExecutionFixture(t)
			fixture.provider.startReference = contracts.TaskReference{Value: reference}

			if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrProviderTaskReference) {
				t.Fatalf("Execute() error = %v, want ErrProviderTaskReference", err)
			}
			// The start reservation survives, so the external side effect can
			// never be repeated even though the reference was unusable.
			stored := fixture.durableTask(t)
			if stored.State != acquisition.ProviderTaskStartReserved || stored.ProviderTaskRef != "" {
				t.Fatalf("durable task = %#v, want an uncommitted START_RESERVED fence", stored)
			}
			if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrExecutionSideEffectUncertain) {
				t.Fatalf("second Execute() error = %v, want ErrExecutionSideEffectUncertain", err)
			}
			if start, _, _ := fixture.provider.counts(); start != 1 {
				t.Fatalf("StartDownload calls = %d, want exactly 1", start)
			}
		})
	}
}

func TestExecutionStepRejectsUnknownProviderTaskState(t *testing.T) {
	fixture := newExecutionFixture(t)
	if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	fixture.provider.setState("provider-task-0001", contracts.TaskState("SOMETHING_NEW"))

	if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrProviderTaskState) {
		t.Fatalf("Execute() error = %v, want ErrProviderTaskState", err)
	}
}

func TestExecutionStepFailsClosedOnDurableTaskIdentityMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*acquisition.ProviderTask)
	}{
		{name: "different provider", mutate: func(task *acquisition.ProviderTask) { task.ProviderID = "other-provider" }},
		{name: "different Job", mutate: func(task *acquisition.ProviderTask) {
			task.JobID = jobs.JobID("b0000000-0000-4000-8000-0000000000ff")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newExecutionFixture(t)
			if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
				t.Fatalf("first Execute() error = %v", err)
			}
			stored := fixture.tasks.values[executionManifestID]
			test.mutate(&stored)
			fixture.tasks.values[executionManifestID] = stored

			if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrExecutionIdentityMismatch) {
				t.Fatalf("Execute() error = %v, want ErrExecutionIdentityMismatch", err)
			}
			start, _, _ := fixture.provider.counts()
			if start != 1 {
				t.Fatalf("StartDownload calls = %d, want 1 (no recreation)", start)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// tests 15-16: uncertain external side effect and the durable fence
// ---------------------------------------------------------------------------

func TestExecutionStepReportsUncertainSideEffectWhenReferenceCannotCommit(t *testing.T) {
	fixture := newExecutionFixture(t)
	fixture.tasks.failClaimCommit = errors.New("injected reference commit failure")

	outcome, err := fixture.service.Execute(context.Background(), fixture.request)
	if !errors.Is(err, acquisition.ErrExecutionSideEffectUncertain) {
		t.Fatalf("Execute() error = %v, want ErrExecutionSideEffectUncertain", err)
	}
	if outcome != "" {
		t.Fatalf("outcome = %q, want empty on uncertain side effect", outcome)
	}
	start, status, _ := fixture.provider.counts()
	if start != 1 {
		t.Fatalf("StartDownload calls = %d, want exactly 1", start)
	}
	if status != 0 {
		t.Fatalf("DownloadStatus calls = %d, want 0 because no reference was durably known", status)
	}
	// The fence is durable even though the reference was lost.
	stored := fixture.durableTask(t)
	if stored.State != acquisition.ProviderTaskStartReserved || stored.ProviderTaskRef != "" {
		t.Fatalf("durable task = %#v, want START_RESERVED with no reference", stored)
	}
}

// TestExecutionStepUncertainFenceForbidsAnyFurtherStartDownload is the Round 2
// blocker evidence: after the external side effect succeeded but its reference
// could not be committed, no later execution in the same process may start
// another provider task.
func TestExecutionStepUncertainFenceForbidsAnyFurtherStartDownload(t *testing.T) {
	fixture := newExecutionFixture(t)
	fixture.tasks.failClaimCommit = errors.New("injected reference commit failure")

	for attempt := 0; attempt < 4; attempt++ {
		outcome, err := fixture.service.Execute(context.Background(), fixture.request)
		if !errors.Is(err, acquisition.ErrExecutionSideEffectUncertain) {
			t.Fatalf("attempt %d error = %v, want ErrExecutionSideEffectUncertain", attempt, err)
		}
		if outcome != "" {
			t.Fatalf("attempt %d outcome = %q, want empty", attempt, outcome)
		}
		start, _, _ := fixture.provider.counts()
		if start != 1 {
			t.Fatalf("after attempt %d StartDownload calls = %d, want exactly 1", attempt, start)
		}
	}
}

// TestExecutionStepUncertainFenceSurvivesRepositoryReconstruction proves the
// fence is durable state rather than process memory: rebuilding the whole service
// against the same store still refuses to start a second provider task.
func TestExecutionStepUncertainFenceSurvivesRepositoryReconstruction(t *testing.T) {
	fixture := newExecutionFixture(t)
	fixture.tasks.failClaimCommit = errors.New("injected reference commit failure")
	if _, err := fixture.service.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrExecutionSideEffectUncertain) {
		t.Fatalf("initial Execute() error = %v", err)
	}

	// Reconstruct: new resolver, new service, same durable store.
	resolver, err := acquisition.NewExecutionInputResolver(fixture.manifests, executionTopology("library"))
	if err != nil {
		t.Fatalf("NewExecutionInputResolver() error = %v", err)
	}
	restarted := newExecutionService(t, resolver, fixture.manifests, fixture.jobsvc, fixture.sessions, fixture.tasks)
	// Even with persistence healthy again, the reservation forbids a new start.
	fixture.tasks.failClaimCommit = nil

	for attempt := 0; attempt < 3; attempt++ {
		if _, err := restarted.Execute(context.Background(), fixture.request); !errors.Is(err, acquisition.ErrExecutionSideEffectUncertain) {
			t.Fatalf("restarted attempt %d error = %v, want ErrExecutionSideEffectUncertain", attempt, err)
		}
	}
	if start, status, _ := fixture.provider.counts(); start != 1 || status != 0 {
		t.Fatalf("restarted provider calls start=%d status=%d, want exactly one start and no poll", start, status)
	}
	stored := fixture.durableTask(t)
	if stored.State != acquisition.ProviderTaskStartReserved {
		t.Fatalf("durable task state = %q, want START_RESERVED", stored.State)
	}
}

// TestExecutionStepRejectsReferenceCommitWithoutReservation fails closed when a
// caller tries to commit a reference for a Manifest that holds no reservation.
func TestExecutionStepRejectsReferenceCommitWithoutReservation(t *testing.T) {
	store := newTaskStoreDouble()
	request := acquisition.ProviderTaskClaimRequest{
		ManifestID: executionManifestID, JobID: executionJobID, ProviderID: executionProviderID,
		Reference: "provider-task-0001", Now: executionNow,
	}
	if _, err := store.ClaimProviderTask(context.Background(), request); !errors.Is(err, acquisition.ErrInvalidProviderTask) {
		t.Fatalf("reference commit without reservation error = %v, want ErrInvalidProviderTask", err)
	}
}

func TestExecutionStepProviderStartFailureIsAttributedAndNotPersisted(t *testing.T) {
	fixture := newExecutionFixture(t)
	fixture.provider.startError = errors.New("provider transport exploded")

	_, err := fixture.service.Execute(context.Background(), fixture.request)
	if err == nil {
		t.Fatal("Execute() succeeded, want attributed provider failure")
	}
	if errors.Is(err, acquisition.ErrExecutionSideEffectUncertain) {
		t.Fatalf("pre-side-effect failure must not be uncertain: %v", err)
	}
	if !strings.Contains(err.Error(), "provider transport exploded") {
		t.Fatalf("error %v does not attribute the provider failure", err)
	}
	stored := fixture.durableTask(t)
	if stored.State != acquisition.ProviderTaskStartReserved {
		t.Fatalf("durable state = %q, want START_RESERVED", stored.State)
	}
}

// TestExecutionStepDefiniteStartFailureKeepsTheFence documents a deliberate
// conservative consequence: because a failed StartDownload cannot be proven to
// have had no external effect, the reservation stays and every later execution
// fails closed. The provider is still called exactly once, and recovery is an
// explicit operator decision rather than an automatic retry.
func TestExecutionStepDefiniteStartFailureKeepsTheFence(t *testing.T) {
	fixture := newExecutionFixture(t)
	fixture.provider.startError = errors.New("provider transport exploded")

	for attempt := 0; attempt < 3; attempt++ {
		if _, err := fixture.service.Execute(context.Background(), fixture.request); err == nil {
			t.Fatalf("attempt %d succeeded, want an attributed provider failure", attempt)
		}
		// The provider keeps failing, and the fence keeps the call count at one:
		// a later execution is never authorized to start another task.
		start, status, _ := fixture.provider.counts()
		if start != 1 {
			t.Fatalf("after attempt %d StartDownload calls = %d, want exactly 1", attempt, start)
		}
		if status != 0 {
			t.Fatalf("after attempt %d DownloadStatus calls = %d, want 0", attempt, status)
		}
	}
	stored := fixture.durableTask(t)
	if stored.State != acquisition.ProviderTaskStartReserved || stored.ProviderTaskRef != "" {
		t.Fatalf("durable task = %#v, want an uncommitted START_RESERVED fence", stored)
	}
}

// TestExecutionStepFenceContentionFailsClosed proves that losing the durable claim
// race is reported as a persistence failure and never authorizes a provider start.
func TestExecutionStepFenceContentionFailsClosed(t *testing.T) {
	fixture := newExecutionFixture(t)
	fixture.tasks.failClaimStart = fmt.Errorf("%w: lock timeout", acquisition.ErrProviderTaskContention)

	_, err := fixture.service.Execute(context.Background(), fixture.request)
	if !errors.Is(err, acquisition.ErrProviderTaskPersistence) {
		t.Fatalf("Execute() error = %v, want ErrProviderTaskPersistence", err)
	}
	if !errors.Is(err, acquisition.ErrProviderTaskContention) {
		t.Fatalf("Execute() error = %v, want the underlying contention cause preserved", err)
	}
	if start, status, _ := fixture.provider.counts(); start != 0 || status != 0 {
		t.Fatalf("provider calls start=%d status=%d, want 0/0", start, status)
	}
	if _, err := fixture.tasks.GetProviderTask(context.Background(), executionManifestID); !errors.Is(err, acquisition.ErrProviderTaskNotFound) {
		t.Fatalf("durable task = %v, want not found after a lost claim race", err)
	}
}

// ---------------------------------------------------------------------------
// concurrency: at most one StartDownload per Manifest
// ---------------------------------------------------------------------------

func TestExecutionStepConcurrentExecutionsStartAtMostOneProviderTask(t *testing.T) {
	const workers = 8
	fixture := newExecutionFixture(t)
	fixture.provider.startReference = contracts.TaskReference{Value: "provider-task-0001"}
	fixture.provider.setState("provider-task-0001", contracts.TaskStateRunning)

	var waitGroup sync.WaitGroup
	outcomes := make([]acquisition.StepOutcome, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			<-start
			outcomes[index], errs[index] = fixture.service.Execute(context.Background(), executionRequest())
		}(worker)
	}
	close(start)
	waitGroup.Wait()

	startCalls, statusCalls, refs := fixture.provider.counts()
	if startCalls != 1 {
		t.Fatalf("StartDownload calls = %d across %d concurrent executions, want exactly 1", startCalls, workers)
	}
	// Every execution that observes the committed reference may poll it; what
	// matters is that all polls use the single committed reference.
	if statusCalls < 1 || statusCalls > workers {
		t.Fatalf("DownloadStatus calls = %d, want between 1 and %d", statusCalls, workers)
	}
	if len(refs) != statusCalls {
		t.Fatalf("polled references = %q, want %d entries", refs, statusCalls)
	}
	for index, ref := range refs {
		if ref != "provider-task-0001" {
			t.Fatalf("polled reference[%d] = %q, want the single committed reference", index, ref)
		}
	}

	var inProgress, uncertain int
	for index := 0; index < workers; index++ {
		if errs[index] == nil {
			if outcomes[index] != acquisition.OutcomeProviderInProgress {
				t.Fatalf("worker %d outcome = %q, want PROVIDER_IN_PROGRESS", index, outcomes[index])
			}
			inProgress++
			continue
		}
		if !errors.Is(errs[index], acquisition.ErrExecutionSideEffectUncertain) {
			t.Fatalf("worker %d error = %v, want nil or ErrExecutionSideEffectUncertain", index, errs[index])
		}
		if outcomes[index] != "" {
			t.Fatalf("worker %d uncertain outcome = %q, want empty", index, outcomes[index])
		}
		uncertain++
	}
	if inProgress+uncertain != workers {
		t.Fatalf("in_progress=%d uncertain=%d, want %d total", inProgress, uncertain, workers)
	}
	if inProgress < 1 {
		t.Fatalf("workers observing the committed reference = %d, want at least 1", inProgress)
	}
	stored := fixture.durableTask(t)
	if !stored.ReferenceKnown() || stored.ProviderTaskRef != "provider-task-0001" {
		t.Fatalf("durable task = %#v, want the single committed reference", stored)
	}
}

// TestExecutionStepConcurrentReservationLosersFailClosed covers the window where
// the winner has reserved the start but has not yet committed a reference.
func TestExecutionStepConcurrentReservationLosersFailClosed(t *testing.T) {
	const workers = 8
	fixture := newExecutionFixture(t)
	fixture.tasks.failClaimCommit = errors.New("injected reference commit failure")

	var waitGroup sync.WaitGroup
	errs := make([]error, workers)
	start := make(chan struct{})
	for worker := 0; worker < workers; worker++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			<-start
			_, errs[index] = fixture.service.Execute(context.Background(), executionRequest())
		}(worker)
	}
	close(start)
	waitGroup.Wait()

	if startCalls, _, _ := fixture.provider.counts(); startCalls != 1 {
		t.Fatalf("StartDownload calls = %d across %d concurrent executions, want exactly 1", startCalls, workers)
	}
	for index, err := range errs {
		if !errors.Is(err, acquisition.ErrExecutionSideEffectUncertain) {
			t.Fatalf("worker %d error = %v, want ErrExecutionSideEffectUncertain", index, err)
		}
	}
	stored := fixture.durableTask(t)
	if stored.State != acquisition.ProviderTaskStartReserved || stored.ProviderTaskRef != "" {
		t.Fatalf("durable task = %#v, want STOPPED at START_RESERVED", stored)
	}
}

// ---------------------------------------------------------------------------
// tests 13-14: restart safety
// ---------------------------------------------------------------------------

func TestExecutionStepSurvivesRestartAndPollsExistingTask(t *testing.T) {
	fixture := newExecutionFixture(t)
	topology := executionTopology("library")
	if _, err := fixture.service.Execute(context.Background(), fixture.request); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	resolver, err := acquisition.NewExecutionInputResolver(fixture.manifests, topology)
	if err != nil {
		t.Fatalf("NewExecutionInputResolver() error = %v", err)
	}
	restarted := newExecutionService(t, resolver, fixture.manifests, fixture.jobsvc, fixture.sessions, fixture.tasks)
	fixture.provider.setState("provider-task-0001", contracts.TaskStateSucceeded)

	outcome, err := restarted.Execute(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("restarted Execute() error = %v", err)
	}
	if outcome != acquisition.OutcomeProviderSucceeded {
		t.Fatalf("restarted outcome = %q, want %q", outcome, acquisition.OutcomeProviderSucceeded)
	}
	start, status, refs := fixture.provider.counts()
	if start != 1 {
		t.Fatalf("StartDownload calls = %d, want exactly 1 after restart", start)
	}
	if status != 2 || refs[1] != "provider-task-0001" {
		t.Fatalf("DownloadStatus calls = %d refs = %q, want the exact stored reference", status, refs)
	}
}

// TestExecutionStepRestartWithFreshProviderPollsStoredReference proves the
// durable linkage alone is enough to resume after a real process restart: a
// brand-new provider instance has no in-memory task table, so only the stored
// reference lets the step succeed without starting a second provider task.
func TestExecutionStepRestartWithFreshProviderPollsStoredReference(t *testing.T) {
	startupProvider := newScriptedProvider(executionProviderID)
	manifests := &manifestReaderDouble{values: map[acquisition.ManifestID]acquisition.Manifest{
		executionManifestID: executionManifest(),
	}}
	jobsvc := &jobReaderDouble{values: map[jobs.JobID]jobs.Job{executionJobID: executionJob()}}
	tasks := newTaskStoreDouble()
	topology := executionTopology("library")
	resolver, err := acquisition.NewExecutionInputResolver(manifests, topology)
	if err != nil {
		t.Fatalf("NewExecutionInputResolver() error = %v", err)
	}
	startupSessions := &sessionDouble{bindings: map[sessionKey]contracts.DownloaderBinding{
		newSessionKey(executionProviderID, string(executionConnID)): {Descriptor: startupProvider.Descriptor(), Downloader: startupProvider},
	}}
	startup := newExecutionService(t, resolver, manifests, jobsvc, startupSessions, tasks)
	if _, err := startup.Execute(context.Background(), executionRequest()); err != nil {
		t.Fatalf("startup Execute() error = %v", err)
	}
	stored, err := tasks.GetProviderTask(context.Background(), executionManifestID)
	if err != nil {
		t.Fatalf("durable provider task after startup execution = %v", err)
	}
	if !stored.ReferenceKnown() {
		t.Fatalf("durable provider task = %#v, want a known reference", stored)
	}

	// Restart: a brand-new provider that only knows the durable reference.
	restartedProvider := newScriptedProvider(executionProviderID)
	restartedProvider.adoptTask(stored.ProviderTaskRef, contracts.TaskStateSucceeded)
	restartedSessions := &sessionDouble{bindings: map[sessionKey]contracts.DownloaderBinding{
		newSessionKey(executionProviderID, string(executionConnID)): {Descriptor: restartedProvider.Descriptor(), Downloader: restartedProvider},
	}}
	restarted := newExecutionService(t, resolver, manifests, jobsvc, restartedSessions, tasks)
	outcome, err := restarted.Execute(context.Background(), executionRequest())
	if err != nil {
		t.Fatalf("restarted Execute() error = %v", err)
	}
	if outcome != acquisition.OutcomeProviderSucceeded {
		t.Fatalf("restarted outcome = %q, want %q", outcome, acquisition.OutcomeProviderSucceeded)
	}
	if start, status, refs := restartedProvider.counts(); start != 0 || status != 1 || refs[0] != stored.ProviderTaskRef {
		t.Fatalf("restarted provider calls start=%d status=%d refs=%q, want no StartDownload and one poll of %q",
			start, status, refs, stored.ProviderTaskRef)
	}
}

func TestExecutionStepNilDependenciesRejected(t *testing.T) {
	fixture := newExecutionFixture(t)
	resolver, err := acquisition.NewExecutionInputResolver(fixture.manifests, executionTopology("library"))
	if err != nil {
		t.Fatalf("NewExecutionInputResolver() error = %v", err)
	}
	cases := []struct {
		name string
		call func() error
	}{
		{name: "nil resolver", call: func() error {
			_, err := acquisition.NewExecutionStepService(nil, fixture.manifests, fixture.jobsvc, fixture.sessions, fixture.tasks)
			return err
		}},
		{name: "nil manifests", call: func() error {
			_, err := acquisition.NewExecutionStepService(resolver, nil, fixture.jobsvc, fixture.sessions, fixture.tasks)
			return err
		}},
		{name: "nil jobs", call: func() error {
			_, err := acquisition.NewExecutionStepService(resolver, fixture.manifests, nil, fixture.sessions, fixture.tasks)
			return err
		}},
		{name: "nil sessions", call: func() error {
			_, err := acquisition.NewExecutionStepService(resolver, fixture.manifests, fixture.jobsvc, nil, fixture.tasks)
			return err
		}},
		{name: "nil tasks", call: func() error {
			_, err := acquisition.NewExecutionStepService(resolver, fixture.manifests, fixture.jobsvc, fixture.sessions, nil)
			return err
		}},
		{name: "nil execution clock", call: func() error {
			_, err := acquisition.NewExecutionStepService(resolver, fixture.manifests, fixture.jobsvc, fixture.sessions, fixture.tasks,
				acquisition.WithExecutionClock(nil))
			return err
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); !errors.Is(err, acquisition.ErrInvalidExecutionRequest) {
				t.Fatalf("error = %v, want ErrInvalidExecutionRequest", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// provider task validation
// ---------------------------------------------------------------------------

func TestProviderTaskValidationEnforcesSideEffectState(t *testing.T) {
	base := acquisition.ProviderTask{
		ManifestID: executionManifestID, JobID: executionJobID, ProviderID: executionProviderID,
		State: acquisition.ProviderTaskReferenceKnown, ProviderTaskRef: "task-1",
		CreatedAt: executionNow, UpdatedAt: executionNow,
	}
	if err := acquisition.ValidateProviderTask(base); err != nil {
		t.Fatalf("ValidateProviderTask(known reference) error = %v", err)
	}
	reserved := base
	reserved.State = acquisition.ProviderTaskStartReserved
	reserved.ProviderTaskRef = ""
	if err := acquisition.ValidateProviderTask(reserved); err != nil {
		t.Fatalf("ValidateProviderTask(reservation) error = %v", err)
	}
	invalid := []struct {
		name   string
		mutate func(*acquisition.ProviderTask)
	}{
		{name: "reservation with a reference", mutate: func(task *acquisition.ProviderTask) {
			task.State = acquisition.ProviderTaskStartReserved
			task.ProviderTaskRef = "task-1"
		}},
		{name: "known state without a reference", mutate: func(task *acquisition.ProviderTask) {
			task.State = acquisition.ProviderTaskReferenceKnown
			task.ProviderTaskRef = ""
		}},
		{name: "known state with whitespace reference", mutate: func(task *acquisition.ProviderTask) {
			task.State = acquisition.ProviderTaskReferenceKnown
			task.ProviderTaskRef = "   "
		}},
		{name: "unknown state", mutate: func(task *acquisition.ProviderTask) { task.State = "SOMETHING" }},
		{name: "empty state", mutate: func(task *acquisition.ProviderTask) { task.State = "" }},
		{name: "zero created_at", mutate: func(task *acquisition.ProviderTask) { task.CreatedAt = time.Time{} }},
		{name: "updated before created", mutate: func(task *acquisition.ProviderTask) {
			task.UpdatedAt = executionNow.Add(-time.Hour)
		}},
		{name: "empty provider id", mutate: func(task *acquisition.ProviderTask) { task.ProviderID = "" }},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			candidate := base
			test.mutate(&candidate)
			if err := acquisition.ValidateProviderTask(candidate); !errors.Is(err, acquisition.ErrInvalidProviderTask) {
				t.Fatalf("ValidateProviderTask() error = %v, want ErrInvalidProviderTask", err)
			}
		})
	}
}
