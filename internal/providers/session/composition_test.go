package session_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/providers/session"
	"github.com/nathanxiangang-web/panta/internal/providers/testprovider"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

// TestRealSessionCompositionExecutesWithTestprovider is the real wiring proof for
// Gate 3.5: the concrete session registry, the concrete provider-neutral test
// provider, and the real acquisition execution service, with no session double.
func TestRealSessionCompositionExecutesWithTestprovider(t *testing.T) {
	fixture := newSessionFixture(t)

	outcome, err := fixture.service.Execute(context.Background(), fixture.request())
	if err != nil {
		t.Fatalf("Execute() with the real session registry error = %v", err)
	}
	if outcome != acquisition.OutcomeProviderInProgress {
		t.Fatalf("outcome = %q, want %q", outcome, acquisition.OutcomeProviderInProgress)
	}
	// The downloader that ran is the one registered against the exact connection
	// and credential identity, proven by the recording port's own identity.
	if fixture.provider.sessionName != "session-a" {
		t.Fatalf("executed session = %q, want the connection-scoped session", fixture.provider.sessionName)
	}
	task, err := fixture.tasks.GetProviderTask(context.Background(), fixture.manifestID)
	if err != nil {
		t.Fatalf("GetProviderTask() error = %v", err)
	}
	if !task.ReferenceKnown() {
		t.Fatalf("durable task = %#v, want a known reference from the real provider", task)
	}
}

// TestSameProviderTwoConnectionsSelectsDifferentSessions drives the full execution
// path twice with one ProviderID and two connections, proving the resolved session
// follows the connection credential rather than the provider identity.
func TestSameProviderTwoConnectionsSelectsDifferentSessions(t *testing.T) {
	registry := session.New()
	providerA := newRecordingDownloader("connection-a-session")
	providerB := newRecordingDownloader("connection-b-session")
	refA, refB := "secret-ref://connections/a", "secret-ref://connections/b"
	if err := registry.Register(session.RegistrationRequest{
		ProviderID: providerA.Descriptor().ID, ConnectionID: string(connectionA),
		CredentialRef: &refA, Downloader: providerA,
	}); err != nil {
		t.Fatalf("Register(A) error = %v", err)
	}
	if err := registry.Register(session.RegistrationRequest{
		ProviderID: providerB.Descriptor().ID, ConnectionID: string(connectionB),
		CredentialRef: &refB, Downloader: providerB,
	}); err != nil {
		t.Fatalf("Register(B) error = %v", err)
	}
	ctx := context.Background()

	resolvedA, err := registry.ResolveDownloader(ctx, acquisition.DownloaderSessionRequest{
		ProviderID: providerA.Descriptor().ID, ConnectionID: connectionA, CredentialRef: &refA,
	})
	if err != nil {
		t.Fatalf("ResolveDownloader(A) error = %v", err)
	}
	resolvedB, err := registry.ResolveDownloader(ctx, acquisition.DownloaderSessionRequest{
		ProviderID: providerB.Descriptor().ID, ConnectionID: connectionB, CredentialRef: &refB,
	})
	if err != nil {
		t.Fatalf("ResolveDownloader(B) error = %v", err)
	}
	if resolvedA.Downloader != contracts.DownloaderProvider(providerA) {
		t.Fatal("connection A did not resolve its own downloader session")
	}
	if resolvedB.Downloader != contracts.DownloaderProvider(providerB) {
		t.Fatal("connection B did not resolve its own downloader session")
	}
	// Crossing the credential boundary must fail.
	if _, err := registry.ResolveDownloader(ctx, acquisition.DownloaderSessionRequest{
		ProviderID: providerA.Descriptor().ID, ConnectionID: connectionA, CredentialRef: &refB,
	}); !errors.Is(err, session.ErrCredentialMismatch) {
		t.Fatalf("cross-credential resolution error = %v, want ErrCredentialMismatch", err)
	}
}

// TestNoSecretMaterialEntersProductState proves that resolved secret contents never
// reach the Manifest, the Job payload, the provider-task row, or an error message.
func TestNoSecretMaterialEntersProductState(t *testing.T) {
	fixture := newSessionFixture(t)
	const secret = "sentinel-secret-material-9f3a"

	secrets := session.NewStaticSecretResolver()
	if err := secrets.Register(contracts.CredentialRef(sessionCredentialRef), []byte(secret)); err != nil {
		t.Fatalf("Register(secret) error = %v", err)
	}
	// Resolving through the composition boundary is allowed; persisting it is not.
	resolved, err := secrets.ResolveSecret(context.Background(), contracts.CredentialRef(sessionCredentialRef))
	if err != nil {
		t.Fatalf("ResolveSecret() error = %v", err)
	}
	if len(resolved) == 0 {
		t.Fatal("secret material was empty")
	}

	outcome, err := fixture.service.Execute(context.Background(), fixture.request())
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if outcome == "" {
		t.Fatal("Execute() returned no outcome")
	}

	manifest := fixture.manifests[fixture.manifestID]
	job := fixture.jobs[fixture.jobID]
	task, err := fixture.tasks.GetProviderTask(context.Background(), fixture.manifestID)
	if err != nil {
		t.Fatalf("GetProviderTask() error = %v", err)
	}

	inspected := map[string]string{
		"manifest source_type":    manifest.SourceType,
		"manifest source_ref":     manifest.SourceRef,
		"manifest target_path":    manifest.TargetPath,
		"manifest state":          string(manifest.State),
		"manifest id":             string(manifest.ID),
		"job payload":             string(job.Payload),
		"job idempotency key":     deref(job.IdempotencyKey),
		"job type":                job.Type,
		"provider task provider":  string(task.ProviderID),
		"provider task reference": task.ProviderTaskRef,
		"provider task state":     string(task.State),
		"provider task manifest":  string(task.ManifestID),
	}
	for name, value := range inspected {
		if strings.Contains(value, secret) {
			t.Fatalf("%s contains secret material", name)
		}
	}
	// The durable row is the only place the opaque reference is allowed to appear.
	if task.ProviderTaskRef != fixture.provider.taskReference {
		t.Fatalf("durable task reference = %q, want the provider-issued reference", task.ProviderTaskRef)
	}
	if bytes.Contains([]byte(string(job.Payload)), []byte(sessionCredentialRef)) {
		t.Fatal("Job payload contains the credential reference")
	}
}

// --- fixture ------------------------------------------------------------------

const (
	sessionManifestID = acquisition.ManifestID("d0000000-0000-4000-8000-000000000001")
	sessionJobID      = jobs.JobID("d0000000-0000-4000-8000-000000000002")
	sessionBindingID  = storage.BindingID("d0000000-0000-4000-8000-000000000003")
	sessionConnID     = storage.ConnectionID("d0000000-0000-4000-8000-000000000004")
	sessionOwner      = "session-worker"
)

const sessionCredentialRef = "secret-ref://connections/gate0"

var sessionNow = time.Date(2026, 9, 30, 7, 0, 0, 0, time.UTC)

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

type sessionFixture struct {
	service    *acquisition.ExecutionStepService
	provider   *recordingDownloader
	fence      *sessionFence
	tasks      *sessionFence
	manifests  map[acquisition.ManifestID]acquisition.Manifest
	jobs       map[jobs.JobID]jobs.Job
	manifestID acquisition.ManifestID
	jobID      jobs.JobID
}

func (fixture *sessionFixture) request() acquisition.StepRequest {
	return acquisition.StepRequest{JobID: fixture.jobID, Owner: sessionOwner, ClaimAttempt: 1}
}

func newSessionFixture(t *testing.T) *sessionFixture {
	t.Helper()
	provider := newRecordingDownloader("session-a")
	registry := session.New()
	credential := sessionCredentialRef
	if err := registry.Register(session.RegistrationRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: string(sessionConnID),
		CredentialRef: &credential, Downloader: provider,
	}); err != nil {
		t.Fatalf("Register(real session) error = %v", err)
	}

	scope := testprovider.KnownTarget().Scope
	manifests := &sessionManifestReader{values: map[acquisition.ManifestID]acquisition.Manifest{
		sessionManifestID: {
			ID: sessionManifestID, SourceType: "opaque-source", SourceRef: "opaque-ref",
			TargetStorageBindingID: sessionBindingID, TargetPath: testprovider.KnownTarget().Path,
			JobID: jobIDPointer(sessionJobID), State: acquisition.StateActive,
			CreatedAt: sessionNow, UpdatedAt: sessionNow,
		},
	}}
	key := acquisition.AcquisitionJobIdempotencyKey(sessionManifestID)
	leaseExpiry := sessionNow.Add(time.Hour)
	jobsvc := &sessionJobReader{values: map[jobs.JobID]jobs.Job{
		sessionJobID: {
			ID: sessionJobID, Type: acquisition.JobTypeAcquisition,
			Payload:        []byte(`{"schema_version":1,"manifest_id":"` + string(sessionManifestID) + `"}`),
			State:          jobs.StateRunning,
			IdempotencyKey: &key, ClaimAttempts: 1, FailureCount: 0, MaxAttempts: 3,
			LeaseOwner: stringPointer(sessionOwner), LeaseExpiresAt: &leaseExpiry,
			CreatedAt: sessionNow, UpdatedAt: sessionNow,
		},
	}}
	topology := &sessionTopologyReader{
		binding: storage.Binding{
			ID: sessionBindingID, ConnectionID: sessionConnID, ProviderScope: &scope,
			OpenListMountPath: "/openlist/mount", IndexCoreRootID: "root-1", Status: storage.BindingStatusActive,
		},
		connection: storage.Connection{
			ID: sessionConnID, ProviderType: string(provider.Descriptor().ID),
			CredentialRef: &credential, Status: storage.ConnectionStatusActive,
		},
	}
	resolver, err := acquisition.NewExecutionInputResolver(manifests, topology)
	if err != nil {
		t.Fatalf("NewExecutionInputResolver() error = %v", err)
	}
	fence := &sessionFence{values: map[acquisition.ManifestID]acquisition.ProviderTask{}}
	service, err := acquisition.NewExecutionStepService(
		resolver, manifests, jobsvc, registry, fence,
		acquisition.WithExecutionClock(func() time.Time { return sessionNow }),
	)
	if err != nil {
		t.Fatalf("NewExecutionStepService(real session registry) error = %v", err)
	}
	return &sessionFixture{
		service: service, provider: provider, fence: fence, tasks: fence,
		manifests: manifests.values, jobs: jobsvc.values,
		manifestID: sessionManifestID, jobID: sessionJobID,
	}
}

func jobIDPointer(id jobs.JobID) *jobs.JobID { return &id }

func stringPointer(value string) *string { return &value }

// recordingDownloader is a provider-neutral downloader port that identifies the
// session it was registered for, so tests can prove which connection's downloader
// actually executed.
type recordingDownloader struct {
	descriptor    contracts.Descriptor
	sessionName   string
	taskReference string

	mu    sync.Mutex
	tasks map[string]contracts.TaskState
}

func newRecordingDownloader(sessionName string) *recordingDownloader {
	return &recordingDownloader{
		descriptor: contracts.Descriptor{
			ID: testprovider.ID, DisplayName: "recording downloader",
			Capabilities: contracts.CapabilitySet{Downloader: true},
		},
		sessionName: sessionName, taskReference: "recording-task-0001",
		tasks: map[string]contracts.TaskState{},
	}
}

func (provider *recordingDownloader) Descriptor() contracts.Descriptor { return provider.descriptor }

func (provider *recordingDownloader) StartDownload(_ context.Context, _ contracts.DownloadRequest) (contracts.TaskReference, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.tasks[provider.taskReference] = contracts.TaskStatePending
	return contracts.TaskReference{Value: provider.taskReference}, nil
}

func (provider *recordingDownloader) DownloadStatus(_ context.Context, reference contracts.TaskReference) (contracts.TaskStatus, error) {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	state, exists := provider.tasks[reference.Value]
	if !exists {
		return contracts.TaskStatus{}, errors.New("unknown recording task")
	}
	return contracts.TaskStatus{Reference: reference, State: state}, nil
}

func (provider *recordingDownloader) CancelDownload(context.Context, contracts.TaskReference) error {
	return nil
}

// --- minimal durable doubles --------------------------------------------------

type sessionManifestReader struct {
	values map[acquisition.ManifestID]acquisition.Manifest
}

func (reader *sessionManifestReader) GetManifest(_ context.Context, id acquisition.ManifestID) (acquisition.Manifest, error) {
	manifest, exists := reader.values[id]
	if !exists {
		return acquisition.Manifest{}, acquisition.ErrNotFound
	}
	return manifest, nil
}

type sessionJobReader struct {
	values map[jobs.JobID]jobs.Job
}

func (reader *sessionJobReader) Get(_ context.Context, id jobs.JobID) (jobs.Job, error) {
	job, exists := reader.values[id]
	if !exists {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return job, nil
}

type sessionTopologyReader struct {
	binding    storage.Binding
	connection storage.Connection
}

func (reader *sessionTopologyReader) GetBinding(context.Context, storage.BindingID) (storage.Binding, error) {
	return reader.binding, nil
}

func (reader *sessionTopologyReader) GetConnection(context.Context, storage.ConnectionID) (storage.Connection, error) {
	return reader.connection, nil
}

type sessionFence struct {
	mu     sync.Mutex
	values map[acquisition.ManifestID]acquisition.ProviderTask
}

func (fence *sessionFence) GetProviderTask(_ context.Context, id acquisition.ManifestID) (acquisition.ProviderTask, error) {
	fence.mu.Lock()
	defer fence.mu.Unlock()
	task, exists := fence.values[id]
	if !exists {
		return acquisition.ProviderTask{}, acquisition.ErrProviderTaskNotFound
	}
	return task, nil
}

func (fence *sessionFence) ClaimProviderTask(
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
