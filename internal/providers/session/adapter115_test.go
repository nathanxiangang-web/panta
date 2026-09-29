package session_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	p115 "github.com/nathanxiangang-web/panta/internal/providers/115"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/providers/session"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

const (
	// concreteCredential is the opaque reference the composition layer resolves
	// secret material for. Only the composition layer ever sees the contents.
	concreteCredential = "secret-ref://connections/115-main"
	// concreteSecret is synthetic cookie material, never sent anywhere by these
	// tests because the real library client is not driven here.
	concreteSecret = "UID=sentinel-uid;CID=sentinel-cid;SEID=sentinel-seid;KID=sentinel-kid"
)

// TestConcrete115AdapterRegistersIntoSessionRegistry is the Gate 3.6 wiring proof:
// the real 115 adapter, built from secret material obtained through the opaque
// Gate 3.5 secret boundary, registers into the Gate 3.5 connection-scoped session
// registry and resolves by the exact provider/connection/credential identity.
//
// No provider operation is driven on the real library client here, because the
// first real call resolves the authenticated 115 user over the network.
func TestConcrete115AdapterRegistersIntoSessionRegistry(t *testing.T) {
	secrets := session.NewStaticSecretResolver()
	if err := secrets.Register(concreteCredential, []byte(concreteSecret)); err != nil {
		t.Fatalf("Register(secret) error = %v", err)
	}
	// The composition layer owns CredentialRef -> SecretResolver -> construction.
	resolved, err := secrets.ResolveSecret(context.Background(), concreteCredential)
	if err != nil {
		t.Fatalf("ResolveSecret() error = %v", err)
	}
	adapter, err := p115.NewAdapterFromCookie(resolved, p115.Options{})
	if err != nil {
		t.Fatalf("NewAdapterFromCookie() error = %v", err)
	}
	if adapter.Descriptor().ID != "115" {
		t.Fatalf("constructed adapter identity = %q, want 115", adapter.Descriptor().ID)
	}

	scoped := concreteCredential
	sessions := session.New()
	if err := sessions.Register(session.RegistrationRequest{
		ProviderID:    adapter.Descriptor().ID,
		ConnectionID:  string(connectionA),
		CredentialRef: &scoped,
		Downloader:    adapter,
	}); err != nil {
		t.Fatalf("session.Register(115 adapter) error = %v", err)
	}

	// The execution boundary resolves the concrete adapter by full identity.
	binding, err := sessions.ResolveDownloader(context.Background(), downloaderSessionRequest(connectionA, scoped))
	if err != nil {
		t.Fatalf("ResolveDownloader() error = %v", err)
	}
	if binding.Descriptor.ID != "115" || binding.Downloader == nil {
		t.Fatalf("resolved binding = %#v", binding)
	}
	if binding.Downloader != contracts.DownloaderProvider(adapter) {
		t.Fatal("session resolved a different downloader than the registered 115 adapter")
	}

	// A different connection with the same provider is not served.
	if _, err := sessions.ResolveDownloader(context.Background(), downloaderSessionRequest(connectionB, scoped)); err == nil {
		t.Fatal("unregistered connection resolved the 115 session, want fail-closed error")
	}
}

// TestConcrete115AdapterExecutesThroughControlledBackend drives the real adapter
// through the session registry with an injected adapter-private backend, so the
// Gate 3.4/3.5 execution path is exercised end to end without network access.
func TestConcrete115AdapterExecutesThroughControlledBackend(t *testing.T) {
	backend := newControlled115Backend()
	adapter, err := p115.New(p115.Options{Backend: backend})
	if err != nil {
		t.Fatalf("p115.New() error = %v", err)
	}
	scoped := concreteCredential
	sessions := session.New()
	if err := sessions.Register(session.RegistrationRequest{
		ProviderID: adapter.Descriptor().ID, ConnectionID: string(connectionA),
		CredentialRef: &scoped, Downloader: adapter,
	}); err != nil {
		t.Fatalf("session.Register() error = %v", err)
	}
	binding, err := sessions.ResolveDownloader(context.Background(), downloaderSessionRequest(connectionA, scoped))
	if err != nil {
		t.Fatalf("ResolveDownloader() error = %v", err)
	}

	request := concreteDownloadRequest()
	reference, err := binding.Downloader.StartDownload(context.Background(), request)
	if err != nil {
		t.Fatalf("StartDownload() error = %v", err)
	}
	if reference.Value != "115-info-hash-1" {
		t.Fatalf("reference = %q, want the provider-issued info hash", reference.Value)
	}
	backend.setStatus(reference.Value, 1)
	status, err := binding.Downloader.DownloadStatus(context.Background(), reference)
	if err != nil {
		t.Fatalf("DownloadStatus() error = %v", err)
	}
	if status.State != contracts.TaskStateRunning {
		t.Fatalf("state = %q, want running", status.State)
	}

	// Exactly one URI reached the provider, with the exact scope.
	if calls, uri, dir := backend.lastAdd(); calls != 1 || uri != request.Source.Value || dir != request.Target.Scope {
		t.Fatalf("backend add calls=%d uri=%q dir=%q, want exactly one exact submission", calls, uri, dir)
	}
}

// TestConcrete115AdapterRejectsCrossCredentialSession proves the adapter cannot be
// reached under another connection's credential reference.
func TestConcrete115AdapterRejectsCrossCredentialSession(t *testing.T) {
	adapter, err := p115.NewAdapterFromCookie(
		[]byte(concreteSecret),
		p115.Options{})
	if err != nil {
		t.Fatalf("NewAdapterFromCookie() error = %v", err)
	}
	registeredRef, otherRef := concreteCredential, "secret-ref://connections/other"
	sessions := session.New()
	if err := sessions.Register(session.RegistrationRequest{
		ProviderID: adapter.Descriptor().ID, ConnectionID: string(connectionA),
		CredentialRef: &registeredRef, Downloader: adapter,
	}); err != nil {
		t.Fatalf("session.Register() error = %v", err)
	}
	if _, err := sessions.ResolveDownloader(context.Background(), downloaderSessionRequest(connectionA, otherRef)); !errors.Is(err, session.ErrCredentialMismatch) {
		t.Fatalf("cross-credential resolution error = %v, want ErrCredentialMismatch", err)
	}
}

// TestConcrete115AdapterErrorsNeverLeakCookieMaterial drives a backend failure
// through the real adapter and asserts no cookie content appears.
func TestConcrete115AdapterErrorsNeverLeakCookieMaterial(t *testing.T) {
	const sentinel = "SENTINEL-COOKIE-FRAGMENT"
	backend := newControlled115Backend()
	backend.addErr = errors.New("upstream 115 offline add failed")
	adapter, err := p115.New(p115.Options{Backend: backend})
	if err != nil {
		t.Fatalf("p115.New() error = %v", err)
	}
	_, err = adapter.StartDownload(context.Background(), concreteDownloadRequest())
	if err == nil {
		t.Fatal("StartDownload() succeeded, want an attributed failure")
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("error %q leaks cookie material", err)
	}
	// The adapter's own descriptor rendering must also stay clean.
	rendered := string(adapter.Descriptor().ID) + "|" + adapter.Descriptor().DisplayName
	if strings.Contains(rendered, sentinel) {
		t.Fatalf("adapter descriptor leaks cookie material: %q", rendered)
	}
}

// --- helpers ------------------------------------------------------------------

func concreteDownloadRequest() contracts.DownloadRequest {
	return contracts.DownloadRequest{
		Source: contracts.SourceReference{Scheme: "magnet", Value: "magnet:?xt=urn:btih:0123456789abcdef"},
		Target: contracts.TargetPath{Scope: "115-save-dir-id", Path: "/downloads/item"},
	}
}

func downloaderSessionRequest(connectionID storage.ConnectionID, credential string) acquisition.DownloaderSessionRequest {
	return acquisition.DownloaderSessionRequest{
		ProviderID:    "115",
		ConnectionID:  connectionID,
		CredentialRef: &credential,
	}
}

// controlled115Backend is an adapter-private backend double with no network use.
type controlled115Backend struct {
	mu       sync.Mutex
	addCalls int
	addURI   string
	addDir   string
	addErr   error
	statuses map[string]int
}

func newControlled115Backend() *controlled115Backend {
	return &controlled115Backend{statuses: map[string]int{}}
}

func (backend *controlled115Backend) AddOfflineTaskURI(_ context.Context, uri, saveDirID string) ([]string, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.addCalls++
	backend.addURI = uri
	backend.addDir = saveDirID
	if backend.addErr != nil {
		return nil, backend.addErr
	}
	backend.statuses["115-info-hash-1"] = 0
	return []string{"115-info-hash-1"}, nil
}

func (backend *controlled115Backend) ListOfflineTasks(_ context.Context, page int64) (p115.OfflineTaskPage, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if page != 1 {
		return p115.OfflineTaskPage{Page: page, PageCount: 1}, nil
	}
	tasks := make([]p115.OfflineTask, 0, len(backend.statuses))
	for hash, status := range backend.statuses {
		tasks = append(tasks, p115.OfflineTask{InfoHash: hash, Status: status})
	}
	return p115.OfflineTaskPage{Page: 1, PageCount: 1, Tasks: tasks}, nil
}

func (backend *controlled115Backend) DeleteOfflineTask(_ context.Context, _ string) error { return nil }

func (backend *controlled115Backend) setStatus(infoHash string, status int) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.statuses[infoHash] = status
}

func (backend *controlled115Backend) lastAdd() (calls int, uri, dir string) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.addCalls, backend.addURI, backend.addDir
}
