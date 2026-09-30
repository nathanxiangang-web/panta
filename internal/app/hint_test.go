package app_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
	"github.com/nathanxiangang-web/panta/internal/store/postgres"
)

const (
	compositionToken = "gate38-composition-hint-token-0123456789"
	compositionRoot  = "composition-root-115"
	compositionScope = "/downloads/movies"

	compositionManifestID = acquisition.ManifestID("a0000000-0000-4000-8000-000000000001")
	compositionJobID      = jobs.JobID("a0000000-0000-4000-8000-000000000002")
	compositionConnection = storage.ConnectionID("a0000000-0000-4000-8000-000000000003")
	compositionBindingID  = storage.BindingID("a0000000-0000-4000-8000-000000000004")

	compositionOwner = "composition-lease-owner"

	// Gate 3.9 requires a frozen direct-child identity before a Hint may be sent.
	compositionExpectedName = "acquired-item.bin"
)

var compositionNow = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)

// --- doubles -----------------------------------------------------------------

type compositionManifestReader struct{ value acquisition.Manifest }

func (reader compositionManifestReader) GetManifest(_ context.Context, id acquisition.ManifestID) (acquisition.Manifest, error) {
	if id != reader.value.ID {
		return acquisition.Manifest{}, acquisition.ErrNotFound
	}
	return reader.value, nil
}

type compositionJobReader struct{ value jobs.Job }

func (reader compositionJobReader) Get(_ context.Context, id jobs.JobID) (jobs.Job, error) {
	if id != reader.value.ID {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return reader.value, nil
}

type compositionBindingReader struct{ value storage.Binding }

func (reader compositionBindingReader) GetBinding(_ context.Context, id storage.BindingID) (storage.Binding, error) {
	if id != reader.value.ID {
		return storage.Binding{}, storage.ErrNotFound
	}
	return reader.value, nil
}

// recordingStore proves the handoff reached the acquisition store exactly once.
type recordingStore struct {
	mu    sync.Mutex
	plans []acquisition.RefreshPlan
}

func (store *recordingStore) CommitRefresh(_ context.Context, plan acquisition.RefreshPlan) (acquisition.RefreshResult, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.plans = append(store.plans, plan)
	return acquisition.RefreshResult{
		Manifest: acquisition.Manifest{ID: plan.ManifestID, State: plan.ManifestState},
		Job:      jobs.Job{ID: plan.JobID, State: plan.JobState},
		Changed:  true,
	}, nil
}

func (store *recordingStore) count() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.plans)
}

// hintEndpoint is a faithful stand-in for IndexCore's P9 trusted Hint transport.
// It records exactly what the composed production adapter sent.
type hintEndpoint struct {
	mu       sync.Mutex
	requests []map[string]any
	auth     []string
	token    string
	status   int
	body     string
}

func (endpoint *hintEndpoint) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.URL.Path != indexcore.HintPath {
		writer.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	body, _ := io.ReadAll(request.Body)
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.auth = append(endpoint.auth, request.Header.Get("Authorization"))
	if request.Header.Get("Authorization") != "Bearer "+endpoint.token {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(writer, `{"error":"unauthorized"}`)
		return
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	endpoint.requests = append(endpoint.requests, decoded)
	if endpoint.status != 0 {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(endpoint.status)
		_, _ = io.WriteString(writer, endpoint.body)
		return
	}
	response, _ := json.Marshal(map[string]any{
		"status": "accepted", "root_id": decoded["root_id"], "scope_key": decoded["scope_key"],
		"work_state": "PENDING", "signal_seq": 4,
	})
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusAccepted)
	_, _ = writer.Write(response)
}

func (endpoint *hintEndpoint) snapshot() (int, []map[string]any, []string) {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return len(endpoint.requests), append([]map[string]any(nil), endpoint.requests...),
		append([]string(nil), endpoint.auth...)
}

// --- composition -------------------------------------------------------------

// TestHintPortComposesRealClient proves the production composition actually works:
// a real indexcore.HintClient is adapted to acquisition.MutationHintPort and the
// wire request is what the frozen D-029 mapping requires.
func TestHintPortComposesRealClient(t *testing.T) {
	endpoint := &hintEndpoint{token: compositionToken}
	server := httptest.NewServer(endpoint)
	t.Cleanup(server.Close)

	// A real client, built through the composition boundary rather than in the test.
	port, err := app.NewHintPort(app.HintConfig{BaseURL: server.URL, Token: compositionToken})
	if err != nil {
		t.Fatalf("NewHintPort() error = %v", err)
	}

	receipt, err := port.SubmitMutationHint(context.Background(), acquisition.MutationHint{
		RootID: compositionRoot, ScopeKey: compositionScope, Reason: acquisition.MutationHintPossibleChange,
	})
	if err != nil {
		t.Fatalf("SubmitMutationHint() error = %v", err)
	}
	if receipt.Status != "accepted" || receipt.RootID != compositionRoot ||
		receipt.ScopeKey != compositionScope || receipt.WorkState != "PENDING" || receipt.SignalSeq != 4 {
		t.Fatalf("receipt = %+v", receipt)
	}

	calls, requests, auth := endpoint.snapshot()
	if calls != 1 {
		t.Fatalf("Hint requests = %d, want exactly 1", calls)
	}
	if len(requests[0]) != 3 {
		t.Fatalf("Hint body carried %d fields, want exactly root_id/scope_key/reason: %v", len(requests[0]), requests[0])
	}
	if requests[0]["root_id"] != compositionRoot {
		t.Fatalf("root_id = %v", requests[0]["root_id"])
	}
	if requests[0]["scope_key"] != compositionScope {
		t.Fatalf("scope_key = %v", requests[0]["scope_key"])
	}
	if requests[0]["reason"] != "POSSIBLE_CHANGE" {
		t.Fatalf("reason = %v, want POSSIBLE_CHANGE", requests[0]["reason"])
	}
	if auth[0] != "Bearer "+compositionToken {
		t.Fatalf("Authorization = %q", auth[0])
	}
}

// TestHintPortPreservesTypedFailures proves the adapter does not swallow or
// flatten the IndexCore failure classification.
func TestHintPortPreservesTypedFailures(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   int
		body     string
		wantKind error
	}{
		{name: "429 backpressure", status: http.StatusTooManyRequests, body: `{"error":"busy"}`, wantKind: indexcore.ErrHintBusy},
		{name: "401 unauthorized", status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`, wantKind: indexcore.ErrHintUnauthorized},
		{name: "503 unavailable", status: http.StatusServiceUnavailable, body: `{"error":"ingest_unavailable"}`, wantKind: indexcore.ErrHintUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint := &hintEndpoint{token: compositionToken, status: test.status, body: test.body}
			server := httptest.NewServer(endpoint)
			t.Cleanup(server.Close)

			port, err := app.NewHintPort(app.HintConfig{BaseURL: server.URL, Token: compositionToken})
			if err != nil {
				t.Fatalf("NewHintPort() error = %v", err)
			}
			_, err = port.SubmitMutationHint(context.Background(), acquisition.MutationHint{
				RootID: compositionRoot, ScopeKey: compositionScope, Reason: acquisition.MutationHintPossibleChange,
			})
			if !errors.Is(err, test.wantKind) {
				t.Fatalf("error = %v, want %v", err, test.wantKind)
			}
		})
	}
}

// TestHintPortRejectsUnsupportedReason proves the reason translation is closed.
func TestHintPortRejectsUnsupportedReason(t *testing.T) {
	endpoint := &hintEndpoint{token: compositionToken}
	server := httptest.NewServer(endpoint)
	t.Cleanup(server.Close)

	port, err := app.NewHintPort(app.HintConfig{BaseURL: server.URL, Token: compositionToken})
	if err != nil {
		t.Fatalf("NewHintPort() error = %v", err)
	}
	for _, reason := range []acquisition.MutationHintReason{"", "DELETE_HINT", "POSSIBLE_CHANGE "} {
		if _, err := port.SubmitMutationHint(context.Background(), acquisition.MutationHint{
			RootID: compositionRoot, ScopeKey: compositionScope, Reason: reason,
		}); !errors.Is(err, acquisition.ErrInvalidRefreshRequest) {
			t.Fatalf("reason %q error = %v, want ErrInvalidRefreshRequest", reason, err)
		}
	}
	if calls, _, _ := endpoint.snapshot(); calls != 0 {
		t.Fatalf("Hint requests = %d, want 0 for an unsupported reason", calls)
	}
}

// TestHintPortFailsClosedOnInsecureConfiguration proves the composition boundary
// cannot be wired to a non-loopback address, which would send the trusted bearer
// token off-host.
func TestHintPortFailsClosedOnInsecureConfiguration(t *testing.T) {
	for _, baseURL := range []string{
		"http://localhost:9100",
		"http://127.0.0.2:9100",
		"http://[::ffff:127.0.0.1]:9100",
		"http://0.0.0.0:9100",
		"http://10.0.0.5:9100",
		"https://indexcore.example.com",
		"http://token@127.0.0.1:9100",
	} {
		t.Run(baseURL, func(t *testing.T) {
			if _, err := app.NewHintPort(app.HintConfig{BaseURL: baseURL, Token: compositionToken}); err == nil {
				t.Fatalf("NewHintPort(%q) succeeded, want a loopback rejection", baseURL)
			}
		})
	}
	// The two literal loopback hosts remain accepted.
	for _, baseURL := range []string{"http://127.0.0.1:9100", "http://[::1]:9100"} {
		if _, err := app.NewHintPort(app.HintConfig{BaseURL: baseURL, Token: compositionToken}); err != nil {
			t.Fatalf("NewHintPort(%q) error = %v, want it accepted", baseURL, err)
		}
	}
}

// TestHintPortDoesNotFollowRedirects proves the trusted ingress never follows a 3xx
// to another address, and reports it as an unexpected status.
//
// It uses 307 Temporary Redirect deliberately: a 307 preserves both the method and
// the body, so a client that followed redirects would deliver the trusted POST to the
// second address. A 302 would degrade to GET, which would mask the defect. The
// target records EVERY request regardless of method, so the assertion cannot pass
// vacuously.
func TestHintPortDoesNotFollowRedirects(t *testing.T) {
	var targetCalls int32
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		atomic.AddInt32(&targetCalls, 1)
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(writer, `{"status":"accepted","root_id":"`+compositionRoot+
			`","scope_key":"`+compositionScope+`","work_state":"PENDING","signal_seq":1}`)
	}))
	t.Cleanup(targetServer.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, targetServer.URL+indexcore.HintPath, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(redirector.Close)

	port, err := app.NewHintPort(app.HintConfig{BaseURL: redirector.URL, Token: compositionToken})
	if err != nil {
		t.Fatalf("NewHintPort() error = %v", err)
	}
	_, err = port.SubmitMutationHint(context.Background(), acquisition.MutationHint{
		RootID: compositionRoot, ScopeKey: compositionScope, Reason: acquisition.MutationHintPossibleChange,
	})
	if !errors.Is(err, indexcore.ErrHintUnexpectedState) {
		t.Fatalf("error = %v, want ErrHintUnexpectedState for a redirect", err)
	}
	// The trusted POST, including its bearer token, must never reach the target.
	if calls := atomic.LoadInt32(&targetCalls); calls != 0 {
		t.Fatalf("redirect target received %d requests, want 0", calls)
	}
}

// TestRefreshStepWithComposedProductionPort is a real composition test: the actual
// acquisition.RefreshStep is wired to the production composition adapter instead of
// a test-local fake, proving the two capabilities connect.
func TestRefreshStepWithComposedProductionPort(t *testing.T) {
	endpoint := &hintEndpoint{token: compositionToken}
	server := httptest.NewServer(endpoint)
	t.Cleanup(server.Close)

	port, err := app.NewHintPort(app.HintConfig{BaseURL: server.URL, Token: compositionToken})
	if err != nil {
		t.Fatalf("NewHintPort() error = %v", err)
	}

	leaseEnd := compositionNow.Add(time.Hour)
	startedAt := compositionNow
	key := acquisition.AcquisitionJobIdempotencyKey(compositionManifestID)
	manifest := acquisition.Manifest{
		ID: compositionManifestID, SourceType: "opaque-source", SourceRef: "opaque-ref",
		TargetStorageBindingID: compositionBindingID, TargetPath: compositionScope,
		ExpectedName: pointerTo(compositionExpectedName),
		JobID:        pointerTo(compositionJobID), State: acquisition.StateAwaitingVisibility,
		CreatedAt: compositionNow, UpdatedAt: compositionNow,
	}
	job := jobs.Job{
		ID: compositionJobID, Type: acquisition.JobTypeAcquisition, State: jobs.StateRunning,
		Payload:        []byte(`{"manifest_id":"` + string(compositionManifestID) + `","schema_version":1}`),
		IdempotencyKey: &key, ClaimAttempts: 3, FailureCount: 1, MaxAttempts: 5,
		LeaseOwner: pointerTo(compositionOwner), LeaseExpiresAt: &leaseEnd,
		CreatedAt: compositionNow, UpdatedAt: compositionNow, StartedAt: &startedAt,
	}
	binding := storage.Binding{
		ID: compositionBindingID, ConnectionID: compositionConnection,
		OpenListMountPath: "/library", IndexCoreRootID: compositionRoot,
		Status: storage.BindingStatusActive, CreatedAt: compositionNow, UpdatedAt: compositionNow,
	}
	store := &recordingStore{}
	step, err := acquisition.NewRefreshStep(
		compositionManifestReader{value: manifest},
		compositionJobReader{value: job},
		compositionBindingReader{value: binding},
		port, store,
	)
	if err != nil {
		t.Fatalf("NewRefreshStep() error = %v", err)
	}

	result, err := step.Submit(context.Background(), acquisition.RefreshRequest{
		ManifestID: compositionManifestID, JobID: compositionJobID, Owner: compositionOwner,
		ExpectedClaim: 3, Now: compositionNow, RetryAt: compositionNow.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if !result.Changed || result.Manifest.State != acquisition.StateAwaitingCanonical ||
		result.Job.State != jobs.StateRetryWait {
		t.Fatalf("result = %+v", result)
	}
	if store.count() != 1 {
		t.Fatalf("store commits = %d, want 1", store.count())
	}
	calls, requests, _ := endpoint.snapshot()
	if calls != 1 {
		t.Fatalf("Hint requests = %d, want exactly 1", calls)
	}
	// The binding's OpenList mount must not reach the Hint: only the root does.
	if requests[0]["root_id"] != compositionRoot || requests[0]["scope_key"] != compositionScope {
		t.Fatalf("Hint = %v", requests[0])
	}
	for _, value := range requests[0] {
		if text, ok := value.(string); ok && strings.Contains(text, "/library") {
			t.Fatal("the OpenList mount leaked into the trusted Hint")
		}
	}
}

func pointerTo[T any](value T) *T { return &value }

// TestRefreshStepWithComposedPortAgainstRealPostgres is the full production
// composition against a real durable store: the real adapter, the real
// RefreshStep, and the real PostgreSQL RefreshRepository. It is opt-in because it
// resets the schema of the database it is given.
func TestRefreshStepWithComposedPortAgainstRealPostgres(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("PANTA_GATE3_HINT_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("PANTA_GATE3_HINT_DATABASE_URL is not set; set it to a dedicated disposable *_e2e database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse database URL: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_e2e") {
		t.Fatalf("refusing destructive test for database %q: name must end with _e2e", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public`); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	migrator, err := postgres.NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	if _, err := migrator.Apply(ctx); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	leaseEnd := compositionNow.Add(time.Hour)
	key := acquisition.AcquisitionJobIdempotencyKey(compositionManifestID)
	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key,
    claim_attempts, attempt_count, max_attempts, lease_owner, lease_expires_at,
    created_at, updated_at, started_at
) VALUES ($1, 'ACQUISITION', $2, 'RUNNING', $3, 3, 1, 5, $4, $5, $6, $6, $6)`,
		string(compositionJobID),
		[]byte(`{"manifest_id":"`+string(compositionManifestID)+`","schema_version":1}`),
		key, compositionOwner, leaseEnd, compositionNow,
	); err != nil {
		t.Fatalf("seed Job: %v", err)
	}
	// Real storage rows, so the binding the step observes is durable state rather than
	// an in-memory double.
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_connections (
    storage_connection_id, provider_type, credential_ref, status, created_at, updated_at
) VALUES ($1, 'p115', NULL, 'ACTIVE', $2, $2)`,
		string(compositionConnection), compositionNow,
	); err != nil {
		t.Fatalf("seed StorageConnection: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_bindings (
    storage_binding_id, storage_connection_id, openlist_mount_path, indexcore_root_id,
    status, created_at, updated_at
) VALUES ($1, $2, '/library', $3, 'ACTIVE', $4, $4)`,
		string(compositionBindingID), string(compositionConnection), compositionRoot, compositionNow,
	); err != nil {
		t.Fatalf("seed StorageBinding: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id, target_path,
    job_id, state, created_at, updated_at
) VALUES ($1, 'opaque-source', 'opaque-ref', $2, $3, $4, 'AWAITING_VISIBILITY', $5, $5)`,
		string(compositionManifestID), string(compositionBindingID), compositionScope,
		string(compositionJobID), compositionNow,
	); err != nil {
		t.Fatalf("seed Manifest: %v", err)
	}

	endpoint := &hintEndpoint{token: compositionToken}
	server := httptest.NewServer(endpoint)
	t.Cleanup(server.Close)
	port, err := app.NewHintPort(app.HintConfig{BaseURL: server.URL, Token: compositionToken})
	if err != nil {
		t.Fatalf("NewHintPort() error = %v", err)
	}
	repository, err := postgres.NewRefreshRepository(pool)
	if err != nil {
		t.Fatalf("NewRefreshRepository() error = %v", err)
	}

	manifestReader, err := postgres.NewAcquisitionManifestRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionManifestRepository() error = %v", err)
	}
	storageRepository, err := postgres.NewStorageRepository(pool)
	if err != nil {
		t.Fatalf("NewStorageRepository() error = %v", err)
	}
	_ = storageRepository
	if err != nil {
		t.Fatalf("NewManifestRepository() error = %v", err)
	}
	jobStore, err := postgres.NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}
	step, err := acquisition.NewRefreshStep(
		manifestReader, jobStore, storageRepository, port, repository,
	)
	if err != nil {
		t.Fatalf("NewRefreshStep() error = %v", err)
	}

	result, err := step.Submit(ctx, acquisition.RefreshRequest{
		ManifestID: compositionManifestID, JobID: compositionJobID, Owner: compositionOwner,
		ExpectedClaim: 3, Now: compositionNow, RetryAt: compositionNow.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if !result.Changed || result.Manifest.State != acquisition.StateAwaitingCanonical ||
		result.Job.State != jobs.StateRetryWait {
		t.Fatalf("result = %+v", result)
	}
	// Real read-back proves the atomic handoff landed.
	var manifestState, jobState string
	var claimAttempts, failureCount int
	if err := pool.QueryRow(ctx, `SELECT state FROM acquisition_manifests WHERE manifest_id = $1`,
		string(compositionManifestID)).Scan(&manifestState); err != nil {
		t.Fatalf("read Manifest: %v", err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT state, claim_attempts, attempt_count FROM jobs WHERE job_id = $1`,
		string(compositionJobID)).Scan(&jobState, &claimAttempts, &failureCount); err != nil {
		t.Fatalf("read Job: %v", err)
	}
	if manifestState != string(acquisition.StateAwaitingCanonical) || jobState != string(jobs.StateRetryWait) {
		t.Fatalf("durable pair = %s/%s", manifestState, jobState)
	}
	if claimAttempts != 3 || failureCount != 1 {
		t.Fatalf("claim generation = %d, failure budget = %d, want 3 and 1 preserved", claimAttempts, failureCount)
	}
	if manifestState == string(acquisition.StateReady) {
		t.Fatal("Hint acceptance marked the Manifest READY")
	}
	calls, _, _ := endpoint.snapshot()
	if calls != 1 {
		t.Fatalf("Hint requests = %d, want exactly 1", calls)
	}
}
