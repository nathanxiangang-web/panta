// Issue #39 test 30 (Gate 3.8): controlled end-to-end observation-Hint test.
//
// It proves three things at once:
//
//  1. Panta's real IndexCore trusted Mutation Hint client reaches a real HTTP
//     endpoint that implements IndexCore's P9 transport contract, and the
//     atomically committed PostgreSQL handoff (Manifest -> AWAITING_CANONICAL,
//     Job -> RETRY_WAIT) happens exactly once per accepted Hint.
//  2. A replay after that durable handoff sends no second Hint and rewrites
//     nothing (Issue #39 requirement 21).
//  3. The acquisition refresh path owns no Panta -> OpenList client: the accepted
//     boundary is Storage -> OpenList -> IndexCore Collector -> Canonical/Journal
//     -> Panta, so Panta must never observe OpenList directly during acquisition
//     (Issue #39 requirement 25).
//
// This file is package observation_test so it can share the package with
// observation_test.go, which owns the pinned-Gate-1 controlled observation E2E.
// Unlike that test, this test needs no IndexCore binary: the only external
// dependency is a disposable PostgreSQL database, because the atomic handoff must
// be genuinely durable. It is opt-in like TestControlledObservation: it skips
// unless PANTA_GATE3_HINT_E2E=1 and shares no database with the parallel
// internal/store/postgres suite.
package observation_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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
	// hintE2EToken mirrors IndexCore's P9 trusted-Hint token floor: the P9
	// transport refuses to start with a token shorter than 32 bytes.
	hintE2EToken = "gate38-controlled-hint-observation-token-0001"

	// hintE2ERootID is the binding's IndexCore canonical root. It is the value the
	// frozen D-029 mapping must place in the Hint root_id.
	hintE2ERootID = "gate38-hint-observation-root"

	// hintE2ETargetPath is the Manifest target path and therefore the Hint
	// scope_key. It must also be a valid IndexCore P3 scope key.
	hintE2ETargetPath = "/library/incoming"

	// hintE2EProviderScope and hintE2EOpenListMount are deliberately distinctive:
	// neither may leak into the trusted Hint, because a provider scope and an
	// OpenList mount path never select IndexCore observation work.
	hintE2EProviderScope = "provider-scope-must-not-leave-panta"
	hintE2EOpenListMount = "/openlist-mount-must-not-leave-panta"

	// hintE2EMaxBodyBytes mirrors IndexCore's hard P9 request body limit.
	hintE2EMaxBodyBytes = 4096

	// Fixture counters. The claim generation fences the lease; the failure budget
	// is the retry budget. A stage handoff must change neither.
	hintE2EClaimGeneration = 3
	hintE2EFailureBudget   = 1
	hintE2EMaxAttempts     = 5

	hintE2EOwner = "gate38-hint-lease-owner"

	// Request intent seeded as ordinary fixture data. D-032 keeps the Handoff
	// scoped only by indexcore_root_id + target_path, so neither this name nor the
	// locator is sent in the Hint.
	hintE2EExpectedName = "acquired-item.bin"
)

var (
	hintE2EManifestID   = acquisition.ManifestID("39000000-0000-4000-8000-000000000001")
	hintE2EJobID        = jobs.JobID("39000000-0000-4000-8000-000000000002")
	hintE2EConnectionID = storage.ConnectionID("39000000-0000-4000-8000-000000000003")
	hintE2EBindingID    = storage.BindingID("39000000-0000-4000-8000-000000000004")
)

// TestControlledIndexCoreHintObservation is the controlled Gate 3.8 E2E. It wires
// the real indexcore.HintClient against an httptest server that mirrors IndexCore's
// P9 contract, adapts that client to acquisition.MutationHintPort, and drives a
// real acquisition.RefreshStep whose RefreshStore is a real PostgreSQL
// RefreshRepository.
func TestControlledIndexCoreHintObservation(t *testing.T) {
	if os.Getenv("PANTA_GATE3_HINT_E2E") != "1" {
		t.Skip("set PANTA_GATE3_HINT_E2E=1 to run the controlled IndexCore Hint observation E2E")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool := hintE2EPool(t, ctx)
	resetHintE2ESchema(t, ctx, pool)

	migrator, err := postgres.NewMigrator(pool)
	if err != nil {
		t.Fatalf("construct Panta migrator: %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("apply Panta migrations: %v", err)
	}
	if !status.Compatible || status.CurrentVersion != status.LatestVersion || status.CurrentVersion == 0 ||
		len(status.Pending) != 0 {
		t.Fatalf("Panta schema status = %#v, want a fully migrated compatible schema", status)
	}

	fixture := newHintE2EFixture()
	fixture.seed(t, ctx, pool)
	fixture.assertSeeded(t, ctx, pool)

	// The stand-in is the only network endpoint this test exposes, and it speaks
	// IndexCore's real P9 contract (see hintE2EEndpoint).
	endpoint := newHintE2EEndpoint(t, hintE2EToken)
	server := httptest.NewServer(endpoint)
	t.Cleanup(server.Close)
	if !strings.HasPrefix(server.URL, "http://127.0.0.1:") {
		t.Fatalf("Hint stand-in must bind loopback, got %q", server.URL)
	}

	// The PRODUCTION composition adapter, built through internal/app, not a
	// test-local bridge. This is the whole point: if the composition boundary lacked
	// the adapter, this test could not compile or wire the step at all.
	hintPort, err := app.NewHintPort(app.HintConfig{BaseURL: server.URL, Token: hintE2EToken})
	if err != nil {
		t.Fatalf("compose production Hint port: %v", err)
	}
	if hintPort == nil {
		t.Fatal("composition returned a nil Hint port")
	}

	manifests := newHintE2EManifestReader(fixture.manifest)
	jobReader := newHintE2EJobReader(fixture.job)
	store, err := postgres.NewRefreshRepository(pool)
	if err != nil {
		t.Fatalf("construct PostgreSQL RefreshRepository: %v", err)
	}
	step, err := acquisition.NewRefreshStep(
		manifests, jobReader, hintE2EBindingReader{binding: fixture.binding}, hintPort, store,
	)
	if err != nil {
		t.Fatalf("construct RefreshStep: %v", err)
	}

	// ---- Phase 1: exactly one trusted Hint, one atomic durable handoff --------
	now := time.Now().UTC()
	result, err := step.Submit(ctx, acquisition.RefreshRequest{
		ManifestID: hintE2EManifestID, JobID: hintE2EJobID, Owner: hintE2EOwner,
		ExpectedClaim: hintE2EClaimGeneration, Now: now, RetryAt: now.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("RefreshStep.Submit() error = %v", err)
	}
	if !result.Changed {
		t.Fatal("RefreshStep.Submit() reported Changed=false for the first handoff")
	}
	if result.Manifest.ID != hintE2EManifestID || result.Manifest.State != acquisition.StateAwaitingCanonical {
		t.Fatalf("returned Manifest = %#v, want %s/%s", result.Manifest, hintE2EManifestID, acquisition.StateAwaitingCanonical)
	}
	if result.Job.ID != hintE2EJobID || result.Job.State != jobs.StateRetryWait {
		t.Fatalf("returned Job = %#v, want %s/%s", result.Job, hintE2EJobID, jobs.StateRetryWait)
	}
	if result.Job.State == jobs.StateSucceeded {
		t.Fatalf("stage handoff marked Job %s SUCCEEDED", result.Job.ID)
	}
	if result.Job.ClaimAttempts != hintE2EClaimGeneration || result.Job.FailureCount != hintE2EFailureBudget ||
		result.Job.MaxAttempts != hintE2EMaxAttempts {
		t.Fatalf("returned Job counters = claim %d, failures %d, max %d; want %d/%d/%d",
			result.Job.ClaimAttempts, result.Job.FailureCount, result.Job.MaxAttempts,
			hintE2EClaimGeneration, hintE2EFailureBudget, hintE2EMaxAttempts)
	}

	// The endpoint saw exactly one faithfully-formed trusted Hint.
	endpoint.assertSingleHintRequest(t, fixture.binding.IndexCoreRootID, fixture.manifest.TargetPath)

	// The handoff must be durable, so read it back with real SQL.
	durable := hintE2EReadDurableState(t, ctx, pool)
	hintE2EAssertHandoffCommitted(t, durable)

	// ---- Phase 2 (Issue #39 requirement 21): the committed handoff replays as a
	// durable no-op and never submits a second Hint ----------------------------
	//
	// A real lease-holding worker re-reads durable state before a later attempt;
	// these readers are the local doubles the task requires, so refresh them from
	// PostgreSQL to present what a next attempt would actually see.
	if err := manifests.setDurableState(ctx, pool); err != nil {
		t.Fatalf("reload durable Manifest state: %v", err)
	}
	if err := jobReader.setDurableState(ctx, pool); err != nil {
		t.Fatalf("reload durable Job state: %v", err)
	}
	// Prove the replay really observes the committed pair. This is what makes the
	// "no second Hint" assertion meaningful: the step must take its durable-replay
	// shortcut, not rely on the store refusing a second handoff after a call.
	if manifests.state() != acquisition.StateAwaitingCanonical || jobReader.state() != jobs.StateRetryWait {
		t.Fatalf("readers did not observe the committed handoff before replay: %s/%s",
			manifests.state(), jobReader.state())
	}
	replayNow := time.Now().UTC()
	replay, err := step.Submit(ctx, acquisition.RefreshRequest{
		ManifestID: hintE2EManifestID, JobID: hintE2EJobID, Owner: hintE2EOwner,
		ExpectedClaim: hintE2EClaimGeneration, Now: replayNow, RetryAt: replayNow.Add(15 * time.Minute),
	})
	if err != nil {
		t.Fatalf("refresh replay error = %v", err)
	}
	if replay.Changed {
		t.Fatal("refresh replay rewrote an already-committed handoff")
	}
	if replay.Manifest.State != acquisition.StateAwaitingCanonical || replay.Job.State != jobs.StateRetryWait {
		t.Fatalf("replay returned %s/%s, want %s/%s",
			replay.Manifest.State, replay.Job.State, acquisition.StateAwaitingCanonical, jobs.StateRetryWait)
	}
	endpoint.assertSingleHintRequest(t, fixture.binding.IndexCoreRootID, fixture.manifest.TargetPath)

	replayed := hintE2EReadDurableState(t, ctx, pool)
	if replayed.manifestState != durable.manifestState || replayed.jobState != durable.jobState ||
		replayed.claimAttempts != durable.claimAttempts || replayed.attemptCount != durable.attemptCount ||
		replayed.maxAttempts != durable.maxAttempts || !hintE2ENullTimesEqual(replayed.nextAttempt, durable.nextAttempt) {
		t.Fatalf("refresh replay changed durable state: before %#v, after %#v", durable, replayed)
	}

	t.Logf("controlled IndexCore Hint observation complete: one Hint, atomic handoff %s -> %s, no Panta -> OpenList call",
		acquisition.StateAwaitingVisibility, acquisition.StateAwaitingCanonical)
}

// TestAcquisitionRefreshOwnsNoOpenListClient is Issue #39 requirement 25. It is a
// real source-level assertion over this revision, so it runs even when no database
// is configured.
//
// The accepted boundary is:
//
//	Storage -> OpenList -> IndexCore Collector -> Canonical/Journal -> Panta
//
// Panta must therefore never observe OpenList directly during acquisition: the
// refresh path hands an IndexCore-owned root/scope to the trusted Hint endpoint and
// nothing else. Concretely this asserts:
//
//	(a) no production .go file under internal/acquisition or internal/store/postgres
//	    imports any OpenList package;
//	(b) the transitive module-local dependency closure of those two packages never
//	    reaches an OpenList package;
//	(c) internal/integrations/openlist is contracts-only on this revision: it ships
//	    no client*.go and no production file imports net/http, so no OpenList HTTP
//	    client exists for Panta to call.
func TestAcquisitionRefreshOwnsNoOpenListClient(t *testing.T) {
	repoRoot := hintE2ERepoRoot(t)
	const modulePrefix = "github.com/nathanxiangang-web/panta"
	roots := []string{
		filepath.Join(repoRoot, "internal", "acquisition"),
		filepath.Join(repoRoot, "internal", "store", "postgres"),
	}

	// (a) direct production imports.
	for _, root := range roots {
		files := hintE2EProductionGoFiles(t, root)
		if len(files) == 0 {
			t.Fatalf("no production Go files found under %s", root)
		}
		for _, file := range files {
			for _, imported := range hintE2EImports(t, file) {
				if strings.Contains(imported, "openlist") {
					t.Fatalf("%s imports forbidden OpenList dependency %s", file, imported)
				}
			}
		}
	}

	// (b) transitive module-local closure.
	visited := map[string]bool{}
	queue := append([]string(nil), roots...)
	for len(queue) > 0 {
		dir := queue[0]
		queue = queue[1:]
		if visited[dir] {
			continue
		}
		visited[dir] = true
		for _, file := range hintE2EProductionGoFiles(t, dir) {
			for _, imported := range hintE2EImports(t, file) {
				if strings.Contains(imported, "openlist") {
					t.Fatalf("acquisition refresh dependency closure reaches OpenList dependency %s via %s", imported, file)
				}
				if !strings.HasPrefix(imported, modulePrefix+"/") {
					continue
				}
				queue = append(queue, filepath.Join(repoRoot, filepath.FromSlash(strings.TrimPrefix(imported, modulePrefix+"/"))))
			}
		}
	}
	if len(visited) < len(roots) {
		t.Fatalf("local dependency walk visited only %d packages, want at least %d", len(visited), len(roots))
	}

	// (c) the OpenList integration is contracts-only: no client, no HTTP transport.
	openListDir := filepath.Join(repoRoot, "internal", "integrations", "openlist")
	entries, err := os.ReadDir(openListDir)
	if err != nil {
		t.Fatalf("scan OpenList integration directory %s: %v", openListDir, err)
	}
	productionFiles := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		productionFiles++
		if strings.HasPrefix(name, "client") {
			t.Fatalf("OpenList integration file %s declares an HTTP client; Panta must not own a Panta -> OpenList client for the acquisition refresh path", name)
		}
		file := filepath.Join(openListDir, name)
		for _, imported := range hintE2EImports(t, file) {
			if imported == "net/http" {
				t.Fatalf("OpenList integration file %s imports net/http; this revision must expose contracts only, with no HTTP client", file)
			}
		}
	}
	if productionFiles == 0 {
		t.Fatalf("OpenList integration directory %s has no production files", openListDir)
	}
}

// TestIndexCoreHintStandInContract proves the stand-in used by the controlled E2E
// is not a rubber stamp: it enforces the same rejections as IndexCore's real P9
// transport, so a passing E2E means the real client sent an acceptable request
// rather than that anything was accepted.
func TestIndexCoreHintStandInContract(t *testing.T) {
	endpoint := newHintE2EEndpoint(t, hintE2EToken)
	server := httptest.NewServer(endpoint)
	t.Cleanup(server.Close)

	validBody := fmt.Sprintf(`{"root_id":%q,"scope_key":%q,"reason":"POSSIBLE_CHANGE"}`, hintE2ERootID, hintE2ETargetPath)

	post := func(authorization, contentType, body string) (int, map[string]any) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, server.URL+indexcore.HintPath, strings.NewReader(body))
		if err != nil {
			t.Fatalf("build Hint request: %v", err)
		}
		if authorization != "" {
			request.Header.Set("Authorization", authorization)
		}
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("send Hint request: %v", err)
		}
		defer func() { _ = response.Body.Close() }()
		var decoded map[string]any
		_ = json.NewDecoder(response.Body).Decode(&decoded)
		return response.StatusCode, decoded
	}

	if status, body := post("", "application/json", validBody); status != http.StatusUnauthorized || body["error"] != "unauthorized" {
		t.Fatalf("unauthenticated request = %d %v, want 401 unauthorized", status, body)
	}
	if status, body := post("Bearer "+hintE2EToken, "text/plain", validBody); status != http.StatusUnsupportedMediaType || body["error"] != "unsupported_media_type" {
		t.Fatalf("wrong content type = %d %v, want 415 unsupported_media_type", status, body)
	}
	if status, body := post("Bearer "+hintE2EToken, "application/json",
		`{"root_id":"r","scope_key":"/a","reason":"POSSIBLE_CHANGE","extra":1}`); status != http.StatusBadRequest || body["error"] != "invalid_request" {
		t.Fatalf("unknown field = %d %v, want 400 invalid_request", status, body)
	}
	if status, body := post("Bearer "+hintE2EToken, "application/json",
		`{"root_id":"r","scope_key":"/a","reason":"POSSIBLE_CHANGE"}{"root_id":"r"}`); status != http.StatusBadRequest || body["error"] != "invalid_request" {
		t.Fatalf("trailing content = %d %v, want 400 invalid_request", status, body)
	}
	for _, scopeKey := range []string{"", "relative/path", "/trailing/", "//empty", "/dot/../path", `\backslash`} {
		payload := fmt.Sprintf(`{"root_id":%q,"scope_key":%q,"reason":"POSSIBLE_CHANGE"}`, hintE2ERootID, scopeKey)
		if status, body := post("Bearer "+hintE2EToken, "application/json", payload); status != http.StatusBadRequest || body["error"] != "invalid_request" {
			t.Fatalf("scope key %q = %d %v, want 400 invalid_request", scopeKey, status, body)
		}
	}
	if status, body := post("Bearer "+hintE2EToken, "application/json",
		`{"root_id":"   ","scope_key":"/a","reason":"POSSIBLE_CHANGE"}`); status != http.StatusBadRequest || body["error"] != "invalid_request" {
		t.Fatalf("blank root_id = %d %v, want 400 invalid_request", status, body)
	}
	status, body := post("Bearer "+hintE2EToken, "application/json; charset=utf-8",
		`{"root_id":"r","scope_key":"/","reason":"POSSIBLE_CHANGE"}`)
	if status != http.StatusAccepted || body["status"] != "accepted" || body["work_state"] != "PENDING" {
		t.Fatalf("root scope = %d %v, want 202 accepted/PENDING", status, body)
	}
	if body["root_id"] != "r" || body["scope_key"] != "/" {
		t.Fatalf("accepted receipt did not echo root/scope: %v", body)
	}
	if signal, ok := body["signal_seq"].(float64); !ok || signal < 1 {
		t.Fatalf("accepted receipt signal_seq = %v, want >= 1", body["signal_seq"])
	}

	for _, test := range []struct {
		name   string
		method string
		path   string
		want   int
	}{
		{name: "wrong method", method: http.MethodGet, path: indexcore.HintPath, want: http.StatusMethodNotAllowed},
		{name: "wrong path", method: http.MethodPost, path: "/internal/v1/other", want: http.StatusNotFound},
	} {
		request, err := http.NewRequest(test.method, server.URL+test.path, strings.NewReader(validBody))
		if err != nil {
			t.Fatalf("build %s request: %v", test.name, err)
		}
		request.Header.Set("Authorization", "Bearer "+hintE2EToken)
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatalf("send %s request: %v", test.name, err)
		}
		_ = response.Body.Close()
		if response.StatusCode != test.want {
			t.Fatalf("%s = %d, want %d", test.name, response.StatusCode, test.want)
		}
	}
}

// ---------------------------------------------------------------------------
// IndexCore P9 trusted Hint stand-in
// ---------------------------------------------------------------------------

// hintE2EObservedRequest is one inbound request as the stand-in saw it.
type hintE2EObservedRequest struct {
	method        string
	path          string
	authorization string
	contentType   string
	body          []byte
}

// hintE2EResponse is IndexCore's accepted-ingest receipt body.
type hintE2EResponse struct {
	Status    string `json:"status"`
	RootID    string `json:"root_id"`
	ScopeKey  string `json:"scope_key"`
	WorkState string `json:"work_state"`
	SignalSeq int64  `json:"signal_seq"`
}

// hintE2EEndpoint is a faithful stand-in for IndexCore's P9 trusted Hint
// transport (IndexCore internal/transport/hintapi/server.go). It mirrors:
//
//   - exactly one method-scoped route, POST /internal/v1/mutation-hints;
//   - bearer authentication before any body work, with a >= 32 byte token and
//     401 {"error":"unauthorized"} plus WWW-Authenticate: Bearer on failure;
//   - Content-Type parsed with mime.ParseMediaType, 415
//     {"error":"unsupported_media_type"} otherwise;
//   - a 4096 byte body limit, json.Decoder + DisallowUnknownFields, and trailing
//     content rejection, all mapped to 400 {"error":"invalid_request"};
//   - root_id non-blank and IndexCore's P3 scope-key rule;
//   - 202 {"status":"accepted","root_id":<echo>,"scope_key":<echo>,
//     "work_state":"PENDING","signal_seq":<n>} on success.
type hintE2EEndpoint struct {
	token string
	mux   *http.ServeMux

	mu        sync.Mutex
	signalSeq int64
	requests  []hintE2EObservedRequest
}

func newHintE2EEndpoint(t *testing.T, token string) *hintE2EEndpoint {
	t.Helper()
	if len(token) < 32 {
		t.Fatalf("Hint stand-in token must be at least 32 bytes, got %d", len(token))
	}
	endpoint := &hintE2EEndpoint{token: token, mux: http.NewServeMux()}
	// The real P9 transport registers one method-scoped route on a fresh ServeMux,
	// so a wrong method or path fails with the same 404/405.
	endpoint.mux.HandleFunc("POST "+indexcore.HintPath, endpoint.handleIngest)
	return endpoint
}

// ServeHTTP records every inbound request before routing, so an assertion of
// "exactly one request" covers even a malformed or unrouted call.
func (endpoint *hintE2EEndpoint) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(io.LimitReader(request.Body, hintE2EMaxBodyBytes+1))
	request.Body = io.NopCloser(bytes.NewReader(body))
	endpoint.mu.Lock()
	endpoint.requests = append(endpoint.requests, hintE2EObservedRequest{
		method:        request.Method,
		path:          request.URL.Path,
		authorization: request.Header.Get("Authorization"),
		contentType:   request.Header.Get("Content-Type"),
		body:          body,
	})
	endpoint.mu.Unlock()
	endpoint.mux.ServeHTTP(w, request)
}

func (endpoint *hintE2EEndpoint) handleIngest(w http.ResponseWriter, request *http.Request) {
	if !endpoint.authenticate(w, request) {
		return
	}
	mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		hintE2EWriteError(w, http.StatusUnsupportedMediaType, "unsupported_media_type")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, request.Body, hintE2EMaxBodyBytes))
	decoder.DisallowUnknownFields()
	var decoded struct {
		RootID   string `json:"root_id"`
		ScopeKey string `json:"scope_key"`
		Reason   string `json:"reason"`
	}
	if err := decoder.Decode(&decoded); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			hintE2EWriteError(w, http.StatusRequestEntityTooLarge, "request_too_large")
			return
		}
		hintE2EWriteError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if decoder.More() {
		hintE2EWriteError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		hintE2EWriteError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	if !hintE2EValidRequest(decoded.RootID, decoded.ScopeKey, decoded.Reason) {
		hintE2EWriteError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	hintE2EWriteJSON(w, http.StatusAccepted, hintE2EResponse{
		Status:    "accepted",
		RootID:    decoded.RootID,
		ScopeKey:  decoded.ScopeKey,
		WorkState: "PENDING",
		SignalSeq: endpoint.nextSignalSeq(),
	})
}

// authenticate mirrors hintapi.authenticate: hash both sides to a fixed width so
// the comparison never leaks the token length.
func (endpoint *hintE2EEndpoint) authenticate(w http.ResponseWriter, request *http.Request) bool {
	const prefix = "Bearer "
	authorization := request.Header.Get("Authorization")
	supplied := sha256.Sum256([]byte(strings.TrimPrefix(authorization, prefix)))
	expected := sha256.Sum256([]byte(endpoint.token))
	if !strings.HasPrefix(authorization, prefix) || subtle.ConstantTimeCompare(supplied[:], expected[:]) != 1 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		hintE2EWriteError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}

func (endpoint *hintE2EEndpoint) nextSignalSeq() int64 {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	endpoint.signalSeq++
	return endpoint.signalSeq
}

func (endpoint *hintE2EEndpoint) snapshot() []hintE2EObservedRequest {
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	return append([]hintE2EObservedRequest(nil), endpoint.requests...)
}

// assertSingleHintRequest proves the stand-in saw exactly one request and that it
// was the exact trusted Hint the frozen D-029 mapping requires.
func (endpoint *hintE2EEndpoint) assertSingleHintRequest(t *testing.T, wantRootID, wantScopeKey string) {
	t.Helper()
	requests := endpoint.snapshot()
	if len(requests) != 1 {
		t.Fatalf("IndexCore Hint stand-in observed %d inbound requests, want exactly 1: %#v", len(requests), requests)
	}
	observed := requests[0]
	if observed.method != http.MethodPost {
		t.Fatalf("Hint method = %q, want POST", observed.method)
	}
	if observed.path != indexcore.HintPath {
		t.Fatalf("Hint path = %q, want %q", observed.path, indexcore.HintPath)
	}
	if observed.authorization != "Bearer "+hintE2EToken {
		t.Fatalf("Hint Authorization = %q, want the configured bearer token", observed.authorization)
	}
	if observed.contentType != "application/json" {
		t.Fatalf("Hint Content-Type = %q, want application/json", observed.contentType)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(observed.body, &fields); err != nil {
		t.Fatalf("Hint body is not JSON: %v (%s)", err, observed.body)
	}
	if len(fields) != 3 {
		t.Fatalf("Hint body carried %d fields, want exactly root_id/scope_key/reason: %s", len(fields), observed.body)
	}
	for _, required := range []string{"root_id", "scope_key", "reason"} {
		if _, exists := fields[required]; !exists {
			t.Fatalf("Hint body is missing %q: %s", required, observed.body)
		}
	}
	var decoded struct {
		RootID   string `json:"root_id"`
		ScopeKey string `json:"scope_key"`
		Reason   string `json:"reason"`
	}
	if err := json.Unmarshal(observed.body, &decoded); err != nil {
		t.Fatalf("decode Hint body: %v", err)
	}
	if decoded.RootID != wantRootID {
		t.Fatalf("Hint root_id = %q, want the binding root %q", decoded.RootID, wantRootID)
	}
	if decoded.ScopeKey != wantScopeKey {
		t.Fatalf("Hint scope_key = %q, want the Manifest target path %q", decoded.ScopeKey, wantScopeKey)
	}
	// Guard the frozen Panta-owned reason vocabulary, then assert what was sent.
	const wantReason = "POSSIBLE_CHANGE"
	if string(acquisition.MutationHintPossibleChange) != wantReason {
		t.Fatalf("acquisition.MutationHintPossibleChange = %q, want %q",
			acquisition.MutationHintPossibleChange, wantReason)
	}
	if decoded.Reason != wantReason {
		t.Fatalf("Hint reason = %q, want %q", decoded.Reason, wantReason)
	}
	// The provider scope and the OpenList mount are not observation identity.
	for _, forbidden := range []string{hintE2EProviderScope, hintE2EOpenListMount, "provider_scope", "openlist_mount_path"} {
		if strings.Contains(string(observed.body), forbidden) {
			t.Fatalf("Hint body leaked unrelated identity %q: %s", forbidden, observed.body)
		}
	}
}

// hintE2EValidRequest mirrors hintapi.validateRequest, including the P8 reason
// vocabulary, so the stand-in is not weaker than the accepted transport.
func hintE2EValidRequest(rootID, scopeKey, reason string) bool {
	if strings.TrimSpace(rootID) == "" {
		return false
	}
	if !hintE2EValidScopeKey(scopeKey) {
		return false
	}
	switch reason {
	case "", "POSSIBLE_CHANGE", "DELETE_HINT", "MOVE_UNCERTAIN", "METADATA_UNCERTAIN":
		return true
	default:
		return false
	}
}

// hintE2EValidScopeKey mirrors IndexCore's state.ValidateScopeKey exactly:
// "" invalid; "/" valid; else root-absolute, no trailing slash, no backslash, and
// no empty, "." or ".." component.
func hintE2EValidScopeKey(key string) bool {
	if key == "" {
		return false
	}
	if key == "/" {
		return true
	}
	if !strings.HasPrefix(key, "/") {
		return false
	}
	if strings.HasSuffix(key, "/") {
		return false
	}
	if strings.Contains(key, `\`) {
		return false
	}
	for _, segment := range strings.Split(key[1:], "/") {
		switch segment {
		case "", ".", "..":
			return false
		}
	}
	return true
}

func hintE2EWriteJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func hintE2EWriteError(w http.ResponseWriter, status int, code string) {
	hintE2EWriteJSON(w, status, map[string]string{"error": code})
}

// ---------------------------------------------------------------------------
// Local read doubles (Manifest/Job/Binding) over durable state
// ---------------------------------------------------------------------------

type hintE2EManifestReader struct {
	mu       sync.Mutex
	manifest acquisition.Manifest
}

var _ acquisition.ManifestReader = (*hintE2EManifestReader)(nil)

func newHintE2EManifestReader(manifest acquisition.Manifest) *hintE2EManifestReader {
	return &hintE2EManifestReader{manifest: manifest}
}

func (reader *hintE2EManifestReader) GetManifest(_ context.Context, id acquisition.ManifestID) (acquisition.Manifest, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.manifest.ID != id {
		return acquisition.Manifest{}, fmt.Errorf("%w: %s", acquisition.ErrNotFound, id)
	}
	return reader.manifest, nil
}

// setDurableState refreshes the cached milestone from PostgreSQL, which is what a
// later worker attempt would read.
func (reader *hintE2EManifestReader) setDurableState(ctx context.Context, pool *pgxpool.Pool) error {
	var state string
	if err := pool.QueryRow(ctx,
		`SELECT state FROM acquisition_manifests WHERE manifest_id = $1`,
		string(reader.manifest.ID)).Scan(&state); err != nil {
		return err
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.manifest.State = acquisition.State(state)
	return nil
}

// state reports the cached milestone, used to prove the replay observed durable
// state rather than a stale in-memory value.
func (reader *hintE2EManifestReader) state() acquisition.State {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.manifest.State
}

type hintE2EJobReader struct {
	mu  sync.Mutex
	job jobs.Job
}

var _ acquisition.JobReader = (*hintE2EJobReader)(nil)

func newHintE2EJobReader(job jobs.Job) *hintE2EJobReader {
	return &hintE2EJobReader{job: job}
}

func (reader *hintE2EJobReader) Get(_ context.Context, id jobs.JobID) (jobs.Job, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.job.ID != id {
		return jobs.Job{}, fmt.Errorf("%w: %s", jobs.ErrNotFound, id)
	}
	return reader.job, nil
}

// setDurableState refreshes the cached Job state from PostgreSQL.
func (reader *hintE2EJobReader) setDurableState(ctx context.Context, pool *pgxpool.Pool) error {
	var state string
	if err := pool.QueryRow(ctx,
		`SELECT state FROM jobs WHERE job_id = $1`,
		string(reader.job.ID)).Scan(&state); err != nil {
		return err
	}
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.job.State = jobs.State(state)
	return nil
}

// state reports the cached Job state for the replay precondition assertion.
func (reader *hintE2EJobReader) state() jobs.State {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.job.State
}

type hintE2EBindingReader struct {
	binding storage.Binding
}

var _ acquisition.RefreshBindingReader = hintE2EBindingReader{}

func (reader hintE2EBindingReader) GetBinding(_ context.Context, id storage.BindingID) (storage.Binding, error) {
	if reader.binding.ID != id {
		return storage.Binding{}, storage.ErrNotFound
	}
	return reader.binding, nil
}

// ---------------------------------------------------------------------------
// Database fixture
// ---------------------------------------------------------------------------

// hintE2EFixture is the seeded durable state: one ACTIVE binding, one
// AWAITING_VISIBILITY Manifest, and its linked RUNNING ACQUISITION Job holding an
// unexpired fenced lease.
type hintE2EFixture struct {
	binding  storage.Binding
	manifest acquisition.Manifest
	job      jobs.Job
}

func newHintE2EFixture() *hintE2EFixture {
	seededAt := time.Date(2026, 9, 30, 3, 0, 0, 0, time.UTC)
	providerScope := hintE2EProviderScope
	owner := hintE2EOwner
	jobID := hintE2EJobID
	idempotencyKey := acquisition.AcquisitionJobIdempotencyKey(hintE2EManifestID)
	leaseExpiresAt := seededAt.Add(time.Hour)
	startedAt := seededAt
	return &hintE2EFixture{
		binding: storage.Binding{
			ID:                hintE2EBindingID,
			ConnectionID:      hintE2EConnectionID,
			ProviderScope:     &providerScope,
			OpenListMountPath: hintE2EOpenListMount,
			IndexCoreRootID:   hintE2ERootID,
			Status:            storage.BindingStatusActive,
			CreatedAt:         seededAt,
			UpdatedAt:         seededAt,
		},
		manifest: acquisition.Manifest{
			ID:                     hintE2EManifestID,
			SourceType:             "opaque-provider-source",
			SourceRef:              "opaque-provider-ref",
			TargetStorageBindingID: hintE2EBindingID,
			TargetPath:             hintE2ETargetPath,
			// D-032: request intent, carried as ordinary fixture data.
			// (identity is not required by the handoff)
			ExpectedName: hintNamePointer(hintE2EExpectedName),
			JobID:        &jobID,
			State:        acquisition.StateAwaitingVisibility,
			CreatedAt:    seededAt,
			UpdatedAt:    seededAt,
		},
		job: jobs.Job{
			ID:             hintE2EJobID,
			Type:           acquisition.JobTypeAcquisition,
			Payload:        json.RawMessage(fmt.Sprintf(`{"manifest_id":%q,"schema_version":1}`, hintE2EManifestID)),
			State:          jobs.StateRunning,
			IdempotencyKey: &idempotencyKey,
			ClaimAttempts:  hintE2EClaimGeneration,
			FailureCount:   hintE2EFailureBudget,
			MaxAttempts:    hintE2EMaxAttempts,
			LeaseOwner:     &owner,
			LeaseExpiresAt: &leaseExpiresAt,
			CreatedAt:      seededAt,
			UpdatedAt:      seededAt,
			StartedAt:      &startedAt,
		},
	}
}

func (fixture *hintE2EFixture) seed(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_connections (storage_connection_id, provider_type, credential_ref, status, created_at, updated_at)
VALUES ($1, 'controlled-openlist', NULL, 'ACTIVE', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		string(fixture.binding.ConnectionID)); err != nil {
		t.Fatalf("seed StorageConnection: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_bindings (
    storage_binding_id, storage_connection_id, openlist_mount_path, provider_scope,
    indexcore_root_id, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, 'ACTIVE', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		string(fixture.binding.ID), string(fixture.binding.ConnectionID), fixture.binding.OpenListMountPath,
		fixture.binding.ProviderScope, fixture.binding.IndexCoreRootID); err != nil {
		t.Fatalf("seed StorageBinding: %v", err)
	}
	// The lease expiry is database time, because the RefreshRepository authorizes
	// the fence with database time too.
	if _, err := pool.Exec(ctx, `
INSERT INTO jobs (
    job_id, job_type, payload, state, idempotency_key, claim_attempts, attempt_count, max_attempts,
    next_attempt_at, lease_owner, lease_expires_at, last_error, created_at, updated_at, started_at, finished_at
) VALUES ($1, $2, $3::jsonb, 'RUNNING', $4, $5, $6, $7, NULL, $8,
          CURRENT_TIMESTAMP + interval '1 hour', NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL)`,
		string(fixture.job.ID), fixture.job.Type, string(fixture.job.Payload), *fixture.job.IdempotencyKey,
		fixture.job.ClaimAttempts, fixture.job.FailureCount, fixture.job.MaxAttempts,
		*fixture.job.LeaseOwner); err != nil {
		t.Fatalf("seed ACQUISITION Job: %v", err)
	}
	// D-032: request intent is seeded as fixture data. The Gate 3.8 handoff does
	// not require it, but a realistic AWAITING_VISIBILITY row is seeded with
	// request intent.
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, user_id, source_type, source_ref, expected_name, target_storage_binding_id,
    target_path, asset_id, release_id, variant_id, job_id, state, created_at, updated_at
) VALUES ($1, NULL, $2, $3, $8, $4, $5, NULL, NULL, NULL, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		string(fixture.manifest.ID), fixture.manifest.SourceType, fixture.manifest.SourceRef,
		string(fixture.manifest.TargetStorageBindingID), fixture.manifest.TargetPath,
		string(*fixture.manifest.JobID), string(fixture.manifest.State),
		hintE2EExpectedName); err != nil {
		t.Fatalf("seed acquisition Manifest: %v", err)
	}
}

// hintNamePointer returns a pointer to a copy, so the fixture literal stays
// immutable and no caller can mutate shared state through it.
func hintNamePointer(name string) *string { return &name }

// assertSeeded proves the literal fixture also satisfies the frozen Gate 3.2
// linkage contract and that the durable rows match it exactly.
func (fixture *hintE2EFixture) assertSeeded(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if err := acquisition.ValidateLinkedAcquisitionJob(fixture.manifest.ID, fixture.job); err != nil {
		t.Fatalf("fixture Job does not satisfy the frozen ACQUISITION linkage contract: %v", err)
	}
	var manifestState, manifestJobID string
	if err := pool.QueryRow(ctx,
		`SELECT state, job_id::text FROM acquisition_manifests WHERE manifest_id = $1`,
		string(fixture.manifest.ID)).Scan(&manifestState, &manifestJobID); err != nil {
		t.Fatalf("read seeded Manifest: %v", err)
	}
	if manifestState != string(acquisition.StateAwaitingVisibility) || manifestJobID != string(fixture.job.ID) {
		t.Fatalf("seeded Manifest = %s/%s, want %s/%s", manifestState, manifestJobID,
			acquisition.StateAwaitingVisibility, fixture.job.ID)
	}

	var jobType, jobState, idempotencyKey, leaseOwner string
	var payload []byte
	var claimAttempts, attemptCount, maxAttempts int
	if err := pool.QueryRow(ctx, `
SELECT job_type, payload, state, idempotency_key, claim_attempts, attempt_count, max_attempts, lease_owner
FROM jobs WHERE job_id = $1`, string(fixture.job.ID)).Scan(
		&jobType, &payload, &jobState, &idempotencyKey, &claimAttempts, &attemptCount, &maxAttempts, &leaseOwner); err != nil {
		t.Fatalf("read seeded Job: %v", err)
	}
	if jobType != acquisition.JobTypeAcquisition || jobState != string(jobs.StateRunning) ||
		idempotencyKey != acquisition.AcquisitionJobIdempotencyKey(fixture.manifest.ID) ||
		claimAttempts != hintE2EClaimGeneration || attemptCount != hintE2EFailureBudget ||
		maxAttempts != hintE2EMaxAttempts || leaseOwner != *fixture.job.LeaseOwner {
		t.Fatalf("seeded Job = type=%s state=%s key=%s claim=%d budget=%d max=%d owner=%s",
			jobType, jobState, idempotencyKey, claimAttempts, attemptCount, maxAttempts, leaseOwner)
	}
	var payloadFields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &payloadFields); err != nil {
		t.Fatalf("seeded Job payload is not JSON: %v", err)
	}
	if len(payloadFields) != 2 {
		t.Fatalf("seeded Job payload carried %d fields, want manifest_id/schema_version: %s", len(payloadFields), payload)
	}
	var decoded struct {
		ManifestID    string `json:"manifest_id"`
		SchemaVersion int    `json:"schema_version"`
	}
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode seeded Job payload: %v", err)
	}
	if decoded.ManifestID != string(fixture.manifest.ID) || decoded.SchemaVersion != acquisition.AcquisitionPayloadSchemaVersion {
		t.Fatalf("seeded Job payload = %s, want manifest_id %s and schema_version %d",
			payload, fixture.manifest.ID, acquisition.AcquisitionPayloadSchemaVersion)
	}
	var leaseLive bool
	if err := pool.QueryRow(ctx,
		`SELECT lease_expires_at > CURRENT_TIMESTAMP FROM jobs WHERE job_id = $1`,
		string(fixture.job.ID)).Scan(&leaseLive); err != nil {
		t.Fatalf("read seeded lease expiry: %v", err)
	}
	if !leaseLive {
		t.Fatal("seeded RUNNING lease is already expired")
	}
}

// hintE2EDurableState is the durable PostgreSQL state read back with real SQL.
type hintE2EDurableState struct {
	manifestState string
	manifestJobID string
	jobState      string
	claimAttempts int
	attemptCount  int
	maxAttempts   int
	leaseOwner    sql.NullString
	leaseExpires  sql.NullTime
	nextAttempt   sql.NullTime
	jobCount      int
	succeededJobs int
}

func hintE2EReadDurableState(t *testing.T, ctx context.Context, pool *pgxpool.Pool) hintE2EDurableState {
	t.Helper()
	var state hintE2EDurableState
	if err := pool.QueryRow(ctx,
		`SELECT state, job_id::text FROM acquisition_manifests WHERE manifest_id = $1`,
		string(hintE2EManifestID)).Scan(&state.manifestState, &state.manifestJobID); err != nil {
		t.Fatalf("read durable Manifest state: %v", err)
	}
	if err := pool.QueryRow(ctx, `
SELECT state, claim_attempts, attempt_count, max_attempts, lease_owner, lease_expires_at, next_attempt_at
FROM jobs WHERE job_id = $1`, string(hintE2EJobID)).Scan(
		&state.jobState, &state.claimAttempts, &state.attemptCount, &state.maxAttempts,
		&state.leaseOwner, &state.leaseExpires, &state.nextAttempt); err != nil {
		t.Fatalf("read durable Job state: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs`).Scan(&state.jobCount); err != nil {
		t.Fatalf("count durable Jobs: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE state = 'SUCCEEDED'`).Scan(&state.succeededJobs); err != nil {
		t.Fatalf("count SUCCEEDED Jobs: %v", err)
	}
	return state
}

// hintE2EAssertHandoffCommitted proves the durable post-handoff shape.
func hintE2EAssertHandoffCommitted(t *testing.T, state hintE2EDurableState) {
	t.Helper()
	if state.manifestState != string(acquisition.StateAwaitingCanonical) {
		t.Fatalf("durable Manifest state = %s, want %s", state.manifestState, acquisition.StateAwaitingCanonical)
	}
	if state.manifestState == string(acquisition.StateReady) {
		t.Fatal("durable Manifest is READY: a Hint is ingest proof, never canonical truth")
	}
	if state.manifestJobID != string(hintE2EJobID) {
		t.Fatalf("durable Manifest job_id = %s, want %s", state.manifestJobID, hintE2EJobID)
	}
	if state.jobState != string(jobs.StateRetryWait) {
		t.Fatalf("durable Job state = %s, want %s", state.jobState, jobs.StateRetryWait)
	}
	if state.jobState == string(jobs.StateSucceeded) {
		t.Fatalf("durable Job %s is SUCCEEDED; a stage handoff must not terminate it", hintE2EJobID)
	}
	if state.claimAttempts != hintE2EClaimGeneration {
		t.Fatalf("durable claim generation = %d, want unchanged %d", state.claimAttempts, hintE2EClaimGeneration)
	}
	if state.attemptCount != hintE2EFailureBudget {
		t.Fatalf("durable failure budget = %d, want unchanged %d", state.attemptCount, hintE2EFailureBudget)
	}
	if state.maxAttempts != hintE2EMaxAttempts {
		t.Fatalf("durable max attempts = %d, want unchanged %d", state.maxAttempts, hintE2EMaxAttempts)
	}
	if state.leaseOwner.Valid || state.leaseExpires.Valid {
		t.Fatalf("durable lease was not released: owner=%v expires=%v", state.leaseOwner, state.leaseExpires)
	}
	if !state.nextAttempt.Valid {
		t.Fatal("durable Job is RETRY_WAIT without next_attempt_at")
	}
	if state.jobCount != 1 {
		t.Fatalf("durable Job count = %d, want the same single Job", state.jobCount)
	}
	if state.succeededJobs != 0 {
		t.Fatalf("durable SUCCEEDED Job count = %d, want 0", state.succeededJobs)
	}
}

func hintE2ENullTimesEqual(first, second sql.NullTime) bool {
	if first.Valid != second.Valid {
		return false
	}
	return !first.Valid || first.Time.Equal(second.Time)
}

// hintE2EPool connects to the opt-in dedicated E2E database.
//
// This test resets the schema of the database it is given, so it must never share a
// database with the parallel internal/store/postgres suite: that suite's own schema
// reset deadlocks against this test's seeding. It therefore uses its own opt-in
// environment variable and its own disposable database, exactly like the pinned
// Gate 1 controlled observation E2E uses PANTA_GATE1_PANTA_DATABASE_URL.
func hintE2EPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("PANTA_GATE3_HINT_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("PANTA_GATE3_HINT_DATABASE_URL is not set; set it to a dedicated disposable *_e2e database to run the controlled IndexCore Hint observation E2E")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse PANTA_GATE3_HINT_DATABASE_URL: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_e2e") && !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("refusing destructive E2E test for database %q: name must end with _e2e or _test",
			config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect E2E database: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping E2E database: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func resetHintE2ESchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, "DROP SCHEMA public CASCADE; CREATE SCHEMA public"); err != nil {
		t.Fatalf("reset E2E schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA public CASCADE; CREATE SCHEMA public")
	})
}

// ---------------------------------------------------------------------------
// Source-level OpenList boundary helpers
// ---------------------------------------------------------------------------

// hintE2ERepoRoot walks up from the test working directory (the package directory)
// to the Panta module root.
func hintE2ERepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get test working directory: %v", err)
	}
	for {
		if info, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil && !info.IsDir() {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate the Panta module root from the test working directory")
		}
		dir = parent
	}
}

// hintE2EProductionGoFiles lists non-test Go files directly inside one directory.
func hintE2EProductionGoFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read package directory %s: %v", dir, err)
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		files = append(files, filepath.Join(dir, name))
	}
	return files
}

// hintE2EImports parses only the import block of one Go file.
func hintE2EImports(t *testing.T, path string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse imports of %s: %v", path, err)
	}
	imports := make([]string, 0, len(file.Imports))
	for _, spec := range file.Imports {
		value, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			t.Fatalf("unquote import in %s: %v", path, err)
		}
		imports = append(imports, value)
	}
	return imports
}
