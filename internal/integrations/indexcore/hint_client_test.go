package indexcore_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
)

const hintTokenValue = "0123456789abcdef0123456789abcdef"

type hintRecorder struct {
	mu       sync.Mutex
	calls    int
	method   string
	path     string
	body     []byte
	headers  http.Header
	response string
	status   int
	retryAt  string
	// fixedBody disables the echo behavior so an exact response body can be asserted.
	fixedBody bool
}

func (rec *hintRecorder) snapshot() (int, string, string, []byte, http.Header) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.calls, rec.method, rec.path, append([]byte(nil), rec.body...), rec.headers.Clone()
}

func newHintServer(t *testing.T, rec *hintRecorder) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		rec.mu.Lock()
		rec.calls++
		rec.method = request.Method
		rec.path = request.URL.Path
		rec.body = body
		rec.headers = request.Header.Clone()
		status := rec.status
		response := rec.response
		retryAt := rec.retryAt
		rec.mu.Unlock()
		if status == 0 {
			status = http.StatusAccepted
		}
		// A real IndexCore echoes the accepted root and scope. When no explicit body
		// was configured, mirror that so the declared request is the one asserted.
		if response == "" && !rec.fixedBody {
			var submitted struct {
				RootID   string `json:"root_id"`
				ScopeKey string `json:"scope_key"`
			}
			if err := json.Unmarshal(body, &submitted); err == nil && submitted.RootID != "" {
				response = acceptedBody(submitted.RootID, submitted.ScopeKey, 1)
			}
		}
		if retryAt != "" {
			writer.Header().Set("Retry-After", retryAt)
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(status)
		_, _ = io.WriteString(writer, response)
	}))
	t.Cleanup(server.Close)
	return server
}

func acceptedBody(rootID, scopeKey string, signalSeq int64) string {
	payload, _ := json.Marshal(map[string]any{
		"status": "accepted", "root_id": rootID, "scope_key": scopeKey,
		"work_state": "PENDING", "signal_seq": signalSeq,
	})
	return string(payload)
}

func newHintClient(t *testing.T, baseURL string, options ...func(*indexcore.HintConfig)) *indexcore.HintClient {
	t.Helper()
	config := indexcore.HintConfig{BaseURL: baseURL, Token: hintTokenValue}
	for _, option := range options {
		option(&config)
	}
	client, err := indexcore.NewHintClient(config)
	if err != nil {
		t.Fatalf("NewHintClient() error = %v", err)
	}
	return client
}

var validHint = indexcore.HintRequest{
	RootID: "root-115-a", ScopeKey: "/downloads/movies", Reason: indexcore.HintReasonPossibleChange,
}

// --- 5: exact method/path/content-type/body ---------------------------------

func TestSubmitHintSendsExactRequest(t *testing.T) {
	rec := &hintRecorder{response: acceptedBody(validHint.RootID, validHint.ScopeKey, 7)}
	server := newHintServer(t, rec)
	client := newHintClient(t, server.URL)

	receipt, err := client.SubmitHint(context.Background(), validHint)
	if err != nil {
		t.Fatalf("SubmitHint() error = %v", err)
	}
	calls, method, path, body, headers := rec.snapshot()
	if calls != 1 {
		t.Fatalf("calls = %d, want exactly 1", calls)
	}
	if method != http.MethodPost {
		t.Fatalf("method = %q, want POST", method)
	}
	if path != "/internal/v1/mutation-hints" {
		t.Fatalf("path = %q, want /internal/v1/mutation-hints", path)
	}
	if got := headers.Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("body is not JSON: %v (%s)", err, body)
	}
	if len(decoded) != 3 {
		t.Fatalf("body carried %d fields, want exactly root_id/scope_key/reason: %s", len(decoded), body)
	}
	if decoded["root_id"] != "root-115-a" || decoded["scope_key"] != "/downloads/movies" ||
		decoded["reason"] != "POSSIBLE_CHANGE" {
		t.Fatalf("body = %s", body)
	}
	// No provider, OpenList, or unrelated control field may appear.
	for _, forbidden := range []string{"provider_scope", "openlist_mount_path", "mount", "path", "target_path", "task_id", "refresh"} {
		if _, exists := decoded[forbidden]; exists {
			t.Fatalf("body carried unrelated field %q", forbidden)
		}
	}
	if receipt.Status != "accepted" || receipt.RootID != "root-115-a" ||
		receipt.ScopeKey != "/downloads/movies" || receipt.WorkState != "PENDING" || receipt.SignalSeq != 7 {
		t.Fatalf("receipt = %+v", receipt)
	}
}

func TestSubmitHintPreservesBasePath(t *testing.T) {
	rec := &hintRecorder{response: acceptedBody(validHint.RootID, validHint.ScopeKey, 1)}
	server := newHintServer(t, rec)
	client := newHintClient(t, server.URL+"/idx/")

	if _, err := client.SubmitHint(context.Background(), validHint); err != nil {
		t.Fatalf("SubmitHint() error = %v", err)
	}
	_, _, path, _, _ := rec.snapshot()
	if path != "/idx/internal/v1/mutation-hints" {
		t.Fatalf("path = %q, want the configured base path preserved", path)
	}
}

// --- 6: bearer header exactly, never leaked ---------------------------------

func TestSubmitHintSendsBearerExactlyAndNeverLeaksIt(t *testing.T) {
	rec := &hintRecorder{response: acceptedBody(validHint.RootID, validHint.ScopeKey, 1)}
	server := newHintServer(t, rec)
	client := newHintClient(t, server.URL)

	if _, err := client.SubmitHint(context.Background(), validHint); err != nil {
		t.Fatalf("SubmitHint() error = %v", err)
	}
	_, _, _, _, headers := rec.snapshot()
	if got := headers.Get("Authorization"); got != "Bearer "+hintTokenValue {
		t.Fatalf("Authorization = %q, want exactly %q", got, "Bearer "+hintTokenValue)
	}

	// Every typed failure path must redact the token.
	tests := []struct {
		name    string
		status  int
		body    string
		retryAt string
	}{
		{name: "400", status: http.StatusBadRequest, body: `{"error":"invalid_request"}`},
		{name: "401", status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`},
		{name: "413", status: http.StatusRequestEntityTooLarge, body: `{"error":"request_too_large"}`},
		{name: "415", status: http.StatusUnsupportedMediaType, body: `{"error":"unsupported_media_type"}`},
		{name: "429", status: http.StatusTooManyRequests, body: `{"error":"busy"}`, retryAt: "2"},
		{name: "503", status: http.StatusServiceUnavailable, body: `{"error":"ingest_unavailable"}`},
		{name: "unexpected 200", status: http.StatusOK, body: `{"status":"accepted"}`},
		{name: "unexpected 500", status: http.StatusInternalServerError, body: `oops`},
		// A hostile remote that echoes the token back must not surface it.
		{name: "leaky 400", status: http.StatusBadRequest, body: `{"error":"` + hintTokenValue + `"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			inner := &hintRecorder{status: test.status, response: test.body, retryAt: test.retryAt}
			innerServer := newHintServer(t, inner)
			innerClient := newHintClient(t, innerServer.URL)
			_, err := innerClient.SubmitHint(context.Background(), validHint)
			if err == nil {
				t.Fatal("SubmitHint() succeeded, want a typed failure")
			}
			if strings.Contains(err.Error(), hintTokenValue) {
				t.Fatalf("error %q leaks the configured token", err)
			}
		})
	}
}

// --- 9-10: typed failure classification and Retry-After ---------------------

func TestSubmitHintClassifiesFailuresDistinctly(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantKind error
	}{
		{name: "400 invalid request", status: http.StatusBadRequest, body: `{"error":"invalid_request"}`, wantKind: indexcore.ErrHintInvalidRequest},
		{name: "401 unauthorized", status: http.StatusUnauthorized, body: `{"error":"unauthorized"}`, wantKind: indexcore.ErrHintUnauthorized},
		{name: "413 too large", status: http.StatusRequestEntityTooLarge, body: `{"error":"request_too_large"}`, wantKind: indexcore.ErrHintRequestTooLarge},
		{name: "415 unsupported media type", status: http.StatusUnsupportedMediaType, body: `{"error":"unsupported_media_type"}`, wantKind: indexcore.ErrHintUnsupportedType},
		{name: "429 busy", status: http.StatusTooManyRequests, body: `{"error":"busy"}`, wantKind: indexcore.ErrHintBusy},
		{name: "503 ingest unavailable", status: http.StatusServiceUnavailable, body: `{"error":"ingest_unavailable"}`, wantKind: indexcore.ErrHintUnavailable},
		{name: "unexpected status", status: http.StatusTeapot, body: `{"error":"weird"}`, wantKind: indexcore.ErrHintUnexpectedState},
		// A non-JSON body must still classify by status, not become malformed.
		{name: "html 503", status: http.StatusServiceUnavailable, body: `<html>down</html>`, wantKind: indexcore.ErrHintUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &hintRecorder{status: test.status, response: test.body, fixedBody: true}
			server := newHintServer(t, rec)
			client := newHintClient(t, server.URL)

			_, err := client.SubmitHint(context.Background(), validHint)
			if !errors.Is(err, test.wantKind) {
				t.Fatalf("error = %v, want %v", err, test.wantKind)
			}
			var hintErr *indexcore.HintError
			if !errors.As(err, &hintErr) {
				t.Fatalf("error = %v, want *HintError", err)
			}
			if hintErr.StatusCode != test.status {
				t.Fatalf("status = %d, want %d", hintErr.StatusCode, test.status)
			}
			// The failure kinds are distinct: no two share a sentinel.
			for _, other := range []error{
				indexcore.ErrHintInvalidRequest, indexcore.ErrHintUnauthorized,
				indexcore.ErrHintRequestTooLarge, indexcore.ErrHintUnsupportedType,
				indexcore.ErrHintBusy, indexcore.ErrHintUnavailable,
			} {
				if other == test.wantKind {
					continue
				}
				if errors.Is(err, other) {
					t.Fatalf("error %v also matches unrelated sentinel %v", err, other)
				}
			}
		})
	}
}

func TestSubmitHintPreservesRetryAfterWithoutSleeping(t *testing.T) {
	tests := []struct {
		name        string
		header      string
		wantRaw     string
		wantSeconds int64
		wantDelta   bool
	}{
		{name: "delta seconds", header: "3", wantRaw: "3", wantSeconds: 3, wantDelta: true},
		{name: "zero", header: "0", wantRaw: "0", wantSeconds: 0, wantDelta: true},
		{name: "http date preserved raw", header: "Wed, 30 Sep 2026 10:00:00 GMT", wantRaw: "Wed, 30 Sep 2026 10:00:00 GMT"},
		{name: "negative preserved raw", header: "-5", wantRaw: "-5"},
		{name: "garbage preserved raw", header: "soon", wantRaw: "soon"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &hintRecorder{status: http.StatusTooManyRequests, response: `{"error":"busy"}`, retryAt: test.header}
			server := newHintServer(t, rec)
			client := newHintClient(t, server.URL)

			start := time.Now()
			_, err := client.SubmitHint(context.Background(), validHint)
			elapsed := time.Since(start)
			if !errors.Is(err, indexcore.ErrHintBusy) {
				t.Fatalf("error = %v, want ErrHintBusy", err)
			}
			var hintErr *indexcore.HintError
			if !errors.As(err, &hintErr) || hintErr.RetryAfter == nil {
				t.Fatalf("error = %v, want RetryAfter preserved", err)
			}
			if hintErr.RetryAfter.Raw != test.wantRaw {
				t.Fatalf("RetryAfter.Raw = %q, want %q", hintErr.RetryAfter.Raw, test.wantRaw)
			}
			if hintErr.RetryAfter.IsDelta != test.wantDelta || hintErr.RetryAfter.Seconds != test.wantSeconds {
				t.Fatalf("RetryAfter = %+v, want seconds=%d delta=%v",
					*hintErr.RetryAfter, test.wantSeconds, test.wantDelta)
			}
			// The client records backpressure and never sleeps on it.
			if elapsed > 2*time.Second {
				t.Fatalf("elapsed = %v, want no internal sleep", elapsed)
			}
		})
	}
}

// --- 7-8: strict accepted receipt -------------------------------------------

func TestSubmitHintRequiresAcceptedMatchingReceipt(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		fixedBody bool
	}{
		{name: "wrong status", body: acceptedBody(validHint.RootID, validHint.ScopeKey, 1)[:0] +
			`{"status":"queued","root_id":"root-115-a","scope_key":"/downloads/movies","work_state":"PENDING","signal_seq":1}`},
		{name: "mismatched root", body: acceptedBody("root-other", validHint.ScopeKey, 1)},
		{name: "mismatched scope", body: acceptedBody(validHint.RootID, "/downloads/other", 1)},
		{name: "missing work state", body: `{"status":"accepted","root_id":"root-115-a","scope_key":"/downloads/movies","signal_seq":1}`},
		{name: "missing root", body: `{"status":"accepted","scope_key":"/downloads/movies","work_state":"PENDING"}`},
		{name: "not json", body: `accepted`},
		{name: "trailing content", body: acceptedBody(validHint.RootID, validHint.ScopeKey, 1) + `{}`},
		{name: "array response", body: `[]`},
		{name: "empty body", body: ``, fixedBody: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := &hintRecorder{status: http.StatusAccepted, response: test.body, fixedBody: test.fixedBody}
			server := newHintServer(t, rec)
			client := newHintClient(t, server.URL)

			_, err := client.SubmitHint(context.Background(), validHint)
			if !errors.Is(err, indexcore.ErrHintMalformed) {
				t.Fatalf("error = %v, want ErrHintMalformed", err)
			}
		})
	}
}

func TestSubmitHintOversizedResponseFailsClosed(t *testing.T) {
	padding := strings.Repeat("a", 8192)
	body := `{"status":"accepted","root_id":"root-115-a","scope_key":"/downloads/movies","work_state":"` + padding + `","signal_seq":1}`
	rec := &hintRecorder{status: http.StatusAccepted, response: body}
	server := newHintServer(t, rec)
	client := newHintClient(t, server.URL, func(config *indexcore.HintConfig) { config.MaxBodyBytes = 512 })

	if _, err := client.SubmitHint(context.Background(), validHint); !errors.Is(err, indexcore.ErrHintMalformed) {
		t.Fatalf("error = %v, want ErrHintMalformed", err)
	}
}

// --- request validation ------------------------------------------------------

func TestSubmitHintRejectsInvalidRequestsLocally(t *testing.T) {
	rec := &hintRecorder{response: acceptedBody(validHint.RootID, validHint.ScopeKey, 1)}
	server := newHintServer(t, rec)
	client := newHintClient(t, server.URL)

	requests := []indexcore.HintRequest{
		{RootID: "", ScopeKey: "/a", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "   ", ScopeKey: "/a", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "relative", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "/trailing/", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "/a//b", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "/a/./b", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "/a/../b", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: `/a\b`, Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "/a\x00b", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "/a", Reason: indexcore.HintReason("DELETE_HINT")},
		{RootID: "root", ScopeKey: "/a", Reason: ""},
		{RootID: strings.Repeat("r", 600), ScopeKey: "/a", Reason: indexcore.HintReasonPossibleChange},
		{RootID: "root", ScopeKey: "/" + strings.Repeat("s", 3000), Reason: indexcore.HintReasonPossibleChange},
	}
	for _, request := range requests {
		if _, err := client.SubmitHint(context.Background(), request); !errors.Is(err, indexcore.ErrHintInvalidRequest) {
			t.Fatalf("SubmitHint(%+v) error = %v, want ErrHintInvalidRequest", request, err)
		}
	}
	if calls, _, _, _, _ := rec.snapshot(); calls != 0 {
		t.Fatalf("calls = %d, want 0 for locally rejected requests", calls)
	}
	// The root scope is the one accepted special case in IndexCore's rule.
	rootScope := indexcore.HintRequest{
		RootID: "root-115-a", ScopeKey: "/", Reason: indexcore.HintReasonPossibleChange,
	}
	rootRec := &hintRecorder{response: acceptedBody(rootScope.RootID, rootScope.ScopeKey, 1)}
	rootServer := newHintServer(t, rootRec)
	rootClient := newHintClient(t, rootServer.URL)
	if _, err := rootClient.SubmitHint(context.Background(), rootScope); err != nil {
		t.Fatalf("SubmitHint(root scope) error = %v", err)
	}
}

func TestNewHintClientRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		config indexcore.HintConfig
	}{
		{name: "empty base URL", config: indexcore.HintConfig{Token: hintTokenValue}},
		{name: "relative base URL", config: indexcore.HintConfig{BaseURL: "/idx", Token: hintTokenValue}},
		{name: "bad scheme", config: indexcore.HintConfig{BaseURL: "ftp://127.0.0.1:9100", Token: hintTokenValue}},
		{name: "with query", config: indexcore.HintConfig{BaseURL: "http://127.0.0.1:9100?x=1", Token: hintTokenValue}},
		{name: "short token", config: indexcore.HintConfig{BaseURL: "http://127.0.0.1:9100", Token: "tooshort"}},
		{name: "padded token", config: indexcore.HintConfig{BaseURL: "http://127.0.0.1:9100", Token: " " + hintTokenValue}},
		{name: "oversized body bound", config: indexcore.HintConfig{
			BaseURL: "http://127.0.0.1:9100", Token: hintTokenValue, MaxBodyBytes: 1 << 30}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := indexcore.NewHintClient(test.config); err == nil {
				t.Fatal("NewHintClient() succeeded, want a rejection")
			}
		})
	}
}

func TestSubmitHintTransportFailureIsTyped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	baseURL := server.URL
	server.Close()

	client := newHintClient(t, baseURL)
	if _, err := client.SubmitHint(context.Background(), validHint); !errors.Is(err, indexcore.ErrHintTransport) {
		t.Fatalf("error = %v, want ErrHintTransport", err)
	}
}

func TestSubmitHintHonoursContextCancellation(t *testing.T) {
	rec := &hintRecorder{response: acceptedBody(validHint.RootID, validHint.ScopeKey, 1)}
	server := newHintServer(t, rec)
	client := newHintClient(t, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := client.SubmitHint(ctx, validHint); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}
