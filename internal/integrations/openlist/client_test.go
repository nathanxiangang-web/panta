package openlist_test

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

	"github.com/nathanxiangang-web/panta/internal/integrations/openlist"
)

var fixedNow = time.Date(2026, 9, 30, 10, 0, 0, 0, time.UTC)

// recorder captures exactly what the client sent so the wire contract can be
// asserted precisely.
type recorder struct {
	mu       sync.Mutex
	requests int
	path     string
	body     []byte
	headers  http.Header
}

func (rec *recorder) snapshot() (int, string, []byte, http.Header) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.requests, rec.path, append([]byte(nil), rec.body...), rec.headers.Clone()
}

func newServer(t *testing.T, status int, body string, rec *recorder) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		payload, _ := io.ReadAll(request.Body)
		rec.mu.Lock()
		rec.requests++
		rec.path = request.URL.Path
		rec.body = payload
		rec.headers = request.Header.Clone()
		rec.mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(status)
		_, _ = writer.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func newClient(t *testing.T, baseURL string, options ...func(*openlist.Config)) *openlist.HTTPClient {
	t.Helper()
	config := openlist.Config{BaseURL: baseURL, Now: func() time.Time { return fixedNow }}
	for _, option := range options {
		option(&config)
	}
	client, err := openlist.NewHTTPClient(config)
	if err != nil {
		t.Fatalf("NewHTTPClient() error = %v", err)
	}
	return client
}

func successBody(name string, size int64, isDir bool, modified string) string {
	object := map[string]any{"name": name, "size": size, "is_dir": isDir}
	if modified != "" {
		object["modified"] = modified
	}
	payload, _ := json.Marshal(map[string]any{"code": 200, "message": "success", "data": []any{object}})
	return string(payload)
}

func notFoundBody() string {
	return `{"code":500,"message":"object not found","data":null}`
}

// --- 5-7: wire contract ------------------------------------------------------

func TestStatSendsExactRequest(t *testing.T) {
	rec := &recorder{}
	server := newServer(t, http.StatusOK, successBody("item", 4096, false, ""), rec)
	client := newClient(t, server.URL)

	fact, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/115", Path: "/115/downloads/item"})
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if !fact.Visible || fact.Name != "item" || fact.SizeBytes != 4096 {
		t.Fatalf("Stat() = %#v", fact)
	}

	calls, path, body, headers := rec.snapshot()
	if calls != 1 {
		t.Fatalf("requests = %d, want exactly 1", calls)
	}
	// 5: exact POST /api/fs/get with exactly the requested path.
	if path != "/api/fs/get" {
		t.Fatalf("request path = %q, want /api/fs/get", path)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("request body is not JSON: %v (%s)", err, body)
	}
	if len(decoded) != 1 {
		t.Fatalf("request body carried %d fields, want exactly the path: %s", len(decoded), body)
	}
	if decoded["path"] != "/115/downloads/item" {
		t.Fatalf("request path field = %v, want the exact requested path", decoded["path"])
	}
	// No mutation or list fields may be present.
	for _, forbidden := range []string{"name", "page", "per_page", "refresh", "password", "as_task"} {
		if _, exists := decoded[forbidden]; exists {
			t.Fatalf("request body carried unrelated field %q", forbidden)
		}
	}
	if headers.Get("Content-Type") != "application/json" {
		t.Fatalf("content type = %q", headers.Get("Content-Type"))
	}
	if headers.Get("Authorization") != "" {
		t.Fatal("client sent an Authorization header without a configured token")
	}
	// The fact echoes the Panta-owned coordinates.
	if fact.Mount != "/115" || fact.Path != "/115/downloads/item" {
		t.Fatalf("fact identity = %s%s", fact.Mount, fact.Path)
	}
	if !fact.ObservedAt.Equal(fixedNow) {
		t.Fatalf("observed at = %v, want the injected clock", fact.ObservedAt)
	}
}

func TestStatPreservesConfiguredBasePath(t *testing.T) {
	rec := &recorder{}
	server := newServer(t, http.StatusOK, successBody("item", 1, false, ""), rec)
	client := newClient(t, server.URL+"/openlist/")

	if _, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/downloads/item"}); err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	_, path, _, _ := rec.snapshot()
	if path != "/openlist/api/fs/get" {
		t.Fatalf("request path = %q, want /openlist/api/fs/get", path)
	}
}

func TestStatSendsConfiguredTokenExactlyAndNeverLeaksIt(t *testing.T) {
	const token = "SENTINEL-OPENLIST-TOKEN"
	rec := &recorder{}
	server := newServer(t, http.StatusOK, successBody("item", 1, false, ""), rec)
	client := newClient(t, server.URL, func(config *openlist.Config) { config.Token = token })

	if _, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/downloads/item"}); err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	_, _, _, headers := rec.snapshot()
	if got := headers.Get("Authorization"); got != token {
		t.Fatalf("Authorization = %q, want the configured token", got)
	}

	// The token must never appear in an error, including a remote error whose message
	// echoes it.
	leaky := newServer(t, http.StatusOK, `{"code":401,"message":"unauthorized `+token+`","data":null}`, &recorder{})
	leakyClient := newClient(t, leaky.URL, func(config *openlist.Config) { config.Token = token })
	_, err := leakyClient.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/downloads/item"})
	if err == nil {
		t.Fatal("Stat() succeeded on a 401 envelope")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("error %q leaks the configured token", err)
	}
	// The client's own rendering must stay clean too.
	if strings.Contains(strings.Join([]string{err.Error()}, ""), token) {
		t.Fatal("error text leaks the token")
	}
}

// --- 8-12: envelope classification ------------------------------------------

func TestStatSuccessEnvelopeMapsToVisibleFact(t *testing.T) {
	rec := &recorder{}
	server := newServer(t, http.StatusOK, successBody("item.bin", 12345, true, "2026-09-30T09:00:00Z"), rec)
	client := newClient(t, server.URL)

	fact, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/115", Path: "/115/downloads/item.bin"})
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if !fact.Visible || fact.Name != "item.bin" || fact.SizeBytes != 12345 || !fact.Directory {
		t.Fatalf("fact = %#v", fact)
	}
	if fact.ModifiedAt == nil || !fact.ModifiedAt.Equal(time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("modified = %v, want the reported timestamp", fact.ModifiedAt)
	}
}

func TestStatQuotedSizeIsAccepted(t *testing.T) {
	body := `{"code":200,"data":[{"name":"big.bin","size":"9007199254740993","is_dir":false}]}`
	server := newServer(t, http.StatusOK, body, &recorder{})
	client := newClient(t, server.URL)

	fact, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/big.bin"})
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if fact.SizeBytes != 9007199254740993 {
		t.Fatalf("size = %d, want the quoted integer", fact.SizeBytes)
	}
}

func TestStatUnknownObjectFieldsAreTolerated(t *testing.T) {
	// Upstream also returns raw download URLs and signatures; they are irrelevant to
	// this gate and must not break the observation.
	body := `{"code":200,"data":[{"name":"item","size":10,"is_dir":false,"raw_url":"https://cdn.example/signed","sign":"abc"}]}`
	server := newServer(t, http.StatusOK, body, &recorder{})
	client := newClient(t, server.URL)

	fact, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/item"})
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if !fact.Visible {
		t.Fatal("a visible object with extra upstream fields must still be visible")
	}
}

func TestStatExactObjectNotFoundMapsToNotVisible(t *testing.T) {
	// Only the application envelope over HTTP 200 authorizes a not-visible
	// observation. A non-2xx response is an integration error even when its body
	// happens to carry the not-found sentinel, because the transport itself failed.
	server := newServer(t, http.StatusOK, notFoundBody(), &recorder{})
	client := newClient(t, server.URL)

	fact, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/115", Path: "/115/downloads/missing"})
	if err != nil {
		t.Fatalf("Stat() error = %v, want a normal not-visible observation", err)
	}
	if fact.Visible {
		t.Fatalf("fact = %#v, want Visible=false", fact)
	}
	if fact.Mount != "/115" || fact.Path != "/115/downloads/missing" {
		t.Fatalf("not-visible fact lost its identity: %#v", fact)
	}
	if fact.ObservedAt.IsZero() {
		t.Fatal("not-visible fact has no observation timestamp")
	}
}

func TestStatNon2xxIsAnIntegrationErrorEvenWithTheNotFoundSentinel(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone, http.StatusBadGateway} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := newServer(t, status, notFoundBody(), &recorder{})
			client := newClient(t, server.URL)

			fact, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/115", Path: "/115/downloads/missing"})
			if err == nil {
				t.Fatalf("Stat() succeeded with %#v, want a typed integration error", fact)
			}
			if fact.Visible {
				t.Fatal("a non-2xx response must never report Visible=true")
			}
			if !errors.Is(err, openlist.ErrRemote) {
				t.Fatalf("error = %v, want ErrRemote", err)
			}
		})
	}
}

func TestStatNeverCollapsesIntegrationFailuresIntoNotVisible(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "unauthorized envelope", status: http.StatusOK, body: `{"code":401,"message":"unauthorized"}`},
		{name: "forbidden envelope", status: http.StatusOK, body: `{"code":403,"message":"forbidden"}`},
		{name: "storage not ready", status: http.StatusOK, body: `{"code":500,"message":"storage not initialized"}`},
		{name: "provider failure", status: http.StatusOK, body: `{"code":500,"message":"failed to get object"}`},
		{name: "near-miss message", status: http.StatusOK, body: `{"code":500,"message":"Object not found"}`},
		{name: "message with suffix", status: http.StatusOK, body: `{"code":500,"message":"object not found in storage"}`},
		{name: "empty message", status: http.StatusOK, body: `{"code":500,"message":""}`},
		{name: "http 401", status: http.StatusUnauthorized, body: `{"message":"unauthorized"}`},
		{name: "http 403", status: http.StatusForbidden, body: `{"message":"forbidden"}`},
		{name: "http 500", status: http.StatusInternalServerError, body: `{"message":"boom"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t, test.status, test.body, &recorder{})
			client := newClient(t, server.URL)

			fact, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/downloads/item"})
			if err == nil {
				t.Fatalf("Stat() succeeded with %#v, want an integration error", fact)
			}
			if fact.Visible {
				t.Fatal("a failed observation must never report Visible=true")
			}
			if !errors.Is(err, openlist.ErrRemote) {
				t.Fatalf("error = %v, want ErrRemote", err)
			}
		})
	}
}

func TestStatMalformedResponsesFailClosed(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "not json", body: `not json at all`},
		{name: "truncated", body: `{"code":200,"data":[`},
		{name: "trailing document", body: `{"code":200,"data":[{"name":"x","size":1}]}{}`},
		{name: "trailing garbage", body: `{"code":200,"data":[{"name":"x","size":1}]} trailing`},
		{name: "code as string", body: `{"code":"200","data":[{"name":"x","size":1}]}`},
		{name: "zero objects on success", body: `{"code":200,"data":[]}`},
		{name: "two objects on success", body: `{"code":200,"data":[{"name":"x","size":1},{"name":"y","size":2}]}`},
		{name: "negative size", body: `{"code":200,"data":[{"name":"x","size":-5}]}`},
		{name: "non-numeric size", body: `{"code":200,"data":[{"name":"x","size":"big"}]}`},
		{name: "bad modified timestamp", body: `{"code":200,"data":[{"name":"x","size":1,"modified":"yesterday"}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t, http.StatusOK, test.body, &recorder{})
			client := newClient(t, server.URL)
			if _, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/x"}); !errors.Is(err, openlist.ErrMalformedResponse) {
				t.Fatalf("Stat() error = %v, want ErrMalformedResponse", err)
			}
		})
	}
}

func TestStatOversizedResponseFailsClosed(t *testing.T) {
	// A large but syntactically valid document must be rejected by the body bound
	// rather than buffered.
	huge := `{"code":200,"data":[{"name":"x","size":1,"padding":"` + strings.Repeat("a", 8192) + `"}]}`
	server := newServer(t, http.StatusOK, huge, &recorder{})
	client := newClient(t, server.URL, func(config *openlist.Config) { config.MaxBodyBytes = 1024 })

	if _, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/x"}); !errors.Is(err, openlist.ErrMalformedResponse) {
		t.Fatalf("Stat() error = %v, want ErrMalformedResponse for an oversized body", err)
	}
}

// --- 14: identity validation -------------------------------------------------

func TestStatMismatchedObjectNameFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		body string
		path string
	}{
		{name: "different basename", body: successBody("other.bin", 1, false, ""), path: "/downloads/item.bin"},
		{name: "empty name", body: `{"code":200,"data":[{"name":"","size":1}]}`, path: "/downloads/item.bin"},
		{name: "blank name", body: `{"code":200,"data":[{"name":"   ","size":1}]}`, path: "/downloads/item.bin"},
		{name: "full path instead of basename", body: successBody("/downloads/item.bin", 1, false, ""), path: "/downloads/item.bin"},
		{name: "name with NUL", body: `{"code":200,"data":[{"name":"ite\u0000m","size":1}]}`, path: "/item"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := newServer(t, http.StatusOK, test.body, &recorder{})
			client := newClient(t, server.URL)
			if _, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: test.path}); !errors.Is(err, openlist.ErrMalformedResponse) {
				t.Fatalf("Stat() error = %v, want ErrMalformedResponse", err)
			}
		})
	}
}

// --- request validation ------------------------------------------------------

func TestStatRejectsInvalidRequestsWithoutCallingOpenList(t *testing.T) {
	rec := &recorder{}
	server := newServer(t, http.StatusOK, successBody("item", 1, false, ""), rec)
	client := newClient(t, server.URL)

	requests := []openlist.StatRequest{
		{Mount: "", Path: "/item"},
		{Mount: "/", Path: ""},
		{Mount: "/", Path: "relative"},
		{Mount: "/", Path: "/a//b"},
		{Mount: "/", Path: "/a/./b"},
		{Mount: "/", Path: "/a/../b"},
		{Mount: "/", Path: "/bad\x00path"},
		{Mount: "/", Path: "/" + strings.Repeat("x", 5000)},
	}
	for _, request := range requests {
		if _, err := client.Stat(context.Background(), request); !errors.Is(err, openlist.ErrInvalidStatRequest) {
			t.Fatalf("Stat(%#v) error = %v, want ErrInvalidStatRequest", request, err)
		}
	}
	if calls, _, _, _ := rec.snapshot(); calls != 0 {
		t.Fatalf("requests = %d, want 0 for rejected requests", calls)
	}
}

func TestNewHTTPClientRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		config openlist.Config
	}{
		{name: "empty base URL", config: openlist.Config{}},
		{name: "relative base URL", config: openlist.Config{BaseURL: "/openlist"}},
		{name: "unsupported scheme", config: openlist.Config{BaseURL: "ftp://example.test"}},
		{name: "with query", config: openlist.Config{BaseURL: "https://example.test?x=1"}},
		{name: "NUL in base URL", config: openlist.Config{BaseURL: "https://exa\x00mple.test"}},
		{name: "body bound too large", config: openlist.Config{BaseURL: "https://example.test", MaxBodyBytes: 1 << 30}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := openlist.NewHTTPClient(test.config); !errors.Is(err, openlist.ErrInvalidConfig) {
				t.Fatalf("NewHTTPClient() error = %v, want ErrInvalidConfig", err)
			}
		})
	}
}

func TestStatTransportFailureIsTyped(t *testing.T) {
	// Point at a closed listener so the transport fails without a real network peer.
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	base := server.URL
	server.Close()

	client := newClient(t, base)
	if _, err := client.Stat(context.Background(), openlist.StatRequest{Mount: "/", Path: "/item"}); !errors.Is(err, openlist.ErrTransport) {
		t.Fatalf("Stat() error = %v, want ErrTransport", err)
	}
}

func TestStatHonoursContextCancellation(t *testing.T) {
	server := newServer(t, http.StatusOK, successBody("item", 1, false, ""), &recorder{})
	client := newClient(t, server.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := client.Stat(ctx, openlist.StatRequest{Mount: "/", Path: "/item"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stat() error = %v, want context.Canceled", err)
	}
}
