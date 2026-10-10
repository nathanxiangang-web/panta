package p115

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SheltonZhu/115driver/pkg/driver"
)

type startRoundTripper func(*http.Request) (*http.Response, error)

func (transport startRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestPinnedStartResponseDecodeDiagnosticHasStatusWithoutBody(t *testing.T) {
	for _, test := range []struct{ name, body, causeType, stage string }{
		{"malformed encrypted data", `{"state":true,"data":"%%%PRIVATE-RESPONSE"}`, "base64.CorruptInputError", "OFFLINE_POST_BASE64_CRYPTO"},
		{"HTTP 200 malformed JSON", `<html>PRIVATE-RESPONSE</html>`, "*json.SyntaxError", "OFFLINE_POST_OUTER_JSON"},
		{"valid JSON invalid outer schema", `{"state":"PRIVATE-RESPONSE"}`, "*json.UnmarshalTypeError", "OFFLINE_POST_OUTER_JSON"},
		// Synthetic RSA ciphertext (integer 9), not an account response. The
		// pinned crypto decoder produces 93 non-JSON bytes, reaching Unmarshal.
		{"decrypted malformed JSON", `{"state":true,"data":"AAk="}`, "*json.SyntaxError", "OFFLINE_POST_DECRYPTED_JSON"},
	} {
		t.Run(test.name, func(t *testing.T) {
			trace := &startHTTPTrace{}
			calls := 0
			transport := startRoundTripper(func(request *http.Request) (*http.Response, error) {
				calls++
				if request.Header.Get("User-Agent") != driver.UADefault {
					t.Fatalf("wire User-Agent = %q, want pinned driver's default", request.Header.Get("User-Agent"))
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
					Body: io.NopCloser(strings.NewReader(test.body)), Request: request}, nil
			})
			client := newDriverClient(&http.Client{Timeout: time.Second, Transport: statusTransport{next: transport, trace: trace}})
			client.UserID = 1 // Avoid lazy user lookup; all requests stay in the fake transport.
			backend, err := NewCookiedBackend(client)
			if err != nil {
				t.Fatal(err)
			}
			backend.trace = trace
			adapter, err := New(Options{Backend: backend})
			if err != nil {
				t.Fatal(err)
			}
			_, err = adapter.StartDownload(context.Background(), validRequest())
			var diagnostic *StartDiagnostic
			if !errors.As(err, &diagnostic) || diagnostic.DiagnosticCategory() != "RESPONSE_DECODE_FAILURE" ||
				diagnostic.DiagnosticHTTPStatus() != 200 || diagnostic.DiagnosticCauseType() != test.causeType || diagnostic.DiagnosticStage() != test.stage {
				t.Fatalf("diagnostic = %v", err)
			}
			if calls != 1 || strings.Contains(err.Error(), "PRIVATE-RESPONSE") {
				t.Fatal("retry or response disclosure")
			}
		})
	}
}

func TestPinnedLazyGetUserFailureNeverReachesOfflinePost(t *testing.T) {
	trace := &startHTTPTrace{}
	posts := 0
	transport := startRoundTripper(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodPost {
			posts++
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader("PRIVATE-RESPONSE")), Request: request}, nil
	})
	client := newDriverClient(&http.Client{Timeout: time.Second, Transport: statusTransport{next: transport, trace: trace}})
	backend, _ := NewCookiedBackend(client)
	backend.trace = trace
	_, err := backend.AddOfflineTaskURI(context.Background(), "synthetic-source", "synthetic-scope")
	var diagnostic *StartDiagnostic
	if !errors.As(err, &diagnostic) || diagnostic.DiagnosticStage() != "USER_INFO_GET" || diagnostic.DiagnosticResponseShape() != "TEXT" || posts != 0 {
		t.Fatalf("diagnostic=%v posts=%d", err, posts)
	}
}

func TestPinnedLazyGetUserSuccessThenOfflineOuterJSONFailure(t *testing.T) {
	trace := &startHTTPTrace{}
	methods := []string{}
	transport := startRoundTripper(func(request *http.Request) (*http.Response, error) {
		methods = append(methods, request.Method)
		body := `{"state":true,"data":{"user_id":1}}`
		if request.Method == http.MethodPost {
			body = "PRIVATE-RESPONSE"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
	client := newDriverClient(&http.Client{Timeout: time.Second, Transport: statusTransport{next: transport, trace: trace}})
	backend, _ := NewCookiedBackend(client)
	backend.trace = trace
	_, err := backend.AddOfflineTaskURI(context.Background(), "synthetic-source", "synthetic-scope")
	var diagnostic *StartDiagnostic
	if !errors.As(err, &diagnostic) || diagnostic.DiagnosticStage() != "OFFLINE_POST_OUTER_JSON" || len(methods) != 2 || methods[0] != http.MethodGet || methods[1] != http.MethodPost {
		t.Fatalf("diagnostic=%v methods=%v", err, methods)
	}
}

func TestParserBoundaryDoesNotGuessWithoutCompleteOuterResponse(t *testing.T) {
	var value any
	cause := json.Unmarshal([]byte("not JSON"), &value)
	for _, test := range []struct {
		observed, valid bool
		want            string
	}{
		{true, false, "OFFLINE_POST_OUTER_JSON"},
		{true, true, "OFFLINE_POST_DECRYPTED_JSON"},
		{false, false, "UNKNOWN"},
	} {
		trace := &startHTTPTrace{operation: "OFFLINE_POST", observed: test.observed, outerValid: test.valid, outerParsed: test.valid, shape: "UNKNOWN"}
		trace.status.Store(200)
		diagnostic := newStartDiagnostic(cause, time.Millisecond)
		diagnostic.locate(cause, trace)
		if diagnostic.DiagnosticStage() != test.want {
			t.Fatalf("stage=%s want=%s", diagnostic.DiagnosticStage(), test.want)
		}
	}
}

func TestResponseTracePreservesBodyAndClearsBoundedBuffer(t *testing.T) {
	for _, content := range []string{`{"state":true}`, strings.Repeat("x", startResponseLimit+1)} {
		trace := &startHTTPTrace{}
		body := &tracedBody{ReadCloser: io.NopCloser(strings.NewReader(content)), trace: trace}
		got, err := io.ReadAll(body)
		if err != nil || string(got) != content {
			t.Fatal("trace altered SDK response")
		}
		body.Close()
		if body.buffer != nil {
			t.Fatal("raw response retained")
		}
		if trace.observed != (len(content) <= startResponseLimit) {
			t.Fatal("oversize response was classified")
		}
	}
}
