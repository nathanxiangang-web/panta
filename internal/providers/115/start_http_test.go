package p115

import (
	"context"
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
	trace := &startHTTPTrace{}
	calls := 0
	transport := startRoundTripper(func(request *http.Request) (*http.Response, error) {
		calls++
		if request.Header.Get("User-Agent") != driver.UADefault {
			t.Fatalf("wire User-Agent = %q, want pinned driver's default", request.Header.Get("User-Agent"))
		}
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"state":true,"data":"%%%PRIVATE-RESPONSE"}`)), Request: request}, nil
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
		diagnostic.DiagnosticHTTPStatus() != 200 || diagnostic.DiagnosticCauseType() != "base64.CorruptInputError" {
		t.Fatalf("diagnostic = %v", err)
	}
	if calls != 1 || strings.Contains(err.Error(), "PRIVATE-RESPONSE") {
		t.Fatal("retry or response disclosure")
	}
}
