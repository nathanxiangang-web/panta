package p115

import (
	"net/http"
	"sync/atomic"
)

// startHTTPTrace records only the final HTTP status. It does not inspect or
// retain headers, URLs, cookies, request bodies, or response bodies.
type startHTTPTrace struct{ status atomic.Int32 }

type statusTransport struct {
	next  http.RoundTripper
	trace *startHTTPTrace
}

func (transport statusTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	transport.trace.status.Store(0)
	response, err := transport.next.RoundTrip(request)
	if response != nil {
		transport.trace.status.Store(int32(response.StatusCode))
	}
	return response, err
}
