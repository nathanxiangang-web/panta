package p115

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"time"

	"github.com/SheltonZhu/115driver/pkg/driver"
)

// StartDiagnostic is the only operator-facing account of a failed 115 task
// submission. Upstream errors can contain the entire response body (including
// the submitted URL), so Error never renders the underlying error. Is retains
// matching against known typed causes without exposing that error via Unwrap.
type StartDiagnostic struct {
	category   string
	elapsed    time.Duration
	base       error
	matches    []error
	causeType  string
	httpStatus int
	stage      string
	shape      string
}

func (diagnostic *StartDiagnostic) Error() string {
	return "115 StartDownload failed: " + diagnostic.category
}

func (diagnostic *StartDiagnostic) DiagnosticCategory() string       { return diagnostic.category }
func (diagnostic *StartDiagnostic) DiagnosticElapsed() time.Duration { return diagnostic.elapsed }
func (diagnostic *StartDiagnostic) DiagnosticCauseType() string      { return diagnostic.causeType }
func (diagnostic *StartDiagnostic) DiagnosticHTTPStatus() int        { return diagnostic.httpStatus }
func (diagnostic *StartDiagnostic) DiagnosticStage() string          { return diagnostic.stage }
func (diagnostic *StartDiagnostic) DiagnosticResponseShape() string  { return diagnostic.shape }

func (diagnostic *StartDiagnostic) locate(cause error, trace *startHTTPTrace) {
	diagnostic.httpStatus = int(trace.status.Load())
	trace.mu.Lock()
	defer trace.mu.Unlock()
	diagnostic.shape = trace.shape
	diagnostic.stage = "UNKNOWN"
	if trace.operation == "USER_INFO_GET" {
		diagnostic.stage = "USER_INFO_GET"
		return
	}
	if trace.operation != "OFFLINE_POST" {
		return
	}
	if diagnostic.httpStatus == 0 || diagnostic.category == "NETWORK_FAILURE" || diagnostic.category == "TIMEOUT" || diagnostic.category == "CONTEXT_CANCELED" {
		diagnostic.stage = "OFFLINE_POST_TRANSPORT"
		return
	}
	var syntax *json.SyntaxError
	var typed *json.UnmarshalTypeError
	var encoded base64.CorruptInputError
	jsonFailure := errors.As(cause, &syntax) || errors.As(cause, &typed)
	switch {
	case trace.observed && !trace.outerParsed && jsonFailure:
		diagnostic.stage = "OFFLINE_POST_OUTER_JSON"
	case errors.As(cause, &encoded):
		diagnostic.stage = "OFFLINE_POST_BASE64_CRYPTO"
	case trace.observed && trace.outerParsed && jsonFailure:
		diagnostic.stage = "OFFLINE_POST_DECRYPTED_JSON"
	}
}

func (diagnostic *StartDiagnostic) Is(target error) bool {
	if errors.Is(diagnostic.base, target) {
		return true
	}
	for _, known := range diagnostic.matches {
		if target == known {
			return true
		}
	}
	return false
}

func newStartDiagnostic(cause error, elapsed time.Duration) *StartDiagnostic {
	diagnostic := &StartDiagnostic{category: startFailureCategory(cause), elapsed: elapsed, base: ErrBackendStart, causeType: rootCauseType(cause)}
	for _, known := range []error{
		context.Canceled, context.DeadlineExceeded,
		driver.ErrNotLogin, driver.ErrCredentialInvalid, driver.ErrSessionExited,
		driver.ErrOfflineInvalidLink, driver.ErrOfflineTaskExisted,
		driver.ErrOfflineNoTimes, driver.ErrWrongParams, driver.ErrUnexpected,
	} {
		if errors.Is(cause, known) {
			diagnostic.matches = append(diagnostic.matches, known)
		}
	}
	return diagnostic
}

// %T renders a compiled Go type, never the error's value or response body.
func rootCauseType(err error) string {
	for i := 0; i < 16; i++ {
		next := errors.Unwrap(err)
		if next == nil {
			break
		}
		err = next
	}
	return fmt.Sprintf("%T", err)
}

func newReferenceDiagnostic(cause error, elapsed time.Duration) *StartDiagnostic {
	category := "REFERENCE_INVALID"
	base := ErrInvalidTaskReference
	if errors.Is(cause, ErrTaskReferenceMissing) {
		category = "REFERENCE_MISSING"
		base = ErrTaskReferenceMissing
	}
	if errors.Is(cause, ErrTaskReferenceAmbiguous) {
		category = "REFERENCE_AMBIGUOUS"
		base = ErrTaskReferenceAmbiguous
	}
	return &StartDiagnostic{category: category, elapsed: elapsed, base: base, stage: "OFFLINE_POST_REFERENCE"}
}

func startFailureCategory(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "CONTEXT_CANCELED"
	case errors.Is(err, context.DeadlineExceeded):
		return "TIMEOUT"
	case errors.Is(err, driver.ErrNotLogin), errors.Is(err, driver.ErrCredentialInvalid),
		errors.Is(err, driver.ErrSessionExited):
		return "AUTH_REJECTED"
	case errors.Is(err, driver.ErrOfflineInvalidLink):
		return "SOURCE_REJECTED"
	case errors.Is(err, driver.ErrOfflineTaskExisted):
		return "TASK_ALREADY_EXISTS"
	case errors.Is(err, driver.ErrOfflineNoTimes):
		return "QUOTA_EXHAUSTED"
	case errors.Is(err, driver.ErrWrongParams):
		return "REQUEST_REJECTED"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return "DNS_FAILURE"
	}
	var certErr x509.UnknownAuthorityError
	if errors.As(err, &certErr) {
		return "TLS_FAILURE"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "TIMEOUT"
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return "NETWORK_FAILURE"
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "NETWORK_FAILURE"
	}
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError
	var base64Err base64.CorruptInputError
	if errors.As(err, &syntaxErr) || errors.As(err, &typeErr) || errors.As(err, &base64Err) {
		return "RESPONSE_DECODE_FAILURE"
	}
	if errors.Is(err, driver.ErrUnexpected) {
		return "PROVIDER_API_UNKNOWN"
	}
	return "BACKEND_UNKNOWN"
}
