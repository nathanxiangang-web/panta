package indexcore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// RetryAfter is IndexCore's backpressure guidance from a 429. Raw preserves the
// header verbatim; Seconds is set only when the remote supplied a delta-seconds
// value. The client records this and never sleeps on it.
type RetryAfter struct {
	Raw     string
	Seconds int64
	IsDelta bool
}

func (value RetryAfter) String() string {
	if value.Raw != "" {
		return value.Raw
	}
	return strconv.FormatInt(value.Seconds, 10)
}

// HintConfig configures the trusted Mutation Hint client.
type HintConfig struct {
	// BaseURL is the absolute HTTP(S) address of IndexCore's separate loopback-only
	// Hint listener, for example http://127.0.0.1:9100.
	BaseURL string
	// Token is the trusted Hint bearer token. IndexCore requires at least 32 bytes.
	// It is sent verbatim and is never returned, logged, or embedded in an error.
	Token string
	// Timeout bounds one Hint submission. Zero selects a bounded default and any
	// value is capped.
	Timeout time.Duration
	// MaxBodyBytes bounds the response body. Zero uses a bounded default.
	MaxBodyBytes int64
	// HTTPClient overrides the transport for tests. The configured Timeout still
	// applies through a per-request deadline.
	HTTPClient *http.Client
}

// HintClient is the concrete trusted Mutation Hint HTTP adapter.
type HintClient struct {
	endpoint     string
	token        string
	httpClient   *http.Client
	timeout      time.Duration
	maxBodyBytes int64
}

var _ HintPort = (*HintClient)(nil)

const (
	hintDefaultTimeout = 10 * time.Second
	// hintMaximumTimeout bounds one submission even when the caller configures more.
	hintMaximumTimeout = time.Minute

	hintDefaultMaxBodyBytes = 1 << 16
	hintMaximumMaxBodyBytes = 1 << 20

	// hintMaximumBaseURLBytes bounds the configured address.
	hintMaximumBaseURLBytes = 2048
	// hintMinimumTokenBytes mirrors IndexCore's own P9 token requirement, so a
	// misconfigured Panta token fails locally instead of as a remote 401.
	hintMinimumTokenBytes = 32
	// hintMaximumTokenBytes bounds the configured token.
	hintMaximumTokenBytes = 4096
	// hintMaximumRootIDBytes bounds a root identity before encoding.
	hintMaximumRootIDBytes = 512
	// hintMaximumScopeKeyBytes bounds a scope key before encoding.
	hintMaximumScopeKeyBytes = 2048
)

// NewHintClient validates configuration and builds the client. Construction
// performs no network call.
func NewHintClient(config HintConfig) (*HintClient, error) {
	base, err := parseHintBaseURL(config.BaseURL)
	if err != nil {
		return nil, err
	}
	token := config.Token
	if len(token) < hintMinimumTokenBytes || len(token) > hintMaximumTokenBytes ||
		!utf8.ValidString(token) || strings.ContainsRune(token, '\x00') ||
		token != strings.TrimSpace(token) {
		return nil, fmt.Errorf("%w: token must be %d..%d bytes with no surrounding whitespace",
			ErrHintInvalidRequest, hintMinimumTokenBytes, hintMaximumTokenBytes)
	}
	timeout := hintEffectiveTimeout(config.Timeout)
	maxBody := config.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = hintDefaultMaxBodyBytes
	}
	if maxBody > hintMaximumMaxBodyBytes {
		return nil, fmt.Errorf("%w: max body bytes exceeds the supported bound", ErrHintInvalidRequest)
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	} else if httpClient.Timeout == 0 || httpClient.Timeout > timeout {
		httpClient.Timeout = timeout
	}
	base.Path = joinHintBasePath(base.Path, HintPath)
	return &HintClient{
		endpoint:     base.String(),
		token:        token,
		httpClient:   httpClient,
		timeout:      timeout,
		maxBodyBytes: maxBody,
	}, nil
}

func hintEffectiveTimeout(configured time.Duration) time.Duration {
	if configured <= 0 {
		return hintDefaultTimeout
	}
	if configured > hintMaximumTimeout {
		return hintMaximumTimeout
	}
	return configured
}

// parseHintBaseURL accepts only an absolute HTTP(S) URL without a query or fragment.
// The trusted listener is loopback-only on IndexCore's side; Panta does not weaken
// that boundary, and a non-loopback address is a deployment decision, not a client
// restriction.
func parseHintBaseURL(raw string) (*url.URL, error) {
	if raw == "" || len(raw) > hintMaximumBaseURLBytes || !utf8.ValidString(raw) ||
		strings.ContainsRune(raw, '\x00') {
		return nil, fmt.Errorf("%w: base URL is required", ErrHintInvalidRequest)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: base URL is not a URL", ErrHintInvalidRequest)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: base URL scheme must be http or https", ErrHintInvalidRequest)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("%w: base URL must be absolute", ErrHintInvalidRequest)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: base URL must not carry a query or fragment", ErrHintInvalidRequest)
	}
	return parsed, nil
}

// joinHintBasePath preserves a configured base path so a proxied deployment still
// reaches the Hint route.
func joinHintBasePath(basePath, endpoint string) string {
	trimmed := strings.TrimSuffix(basePath, "/")
	if trimmed == "" {
		return endpoint
	}
	return trimmed + endpoint
}

// SubmitHint performs exactly one trusted Hint submission.
func (client *HintClient) SubmitHint(ctx context.Context, request HintRequest) (HintReceipt, error) {
	if err := ctx.Err(); err != nil {
		return HintReceipt{}, err
	}
	if client == nil || client.endpoint == "" {
		return HintReceipt{}, &HintError{Kind: ErrHintInvalidRequest, Cause: errors.New("client is not configured")}
	}
	if err := validateHintRequest(request); err != nil {
		return HintReceipt{}, err
	}
	payload, err := json.Marshal(hintRequestWire{
		RootID:   request.RootID,
		ScopeKey: request.ScopeKey,
		Reason:   string(request.Reason),
	})
	if err != nil {
		return HintReceipt{}, &HintError{Kind: ErrHintInvalidRequest, Cause: err}
	}
	// IndexCore caps the body at 4096 bytes. Refuse locally so an oversized scope
	// never depends on the remote 413 path.
	if len(payload) > MaxHintBodyBytes {
		return HintReceipt{}, &HintError{
			Kind:       ErrHintRequestTooLarge,
			RemoteCode: "request_too_large",
			Cause:      fmt.Errorf("encoded request is %d bytes, limit %d", len(payload), MaxHintBodyBytes),
		}
	}

	requestCtx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	httpRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, client.endpoint, bytes.NewReader(payload))
	if err != nil {
		return HintReceipt{}, &HintError{Kind: ErrHintTransport, Cause: err}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("Authorization", "Bearer "+client.token)

	response, err := client.httpClient.Do(httpRequest)
	if err != nil {
		return HintReceipt{}, &HintError{Kind: ErrHintTransport, Cause: errors.New(redactHintToken(err.Error(), client.token))}
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(response.Body, client.maxBodyBytes+1))
	if err != nil {
		return HintReceipt{}, &HintError{Kind: ErrHintTransport, Cause: err}
	}
	if int64(len(body)) > client.maxBodyBytes {
		return HintReceipt{}, &HintError{
			Kind:       ErrHintMalformed,
			StatusCode: response.StatusCode,
			Cause:      fmt.Errorf("response exceeds %d bytes", client.maxBodyBytes),
		}
	}
	if response.StatusCode != http.StatusAccepted {
		return HintReceipt{}, classifyHintStatus(response, body, client.token)
	}
	return decodeHintReceipt(request, body)
}

// hintRequestWire is the exact trusted Hint body. IndexCore decodes it with
// DisallowUnknownFields, so the field set is closed.
type hintRequestWire struct {
	RootID   string `json:"root_id"`
	ScopeKey string `json:"scope_key"`
	Reason   string `json:"reason"`
}

// hintReceiptWire is IndexCore's 202 accepted receipt.
type hintReceiptWire struct {
	Status    string `json:"status"`
	RootID    string `json:"root_id"`
	ScopeKey  string `json:"scope_key"`
	WorkState string `json:"work_state"`
	SignalSeq int64  `json:"signal_seq"`
}

func validateHintRequest(request HintRequest) error {
	if strings.TrimSpace(request.RootID) == "" || !utf8.ValidString(request.RootID) ||
		strings.ContainsRune(request.RootID, '\x00') || len(request.RootID) > hintMaximumRootIDBytes {
		return &HintError{Kind: ErrHintInvalidRequest, RemoteCode: "invalid_request",
			Cause: errors.New("root ID is required and bounded")}
	}
	if err := validateHintScopeKey(request.ScopeKey); err != nil {
		return err
	}
	if !request.Reason.valid() {
		return &HintError{Kind: ErrHintInvalidRequest, RemoteCode: "invalid_request",
			Cause: fmt.Errorf("reason %q is not supported", request.Reason)}
	}
	return nil
}

// validateHintScopeKey mirrors IndexCore's ValidateScopeKey exactly. Panta must not
// send a scope the accepted P9 ingest would reject as a 400, and the mirrored rule
// keeps a malformed scope from reaching the trusted listener at all.
//
// IndexCore: "" -> invalid; "/" -> valid; else must be root-absolute, no trailing
// slash, no backslash, and no empty, "." or ".." component.
func validateHintScopeKey(scopeKey string) error {
	invalid := func(message string) error {
		return &HintError{Kind: ErrHintInvalidRequest, RemoteCode: "invalid_request",
			Cause: errors.New(message)}
	}
	if scopeKey == "" {
		return invalid("scope key is required")
	}
	if !utf8.ValidString(scopeKey) || strings.ContainsRune(scopeKey, '\x00') || len(scopeKey) > hintMaximumScopeKeyBytes {
		return invalid("scope key must be valid, bounded UTF-8")
	}
	if scopeKey == "/" {
		return nil
	}
	if !strings.HasPrefix(scopeKey, "/") {
		return invalid("scope key must be root-absolute")
	}
	if strings.HasSuffix(scopeKey, "/") {
		return invalid("scope key must not have a trailing slash")
	}
	if strings.Contains(scopeKey, `\`) {
		return invalid(`scope key must not use a backslash separator`)
	}
	for _, segment := range strings.Split(scopeKey[1:], "/") {
		switch segment {
		case "":
			return invalid("scope key has an empty component")
		case ".", "..":
			return invalid("scope key has a dot component")
		}
	}
	return nil
}

// classifyHintStatus maps a non-202 response onto a typed failure. The body is read
// best-effort for IndexCore's stable error code and is never required to be JSON.
func classifyHintStatus(response *http.Response, body []byte, token string) error {
	remoteCode := redactHintToken(extractHintErrorCode(body), token)
	failure := &HintError{StatusCode: response.StatusCode, RemoteCode: remoteCode}
	switch response.StatusCode {
	case http.StatusBadRequest:
		failure.Kind = ErrHintInvalidRequest
	case http.StatusUnauthorized:
		failure.Kind = ErrHintUnauthorized
	case http.StatusRequestEntityTooLarge:
		failure.Kind = ErrHintRequestTooLarge
	case http.StatusUnsupportedMediaType:
		failure.Kind = ErrHintUnsupportedType
	case http.StatusTooManyRequests:
		failure.Kind = ErrHintBusy
		failure.RetryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
	case http.StatusServiceUnavailable:
		failure.Kind = ErrHintUnavailable
	default:
		failure.Kind = ErrHintUnexpectedState
	}
	return failure
}

func decodeHintReceipt(request HintRequest, body []byte) (HintReceipt, error) {
	var wire hintReceiptWire
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(&wire); err != nil {
		return HintReceipt{}, &HintError{Kind: ErrHintMalformed, StatusCode: http.StatusAccepted, Cause: err}
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return HintReceipt{}, &HintError{Kind: ErrHintMalformed, StatusCode: http.StatusAccepted,
			Cause: errors.New("unexpected trailing content")}
	}
	if wire.Status != "accepted" {
		return HintReceipt{}, &HintError{Kind: ErrHintMalformed, StatusCode: http.StatusAccepted,
			Cause: fmt.Errorf("receipt status %q is not accepted", wire.Status)}
	}
	if wire.RootID == "" || wire.ScopeKey == "" || wire.WorkState == "" ||
		len(wire.RootID) > hintReceiptFieldBytes || len(wire.ScopeKey) > hintReceiptFieldBytes ||
		len(wire.WorkState) > hintReceiptFieldBytes {
		return HintReceipt{}, &HintError{Kind: ErrHintMalformed, StatusCode: http.StatusAccepted,
			Cause: errors.New("receipt is missing required fields")}
	}
	// The receipt must describe exactly the hint that was submitted. A receipt for a
	// different root or scope is not proof that this scope was ingested.
	if wire.RootID != request.RootID || wire.ScopeKey != request.ScopeKey {
		return HintReceipt{}, &HintError{Kind: ErrHintMalformed, StatusCode: http.StatusAccepted,
			Cause: fmt.Errorf("receipt identity root=%q scope=%q does not match the submitted hint",
				wire.RootID, wire.ScopeKey)}
	}
	return HintReceipt{
		Status: wire.Status, RootID: wire.RootID, ScopeKey: wire.ScopeKey,
		WorkState: wire.WorkState, SignalSeq: wire.SignalSeq,
	}, nil
}

// extractHintErrorCode reads IndexCore's stable error code, for example
// {"error":"invalid_request"}. A non-JSON body yields no code rather than an error.
func extractHintErrorCode(body []byte) string {
	if len(bytes.TrimSpace(body)) == 0 {
		return ""
	}
	var envelope struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ""
	}
	return envelope.Error
}

// parseRetryAfter preserves IndexCore's backpressure guidance. Delta-seconds is
// recorded as a duration; an HTTP-date or malformed value is preserved raw only. The
// client never sleeps, so a hostile value cannot stall a caller.
func parseRetryAfter(raw string) *RetryAfter {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	value := &RetryAfter{Raw: trimmed}
	seconds, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || seconds < 0 {
		return value
	}
	value.Seconds = seconds
	value.IsDelta = true
	return value
}

// redactHintToken removes the configured bearer token from any surfaced text.
func redactHintToken(text, token string) string {
	if token == "" || text == "" {
		return text
	}
	return strings.ReplaceAll(text, token, "[redacted]")
}
