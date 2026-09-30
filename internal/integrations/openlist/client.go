package openlist

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	// fsGetEndpoint is the upstream known-path object lookup route.
	fsGetEndpoint = "/api/fs/get"

	// objectNotFoundMessage is the upstream sentinel for a missing object
	// (internal/errs.ObjectNotFound). Only this exact message, delivered in an HTTP
	// 200 application envelope, maps to a not-visible observation.
	objectNotFoundMessage = "object not found"

	defaultTimeout = 15 * time.Second
	// maximumTimeout bounds one request even when the caller supplies an unbounded
	// or very large timeout.
	maximumTimeout = 2 * time.Minute

	defaultMaxBodyBytes  = 1 << 20
	maximumMaxBodyBytes  = 8 << 20
	maximumPathLength    = 4096
	maximumTokenLength   = 4096
	maximumBaseURLLength = 2048
)

var (
	ErrInvalidConfig      = errors.New("invalid openlist client configuration")
	ErrInvalidStatRequest = errors.New("invalid openlist stat request")
	ErrTransport          = errors.New("openlist transport failure")
	ErrMalformedResponse  = errors.New("openlist malformed response")
	ErrRemote             = errors.New("openlist remote error")
)

// RemoteError is a typed OpenList failure that is not a valid visibility
// observation: an application envelope with a non-200 code, or a non-2xx HTTP
// response. It carries upstream diagnosis but never the configured token.
//
// A non-2xx response is reported here even when its body is not JSON at all, so a
// gateway HTML error page stays an integration error instead of becoming a
// malformed-response error.
type RemoteError struct {
	HTTPStatus   int
	EnvelopeCode int
	Message      string
}

func (err *RemoteError) Error() string {
	return fmt.Sprintf("%s: http=%d code=%d message=%q",
		ErrRemote, err.HTTPStatus, err.EnvelopeCode, err.Message)
}

func (err *RemoteError) Is(target error) bool { return target == ErrRemote }

// Config configures the OpenList HTTP client.
type Config struct {
	// BaseURL is an absolute HTTP(S) base URL, for example https://openlist.example
	// or https://example/openlist when the deployment is served under a base path.
	BaseURL string
	// Token is the optional OpenList API token, sent as the Authorization header
	// exactly as configured. Because it is preserved byte for byte, leading or
	// trailing whitespace is rejected rather than silently trimmed. The token is
	// never logged, returned, or embedded in an error.
	Token string
	// Timeout bounds one request. Zero selects a bounded default and any value is
	// capped, so the client can never issue an unbounded request even when
	// HTTPClient is supplied.
	Timeout time.Duration
	// MaxBodyBytes bounds the response body. Zero uses a bounded default.
	MaxBodyBytes int64
	// HTTPClient overrides the transport, which tests use to avoid real network
	// access. The configured Timeout still applies through a per-request deadline.
	HTTPClient *http.Client
	// Now supplies the observation timestamp. Nil uses time.Now.
	Now func() time.Time
}

// HTTPClient performs exact known-path visibility lookups against OpenList.
//
// StatRequest carries Panta port coordinates: Mount is the binding mount and Path is
// the binding-relative target. The client owns the D-028 join that turns them into
// the OpenList wire path.
type HTTPClient struct {
	baseURL      *url.URL
	endpoint     string
	token        string
	httpClient   *http.Client
	timeout      time.Duration
	maxBodyBytes int64
	now          func() time.Time
}

var _ VisibilityPort = (*HTTPClient)(nil)

// NewHTTPClient validates configuration and builds the client. Construction
// performs no network call.
func NewHTTPClient(config Config) (*HTTPClient, error) {
	base, err := parseBaseURL(config.BaseURL)
	if err != nil {
		return nil, err
	}
	token := config.Token
	// The token is sent verbatim, so surrounding whitespace is rejected instead of
	// being quietly rewritten.
	if len(token) > maximumTokenLength || strings.ContainsRune(token, '\x00') || !utf8.ValidString(token) ||
		token != strings.TrimSpace(token) {
		return nil, fmt.Errorf("%w: token is invalid", ErrInvalidConfig)
	}
	timeout := effectiveTimeout(config.Timeout)
	maxBody := config.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxBodyBytes
	}
	if maxBody > maximumMaxBodyBytes {
		return nil, fmt.Errorf("%w: max body bytes exceeds the supported bound", ErrInvalidConfig)
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: timeout}
	} else if httpClient.Timeout == 0 || httpClient.Timeout > timeout {
		// Never leave the injected client unbounded, and never let it sit looser than
		// the enforced bound. A tighter value the caller chose is preserved.
		httpClient.Timeout = timeout
	}
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &HTTPClient{
		baseURL:      base,
		endpoint:     joinBasePath(base.Path, fsGetEndpoint),
		token:        token,
		httpClient:   httpClient,
		timeout:      timeout,
		maxBodyBytes: maxBody,
		now:          now,
	}, nil
}

// effectiveTimeout selects a bounded per-request timeout, capping any value the
// caller supplied.
func effectiveTimeout(configured time.Duration) time.Duration {
	if configured <= 0 {
		return defaultTimeout
	}
	if configured > maximumTimeout {
		return maximumTimeout
	}
	return configured
}

// parseBaseURL accepts only an absolute HTTP(S) URL without a query or fragment.
func parseBaseURL(raw string) (*url.URL, error) {
	if raw == "" || len(raw) > maximumBaseURLLength || strings.ContainsRune(raw, '\x00') || !utf8.ValidString(raw) {
		return nil, fmt.Errorf("%w: base URL is required", ErrInvalidConfig)
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: base URL is not a URL", ErrInvalidConfig)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("%w: base URL scheme must be http or https", ErrInvalidConfig)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("%w: base URL must be absolute", ErrInvalidConfig)
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%w: base URL must not carry a query or fragment", ErrInvalidConfig)
	}
	return parsed, nil
}

// joinBasePath preserves a configured base path so a deployment served under a
// prefix still reaches the right route.
func joinBasePath(basePath, endpoint string) string {
	trimmed := strings.TrimSuffix(basePath, "/")
	if trimmed == "" {
		return endpoint
	}
	return trimmed + endpoint
}

// Stat performs exactly one known-path lookup.
//
// request.Path is the binding-relative target from Panta's port coordinates. The
// D-028 join happens here, at the OpenList boundary.
func (client *HTTPClient) Stat(ctx context.Context, request StatRequest) (VisibilityFact, error) {
	if err := ctx.Err(); err != nil {
		return VisibilityFact{}, err
	}
	wirePath, err := client.resolveWirePath(request)
	if err != nil {
		return VisibilityFact{}, err
	}
	payload, err := json.Marshal(map[string]string{"path": wirePath})
	if err != nil {
		return VisibilityFact{}, fmt.Errorf("%w: encode request: %v", ErrInvalidStatRequest, err)
	}

	// The per-request deadline is what actually bounds the call, so a supplied
	// HTTPClient cannot make the request unbounded.
	requestCtx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	status, body, err := client.do(requestCtx, payload)
	if err != nil {
		return VisibilityFact{}, err
	}
	return client.classify(request, status, body)
}

// resolveWirePath validates Panta's port coordinates and applies the D-028 join.
func (client *HTTPClient) resolveWirePath(request StatRequest) (string, error) {
	if client == nil || client.baseURL == nil {
		return "", fmt.Errorf("%w: client is not configured", ErrInvalidConfig)
	}
	if err := validateAbsolutePath("mount", request.Mount); err != nil {
		return "", err
	}
	if err := validateAbsolutePath("path", request.Path); err != nil {
		return "", err
	}
	wirePath := joinMountAndTarget(request.Mount, request.Path)
	if wirePath == "" || path.Clean(wirePath) != wirePath || !strings.HasPrefix(wirePath, "/") {
		return "", fmt.Errorf("%w: joined path is not canonical", ErrInvalidStatRequest)
	}
	// The joined wire path must stay beneath or equal to the mount.
	if request.Mount != "/" && wirePath != request.Mount &&
		!strings.HasPrefix(wirePath, request.Mount+"/") {
		return "", fmt.Errorf("%w: joined path escapes mount", ErrInvalidStatRequest)
	}
	return wirePath, nil
}

// joinMountAndTarget implements D-028 at the wire boundary: the root mount collapses
// to the target, otherwise the two are joined.
func joinMountAndTarget(mount, target string) string {
	if mount == "/" {
		return target
	}
	return path.Join(mount, target)
}

func validateAbsolutePath(field, value string) error {
	if value == "" || !utf8.ValidString(value) || !strings.HasPrefix(value, "/") ||
		strings.ContainsRune(value, '\x00') || len(value) > maximumPathLength {
		return fmt.Errorf("%w: %s must be an absolute, bounded, NUL-free path", ErrInvalidStatRequest, field)
	}
	// path.Clean collapses repeated separators, so a normalized value that differs
	// from the request would mean the caller passed an unnormalized path.
	if path.Clean(value) != value {
		return fmt.Errorf("%w: %s must be normalized", ErrInvalidStatRequest, field)
	}
	return nil
}

// do sends the single lookup request and returns the status and bounded body.
func (client *HTTPClient) do(ctx context.Context, payload []byte) (int, []byte, error) {
	endpoint := *client.baseURL
	endpoint.Path = client.endpoint
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(payload))
	if err != nil {
		return 0, nil, fmt.Errorf("%w: build request: %v", ErrTransport, err)
	}
	request.Header.Set("Content-Type", "application/json")
	if client.token != "" {
		request.Header.Set("Authorization", client.token)
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		// The transport error text can embed the URL, which never contains the token,
		// and the token is redacted anyway before surfacing.
		return 0, nil, fmt.Errorf("%w: %v", ErrTransport, errors.New(redactToken(err.Error(), client.token)))
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(response.Body, client.maxBodyBytes+1))
	if err != nil {
		return 0, nil, fmt.Errorf("%w: read response: %v", ErrTransport, err)
	}
	if int64(len(body)) > client.maxBodyBytes {
		return 0, nil, fmt.Errorf("%w: response body exceeds %d bytes", ErrMalformedResponse, client.maxBodyBytes)
	}
	return response.StatusCode, body, nil
}

// classify maps one HTTP response into a visibility fact or a typed error.
//
// The HTTP status is checked first. A non-2xx response is always an integration
// error and is never parsed as an application envelope, so a gateway's plain-text or
// HTML error page cannot be mistaken for a malformed application response. Only the
// exact upstream not-found sentinel inside an HTTP 200 envelope yields
// Visible=false.
func (client *HTTPClient) classify(request StatRequest, status int, body []byte) (VisibilityFact, error) {
	if status < 200 || status > 299 {
		// Best-effort diagnosis only: the body is never required to be JSON.
		return VisibilityFact{}, &RemoteError{
			HTTPStatus: status,
			Message:    redactToken(extractJSONMessage(body), client.token),
		}
	}

	envelope, err := decodeEnvelope(body)
	if err != nil {
		return VisibilityFact{}, err
	}
	message := redactToken(envelope.Message, client.token)
	if envelope.Code != http.StatusOK {
		if strings.TrimSpace(message) == objectNotFoundMessage {
			// Normal observation: OpenList does not expose the exact path. This is
			// authorized only for the exact upstream sentinel.
			return VisibilityFact{
				Mount: request.Mount, Path: request.Path, Visible: false,
				ObservedAt: client.now().UTC(),
			}, nil
		}
		// Authorization failures, storage-not-ready, provider failures, and unknown
		// messages all stay errors.
		return VisibilityFact{}, &RemoteError{
			HTTPStatus: status, EnvelopeCode: envelope.Code, Message: message,
		}
	}
	return client.visibleFact(request, envelope.Data)
}

// responseEnvelope is the OpenList application envelope. Upstream declares
// Resp[T]{Code int; Message string; Data T} and serialises T as-is, so an object
// lookup carries a single object while a directory listing carries an array. The
// raw payload is therefore decoded by shape at the use site. Unknown envelope fields
// are tolerated because upstream adds fields over time.
type responseEnvelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func decodeEnvelope(body []byte) (responseEnvelope, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var envelope responseEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return responseEnvelope{}, fmt.Errorf("%w: decode envelope: %v", ErrMalformedResponse, err)
	}
	if err := requireEndOfDocument(decoder); err != nil {
		return responseEnvelope{}, err
	}
	return envelope, nil
}

// visibleFact validates the returned object identity before reporting visibility.
//
// The payload must be a single object, which is what a successful /api/fs/get
// returns. Unknown object fields are tolerated: the upstream response also carries
// download URLs and signatures, which this gate neither persists nor treats as proof.
func (client *HTTPClient) visibleFact(request StatRequest, data json.RawMessage) (VisibilityFact, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return VisibilityFact{}, fmt.Errorf("%w: success envelope carried no object", ErrMalformedResponse)
	}
	if trimmed[0] == '[' {
		return VisibilityFact{}, fmt.Errorf("%w: success envelope carried an array, want a single object",
			ErrMalformedResponse)
	}
	if trimmed[0] != '{' {
		return VisibilityFact{}, fmt.Errorf("%w: success envelope carried a non-object payload",
			ErrMalformedResponse)
	}

	var object struct {
		Name     string      `json:"name"`
		Size     json.Number `json:"size"`
		IsDir    *bool       `json:"is_dir"`
		Modified string      `json:"modified"`
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return VisibilityFact{}, fmt.Errorf("%w: decode object: %v", ErrMalformedResponse, err)
	}
	if err := requireEndOfDocument(decoder); err != nil {
		return VisibilityFact{}, err
	}

	if object.Name == "" || !utf8.ValidString(object.Name) || strings.TrimSpace(object.Name) == "" ||
		strings.ContainsRune(object.Name, '\x00') {
		return VisibilityFact{}, fmt.Errorf("%w: object name is missing or invalid", ErrMalformedResponse)
	}
	basename := path.Base(request.Path)
	if object.Name != basename {
		return VisibilityFact{}, fmt.Errorf("%w: object name %q does not match requested basename %q",
			ErrMalformedResponse, object.Name, basename)
	}

	size, err := parseSize(object.Size)
	if err != nil {
		return VisibilityFact{}, err
	}
	fact := VisibilityFact{
		// The fact echoes Panta's port coordinates, not the OpenList wire path.
		Mount: request.Mount, Path: request.Path, Visible: true,
		Name: object.Name, Directory: object.IsDir != nil && *object.IsDir,
		SizeBytes: size, ObservedAt: client.now().UTC(),
	}
	if strings.TrimSpace(object.Modified) != "" {
		modified, err := time.Parse(time.RFC3339, object.Modified)
		if err != nil {
			return VisibilityFact{}, fmt.Errorf("%w: object modified time %q is not RFC3339",
				ErrMalformedResponse, object.Modified)
		}
		utc := modified.UTC()
		fact.ModifiedAt = &utc
	}
	return fact, nil
}

// parseSize accepts a JSON number or a quoted numeric string, because deployments
// differ on how they encode large sizes, and rejects anything negative.
func parseSize(value json.Number) (int64, error) {
	text := strings.TrimSpace(value.String())
	if text == "" {
		return 0, nil
	}
	text = strings.Trim(text, `"`)
	size, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: object size %q is not an integer", ErrMalformedResponse, text)
	}
	if size < 0 {
		return 0, fmt.Errorf("%w: object size %d is negative", ErrMalformedResponse, size)
	}
	return size, nil
}

// requireEndOfDocument rejects trailing content after the JSON document.
func requireEndOfDocument(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: unexpected trailing content", ErrMalformedResponse)
	}
	return nil
}

// extractJSONMessage best-effort reads a message from a non-2xx body. It never
// decides visibility and returns empty when the body is not a JSON object.
func extractJSONMessage(body []byte) string {
	var envelope struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return ""
	}
	return envelope.Message
}

// redactToken removes the configured token from any text that may be surfaced.
func redactToken(text, token string) string {
	if token == "" || text == "" {
		return text
	}
	return strings.ReplaceAll(text, token, "[redacted]")
}
