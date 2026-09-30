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

	// objectNotFoundMessage is the upstream sentinel for a missing object. Only this
	// exact message maps to a not-visible observation.
	objectNotFoundMessage = "object not found"

	defaultTimeout       = 15 * time.Second
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

// RemoteError is a typed OpenList application-envelope failure. It carries the
// upstream envelope code and message so callers can distinguish an authorization or
// storage problem from a genuinely missing object. It never carries the configured
// token.
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
	// Token is an optional OpenList API token. It is sent as the Authorization
	// header and is never logged, never returned, and never embedded in an error.
	Token string
	// Timeout bounds one request. Zero uses a bounded default.
	Timeout time.Duration
	// MaxBodyBytes bounds the response body. Zero uses a bounded default.
	MaxBodyBytes int64
	// HTTPClient overrides the transport, which tests use to avoid real network
	// access.
	HTTPClient *http.Client
	// Now supplies the observation timestamp. Nil uses time.Now.
	Now func() time.Time
}

// HTTPClient performs exact known-path visibility lookups against OpenList.
type HTTPClient struct {
	baseURL      *url.URL
	endpoint     string
	token        string
	httpClient   *http.Client
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
	token := strings.TrimSpace(config.Token)
	if len(token) > maximumTokenLength || strings.ContainsRune(token, '\x00') || !utf8.ValidString(token) {
		return nil, fmt.Errorf("%w: token is invalid", ErrInvalidConfig)
	}
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
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
		maxBodyBytes: maxBody,
		now:          now,
	}, nil
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
func (client *HTTPClient) Stat(ctx context.Context, request StatRequest) (VisibilityFact, error) {
	if err := ctx.Err(); err != nil {
		return VisibilityFact{}, err
	}
	if err := client.validateRequest(request); err != nil {
		return VisibilityFact{}, err
	}
	payload, err := json.Marshal(map[string]string{"path": request.Path})
	if err != nil {
		return VisibilityFact{}, fmt.Errorf("%w: encode request: %v", ErrInvalidStatRequest, err)
	}
	status, body, err := client.do(ctx, payload)
	if err != nil {
		return VisibilityFact{}, err
	}
	return client.classify(request, status, body)
}

// validateRequest rejects anything that is not one exact known path.
func (client *HTTPClient) validateRequest(request StatRequest) error {
	if client == nil || client.baseURL == nil {
		return fmt.Errorf("%w: client is not configured", ErrInvalidConfig)
	}
	if err := validateAbsolutePath("mount", request.Mount); err != nil {
		return err
	}
	return validateAbsolutePath("path", request.Path)
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

// classify maps one HTTP response into a visibility fact or a typed error. Only the
// exact upstream not-found envelope may yield Visible=false; every other non-success
// outcome is an integration error.
func (client *HTTPClient) classify(request StatRequest, status int, body []byte) (VisibilityFact, error) {
	envelope, err := decodeEnvelope(body)
	if err != nil {
		return VisibilityFact{}, err
	}
	message := redactToken(envelope.Message, client.token)

	if status < 200 || status > 299 {
		return VisibilityFact{}, &RemoteError{
			HTTPStatus: status, EnvelopeCode: envelope.Code, Message: message,
		}
	}
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
	if len(envelope.Data) != 1 {
		return VisibilityFact{}, fmt.Errorf("%w: success envelope carried %d objects, want exactly 1",
			ErrMalformedResponse, len(envelope.Data))
	}
	return client.visibleFact(request, envelope.Data[0])
}

// responseEnvelope is the OpenList application envelope. It deliberately does not
// reject unknown envelope fields, because upstream adds fields over time.
type responseEnvelope struct {
	Code    int               `json:"code"`
	Message string            `json:"message"`
	Data    []json.RawMessage `json:"data"`
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
// Unknown object fields are tolerated: the upstream response also carries download
// URLs and signatures, which this gate neither persists nor treats as proof.
func (client *HTTPClient) visibleFact(request StatRequest, raw json.RawMessage) (VisibilityFact, error) {
	var object struct {
		Name     string      `json:"name"`
		Size     json.Number `json:"size"`
		IsDir    *bool       `json:"is_dir"`
		Modified string      `json:"modified"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&object); err != nil {
		return VisibilityFact{}, fmt.Errorf("%w: decode object: %v", ErrMalformedResponse, err)
	}
	if err := requireEndOfDocument(decoder); err != nil {
		return VisibilityFact{}, err
	}

	if object.Name == "" || !utf8.ValidString(object.Name) || strings.ContainsRune(object.Name, '\x00') {
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

// redactToken removes the configured token from any text that may be surfaced.
func redactToken(text, token string) string {
	if token == "" || text == "" {
		return text
	}
	return strings.ReplaceAll(text, token, "[redacted]")
}
