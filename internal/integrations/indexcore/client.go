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
)

const (
	defaultHTTPTimeout = 10 * time.Second
	maxResponseBytes   = 8 << 20
)

var (
	ErrInvalidCursor     = errors.New("indexcore invalid cursor")
	ErrInvalidRequest    = errors.New("indexcore invalid request")
	ErrNotFound          = errors.New("indexcore not found")
	ErrStaleCursor       = errors.New("indexcore stale cursor")
	ErrInternal          = errors.New("indexcore internal error")
	ErrNotReady          = errors.New("indexcore not ready")
	ErrTransport         = errors.New("indexcore transport error")
	ErrMalformedResponse = errors.New("indexcore malformed response")
	ErrUnexpectedStatus  = errors.New("indexcore unexpected HTTP status")
)

// Error provides stable classification while retaining remote diagnostics and
// wrapped transport errors. Callers should use errors.Is with the sentinels.
type Error struct {
	Kind       error
	Operation  string
	StatusCode int
	RemoteCode string
	Message    string
	Cause      error
}

func (e *Error) Error() string {
	parts := []string{"indexcore", e.Operation, e.Kind.Error()}
	if e.StatusCode != 0 {
		parts = append(parts, "status="+strconv.Itoa(e.StatusCode))
	}
	if e.RemoteCode != "" {
		parts = append(parts, "code="+e.RemoteCode)
	}
	if e.Message != "" {
		parts = append(parts, e.Message)
	}
	if e.Cause != nil {
		parts = append(parts, e.Cause.Error())
	}
	return strings.Join(parts, ": ")
}

func (e *Error) Unwrap() []error {
	if e.Cause == nil {
		return []error{e.Kind}
	}
	return []error{e.Kind, e.Cause}
}

// Client is the server-side, read-only IndexCore HTTP adapter.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

// ClientOption customizes a Client without adding transport dependencies.
type ClientOption func(*http.Client) error

// WithHTTPClient supplies an HTTP client. It must have a positive timeout.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(target *http.Client) error {
		if client == nil {
			return errors.New("http client is required")
		}
		if client.Timeout <= 0 {
			return errors.New("http client timeout must be positive")
		}
		*target = *client
		return nil
	}
}

// WithTimeout changes the bounded default HTTP timeout.
func WithTimeout(timeout time.Duration) ClientOption {
	return func(client *http.Client) error {
		if timeout <= 0 {
			return errors.New("timeout must be positive")
		}
		client.Timeout = timeout
		return nil
	}
}

// NewClient constructs a read-only client for an absolute HTTP(S) base URL.
func NewClient(rawBaseURL string, options ...ClientOption) (*Client, error) {
	baseURL, err := url.Parse(strings.TrimSpace(rawBaseURL))
	if err != nil {
		return nil, fmt.Errorf("parse IndexCore base URL: %w", err)
	}
	if (baseURL.Scheme != "http" && baseURL.Scheme != "https") || baseURL.Host == "" {
		return nil, errors.New("IndexCore base URL must be an absolute HTTP(S) URL")
	}
	if baseURL.RawQuery != "" || baseURL.Fragment != "" {
		return nil, errors.New("IndexCore base URL must not contain a query or fragment")
	}
	baseURL.Path = strings.TrimRight(baseURL.Path, "/")
	baseURL.RawPath = strings.TrimRight(baseURL.EscapedPath(), "/")

	httpClient := &http.Client{Timeout: defaultHTTPTimeout}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(httpClient); err != nil {
			return nil, fmt.Errorf("configure IndexCore client: %w", err)
		}
	}
	return &Client{baseURL: baseURL, httpClient: httpClient}, nil
}

var (
	_ ReadPort           = (*Client)(nil)
	_ JournalReadPort    = (*Client)(nil)
	_ RootStatusReadPort = (*Client)(nil)
)

// Browse implements Q4 hierarchy-child browsing.
func (c *Client) Browse(ctx context.Context, request BrowseRequest) (ResourcePage, error) {
	if request.RootID == "" {
		return ResourcePage{}, localRequestError("browse", "root ID is required")
	}
	if request.Limit < 0 {
		return ResourcePage{}, localRequestError("browse", "limit must not be negative")
	}
	query := make(url.Values)
	if request.ParentResourceID != nil {
		query.Set("parent_id", *request.ParentResourceID)
	}
	if request.Cursor != "" {
		query.Set("cursor", request.Cursor)
	}
	addLimit(query, request.Limit)
	addVisibility(query, request.ReadVisibility)

	var response resourcePageWire
	if err := c.get(ctx, "browse", request.RootID, "resources", query, &response); err != nil {
		return ResourcePage{}, err
	}
	if response.Items == nil {
		return ResourcePage{}, malformed("browse", "items is required", nil)
	}
	items, err := decodeResources(*response.Items)
	if err != nil {
		return ResourcePage{}, malformed("browse", "invalid resource item", err)
	}
	return ResourcePage{Items: items, NextCursor: response.NextCursor}, nil
}

// Resolve implements Q5 and preserves every match plus ambiguity.
func (c *Client) Resolve(ctx context.Context, request ResolveRequest) (ResolveResult, error) {
	if request.RootID == "" || request.Path == "" {
		return ResolveResult{}, localRequestError("resolve", "root ID and path are required")
	}
	query := url.Values{"path": []string{request.Path}}
	addVisibility(query, request.ReadVisibility)

	var response resolveWire
	if err := c.get(ctx, "resolve", request.RootID, "resolve", query, &response); err != nil {
		return ResolveResult{}, err
	}
	if response.Matches == nil || response.Ambiguous == nil {
		return ResolveResult{}, malformed("resolve", "matches and ambiguous are required", nil)
	}
	matches, err := decodeResources(*response.Matches)
	if err != nil {
		return ResolveResult{}, malformed("resolve", "invalid resource match", err)
	}
	return ResolveResult{Matches: matches, Ambiguous: *response.Ambiguous}, nil
}

// ReadJournal implements Q8 without altering AfterSeq or response order.
func (c *Client) ReadJournal(ctx context.Context, request JournalRequest) ([]JournalEvent, error) {
	if request.RootID == "" {
		return nil, localRequestError("read journal", "root ID is required")
	}
	if request.AfterSeq < 0 || request.Limit < 0 {
		return nil, localRequestError("read journal", "after sequence and limit must not be negative")
	}
	query := url.Values{"after_seq": []string{strconv.FormatInt(request.AfterSeq, 10)}}
	addLimit(query, request.Limit)

	var response journalPageWire
	if err := c.get(ctx, "read journal", request.RootID, "journal", query, &response); err != nil {
		return nil, err
	}
	if response.Items == nil {
		return nil, malformed("read journal", "items is required", nil)
	}
	events := make([]JournalEvent, 0, len(*response.Items))
	for i, item := range *response.Items {
		if item.EventSeq == nil || item.GenerationNumber == nil || item.IntraGenerationSeq == nil ||
			item.EventType == "" || item.CommittedAt == nil {
			return nil, malformed("read journal", fmt.Sprintf("event %d is missing required fields", i), nil)
		}
		eventType := JournalEventType(item.EventType)
		if !eventType.valid() {
			return nil, malformed("read journal", fmt.Sprintf("event %d has unknown event_type", i), nil)
		}
		var payload json.RawMessage
		if item.Payload != "" {
			payload = json.RawMessage(item.Payload)
			if !json.Valid(payload) {
				return nil, malformed("read journal", fmt.Sprintf("event %d has invalid payload JSON", i), nil)
			}
		}
		events = append(events, JournalEvent{
			EventSeq:           *item.EventSeq,
			GenerationNumber:   *item.GenerationNumber,
			IntraGenerationSeq: *item.IntraGenerationSeq,
			EventType:          eventType,
			ResourceID:         item.ResourceID,
			Payload:            payload,
			CommittedAt:        *item.CommittedAt,
		})
	}
	return events, nil
}

// RootStatus implements Q9 without adding root mutation capabilities.
func (c *Client) RootStatus(ctx context.Context, request RootStatusRequest) (RootStatus, error) {
	if request.RootID == "" {
		return RootStatus{}, localRequestError("root status", "root ID is required")
	}
	query := make(url.Values)
	if request.IncludeDeprecatedRoot {
		query.Set("include_deprecated_root", "true")
	}
	if request.IncludeDeletedRoot {
		query.Set("include_deleted_root", "true")
	}

	var response rootStatusWire
	if err := c.get(ctx, "root status", request.RootID, "status", query, &response); err != nil {
		return RootStatus{}, err
	}
	if response.RootID == "" || response.LifecycleState == "" || response.CurrentGeneration == nil {
		return RootStatus{}, malformed("root status", "required status fields are missing", nil)
	}
	state := RootLifecycleState(response.LifecycleState)
	if !state.valid() {
		return RootStatus{}, malformed("root status", "unknown lifecycle_state", nil)
	}
	return RootStatus{
		RootID: response.RootID, LifecycleState: state,
		CurrentGeneration:       *response.CurrentGeneration,
		LastAppliedAdmissionSeq: response.LastAppliedAdmissionSeq,
	}, nil
}

func (c *Client) get(ctx context.Context, operation, rootID, endpoint string, query url.Values, target any) error {
	requestURL := c.endpoint(rootID, endpoint)
	requestURL.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return &Error{Kind: ErrTransport, Operation: operation, Cause: err}
	}
	response, err := c.httpClient.Do(req)
	if err != nil {
		return &Error{Kind: ErrTransport, Operation: operation, Cause: err}
	}
	defer response.Body.Close()

	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return &Error{Kind: ErrTransport, Operation: operation, Cause: err}
	}
	if len(body) > maxResponseBytes {
		return malformed(operation, "response exceeds size limit", nil)
	}
	if response.StatusCode != http.StatusOK {
		return classifyRemote(operation, response.StatusCode, body)
	}
	if err := decodeSingleJSON(body, target); err != nil {
		return malformed(operation, "invalid JSON response", err)
	}
	return nil
}

func (c *Client) endpoint(rootID, endpoint string) *url.URL {
	result := *c.baseURL
	prefixPath := strings.TrimRight(c.baseURL.Path, "/")
	prefixEscaped := strings.TrimRight(c.baseURL.EscapedPath(), "/")
	result.Path = prefixPath + "/v1/roots/" + rootID + "/" + endpoint
	result.RawPath = prefixEscaped + "/v1/roots/" + url.PathEscape(rootID) + "/" + url.PathEscape(endpoint)
	return &result
}

func addLimit(query url.Values, limit int) {
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
}

func addVisibility(query url.Values, visibility ReadVisibility) {
	if visibility.IncludeRemoved {
		query.Set("include_removed", "true")
	}
	if visibility.IncludeDeprecatedRoot {
		query.Set("include_deprecated_root", "true")
	}
	if visibility.IncludeDeletedRoot {
		query.Set("include_deleted_root", "true")
	}
}

func classifyRemote(operation string, statusCode int, body []byte) error {
	var envelope struct {
		Code    string `json:"error"`
		Message string `json:"message"`
	}
	if err := decodeSingleJSON(body, &envelope); err != nil {
		return malformed(operation, "invalid JSON error response", err)
	}
	expected := map[string]struct {
		status int
		kind   error
	}{
		"invalid_cursor":  {http.StatusBadRequest, ErrInvalidCursor},
		"invalid_request": {http.StatusBadRequest, ErrInvalidRequest},
		"not_found":       {http.StatusNotFound, ErrNotFound},
		"stale_cursor":    {http.StatusConflict, ErrStaleCursor},
		"internal_error":  {http.StatusInternalServerError, ErrInternal},
		"not_ready":       {http.StatusServiceUnavailable, ErrNotReady},
	}
	mapping, ok := expected[envelope.Code]
	if !ok || mapping.status != statusCode {
		return &Error{Kind: ErrUnexpectedStatus, Operation: operation, StatusCode: statusCode,
			RemoteCode: envelope.Code, Message: envelope.Message}
	}
	return &Error{Kind: mapping.kind, Operation: operation, StatusCode: statusCode,
		RemoteCode: envelope.Code, Message: envelope.Message}
}

func decodeSingleJSON(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func localRequestError(operation, message string) error {
	return &Error{Kind: ErrInvalidRequest, Operation: operation, Message: message}
}

func malformed(operation, message string, cause error) error {
	return &Error{Kind: ErrMalformedResponse, Operation: operation, Message: message, Cause: cause}
}

type resourceWire struct {
	ResourceID              string     `json:"resource_id"`
	RootID                  string     `json:"root_id"`
	CanonicalPath           *string    `json:"canonical_path"`
	ParentResourceID        *string    `json:"parent_resource_id"`
	Name                    *string    `json:"name"`
	IsDir                   *bool      `json:"is_dir"`
	Size                    *int64     `json:"size"`
	Mtime                   *time.Time `json:"mtime"`
	ResourcePresence        string     `json:"resource_presence"`
	IntroducedAtGeneration  *int64     `json:"introduced_at_generation"`
	LastConfirmedGeneration *int64     `json:"last_confirmed_generation"`
}

type resourcePageWire struct {
	Items      *[]resourceWire `json:"items"`
	NextCursor string          `json:"next_cursor"`
}

type resolveWire struct {
	Matches   *[]resourceWire `json:"matches"`
	Ambiguous *bool           `json:"ambiguous"`
}

type journalEventWire struct {
	EventSeq           *int64     `json:"event_seq"`
	GenerationNumber   *int64     `json:"generation_number"`
	IntraGenerationSeq *int32     `json:"intra_generation_seq"`
	EventType          string     `json:"event_type"`
	ResourceID         *string    `json:"resource_id"`
	Payload            string     `json:"payload"`
	CommittedAt        *time.Time `json:"committed_at"`
}

type journalPageWire struct {
	Items *[]journalEventWire `json:"items"`
}

type rootStatusWire struct {
	RootID                  string `json:"root_id"`
	LifecycleState          string `json:"lifecycle_state"`
	CurrentGeneration       *int64 `json:"current_generation"`
	LastAppliedAdmissionSeq *int64 `json:"last_applied_admission_seq"`
}

func decodeResources(wires []resourceWire) ([]ResourceContext, error) {
	resources := make([]ResourceContext, 0, len(wires))
	for i, item := range wires {
		if item.ResourceID == "" || item.RootID == "" || item.ResourcePresence == "" ||
			item.IntroducedAtGeneration == nil || item.LastConfirmedGeneration == nil {
			return nil, fmt.Errorf("resource %d is missing required fields", i)
		}
		presence := ResourcePresence(item.ResourcePresence)
		if !presence.valid() {
			return nil, fmt.Errorf("resource %d has unknown resource_presence", i)
		}
		resources = append(resources, ResourceContext{
			ResourceID: item.ResourceID, RootID: item.RootID,
			CanonicalPath: item.CanonicalPath, ParentResourceID: item.ParentResourceID,
			Name: item.Name, Directory: item.IsDir, SizeBytes: item.Size, ModifiedAt: item.Mtime,
			Presence: presence, IntroducedAtGeneration: *item.IntroducedAtGeneration,
			LastConfirmedGeneration: *item.LastConfirmedGeneration,
		})
	}
	return resources, nil
}
