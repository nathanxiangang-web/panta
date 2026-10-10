package p115

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/SheltonZhu/115driver/pkg/driver"
)

// OfflineClient is the narrow view of the pinned 115driver client that this
// adapter uses. Declaring it explicitly keeps every upstream call in one place
// and keeps the dependency surface auditable.
type OfflineClient interface {
	AddOfflineTaskURIs(uris []string, saveDirID string, opts ...driver.OfflineOption) ([]string, error)
	ListOfflineTask(page int64) (driver.OfflineTaskResp, error)
	DeleteOfflineTasks(hashes []string, deleteFiles bool) error
}

var _ OfflineClient = (*driver.Pan115Client)(nil)

// CookiedBackend implements Backend over a 115driver client that already holds an
// imported credential.
//
// It never holds the cookie itself: credential material lives only inside the
// 115driver client's cookie jar, so no secret can leak through this struct, its
// errors, or any diagnostic rendering of it.
type CookiedBackend struct {
	client OfflineClient
	trace  *startHTTPTrace
	// mu keeps this backend safe if it is ever driven concurrently; the adapter
	// also serializes its own calls.
	mu sync.Mutex
}

var _ Backend = (*CookiedBackend)(nil)

// NewCookiedBackend wraps an authenticated 115driver client. Construction
// performs no provider call.
func NewCookiedBackend(client OfflineClient) (*CookiedBackend, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: 115 client is required", ErrInvalidDownloadRequest)
	}
	return &CookiedBackend{client: client}, nil
}

// AddOfflineTaskURI submits exactly one URI to one destination directory. The URI
// is passed through byte-for-byte with no normalization or truncation.
func (backend *CookiedBackend) AddOfflineTaskURI(ctx context.Context, uri string, saveDirID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.trace != nil {
		backend.trace.reset()
	}
	hashes, err := backend.client.AddOfflineTaskURIs([]string{uri}, saveDirID)
	if err != nil && backend.trace != nil {
		diagnostic := newStartDiagnostic(err, 0)
		diagnostic.locate(err, backend.trace)
		return nil, diagnostic
	}
	return hashes, err
}

// ListOfflineTasks reads one page and projects it into adapter-private terms so no
// 115-specific type escapes this package.
func (backend *CookiedBackend) ListOfflineTasks(ctx context.Context, page int64) (OfflineTaskPage, error) {
	if err := ctx.Err(); err != nil {
		return OfflineTaskPage{}, err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	response, err := backend.client.ListOfflineTask(page)
	if err != nil {
		return OfflineTaskPage{}, err
	}
	projected := OfflineTaskPage{
		Page:      response.Page,
		PageCount: response.PageCount,
		Total:     response.Total,
		Tasks:     make([]OfflineTask, 0, len(response.Tasks)),
	}
	for _, task := range response.Tasks {
		if task == nil {
			continue
		}
		projected.Tasks = append(projected.Tasks, OfflineTask{
			InfoHash: task.InfoHash,
			Status:   task.Status,
			Name:     task.Name,
		})
	}
	return projected, nil
}

// DeleteOfflineTask removes exactly one control-plane task. deleteFiles is
// mandatory false so cancelling a Panta task can never delete already-created
// provider storage, and no broad clear operation is ever used.
func (backend *CookiedBackend) DeleteOfflineTask(ctx context.Context, infoHash string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.client.DeleteOfflineTasks([]string{infoHash}, false)
}

// errCredentialInvalid is deliberately static: a rejected cookie must never be
// echoed back, because cookie material is a secret.
var errCredentialInvalid = errors.New("115 cookie credential is invalid")

// parseCredential parses a 115 cookie string into a credential. It performs no
// provider call and no login check.
//
// The pinned release exposes cookie parsing as a method on Credential rather than
// the package-level CredentialFromCookie helper, so this wrapper is the single
// place that adapts to the pinned API.
func parseCredential(cookie []byte) (*driver.Credential, error) {
	credential := &driver.Credential{}
	if err := credential.FromCookie(string(cookie)); err != nil {
		// The upstream error may embed cookie fragments, so it is intentionally
		// replaced instead of wrapped.
		return nil, errCredentialInvalid
	}
	return credential, nil
}

// NewAdapterFromCookie builds a ready 115 DownloaderProvider from resolved secret
// material supplied by the Gate 3.5 secret boundary.
//
// The secret bytes are the 115 cookie string. Construction parses the cookie and
// imports it into a fresh client; it never calls LoginCheck, and it makes no
// request of its own. The cookie value is never logged, never returned, and never
// embedded in an error.
//
// Note that the first real provider operation on 115driver may lazily resolve the
// authenticated user, so even a successful construction is not by itself a
// guarantee of network isolation; tests drive the adapter through an injected
// Backend instead.
func NewAdapterFromCookie(cookie []byte, options Options) (*Adapter, error) {
	timeout := options.HTTPTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	if timeout < time.Second || timeout > 10*time.Second {
		return nil, fmt.Errorf("%w: 115 HTTP timeout outside supported bound", ErrInvalidDownloadRequest)
	}
	credential, err := parseCredential(cookie)
	if err != nil {
		return nil, err
	}
	trace := &startHTTPTrace{}
	client := newDriverClient(&http.Client{Timeout: timeout,
		Transport: statusTransport{next: http.DefaultTransport, trace: trace}}).ImportCredential(credential)
	// Drop this package's only reference to the secret material. The imported
	// client owns its own cookies from here on.
	forgetCredential(credential)

	backend, err := NewCookiedBackend(client)
	if err != nil {
		return nil, err
	}
	backend.trace = trace
	options.Backend = backend
	return New(options)
}

// SetHttpClient replaces the pinned driver's resty client, including its
// headers. Install the bounded HTTP client before applying the SDK's default
// User-Agent, otherwise task submission sends resty's default identity.
func newDriverClient(client *http.Client) *driver.Pan115Client {
	return driver.New(driver.WithClient(client), driver.UA())
}

// forgetCredential clears the parsed credential fields so this package does not
// retain a second copy of the secret after import.
func forgetCredential(credential *driver.Credential) {
	if credential == nil {
		return
	}
	credential.UID = ""
	credential.CID = ""
	credential.SEID = ""
	credential.KID = ""
}
