// Package p115 implements Panta's first concrete provider adapter: 115 offline
// (cloud) download, backed by the pinned github.com/SheltonZhu/115driver library.
//
// The package is deliberately isolated. It depends only on
// internal/providers/contracts, the pinned 115driver driver package, and the
// standard library. It has no knowledge of Panta persistence, jobs, OpenList,
// IndexCore, search, agent, auth, or the session registry, and it never resolves
// a CredentialRef itself: the composition layer owns
// CredentialRef -> SecretResolver -> adapter construction.
package p115

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// Fixed adapter identity and request bounds.
const (
	MaxSourceValueLength = 4096
	MaxTargetScopeLength = 256
	MaxTargetPathLength  = 2048

	// DefaultMaxPages bounds how many provider task pages a status lookup may
	// read. Pagination is fail-closed: a malformed or non-progressing page
	// sequence aborts instead of looping.
	DefaultMaxPages = 50

	// DefaultTaskPageSize is informational only; the provider owns page sizing.
	DefaultTaskPageSize = 32
)

// OfflineTask is the provider-neutral projection of one 115 offline task.
// It is adapter-private so no 115-specific type escapes this package.
type OfflineTask struct {
	InfoHash string
	Status   int
	Name     string
}

// OfflineTaskPage is one page of provider tasks.
type OfflineTaskPage struct {
	Tasks     []OfflineTask
	Page      int64
	PageCount int64
	Total     int64
}

// Backend is the narrow adapter-private port over 115driver offline operations.
//
// The concrete implementation is provided by the pinned library in this package;
// unit tests supply a controlled in-memory double so they need neither real 115
// credentials nor network access.
type Backend interface {
	// AddOfflineTaskURI submits exactly one URI to one destination directory and
	// returns the provider-issued info hashes.
	AddOfflineTaskURI(ctx context.Context, uri string, saveDirID string) ([]string, error)
	// ListOfflineTasks reads one page of provider tasks.
	ListOfflineTasks(ctx context.Context, page int64) (OfflineTaskPage, error)
	// DeleteOfflineTask removes exactly one control-plane task and must never
	// delete already-downloaded files.
	DeleteOfflineTask(ctx context.Context, infoHash string) error
}

// Options configures an adapter.
type Options struct {
	// Backend is required. It is the only way the adapter reaches 115.
	Backend Backend
	// Now is an optional clock, used for context deadlines in tests.
	Now func() time.Time
	// MaxPages bounds status pagination. Zero uses DefaultMaxPages.
	MaxPages int
}

// Adapter is the concrete 115 DownloaderProvider.
type Adapter struct {
	backend    Backend
	descriptor contracts.Descriptor
	now        func() time.Time
	maxPages   int64

	// mu serializes backend calls. The pinned client holds mutable per-client
	// state (user id, request object), so a single adapter instance must not be
	// driven concurrently from multiple goroutines.
	mu sync.Mutex
}

var _ contracts.DownloaderProvider = (*Adapter)(nil)

// New builds an adapter around a backend. It performs no provider call.
func New(options Options) (*Adapter, error) {
	if options.Backend == nil {
		return nil, fmt.Errorf("%w: backend is required", ErrInvalidDownloadRequest)
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	maxPages := int64(options.MaxPages)
	if maxPages <= 0 {
		maxPages = DefaultMaxPages
	}
	return &Adapter{
		backend: options.Backend,
		descriptor: contracts.Descriptor{
			ID:          ProviderID,
			DisplayName: "115 offline download",
			// This adapter slice implements downloader capability only.
			Capabilities: contracts.CapabilitySet{Downloader: true},
		},
		now: now, maxPages: maxPages,
	}, nil
}

func (adapter *Adapter) Descriptor() contracts.Descriptor { return adapter.descriptor }

// StartDownload submits exactly one 115 offline URI task and returns the single
// provider-issued info hash. There is deliberately no retry here: the Gate 3.4
// fence and the Job Engine own replay and recovery policy.
func (adapter *Adapter) StartDownload(ctx context.Context, request contracts.DownloadRequest) (contracts.TaskReference, error) {
	if err := ctx.Err(); err != nil {
		return contracts.TaskReference{}, err
	}
	if err := validateDownloadRequest(request); err != nil {
		return contracts.TaskReference{}, err
	}

	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	hashes, err := adapter.backend.AddOfflineTaskURI(ctx, request.Source.Value, request.Target.Scope)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return contracts.TaskReference{}, err
		}
		// The cause is wrapped for diagnosis. It never contains secret material
		// because the backend boundary never receives credentials.
		return contracts.TaskReference{}, fmt.Errorf("%w: %w", ErrBackendStart, err)
	}
	reference, err := singleInfoHash(hashes)
	if err != nil {
		return contracts.TaskReference{}, err
	}
	return contracts.TaskReference{Value: reference}, nil
}

// DownloadStatus finds the exact info hash across a bounded number of provider
// task pages and maps its status. Provider success is never translated into
// Manifest READY.
//
// D-032: a succeeded task additionally reports the acquired top-level object name
// when the provider exposes a usable one. A blank or absent upstream name is NOT a
// download failure: it yields SUCCEEDED with no result name, so the acquisition
// layer can fall back to the request intent. A present but malformed name is a
// contract violation and fails closed. Only OfflineTask.Name crosses this boundary;
// FileId and DirId stay provider-private.
func (adapter *Adapter) DownloadStatus(ctx context.Context, reference contracts.TaskReference) (contracts.TaskStatus, error) {
	if err := ctx.Err(); err != nil {
		return contracts.TaskStatus{}, err
	}
	if !validInfoHash(reference.Value) {
		return contracts.TaskStatus{}, fmt.Errorf("%w: %q", ErrInvalidTaskReference, reference.Value)
	}

	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	task, err := adapter.findTask(ctx, reference.Value)
	if err != nil {
		return contracts.TaskStatus{}, err
	}
	state, err := mapTaskStatus(task.Status)
	if err != nil {
		return contracts.TaskStatus{}, fmt.Errorf("%w: %d", ErrTaskStateUnknown, task.Status)
	}
	status := contracts.TaskStatus{Reference: reference, State: state}
	if state != contracts.TaskStateSucceeded {
		// Pending, running, failed, and canceled tasks carry no result identity.
		return status, nil
	}
	// The download succeeded. A blank name means the provider exposed no usable
	// locator; that is a missing identifier, not a failed acquisition.
	if strings.TrimSpace(task.Name) == "" {
		return status, nil
	}
	if err := contracts.ValidateDirectChildName(task.Name); err != nil {
		return contracts.TaskStatus{}, fmt.Errorf("%w: succeeded task %s reported name %q: %w",
			ErrTaskResultNameInvalid, reference.Value, task.Name, err)
	}
	status.Result = &contracts.DownloadResult{Name: task.Name}
	return status, nil
}

// findTask walks provider pages until the exact info hash is found. Pagination is
// bounded and fail-closed: a page must report a usable, progressing page index.
func (adapter *Adapter) findTask(ctx context.Context, infoHash string) (OfflineTask, error) {
	seenPages := map[int64]bool{}
	page := int64(1)
	for page <= adapter.maxPages {
		if err := ctx.Err(); err != nil {
			return OfflineTask{}, err
		}
		listed, err := adapter.backend.ListOfflineTasks(ctx, page)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return OfflineTask{}, err
			}
			return OfflineTask{}, fmt.Errorf("%w: %w", ErrBackendList, err)
		}
		for _, task := range listed.Tasks {
			if task.InfoHash == infoHash {
				return task, nil
			}
		}
		// A page must report a usable page count so the walk can terminate. A
		// malformed or repeated page index aborts instead of looping forever.
		if listed.PageCount <= 0 {
			return OfflineTask{}, fmt.Errorf("%w: page %d reported page_count %d",
				ErrPaginationUnbounded, page, listed.PageCount)
		}
		if listed.Page != 0 && listed.Page != page {
			return OfflineTask{}, fmt.Errorf("%w: requested page %d, provider reported %d",
				ErrPaginationUnbounded, page, listed.Page)
		}
		if page >= listed.PageCount {
			return OfflineTask{}, fmt.Errorf("%w: %s", ErrTaskNotFound, infoHash)
		}
		next := page + 1
		if seenPages[next] {
			return OfflineTask{}, fmt.Errorf("%w: page %d repeated", ErrPaginationUnbounded, next)
		}
		seenPages[page] = true
		page = next
	}
	return OfflineTask{}, fmt.Errorf("%w: exceeded %d pages", ErrPaginationUnbounded, adapter.maxPages)
}

// CancelDownload removes exactly one control-plane offline task. Files already
// created by the provider are never deleted: the adapter always passes
// deleteFiles=false, and it never clears unrelated tasks.
func (adapter *Adapter) CancelDownload(ctx context.Context, reference contracts.TaskReference) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validInfoHash(reference.Value) {
		return fmt.Errorf("%w: %q", ErrInvalidTaskReference, reference.Value)
	}

	adapter.mu.Lock()
	defer adapter.mu.Unlock()

	if err := adapter.backend.DeleteOfflineTask(ctx, reference.Value); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return fmt.Errorf("%w: %w", ErrBackendDelete, err)
	}
	return nil
}

// validateDownloadRequest enforces the frozen D-025 mapping before any provider
// call. Target.Path is validated as Panta's expected observation path only: it is
// never translated into the provider destination scope.
func validateDownloadRequest(request contracts.DownloadRequest) error {
	scheme := strings.ToLower(request.Source.Scheme)
	switch scheme {
	case "http", "https", "magnet", "ed2k":
	default:
		return fmt.Errorf("%w: %q", ErrUnsupportedSource, request.Source.Scheme)
	}
	if err := requireOpaqueText("source value", request.Source.Value, MaxSourceValueLength); err != nil {
		return err
	}
	if err := requireOpaqueText("target scope", request.Target.Scope, MaxTargetScopeLength); err != nil {
		return err
	}
	if strings.TrimSpace(request.Target.Path) == "" || !utf8.ValidString(request.Target.Path) ||
		strings.ContainsRune(request.Target.Path, '\x00') || utf8.RuneCountInString(request.Target.Path) > MaxTargetPathLength {
		return fmt.Errorf("%w: target path is required", ErrInvalidDownloadRequest)
	}
	return nil
}

// requireOpaqueText validates opaque adapter-owned text without normalizing or
// rewriting it.
func requireOpaqueText(field, value string, maximum int) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') ||
		utf8.RuneCountInString(value) > maximum {
		return fmt.Errorf("%w: %s", ErrInvalidDownloadRequest, field)
	}
	return nil
}

// singleInfoHash requires exactly one non-empty provider-issued hash. The pinned
// upstream may return a slice sized by the request, so any blank or extra entry
// fails closed rather than being reported as a task reference.
func singleInfoHash(hashes []string) (string, error) {
	found := ""
	for _, hash := range hashes {
		if strings.TrimSpace(hash) == "" {
			return "", fmt.Errorf("%w: provider returned a blank info hash", ErrTaskReferenceMissing)
		}
		if found != "" {
			return "", fmt.Errorf("%w: provider returned %d info hashes", ErrTaskReferenceAmbiguous, len(hashes))
		}
		found = hash
	}
	if found == "" {
		return "", ErrTaskReferenceMissing
	}
	return found, nil
}

// validInfoHash reports whether an opaque task reference is usable. 115 info
// hashes are hex, but the adapter only requires non-blank bounded text so the
// value stays opaque at the Panta boundary.
func validInfoHash(value string) bool {
	return utf8.ValidString(value) && strings.TrimSpace(value) != "" &&
		!strings.ContainsRune(value, '\x00') && utf8.RuneCountInString(value) <= 256
}
