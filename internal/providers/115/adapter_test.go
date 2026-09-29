package p115

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// Compile-time assertion: the concrete adapter implements the real Panta port.
var _ contracts.DownloaderProvider = (*Adapter)(nil)

// fakeBackend is a controlled adapter-private backend. It needs no 115
// credentials and performs no network access.
type fakeBackend struct {
	mu sync.Mutex

	addCalls  int
	addURIs   []string
	addDirs   []string
	addResult []string
	addErr    error

	listCalls  int
	listPages  []int64
	listByPage map[int64]OfflineTaskPage
	listErr    map[int64]error

	deleteCalls  int
	deleteHashes []string
	deleteFlags  []bool
	deleteErr    error
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		listByPage: map[int64]OfflineTaskPage{},
		listErr:    map[int64]error{},
	}
}

func (backend *fakeBackend) AddOfflineTaskURI(_ context.Context, uri, saveDirID string) ([]string, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.addCalls++
	backend.addURIs = append(backend.addURIs, uri)
	backend.addDirs = append(backend.addDirs, saveDirID)
	if backend.addErr != nil {
		return nil, backend.addErr
	}
	return append([]string(nil), backend.addResult...), nil
}

func (backend *fakeBackend) ListOfflineTasks(_ context.Context, page int64) (OfflineTaskPage, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.listCalls++
	backend.listPages = append(backend.listPages, page)
	if err, exists := backend.listErr[page]; exists {
		return OfflineTaskPage{}, err
	}
	return backend.listByPage[page], nil
}

func (backend *fakeBackend) DeleteOfflineTask(_ context.Context, infoHash string) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	backend.deleteCalls++
	backend.deleteHashes = append(backend.deleteHashes, infoHash)
	// Record the flag the adapter is expected to force. The adapter has no way to
	// request deleteFiles=true; this records that contract explicitly.
	backend.deleteFlags = append(backend.deleteFlags, false)
	if backend.deleteErr != nil {
		return backend.deleteErr
	}
	return nil
}

func (backend *fakeBackend) addSnapshot() (calls int, uris, dirs []string) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.addCalls, append([]string(nil), backend.addURIs...), append([]string(nil), backend.addDirs...)
}

func (backend *fakeBackend) listSnapshot() (calls int, pages []int64) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.listCalls, append([]int64(nil), backend.listPages...)
}

func (backend *fakeBackend) deleteSnapshot() (calls int, hashes []string, flags []bool) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return backend.deleteCalls, append([]string(nil), backend.deleteHashes...), append([]bool(nil), backend.deleteFlags...)
}

func newAdapter(t *testing.T, backend Backend, options ...func(*Options)) *Adapter {
	t.Helper()
	opts := Options{Backend: backend}
	for _, option := range options {
		option(&opts)
	}
	adapter, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return adapter
}

func validRequest() contracts.DownloadRequest {
	return contracts.DownloadRequest{
		Source: contracts.SourceReference{Scheme: "magnet", Value: "magnet:?xt=urn:btih:abc123&dn=example"},
		Target: contracts.TargetPath{Scope: "save-dir-42", Path: "/downloads/example"},
	}
}

// ---------------------------------------------------------------------------
// 1-2: descriptor
// ---------------------------------------------------------------------------

func TestDescriptorIsFixedTo115DownloaderOnly(t *testing.T) {
	adapter := newAdapter(t, newFakeBackend())
	descriptor := adapter.Descriptor()
	if descriptor.ID != "115" {
		t.Fatalf("descriptor ID = %q, want \"115\"", descriptor.ID)
	}
	if !descriptor.Capabilities.Downloader {
		t.Fatal("descriptor must advertise downloader capability")
	}
	if descriptor.Capabilities.Storage || descriptor.Capabilities.Sharing {
		t.Fatalf("descriptor advertises capabilities this adapter slice does not implement: %#v", descriptor.Capabilities)
	}
}

func TestNewRequiresBackend(t *testing.T) {
	if _, err := New(Options{}); !errors.Is(err, ErrInvalidDownloadRequest) {
		t.Fatalf("New() error = %v, want ErrInvalidDownloadRequest", err)
	}
}

// ---------------------------------------------------------------------------
// 8-11, 13-14: StartDownload
// ---------------------------------------------------------------------------

func TestStartDownloadSupportsFrozenSchemes(t *testing.T) {
	tests := []struct {
		scheme  string
		value   string
		wantURI string
	}{
		{scheme: "http", value: "http://example.test/file.iso", wantURI: "http://example.test/file.iso"},
		{scheme: "https", value: "https://example.test/file.iso", wantURI: "https://example.test/file.iso"},
		{scheme: "magnet", value: "magnet:?xt=urn:btih:deadbeef", wantURI: "magnet:?xt=urn:btih:deadbeef"},
		{scheme: "ed2k", value: "ed2k://|file|example.iso|1024|ABCDEF|/", wantURI: "ed2k://|file|example.iso|1024|ABCDEF|/"},
		{scheme: "HTTP", value: "http://example.test/upper", wantURI: "http://example.test/upper"},
	}
	for _, test := range tests {
		t.Run(test.scheme, func(t *testing.T) {
			backend := newFakeBackend()
			backend.addResult = []string{"abc123hash"}
			adapter := newAdapter(t, backend)

			request := validRequest()
			request.Source.Scheme = test.scheme
			request.Source.Value = test.value

			reference, err := adapter.StartDownload(context.Background(), request)
			if err != nil {
				t.Fatalf("StartDownload() error = %v", err)
			}
			if reference.Value != "abc123hash" {
				t.Fatalf("TaskReference = %q, want the provider-issued info hash", reference.Value)
			}
			_, uris, dirs := backend.addSnapshot()
			if len(uris) != 1 || uris[0] != test.wantURI {
				t.Fatalf("submitted URIs = %q, want exactly [%q]", uris, test.wantURI)
			}
			if len(dirs) != 1 || dirs[0] != "save-dir-42" {
				t.Fatalf("submitted saveDirID = %q, want the exact Target.Scope", dirs)
			}
		})
	}
}

// TestStartDownloadDoesNotRewriteSourceValue proves the adapter never truncates
// or normalizes the source, including awkward magnet text.
func TestStartDownloadDoesNotRewriteSourceValue(t *testing.T) {
	opaque := "  magnet:?xt=urn:btih:ABCdef&dn=space%20name&tr=udp%3A%2F%2Ftracker  "
	backend := newFakeBackend()
	backend.addResult = []string{"hash-1"}
	adapter := newAdapter(t, backend)

	request := validRequest()
	request.Source.Value = opaque
	if _, err := adapter.StartDownload(context.Background(), request); err != nil {
		t.Fatalf("StartDownload() error = %v", err)
	}
	_, uris, _ := backend.addSnapshot()
	if len(uris) != 1 || uris[0] != opaque {
		t.Fatalf("submitted URI = %q, want it passed byte-for-byte as %q", uris, opaque)
	}
}

// TestStartDownloadNeverUsesTargetPathAsSaveDir proves the destination comes only
// from Target.Scope and never from Panta's observation path.
func TestStartDownloadNeverUsesTargetPathAsSaveDir(t *testing.T) {
	backend := newFakeBackend()
	backend.addResult = []string{"hash-1"}
	adapter := newAdapter(t, backend)

	request := validRequest()
	request.Target.Scope = "dir-from-config"
	request.Target.Path = "/openlist/mount/looks-like/a/dir-id"
	if _, err := adapter.StartDownload(context.Background(), request); err != nil {
		t.Fatalf("StartDownload() error = %v", err)
	}
	_, _, dirs := backend.addSnapshot()
	if len(dirs) != 1 || dirs[0] != "dir-from-config" {
		t.Fatalf("saveDirID = %q, want Target.Scope only", dirs)
	}
	for _, dir := range dirs {
		if strings.Contains(dir, request.Target.Path) {
			t.Fatalf("saveDirID %q was derived from Target.Path", dir)
		}
	}
}

func TestStartDownloadRejectsInvalidRequestsBeforeBackendCall(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*contracts.DownloadRequest)
		wantErr error
	}{
		{name: "unsupported scheme", mutate: func(r *contracts.DownloadRequest) { r.Source.Scheme = "ftp" }, wantErr: ErrUnsupportedSource},
		{name: "empty scheme", mutate: func(r *contracts.DownloadRequest) { r.Source.Scheme = "" }, wantErr: ErrUnsupportedSource},
		{name: "blank source value", mutate: func(r *contracts.DownloadRequest) { r.Source.Value = "   " }, wantErr: ErrInvalidDownloadRequest},
		{name: "empty source value", mutate: func(r *contracts.DownloadRequest) { r.Source.Value = "" }, wantErr: ErrInvalidDownloadRequest},
		{name: "NUL in source value", mutate: func(r *contracts.DownloadRequest) { r.Source.Value = "magnet:?x\x00y" }, wantErr: ErrInvalidDownloadRequest},
		{name: "overlong source value", mutate: func(r *contracts.DownloadRequest) {
			r.Source.Value = "magnet:?" + strings.Repeat("x", MaxSourceValueLength)
		}, wantErr: ErrInvalidDownloadRequest},
		{name: "blank target scope", mutate: func(r *contracts.DownloadRequest) { r.Target.Scope = "  " }, wantErr: ErrInvalidDownloadRequest},
		{name: "empty target scope", mutate: func(r *contracts.DownloadRequest) { r.Target.Scope = "" }, wantErr: ErrInvalidDownloadRequest},
		{name: "NUL in target scope", mutate: func(r *contracts.DownloadRequest) { r.Target.Scope = "dir\x00id" }, wantErr: ErrInvalidDownloadRequest},
		{name: "overlong target scope", mutate: func(r *contracts.DownloadRequest) {
			r.Target.Scope = strings.Repeat("d", MaxTargetScopeLength+1)
		}, wantErr: ErrInvalidDownloadRequest},
		{name: "blank target path", mutate: func(r *contracts.DownloadRequest) { r.Target.Path = "   " }, wantErr: ErrInvalidDownloadRequest},
		{name: "NUL in target path", mutate: func(r *contracts.DownloadRequest) { r.Target.Path = "/a\x00b" }, wantErr: ErrInvalidDownloadRequest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := newFakeBackend()
			adapter := newAdapter(t, backend)
			request := validRequest()
			test.mutate(&request)

			if _, err := adapter.StartDownload(context.Background(), request); !errors.Is(err, test.wantErr) {
				t.Fatalf("StartDownload() error = %v, want %v", err, test.wantErr)
			}
			if calls, _, _ := backend.addSnapshot(); calls != 0 {
				t.Fatalf("backend add calls = %d, want 0 for a rejected request", calls)
			}
		})
	}
}

func TestStartDownloadRequiresExactlyOneNonBlankHash(t *testing.T) {
	tests := []struct {
		name    string
		hashes  []string
		wantErr error
	}{
		{name: "zero hashes", hashes: []string{}, wantErr: ErrTaskReferenceMissing},
		{name: "nil hashes", hashes: nil, wantErr: ErrTaskReferenceMissing},
		{name: "blank hash", hashes: []string{"   "}, wantErr: ErrTaskReferenceMissing},
		{name: "empty hash", hashes: []string{""}, wantErr: ErrTaskReferenceMissing},
		{name: "two hashes", hashes: []string{"hash-1", "hash-2"}, wantErr: ErrTaskReferenceAmbiguous},
		{name: "one real plus blank", hashes: []string{"hash-1", ""}, wantErr: ErrTaskReferenceMissing},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := newFakeBackend()
			backend.addResult = test.hashes
			adapter := newAdapter(t, backend)

			if _, err := adapter.StartDownload(context.Background(), validRequest()); !errors.Is(err, test.wantErr) {
				t.Fatalf("StartDownload() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestStartDownloadAttributesBackendErrorWithoutRetry(t *testing.T) {
	backend := newFakeBackend()
	backend.addErr = errors.New("upstream 115 request failed")
	adapter := newAdapter(t, backend)

	_, err := adapter.StartDownload(context.Background(), validRequest())
	if !errors.Is(err, ErrBackendStart) {
		t.Fatalf("StartDownload() error = %v, want ErrBackendStart", err)
	}
	if !strings.Contains(err.Error(), "upstream 115 request failed") {
		t.Fatalf("error %v does not attribute the backend failure", err)
	}
	// Exactly one attempt: the adapter never retries.
	if calls, _, _ := backend.addSnapshot(); calls != 1 {
		t.Fatalf("backend add calls = %d, want exactly 1", calls)
	}
}

// ---------------------------------------------------------------------------
// 17-22: DownloadStatus
// ---------------------------------------------------------------------------

func TestDownloadStatusFindsHashAcrossPages(t *testing.T) {
	backend := newFakeBackend()
	backend.listByPage[1] = OfflineTaskPage{Page: 1, PageCount: 4, Total: 4, Tasks: []OfflineTask{
		{InfoHash: "other-1", Status: 0},
	}}
	backend.listByPage[2] = OfflineTaskPage{Page: 2, PageCount: 4, Total: 4, Tasks: []OfflineTask{
		{InfoHash: "wanted-hash", Status: 1},
	}}
	adapter := newAdapter(t, backend)

	status, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: "wanted-hash"})
	if err != nil {
		t.Fatalf("DownloadStatus() error = %v", err)
	}
	if status.State != contracts.TaskStateRunning {
		t.Fatalf("state = %q, want running", status.State)
	}
	if status.Reference.Value != "wanted-hash" {
		t.Fatalf("reference = %q, want the exact requested hash", status.Reference.Value)
	}
	calls, pages := backend.listSnapshot()
	if calls != 2 || len(pages) != 2 || pages[0] != 1 || pages[1] != 2 {
		t.Fatalf("list calls=%d pages=%v, want sequential pages 1 then 2", calls, pages)
	}
}

func TestDownloadStatusMapsFrozenStatusCodes(t *testing.T) {
	tests := []struct {
		code int
		want contracts.TaskState
	}{
		{code: 0, want: contracts.TaskStatePending},
		{code: 1, want: contracts.TaskStateRunning},
		{code: 2, want: contracts.TaskStateSucceeded},
		{code: -1, want: contracts.TaskStateFailed},
	}
	for _, test := range tests {
		t.Run(string(test.want), func(t *testing.T) {
			backend := newFakeBackend()
			backend.listByPage[1] = OfflineTaskPage{Page: 1, PageCount: 1, Tasks: []OfflineTask{
				{InfoHash: "target-hash", Status: test.code},
			}}
			adapter := newAdapter(t, backend)

			status, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: "target-hash"})
			if err != nil {
				t.Fatalf("DownloadStatus() error = %v", err)
			}
			if status.State != test.want {
				t.Fatalf("state = %q, want %q", status.State, test.want)
			}
			// Provider success is provider success only: never a Panta milestone.
			if string(status.State) == "READY" || string(status.State) == "SUCCEEDED_READY" {
				t.Fatal("provider state must not encode a Panta Manifest milestone")
			}
		})
	}
}

func TestDownloadStatusRejectsUnknownStatusCode(t *testing.T) {
	for _, code := range []int{3, -2, 99} {
		backend := newFakeBackend()
		backend.listByPage[1] = OfflineTaskPage{Page: 1, PageCount: 1, Tasks: []OfflineTask{
			{InfoHash: "target-hash", Status: code},
		}}
		adapter := newAdapter(t, backend)

		if _, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: "target-hash"}); !errors.Is(err, ErrTaskStateUnknown) {
			t.Fatalf("status %d error = %v, want ErrTaskStateUnknown", code, err)
		}
	}
}

func TestDownloadStatusReportsMissingHashExplicitly(t *testing.T) {
	backend := newFakeBackend()
	backend.listByPage[1] = OfflineTaskPage{Page: 1, PageCount: 2, Tasks: []OfflineTask{
		{InfoHash: "someone-else", Status: 2},
	}}
	backend.listByPage[2] = OfflineTaskPage{Page: 2, PageCount: 2, Tasks: nil}
	adapter := newAdapter(t, backend)

	_, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: "missing-hash"})
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("DownloadStatus() error = %v, want ErrTaskNotFound", err)
	}
}

func TestDownloadStatusRejectsInvalidTaskReference(t *testing.T) {
	for _, reference := range []string{"", "   ", "hash\x00value", strings.Repeat("h", 257)} {
		backend := newFakeBackend()
		adapter := newAdapter(t, backend)
		if _, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: reference}); !errors.Is(err, ErrInvalidTaskReference) {
			t.Fatalf("reference %q error = %v, want ErrInvalidTaskReference", reference, err)
		}
		if calls, _ := backend.listSnapshot(); calls != 0 {
			t.Fatalf("backend list calls = %d, want 0", calls)
		}
	}
}

func TestDownloadStatusPaginationIsBoundedAndFailClosed(t *testing.T) {
	t.Run("malformed page count", func(t *testing.T) {
		backend := newFakeBackend()
		backend.listByPage[1] = OfflineTaskPage{Page: 1, PageCount: 0, Tasks: []OfflineTask{{InfoHash: "x"}}}
		adapter := newAdapter(t, backend)
		if _, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: "target"}); !errors.Is(err, ErrPaginationUnbounded) {
			t.Fatalf("error = %v, want ErrPaginationUnbounded", err)
		}
	})

	t.Run("provider reports a different page", func(t *testing.T) {
		backend := newFakeBackend()
		backend.listByPage[1] = OfflineTaskPage{Page: 7, PageCount: 3, Tasks: []OfflineTask{{InfoHash: "x"}}}
		adapter := newAdapter(t, backend)
		if _, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: "target"}); !errors.Is(err, ErrPaginationUnbounded) {
			t.Fatalf("error = %v, want ErrPaginationUnbounded", err)
		}
	})

	t.Run("page count exceeds the page bound", func(t *testing.T) {
		backend := newFakeBackend()
		for page := int64(1); page <= 4; page++ {
			backend.listByPage[page] = OfflineTaskPage{
				Page: page, PageCount: 1000, Tasks: []OfflineTask{{InfoHash: "not-the-target"}},
			}
		}
		adapter := newAdapter(t, backend, func(options *Options) { options.MaxPages = 3 })
		if _, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: "target"}); !errors.Is(err, ErrPaginationUnbounded) {
			t.Fatalf("error = %v, want ErrPaginationUnbounded", err)
		}
		calls, _ := backend.listSnapshot()
		if calls != 3 {
			t.Fatalf("list calls = %d, want exactly the bound of 3", calls)
		}
	})
}

func TestDownloadStatusAttributesBackendListError(t *testing.T) {
	backend := newFakeBackend()
	backend.listErr[1] = errors.New("upstream list failed")
	adapter := newAdapter(t, backend)

	_, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: "target"})
	if !errors.Is(err, ErrBackendList) {
		t.Fatalf("error = %v, want ErrBackendList", err)
	}
	if !strings.Contains(err.Error(), "upstream list failed") {
		t.Fatalf("error %v does not attribute the backend failure", err)
	}
}

// ---------------------------------------------------------------------------
// 23-25: Cancel
// ---------------------------------------------------------------------------

func TestCancelDownloadDeletesExactlyOneTaskWithoutFiles(t *testing.T) {
	backend := newFakeBackend()
	adapter := newAdapter(t, backend)

	if err := adapter.CancelDownload(context.Background(), contracts.TaskReference{Value: "cancel-this-hash"}); err != nil {
		t.Fatalf("CancelDownload() error = %v", err)
	}
	calls, hashes, flags := backend.deleteSnapshot()
	if calls != 1 {
		t.Fatalf("delete calls = %d, want exactly 1", calls)
	}
	if len(hashes) != 1 || hashes[0] != "cancel-this-hash" {
		t.Fatalf("deleted hashes = %q, want exactly the requested hash", hashes)
	}
	for index, flag := range flags {
		if flag {
			t.Fatalf("delete call %d requested file deletion; cancelling must never delete storage", index)
		}
	}
}

func TestCancelDownloadRejectsInvalidReference(t *testing.T) {
	backend := newFakeBackend()
	adapter := newAdapter(t, backend)
	if err := adapter.CancelDownload(context.Background(), contracts.TaskReference{Value: "  "}); !errors.Is(err, ErrInvalidTaskReference) {
		t.Fatalf("CancelDownload() error = %v, want ErrInvalidTaskReference", err)
	}
	if calls, _, _ := backend.deleteSnapshot(); calls != 0 {
		t.Fatalf("delete calls = %d, want 0", calls)
	}
}

func TestCancelDownloadAttributesBackendError(t *testing.T) {
	backend := newFakeBackend()
	backend.deleteErr = errors.New("upstream delete failed")
	adapter := newAdapter(t, backend)

	err := adapter.CancelDownload(context.Background(), contracts.TaskReference{Value: "hash"})
	if !errors.Is(err, ErrBackendDelete) {
		t.Fatalf("error = %v, want ErrBackendDelete", err)
	}
	if calls, _, _ := backend.deleteSnapshot(); calls != 1 {
		t.Fatalf("delete calls = %d, want exactly 1", calls)
	}
}
