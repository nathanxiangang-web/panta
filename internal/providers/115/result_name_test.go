package p115

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/SheltonZhu/115driver/pkg/driver"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// ---------------------------------------------------------------------------
// D-031 acquired-result identity: 115 adapter result-name coverage (tests 5-8).
//
// These tests drive only the adapter's public boundary. They reuse the package's
// existing fakeBackend / newAdapter / OfflineTask / OfflineTaskPage helpers from
// adapter_test.go and need neither 115 credentials nor network access.
// ---------------------------------------------------------------------------

// resultNameTaskHash is the single info hash every case below looks up.
const resultNameTaskHash = "result-name-task-hash"

// resultNamePage builds a one-page provider listing holding exactly one task.
func resultNamePage(task OfflineTask) OfflineTaskPage {
	return OfflineTaskPage{Page: 1, PageCount: 1, Total: 1, Tasks: []OfflineTask{task}}
}

// ---------------------------------------------------------------------------
// 5: exact mapping of the provider-reported name into TaskStatus.Result.Name
// ---------------------------------------------------------------------------

func TestDownloadResultNameMapsProviderNameByteForByte(t *testing.T) {
	cases := []struct {
		label string
		name  string
	}{
		{label: "plain", name: "plain-object.bin"},
		{label: "unicode", name: "季度归档-Ünïcødé-📦.bin"},
		{label: "interior spaces", name: "quarterly archive 2026.bin"},
		{label: "leading space", name: " leading-space.bin"},
		{label: "trailing space", name: "trailing-space.bin "},
		{label: "leading and trailing spaces", name: "  padded name  "},
		{label: "exactly at the rune bound", name: strings.Repeat("n", contracts.MaxDownloadResultNameRunes)},
	}
	for _, test := range cases {
		test := test
		t.Run(test.label, func(t *testing.T) {
			backend := newFakeBackend()
			backend.listByPage[1] = resultNamePage(OfflineTask{
				InfoHash: resultNameTaskHash,
				Status:   statusDone,
				Name:     test.name,
			})
			adapter := newAdapter(t, backend)

			status, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: resultNameTaskHash})
			if err != nil {
				t.Fatalf("DownloadStatus() error = %v, want nil for a succeeded task with a usable name", err)
			}
			if status.State != contracts.TaskStateSucceeded {
				t.Fatalf("State = %q, want %q", status.State, contracts.TaskStateSucceeded)
			}
			if status.Result == nil {
				t.Fatal("Result = nil, want the provider-reported acquired name")
			}
			// Byte-for-byte: no TrimSpace, no path.Clean, no normalization. A
			// provider name must round-trip unchanged to stay usable as canonical
			// identity input.
			if !bytes.Equal([]byte(status.Result.Name), []byte(test.name)) {
				t.Fatalf("Result.Name = %q (% x), want the exact provider name %q (% x)",
					status.Result.Name, []byte(status.Result.Name), test.name, []byte(test.name))
			}
			if err := status.Result.Validate(); err != nil {
				t.Fatalf("Result.Validate() error = %v, want nil", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// D-032 separates a MISSING name from a MALFORMED one. A blank or absent upstream
// name means the provider exposed no locator, so the task is still a legitimate
// SUCCEEDED download with no result name, and the acquisition layer may fall back to
// the request intent. A present-but-malformed name is a contract violation and fails
// closed. Treating a blank name as a download failure would misreport a successful
// 115 download as failed.
func TestDownloadResultNameBlankNameIsSucceededWithoutLocator(t *testing.T) {
	for _, test := range []struct {
		label string
		name  string
	}{
		{label: "empty", name: ""},
		{label: "spaces", name: "   "},
		{label: "tab", name: "\t"},
		{label: "newline", name: "\n"},
	} {
		t.Run(test.label, func(t *testing.T) {
			backend := newFakeBackend()
			backend.listByPage[1] = resultNamePage(OfflineTask{
				InfoHash: resultNameTaskHash,
				Status:   statusDone,
				Name:     test.name,
			})
			adapter := newAdapter(t, backend)

			status, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: resultNameTaskHash})
			if err != nil {
				t.Fatalf("DownloadStatus() error = %v with a blank name %q, want a legitimate SUCCEEDED task",
					err, test.name)
			}
			if status.State != contracts.TaskStateSucceeded {
				t.Fatalf("State = %q with a blank name, want SUCCEEDED", status.State)
			}
			// No usable locator was observed, so none may be invented.
			if status.Result != nil {
				t.Fatalf("Result = %#v with a blank name, want nil so the intent fallback applies", status.Result)
			}
		})
	}
}

func TestDownloadResultNameMalformedNameFailsClosed(t *testing.T) {
	cases := []struct {
		label string
		name  string
	}{
		{label: "dot", name: "."},
		{label: "dot dot", name: ".."},
		{label: "forward slash", name: "a/b"},
		{label: "backslash", name: `a\b`},
		{label: "overlong runes", name: strings.Repeat("x", contracts.MaxDownloadResultNameRunes+1)},
		{label: "overlong multibyte runes", name: strings.Repeat("界", contracts.MaxDownloadResultNameRunes+1)},
		{label: "NUL byte", name: "a\x00b"},
	}
	for _, test := range cases {
		test := test
		t.Run(test.label, func(t *testing.T) {
			backend := newFakeBackend()
			backend.listByPage[1] = resultNamePage(OfflineTask{
				InfoHash: resultNameTaskHash,
				Status:   statusDone,
				Name:     test.name,
			})
			adapter := newAdapter(t, backend)

			status, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: resultNameTaskHash})
			if err == nil {
				t.Fatalf("DownloadStatus() error = nil with malformed name %q, want a fail-closed error", test.name)
			}
			if !errors.Is(err, ErrTaskResultNameInvalid) {
				t.Fatalf("DownloadStatus() error = %v, want errors.Is(err, ErrTaskResultNameInvalid)", err)
			}
			if !errors.Is(err, contracts.ErrInvalidDownloadResultName) {
				t.Fatalf("DownloadStatus() error = %v, want it to attribute contracts.ErrInvalidDownloadResultName", err)
			}
			if status.Result != nil {
				t.Fatalf("Result = %#v with malformed name %q, want nil", status.Result, test.name)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 7: pending / running / failed tasks stay pollable and carry no result
// ---------------------------------------------------------------------------

func TestDownloadResultNameNonSucceededStatesStayPollable(t *testing.T) {
	cases := []struct {
		label string
		code  int
		want  contracts.TaskState
	}{
		{label: "pending", code: statusTodo, want: contracts.TaskStatePending},
		{label: "running", code: statusRunning, want: contracts.TaskStateRunning},
		{label: "failed", code: statusFailed, want: contracts.TaskStateFailed},
	}
	for _, test := range cases {
		test := test
		t.Run(test.label, func(t *testing.T) {
			backend := newFakeBackend()
			// A name is present but the task has not succeeded: the adapter must
			// still return a pollable status with no result identity.
			backend.listByPage[1] = resultNamePage(OfflineTask{
				InfoHash: resultNameTaskHash,
				Status:   test.code,
				Name:     "not-yet-usable.bin",
			})
			adapter := newAdapter(t, backend)

			status, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: resultNameTaskHash})
			if err != nil {
				t.Fatalf("DownloadStatus() error = %v, want a pollable status", err)
			}
			if status.State != test.want {
				t.Fatalf("State = %q, want %q", status.State, test.want)
			}
			if status.Reference.Value != resultNameTaskHash {
				t.Fatalf("Reference.Value = %q, want the exact requested hash", status.Reference.Value)
			}
			if status.Result != nil {
				t.Fatalf("Result = %#v, want nil for state %q", status.Result, test.want)
			}
		})
	}
}

// TestDownloadResultNameResultOnlyPopulatedForSucceeded walks the whole frozen
// 115 status set and asserts the result is populated if and only if the mapped
// state is succeeded.
func TestDownloadResultNameResultOnlyPopulatedForSucceeded(t *testing.T) {
	cases := []struct {
		label      string
		code       int
		name       string
		wantState  contracts.TaskState
		wantResult bool
	}{
		{label: "todo", code: statusTodo, name: "", wantState: contracts.TaskStatePending},
		{label: "running", code: statusRunning, name: "", wantState: contracts.TaskStateRunning},
		{label: "done", code: statusDone, name: "acquired-object.bin", wantState: contracts.TaskStateSucceeded, wantResult: true},
		{label: "failed", code: statusFailed, name: "", wantState: contracts.TaskStateFailed},
	}
	for _, test := range cases {
		test := test
		t.Run(test.label, func(t *testing.T) {
			backend := newFakeBackend()
			backend.listByPage[1] = resultNamePage(OfflineTask{
				InfoHash: resultNameTaskHash,
				Status:   test.code,
				Name:     test.name,
			})
			adapter := newAdapter(t, backend)

			status, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: resultNameTaskHash})
			if err != nil {
				t.Fatalf("DownloadStatus() error = %v, want nil", err)
			}
			if status.State != test.wantState {
				t.Fatalf("State = %q, want %q", status.State, test.wantState)
			}
			if populated := status.Result != nil; populated != test.wantResult {
				t.Fatalf("Result populated = %v, want %v for state %q", populated, test.wantResult, status.State)
			}
			if succeeded := status.State == contracts.TaskStateSucceeded; succeeded != test.wantResult {
				t.Fatalf("State %q succeeded = %v, want result populated = %v", status.State, succeeded, test.wantResult)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 8: no FileId / DirId escapes the 115 package
// ---------------------------------------------------------------------------

// resultNameUpstreamClient is a minimal double for the upstream OfflineClient
// port consumed by CookiedBackend. It is deliberately NOT a Backend double and
// does not compete with the package's fakeBackend: it exists because the
// provider-private FileId/DirId fields live only on the pinned upstream task type
// (github.com/SheltonZhu/115driver driver.OfflineTask). The adapter-private
// OfflineTask projection has no such fields by design, so injecting the sentinels
// upstream and driving the real CookiedBackend.ListOfflineTasks projection is the
// only way to prove end-to-end that they are dropped.
type resultNameUpstreamClient struct {
	pages map[int64]driver.OfflineTaskResp
}

func (client *resultNameUpstreamClient) AddOfflineTaskURIs(_ []string, _ string, _ ...driver.OfflineOption) ([]string, error) {
	return nil, nil
}

func (client *resultNameUpstreamClient) ListOfflineTask(page int64) (driver.OfflineTaskResp, error) {
	return client.pages[page], nil
}

func (client *resultNameUpstreamClient) DeleteOfflineTasks(_ []string, _ bool) error { return nil }

// TestDownloadResultNameNeverLeaksProviderFileAndDirIDs proves that neither the
// returned contracts.TaskStatus nor its Result carries a provider-private
// FileId/DirId anywhere. FileId and DirId are 115-internal identifiers: they must
// never become Panta or IndexCore identity, because provider IDs are not
// portable canonical identity and would silently couple the domain to 115.
func TestDownloadResultNameNeverLeaksProviderFileAndDirIDs(t *testing.T) {
	// Distinctive sentinels: if any of them appears in the returned status, an
	// internal provider identifier has escaped the adapter.
	const (
		fileIDSentinel    = "115-FILEID-SENTINEL-4c1f9d"
		delFileIDSentinel = "115-DELFILEID-SENTINEL-77e0a2"
		dirIDSentinel     = "115-WPPATHID-SENTINEL-9ab2c4"
	)
	sentinels := []string{fileIDSentinel, delFileIDSentinel, dirIDSentinel}

	cases := []struct {
		label     string
		code      int
		name      string
		wantState contracts.TaskState
	}{
		{label: "succeeded", code: statusDone, name: "acquired-object.bin", wantState: contracts.TaskStateSucceeded},
		{label: "pending", code: statusTodo, name: "acquired-object.bin", wantState: contracts.TaskStatePending},
		{label: "running", code: statusRunning, name: "acquired-object.bin", wantState: contracts.TaskStateRunning},
		{label: "failed", code: statusFailed, name: "acquired-object.bin", wantState: contracts.TaskStateFailed},
	}
	for _, test := range cases {
		test := test
		t.Run(test.label, func(t *testing.T) {
			client := &resultNameUpstreamClient{pages: map[int64]driver.OfflineTaskResp{
				1: {
					Page:      1,
					PageCount: 1,
					Total:     1,
					Tasks: []*driver.OfflineTask{{
						InfoHash:  resultNameTaskHash,
						Status:    test.code,
						Name:      test.name,
						FileId:    fileIDSentinel,
						DelFileId: delFileIDSentinel,
						DirId:     dirIDSentinel,
					}},
				},
			}}
			backend, err := NewCookiedBackend(client)
			if err != nil {
				t.Fatalf("NewCookiedBackend() error = %v", err)
			}
			adapter := newAdapter(t, backend)

			status, err := adapter.DownloadStatus(context.Background(), contracts.TaskReference{Value: resultNameTaskHash})
			if err != nil {
				t.Fatalf("DownloadStatus() error = %v", err)
			}
			if status.State != test.wantState {
				t.Fatalf("State = %q, want %q", status.State, test.wantState)
			}

			// Real check over the whole value: marshalling the returned status to
			// JSON walks every exported field of TaskStatus and of its Result.
			encoded, err := json.Marshal(status)
			if err != nil {
				t.Fatalf("json.Marshal(TaskStatus) error = %v", err)
			}
			payload := string(encoded)
			for _, sentinel := range sentinels {
				if strings.Contains(payload, sentinel) {
					t.Fatalf("TaskStatus JSON %s leaked provider-private ID %q", payload, sentinel)
				}
			}
			// Positive control: the JSON does carry what the adapter may expose, so
			// the absence checks above cannot pass vacuously.
			if !strings.Contains(payload, resultNameTaskHash) {
				t.Fatalf("TaskStatus JSON %s does not carry the task reference; absence checks are vacuous", payload)
			}
			if test.wantState == contracts.TaskStateSucceeded {
				if status.Result == nil || status.Result.Name != test.name {
					t.Fatalf("Result = %#v, want the exact name %q", status.Result, test.name)
				}
				if !strings.Contains(payload, test.name) {
					t.Fatalf("TaskStatus JSON %s does not carry the allowed result name", payload)
				}
			} else {
				if status.Result != nil {
					t.Fatalf("Result = %#v, want nil for a non-succeeded task", status.Result)
				}
				if strings.Contains(payload, test.name) {
					t.Fatalf("TaskStatus JSON %s carries a name for a non-succeeded task", payload)
				}
			}
		})
	}
}

// TestDownloadResultNameTypesExposeNoProviderIDs is the structural half of test
// 8: the adapter-private projection and the provider-neutral result/status types
// must have no field that could carry a provider file or directory ID.
func TestDownloadResultNameTypesExposeNoProviderIDs(t *testing.T) {
	forbidden := []string{"fileid", "dirid", "wppathid"}
	types := map[string]reflect.Type{
		"OfflineTask":              reflect.TypeOf(OfflineTask{}),
		"contracts.DownloadResult": reflect.TypeOf(contracts.DownloadResult{}),
		"contracts.TaskStatus":     reflect.TypeOf(contracts.TaskStatus{}),
	}
	for label, typ := range types {
		for index := 0; index < typ.NumField(); index++ {
			field := typ.Field(index).Name
			lowered := strings.ToLower(field)
			for _, bad := range forbidden {
				if strings.Contains(lowered, bad) {
					t.Fatalf("%s.%s exposes a provider-private identifier field %q; FileId/DirId must never become Panta or IndexCore identity",
						label, field, bad)
				}
			}
		}
	}
}
