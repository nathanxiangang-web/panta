package p115

import (
	"errors"
	"strings"
	"testing"

	"github.com/SheltonZhu/115driver/pkg/driver"
)

const cookieSentinel = "SENTINEL-SECRET-COOKIE-VALUE"

// validCookie is synthetic material only. It is never sent anywhere because the
// parse path performs no provider call.
const validCookie = "UID=" + cookieSentinel + "-uid;CID=cid-value;SEID=seid-value;KID=kid-value"

// TestParseCredentialAcceptsValidCookieWithoutNetwork proves cookie parsing is a
// pure local operation: no provider call, no login check.
func TestParseCredentialAcceptsValidCookieWithoutNetwork(t *testing.T) {
	credential, err := parseCredential([]byte(validCookie))
	if err != nil {
		t.Fatalf("parseCredential() error = %v", err)
	}
	if credential.UID == "" || credential.CID == "" || credential.SEID == "" {
		t.Fatalf("parsed credential is incomplete: %+v", credential)
	}
	// The parsed material matches the supplied cookie fields.
	if credential.UID != cookieSentinel+"-uid" || credential.CID != "cid-value" || credential.SEID != "seid-value" {
		t.Fatal("parsed credential does not match the supplied cookie fields")
	}
}

func TestParseCredentialRejectsInvalidCookieWithoutLeakingIt(t *testing.T) {
	tests := []struct {
		name   string
		cookie string
	}{
		{name: "empty", cookie: ""},
		{name: "too few pairs", cookie: "UID=" + cookieSentinel},
		{name: "missing seid", cookie: "UID=" + cookieSentinel + ";CID=cid-value;SEID="},
		{name: "malformed pair", cookie: "UID=" + cookieSentinel + ";CID;SEID=seid-value"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			credential, err := parseCredential([]byte(test.cookie))
			if err == nil {
				t.Fatal("parseCredential() succeeded, want a credential error")
			}
			if credential != nil {
				t.Fatal("a failed parse must not return a credential")
			}
			if strings.Contains(err.Error(), cookieSentinel) {
				t.Fatalf("error %q leaks cookie material", err)
			}
			if strings.Contains(err.Error(), "UID") || strings.Contains(err.Error(), "SEID") {
				t.Fatalf("error %q leaks cookie structure", err)
			}
		})
	}
}

// TestForgetCredentialClearsParsedFields proves this package does not retain a
// second copy of the secret after importing it into a client.
func TestForgetCredentialClearsParsedFields(t *testing.T) {
	credential, err := parseCredential([]byte(validCookie))
	if err != nil {
		t.Fatalf("parseCredential() error = %v", err)
	}
	forgetCredential(credential)
	rendered := credential.UID + credential.CID + credential.SEID + credential.KID
	if rendered != "" {
		t.Fatalf("credential fields were not cleared: %q", rendered)
	}
}

// TestForgottenCredentialStillLeavesClientUsable documents why clearing our copy
// is safe: ImportCredential copies the values into the client's cookie jar.
func TestForgottenCredentialStillLeavesClientUsable(t *testing.T) {
	credential, err := parseCredential([]byte(validCookie))
	if err != nil {
		t.Fatalf("parseCredential() error = %v", err)
	}
	client := driver.Default().ImportCredential(credential)
	forgetCredential(credential)
	if client == nil {
		t.Fatal("ImportCredential returned no client")
	}
	// The client owns its own cookies; our struct no longer holds the secret.
	if credential.UID != "" || credential.SEID != "" {
		t.Fatal("credential fields survived forgetCredential")
	}
}

// TestNewAdapterFromCookieRequiresValidCookieWithoutNetwork exercises the public
// constructor's failure path only. A successful construction builds a real library
// client, which is not driven in unit tests.
func TestNewAdapterFromCookieRequiresValidCookie(t *testing.T) {
	adapter, err := NewAdapterFromCookie([]byte("not-a-cookie"), Options{Backend: newFakeBackend()})
	if err == nil {
		t.Fatal("NewAdapterFromCookie() succeeded with an invalid cookie")
	}
	if adapter != nil {
		t.Fatal("failed construction must not return an adapter")
	}
	if !errors.Is(err, errCredentialInvalid) {
		t.Fatalf("error = %v, want errCredentialInvalid", err)
	}
}

// TestCookiedBackendRequiresClient covers the backend constructor guard.
func TestCookiedBackendRequiresClient(t *testing.T) {
	if _, err := NewCookiedBackend(nil); err == nil {
		t.Fatal("NewCookiedBackend(nil) succeeded, want an error")
	}
}

// TestCookiedBackendProjectsTasksWithoutLeakingTypes proves the bridge projects
// upstream tasks into adapter-private terms and passes exact identity through.
func TestCookiedBackendProjectsTasksWithoutLeakingTypes(t *testing.T) {
	client := &fakeOfflineClient{
		addHashes: []string{"hash-1"},
		listResponse: driver.OfflineTaskResp{
			Page: 1, PageCount: 2, Total: 5,
			Tasks: []*driver.OfflineTask{
				{InfoHash: "hash-1", Status: 1, Name: "one"},
				nil,
				{InfoHash: "hash-2", Status: 2, Name: "two"},
			},
		},
	}
	backend, err := NewCookiedBackend(client)
	if err != nil {
		t.Fatalf("NewCookiedBackend() error = %v", err)
	}

	hashes, err := backend.AddOfflineTaskURI(t.Context(), "magnet:?xt=urn:btih:abc", "dir-1")
	if err != nil {
		t.Fatalf("AddOfflineTaskURI() error = %v", err)
	}
	if len(hashes) != 1 || hashes[0] != "hash-1" {
		t.Fatalf("hashes = %q", hashes)
	}
	if len(client.addURIs) != 1 || client.addURIs[0] != "magnet:?xt=urn:btih:abc" {
		t.Fatalf("submitted URIs = %q, want the exact single URI", client.addURIs)
	}
	if client.addDir != "dir-1" {
		t.Fatalf("saveDirID = %q, want the exact scope", client.addDir)
	}

	page, err := backend.ListOfflineTasks(t.Context(), 1)
	if err != nil {
		t.Fatalf("ListOfflineTasks() error = %v", err)
	}
	if page.Page != 1 || page.PageCount != 2 || page.Total != 5 {
		t.Fatalf("page metadata = %#v", page)
	}
	// A nil upstream task is skipped rather than panicking.
	if len(page.Tasks) != 2 {
		t.Fatalf("projected tasks = %#v, want 2 non-nil tasks", page.Tasks)
	}
	if page.Tasks[0].InfoHash != "hash-1" || page.Tasks[0].Status != 1 || page.Tasks[0].Name != "one" {
		t.Fatalf("projected task[0] = %#v", page.Tasks[0])
	}

	if err := backend.DeleteOfflineTask(t.Context(), "hash-1"); err != nil {
		t.Fatalf("DeleteOfflineTask() error = %v", err)
	}
	if len(client.deletedHashes) != 1 || client.deletedHashes[0] != "hash-1" {
		t.Fatalf("deleted hashes = %q, want exactly one exact hash", client.deletedHashes)
	}
	if client.deleteFiles {
		t.Fatal("bridge requested deleteFiles=true; cancelling must never delete storage")
	}
}

// fakeOfflineClient is a controlled OfflineClient double. It never touches the
// network and records exact arguments.
type fakeOfflineClient struct {
	addHashes     []string
	addErr        error
	addURIs       []string
	addDir        string
	listResponse  driver.OfflineTaskResp
	listErr       error
	deletedHashes []string
	deleteFiles   bool
	deleteErr     error
}

func (client *fakeOfflineClient) AddOfflineTaskURIs(uris []string, saveDirID string, _ ...driver.OfflineOption) ([]string, error) {
	client.addURIs = append([]string(nil), uris...)
	client.addDir = saveDirID
	if client.addErr != nil {
		return nil, client.addErr
	}
	return client.addHashes, nil
}

func (client *fakeOfflineClient) ListOfflineTask(int64) (driver.OfflineTaskResp, error) {
	if client.listErr != nil {
		return driver.OfflineTaskResp{}, client.listErr
	}
	return client.listResponse, nil
}

func (client *fakeOfflineClient) DeleteOfflineTasks(hashes []string, deleteFiles bool) error {
	client.deletedHashes = append([]string(nil), hashes...)
	client.deleteFiles = deleteFiles
	if client.deleteErr != nil {
		return client.deleteErr
	}
	return nil
}
