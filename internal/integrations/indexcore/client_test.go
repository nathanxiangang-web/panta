package indexcore_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
)

const resourceJSON = `{
  "resource_id":"resource-1","root_id":"root-1","canonical_path":"/docs/report.txt",
  "parent_resource_id":null,"name":"report.txt","is_dir":false,"size":42,
  "mtime":"2026-09-28T01:02:03Z","resource_presence":"PRESENT",
  "introduced_at_generation":2,"last_confirmed_generation":3
}`

func TestBrowsePreservesHierarchyPaginationAndVisibility(t *testing.T) {
	const cursor = "opaque+/=?& cursor"
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/roots/root-1/resources" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		query := r.URL.Query()
		switch requestCount {
		case 1:
			if query.Has("parent_id") {
				t.Errorf("root request unexpectedly has parent_id=%q", query.Get("parent_id"))
			}
			if query.Get("cursor") != cursor || query.Get("limit") != "25" {
				t.Errorf("cursor/limit = %q/%q", query.Get("cursor"), query.Get("limit"))
			}
			for _, name := range []string{"include_removed", "include_deprecated_root", "include_deleted_root"} {
				if query.Get(name) != "true" {
					t.Errorf("%s = %q", name, query.Get(name))
				}
			}
		case 2:
			if query.Get("parent_id") != "parent+/=?& exact" {
				t.Errorf("parent_id = %q", query.Get("parent_id"))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"items":[%s],"next_cursor":"next opaque+/=","future_field":true}`, resourceJSON)
	}))
	defer server.Close()

	client := mustClient(t, server.URL)
	page, err := client.Browse(context.Background(), indexcore.BrowseRequest{
		RootID: "root-1", Cursor: cursor, Limit: 25,
		ReadVisibility: indexcore.ReadVisibility{
			IncludeRemoved: true, IncludeDeprecatedRoot: true, IncludeDeletedRoot: true,
		},
	})
	if err != nil {
		t.Fatalf("Browse(root) error = %v", err)
	}
	if len(page.Items) != 1 || page.NextCursor != "next opaque+/=" || page.Items[0].Presence != indexcore.ResourcePresent {
		t.Fatalf("Browse(root) = %#v", page)
	}
	parentID := "parent+/=?& exact"
	if _, err := client.Browse(context.Background(), indexcore.BrowseRequest{RootID: "root-1", ParentResourceID: &parentID}); err != nil {
		t.Fatalf("Browse(child) error = %v", err)
	}
}

func TestResolvePreservesAllMatchesAndAmbiguity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "path=%2Fdocs%2Freport+") {
			t.Errorf("path was not query encoded: %q", r.URL.RawQuery)
		}
		if r.URL.Query().Get("path") != "/docs/report 2026.txt" {
			t.Errorf("decoded path = %q", r.URL.Query().Get("path"))
		}
		for _, name := range []string{"include_removed", "include_deprecated_root", "include_deleted_root"} {
			if r.URL.Query().Get(name) != "true" {
				t.Errorf("%s = %q", name, r.URL.Query().Get(name))
			}
		}
		fmt.Fprintf(w, `{"matches":[%s,%s],"ambiguous":true}`, resourceJSON,
			strings.Replace(resourceJSON, "resource-1", "resource-2", 1))
	}))
	defer server.Close()

	result, err := mustClient(t, server.URL).Resolve(context.Background(), indexcore.ResolveRequest{
		RootID: "root-1", Path: "/docs/report 2026.txt",
		ReadVisibility: indexcore.ReadVisibility{
			IncludeRemoved: true, IncludeDeprecatedRoot: true, IncludeDeletedRoot: true,
		},
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if !result.Ambiguous || len(result.Matches) != 2 || result.Matches[0].ResourceID != "resource-1" || result.Matches[1].ResourceID != "resource-2" {
		t.Fatalf("Resolve() = %#v", result)
	}
}

func TestResolveMapsZeroAndOneMatch(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		count     int
		ambiguous bool
	}{
		{name: "zero", body: `{"matches":[],"ambiguous":false}`, count: 0},
		{name: "one", body: fmt.Sprintf(`{"matches":[%s],"ambiguous":false}`, resourceJSON), count: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, test.body) }))
			defer server.Close()
			result, err := mustClient(t, server.URL).Resolve(context.Background(), indexcore.ResolveRequest{RootID: "root-1", Path: "/x"})
			if err != nil || len(result.Matches) != test.count || result.Ambiguous != test.ambiguous {
				t.Fatalf("Resolve() = %#v, %v", result, err)
			}
		})
	}
}

func TestReadJournalSendsExactCursorAndKeepsCanonicalOrder(t *testing.T) {
	types := []indexcore.JournalEventType{
		indexcore.EventResourceAdded, indexcore.EventResourceUpdated, indexcore.EventResourceRenamed,
		indexcore.EventResourceMoved, indexcore.EventResourceRemoved, indexcore.EventRootDeprecated,
		indexcore.EventRootDeleted,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("after_seq") != "42" || r.URL.Query().Get("limit") != "7" {
			t.Errorf("journal query = %q", r.URL.RawQuery)
		}
		fmt.Fprint(w, `{"items":[`)
		for i, eventType := range types {
			if i > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprintf(w, `{"event_seq":%d,"generation_number":2,"intra_generation_seq":%d,"event_type":%q,"resource_id":null,"payload":"{}","committed_at":"2026-09-28T01:02:03Z"}`, 43+i, i+1, eventType)
		}
		fmt.Fprint(w, `]}`)
	}))
	defer server.Close()

	events, err := mustClient(t, server.URL).ReadJournal(context.Background(), indexcore.JournalRequest{RootID: "root-1", AfterSeq: 42, Limit: 7})
	if err != nil {
		t.Fatalf("ReadJournal() error = %v", err)
	}
	gotTypes := make([]indexcore.JournalEventType, 0, len(events))
	for i, event := range events {
		if event.EventSeq != int64(43+i) || string(event.Payload) != "{}" {
			t.Fatalf("event[%d] = %#v", i, event)
		}
		gotTypes = append(gotTypes, event.EventType)
	}
	if !reflect.DeepEqual(gotTypes, types) {
		t.Fatalf("event types = %#v, want %#v", gotTypes, types)
	}
}

func TestRootStatusMapsFieldsAndVisibility(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("include_deprecated_root") != "true" || r.URL.Query().Get("include_deleted_root") != "true" {
			t.Errorf("visibility query = %q", r.URL.RawQuery)
		}
		fmt.Fprint(w, `{"root_id":"root-1","lifecycle_state":"DEPRECATED","current_generation":8,"last_applied_admission_seq":91}`)
	}))
	defer server.Close()

	status, err := mustClient(t, server.URL).RootStatus(context.Background(), indexcore.RootStatusRequest{
		RootID: "root-1", IncludeDeprecatedRoot: true, IncludeDeletedRoot: true,
	})
	if err != nil || status.RootID != "root-1" || status.LifecycleState != indexcore.RootDeprecated ||
		status.CurrentGeneration != 8 || status.LastAppliedAdmissionSeq == nil || *status.LastAppliedAdmissionSeq != 91 {
		t.Fatalf("RootStatus() = %#v, %v", status, err)
	}
}

func TestRemoteErrorsRemainTyped(t *testing.T) {
	tests := []struct {
		name   string
		status int
		code   string
		want   error
	}{
		{"invalid cursor", 400, "invalid_cursor", indexcore.ErrInvalidCursor},
		{"invalid request", 400, "invalid_request", indexcore.ErrInvalidRequest},
		{"not found", 404, "not_found", indexcore.ErrNotFound},
		{"stale cursor", 409, "stale_cursor", indexcore.ErrStaleCursor},
		{"internal", 500, "internal_error", indexcore.ErrInternal},
		{"not ready", 503, "not_ready", indexcore.ErrNotReady},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				fmt.Fprintf(w, `{"error":%q,"message":"fixture"}`, test.code)
			}))
			defer server.Close()
			_, err := mustClient(t, server.URL).Browse(context.Background(), indexcore.BrowseRequest{RootID: "root-1"})
			if !errors.Is(err, test.want) {
				t.Fatalf("Browse() error = %v, want errors.Is(%v)", err, test.want)
			}
			var typed *indexcore.Error
			if !errors.As(err, &typed) || typed.RemoteCode != test.code || typed.StatusCode != test.status {
				t.Fatalf("typed error = %#v", typed)
			}
		})
	}
}

func TestTransportMalformedAndUnexpectedStatusAreDistinct(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		client, err := indexcore.NewClient("http://127.0.0.1:1", indexcore.WithTimeout(100*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Browse(context.Background(), indexcore.BrowseRequest{RootID: "root-1"})
		if !errors.Is(err, indexcore.ErrTransport) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(50 * time.Millisecond)
			fmt.Fprint(w, `{"items":[]}`)
		}))
		defer server.Close()
		client, err := indexcore.NewClient(server.URL, indexcore.WithTimeout(5*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Browse(context.Background(), indexcore.BrowseRequest{RootID: "root-1"})
		if !errors.Is(err, indexcore.ErrTransport) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("malformed JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{`) }))
		defer server.Close()
		_, err := mustClient(t, server.URL).Browse(context.Background(), indexcore.BrowseRequest{RootID: "root-1"})
		if !errors.Is(err, indexcore.ErrMalformedResponse) {
			t.Fatalf("error = %v", err)
		}
	})

	t.Run("unexpected status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTeapot)
			fmt.Fprint(w, `{"error":"future_error","message":"fixture"}`)
		}))
		defer server.Close()
		_, err := mustClient(t, server.URL).Browse(context.Background(), indexcore.BrowseRequest{RootID: "root-1"})
		if !errors.Is(err, indexcore.ErrUnexpectedStatus) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestMalformedJournalEventFailsExplicitly(t *testing.T) {
	tests := []string{
		`{"items":[{"event_type":"resource-added"}]}`,
		`{"items":[{"event_seq":1,"generation_number":1,"intra_generation_seq":1,"event_type":"future-event","payload":"{}","committed_at":"2026-09-28T01:02:03Z"}]}`,
		`{"items":[{"event_seq":1,"generation_number":1,"intra_generation_seq":1,"event_type":"resource-added","payload":"not-json","committed_at":"2026-09-28T01:02:03Z"}]}`,
	}
	for i, body := range tests {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			_, err := mustClient(t, server.URL).ReadJournal(context.Background(), indexcore.JournalRequest{RootID: "root-1"})
			if !errors.Is(err, indexcore.ErrMalformedResponse) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestRootIDPathSegmentIsEncoded(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.RequestURI, "/root%2Fwith%20space/status") {
			t.Errorf("RequestURI = %q", r.RequestURI)
		}
		fmt.Fprint(w, `{"root_id":"root/with space","lifecycle_state":"ACTIVE","current_generation":0}`)
	}))
	defer server.Close()
	_, err := mustClient(t, server.URL).RootStatus(context.Background(), indexcore.RootStatusRequest{RootID: "root/with space"})
	if err != nil {
		t.Fatalf("RootStatus() error = %v", err)
	}
}

func mustClient(t *testing.T, rawURL string) *indexcore.Client {
	t.Helper()
	client, err := indexcore.NewClient(rawURL)
	if err != nil {
		t.Fatalf("NewClient(%q) error = %v", rawURL, err)
	}
	return client
}

func TestNewClientValidatesBaseURLAndTimeout(t *testing.T) {
	for _, rawURL := range []string{"", "indexcore", "ftp://example.test", "http://example.test?x=1"} {
		if _, err := indexcore.NewClient(rawURL); err == nil {
			t.Errorf("NewClient(%q) unexpectedly succeeded", rawURL)
		}
	}
	if _, err := indexcore.NewClient("http://example.test", indexcore.WithTimeout(0)); err == nil {
		t.Error("NewClient() accepted an unbounded timeout")
	}
}
