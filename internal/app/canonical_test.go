package app_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/projector"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

// ---------------------------------------------------------------------------
// Gate 3.10 (D-033) composition adapter coverage.
//
// These tests drive the production adapters in internal/app/canonical.go through
// the acquisition-owned ports. The upstream collaborators are controlled doubles so
// no network, database, or IndexCore process is needed.
// ---------------------------------------------------------------------------

// errCanonicalUpstream is the distinct upstream failure every propagation test uses.
var errCanonicalUpstream = errors.New("canonical upstream failure")

// --- doubles -----------------------------------------------------------------

// canonicalResolveFake is a controlled indexcore.ResolvePort. It records the exact
// request the composed production adapter sent.
type canonicalResolveFake struct {
	requests []indexcore.ResolveRequest
	result   indexcore.ResolveResult
	err      error
}

func (fake *canonicalResolveFake) Resolve(_ context.Context, request indexcore.ResolveRequest) (indexcore.ResolveResult, error) {
	fake.requests = append(fake.requests, request)
	if fake.err != nil {
		return indexcore.ResolveResult{}, fake.err
	}
	return fake.result, nil
}

// canonicalProjectorFake counts every upstream ProjectOnce call so an internal loop
// in the adapter cannot pass unnoticed.
type canonicalProjectorFake struct {
	calls    int
	bindings []storage.BindingID
	limits   []int
	result   projector.Result
	err      error
}

func (fake *canonicalProjectorFake) ProjectOnce(_ context.Context, bindingID storage.BindingID, limit int) (projector.Result, error) {
	fake.calls++
	fake.bindings = append(fake.bindings, bindingID)
	fake.limits = append(fake.limits, limit)
	if fake.err != nil {
		return projector.Result{}, fake.err
	}
	return fake.result, nil
}

type canonicalCopyReaderFake struct {
	roots     []string
	resources []string
	copy      catalog.Copy
	err       error
}

func (fake *canonicalCopyReaderFake) GetCopyByPhysicalIdentity(_ context.Context, rootID, resourceID string) (catalog.Copy, error) {
	fake.roots = append(fake.roots, rootID)
	fake.resources = append(fake.resources, resourceID)
	if fake.err != nil {
		return catalog.Copy{}, fake.err
	}
	return fake.copy, nil
}

// canonicalBinderFake is the atomic port behind a real catalog.ClassificationService.
type canonicalBinderFake struct {
	calls     int
	copyID    catalog.CopyID
	variantID catalog.VariantID
	result    catalog.BindCopyResult
	err       error
}

func (fake *canonicalBinderFake) BindCopyToVariant(_ context.Context, copyID catalog.CopyID, variantID catalog.VariantID) (catalog.BindCopyResult, error) {
	fake.calls++
	fake.copyID = copyID
	fake.variantID = variantID
	if fake.err != nil {
		return catalog.BindCopyResult{}, fake.err
	}
	return fake.result, nil
}

// --- 1: nil dependency validation --------------------------------------------

func TestCanonicalPortConstructorsRejectNilDependencies(t *testing.T) {
	cases := []struct {
		name  string
		build func() (any, error)
	}{
		{name: "resolve adapter", build: func() (any, error) { return app.NewCanonicalResolveAdapter(nil) }},
		{name: "projector adapter", build: func() (any, error) { return app.NewCanonicalProjectorAdapter(nil) }},
		{name: "copy reader adapter", build: func() (any, error) { return app.NewCanonicalCopyReaderAdapter(nil) }},
		{name: "classifier adapter", build: func() (any, error) { return app.NewCanonicalClassifierAdapter(nil) }},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			port, err := test.build()
			if err == nil {
				t.Fatalf("constructor for %s returned no error for a nil dependency", test.name)
			}
			// A failed constructor must return a nil port. The value is carried in an
			// any, so it is a non-nil interface holding a typed nil pointer.
			if port != nil && !reflect.ValueOf(port).IsNil() {
				t.Fatalf("constructor for %s returned %#v alongside the error, want a nil port", test.name, port)
			}
		})
	}
}

// --- 2: resolve mapping and ambiguity ----------------------------------------

func TestCanonicalPortResolveMapsMatchesAndAmbiguity(t *testing.T) {
	fake := &canonicalResolveFake{result: indexcore.ResolveResult{
		Matches: []indexcore.ResourceContext{
			{
				ResourceID: "res-1", RootID: "root-1",
				CanonicalPath: pointerTo("/library/acquired.bin"), Name: pointerTo("acquired.bin"),
				Presence: indexcore.ResourcePresent,
			},
			{
				ResourceID: "res-2", RootID: "root-1",
				CanonicalPath: pointerTo("/library/other.bin"), Name: pointerTo("other.bin"),
				Presence: indexcore.ResourceRemoved,
			},
		},
		Ambiguous: true,
	}}
	adapter, err := app.NewCanonicalResolveAdapter(fake)
	if err != nil {
		t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
	}

	resolution, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
		RootID: "root-1", Path: "/library/acquired.bin",
	})
	if err != nil {
		t.Fatalf("ResolveCanonical() error = %v", err)
	}
	if !resolution.Ambiguous {
		t.Fatal("Ambiguous = false, want the upstream ambiguity forwarded unchanged")
	}
	if len(resolution.Matches) != 2 {
		t.Fatalf("Matches = %d, want 2", len(resolution.Matches))
	}
	first := resolution.Matches[0]
	if first.RootID != "root-1" || first.ResourceID != "res-1" ||
		first.CanonicalPath != "/library/acquired.bin" || first.Presence != acquisition.CanonicalPresencePresent {
		t.Fatalf("first match = %+v", first)
	}
	second := resolution.Matches[1]
	if second.RootID != "root-1" || second.ResourceID != "res-2" ||
		second.CanonicalPath != "/library/other.bin" || second.Presence != acquisition.CanonicalPresenceRemoved {
		t.Fatalf("second match = %+v", second)
	}

	if len(fake.requests) != 1 {
		t.Fatalf("upstream resolve calls = %d, want exactly 1", len(fake.requests))
	}
	if fake.requests[0].RootID != "root-1" || fake.requests[0].Path != "/library/acquired.bin" {
		t.Fatalf("upstream request = %+v, want the acquisition root and path", fake.requests[0])
	}
}

// --- 3: exhaustive presence mapping ------------------------------------------

func TestCanonicalPortResolvePresenceMappingIsExhaustive(t *testing.T) {
	t.Run("PRESENT maps to CanonicalPresencePresent", func(t *testing.T) {
		fake := &canonicalResolveFake{result: indexcore.ResolveResult{Matches: []indexcore.ResourceContext{{
			ResourceID: "res-1", RootID: "root-1",
			CanonicalPath: pointerTo("/library/acquired.bin"), Presence: indexcore.ResourcePresent,
		}}}}
		adapter, err := app.NewCanonicalResolveAdapter(fake)
		if err != nil {
			t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
		}
		resolution, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
			RootID: "root-1", Path: "/library/acquired.bin",
		})
		if err != nil {
			t.Fatalf("ResolveCanonical() error = %v", err)
		}
		if len(resolution.Matches) != 1 || resolution.Matches[0].Presence != acquisition.CanonicalPresencePresent {
			t.Fatalf("Matches = %+v, want exactly one PRESENT match", resolution.Matches)
		}
	})

	t.Run("REMOVED maps to a distinct non-PRESENT value", func(t *testing.T) {
		fake := &canonicalResolveFake{result: indexcore.ResolveResult{Matches: []indexcore.ResourceContext{{
			ResourceID: "res-1", RootID: "root-1",
			CanonicalPath: pointerTo("/library/acquired.bin"), Presence: indexcore.ResourceRemoved,
		}}}}
		adapter, err := app.NewCanonicalResolveAdapter(fake)
		if err != nil {
			t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
		}
		resolution, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
			RootID: "root-1", Path: "/library/acquired.bin",
		})
		if err != nil {
			t.Fatalf("ResolveCanonical() error = %v", err)
		}
		if len(resolution.Matches) != 1 {
			t.Fatalf("Matches = %d, want 1", len(resolution.Matches))
		}
		presence := resolution.Matches[0].Presence
		if presence == acquisition.CanonicalPresencePresent {
			t.Fatal("REMOVED mapped to PRESENT")
		}
		if presence != acquisition.CanonicalPresenceRemoved {
			t.Fatalf("Presence = %q, want %q", presence, acquisition.CanonicalPresenceRemoved)
		}
	})

	// The adapter chooses to fail closed for a presence value outside the frozen
	// set: a presence this boundary cannot name is not usable physical identity, so
	// it must not be silently forwarded as PRESENT.
	t.Run("unknown presence fails closed", func(t *testing.T) {
		for _, presence := range []indexcore.ResourcePresence{"UNKNOWN", "present", ""} {
			fake := &canonicalResolveFake{result: indexcore.ResolveResult{Matches: []indexcore.ResourceContext{{
				ResourceID: "res-1", RootID: "root-1",
				CanonicalPath: pointerTo("/library/acquired.bin"), Presence: presence,
			}}}}
			adapter, err := app.NewCanonicalResolveAdapter(fake)
			if err != nil {
				t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
			}
			resolution, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
				RootID: "root-1", Path: "/library/acquired.bin",
			})
			if err == nil {
				t.Fatalf("presence %q error = nil, want a fail-closed error", presence)
			}
			if !errors.Is(err, acquisition.ErrCanonicalIdentity) {
				t.Fatalf("presence %q error = %v, want it to attribute acquisition.ErrCanonicalIdentity", presence, err)
			}
			if resolution.Matches != nil || resolution.Ambiguous {
				t.Fatalf("presence %q resolution = %+v, want no resolution on failure", presence, resolution)
			}
		}
	})
}

// --- 4: nullable upstream fields ---------------------------------------------

func TestCanonicalPortResolveToleratesNilCanonicalPathAndName(t *testing.T) {
	t.Run("both path and name are nil", func(t *testing.T) {
		fake := &canonicalResolveFake{result: indexcore.ResolveResult{Matches: []indexcore.ResourceContext{{
			ResourceID: "res-1", RootID: "root-1",
			CanonicalPath: nil, Name: nil, Presence: indexcore.ResourcePresent,
		}}}}
		adapter, err := app.NewCanonicalResolveAdapter(fake)
		if err != nil {
			t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
		}
		resolution, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
			RootID: "root-1", Path: "/library/acquired.bin",
		})
		if err != nil {
			t.Fatalf("ResolveCanonical() error = %v", err)
		}
		if len(resolution.Matches) != 1 {
			t.Fatalf("Matches = %d, want 1", len(resolution.Matches))
		}
		if resolution.Matches[0].CanonicalPath != "" {
			t.Fatalf("CanonicalPath = %q, want empty for a nil upstream path", resolution.Matches[0].CanonicalPath)
		}
	})

	t.Run("nil path never falls back to the resource name", func(t *testing.T) {
		fake := &canonicalResolveFake{result: indexcore.ResolveResult{Matches: []indexcore.ResourceContext{{
			ResourceID: "res-1", RootID: "root-1",
			CanonicalPath: nil, Name: pointerTo("fabricated-name.bin"), Presence: indexcore.ResourcePresent,
		}}}}
		adapter, err := app.NewCanonicalResolveAdapter(fake)
		if err != nil {
			t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
		}
		resolution, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
			RootID: "root-1", Path: "/library/acquired.bin",
		})
		if err != nil {
			t.Fatalf("ResolveCanonical() error = %v", err)
		}
		if len(resolution.Matches) != 1 {
			t.Fatalf("Matches = %d, want 1", len(resolution.Matches))
		}
		if got := resolution.Matches[0].CanonicalPath; got != "" {
			t.Fatalf("CanonicalPath = %q, want empty: the adapter must not fabricate a path from Name", got)
		}
	})
}

// --- 5: D-033 default PRESENT-only visibility --------------------------------

func TestCanonicalPortResolveSendsNoVisibilityOverride(t *testing.T) {
	fake := &canonicalResolveFake{result: indexcore.ResolveResult{Matches: []indexcore.ResourceContext{{
		ResourceID: "res-1", RootID: "root-1",
		CanonicalPath: pointerTo("/library/acquired.bin"), Presence: indexcore.ResourcePresent,
	}}}}
	adapter, err := app.NewCanonicalResolveAdapter(fake)
	if err != nil {
		t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
	}
	if _, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
		RootID: "root-1", Path: "/library/acquired.bin",
	}); err != nil {
		t.Fatalf("ResolveCanonical() error = %v", err)
	}
	if len(fake.requests) != 1 {
		t.Fatalf("upstream resolve calls = %d, want 1", len(fake.requests))
	}
	request := fake.requests[0]
	if request.RootID != "root-1" || request.Path != "/library/acquired.bin" {
		t.Fatalf("upstream request = %+v", request)
	}
	// D-033 requires the default PRESENT-only visibility. Any non-zero override
	// would widen the read to REMOVED/deprecated/deleted state.
	if request.ReadVisibility != (indexcore.ReadVisibility{}) {
		t.Fatalf("ReadVisibility = %+v, want the zero default so IndexCore stays PRESENT-only",
			request.ReadVisibility)
	}
}

// TestCanonicalPortResolveRealClientKeepsDefaultVisibility is the wire-level proof:
// the real production IndexCore HTTP client is driven through the composed adapter,
// and the request it puts on the socket carries no visibility override.
func TestCanonicalPortResolveRealClientKeepsDefaultVisibility(t *testing.T) {
	var (
		mu       sync.Mutex
		rawQuery string
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		rawQuery = request.URL.RawQuery
		mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"matches":[{"resource_id":"res-1","root_id":"root-1",` +
			`"canonical_path":"/library/acquired.bin","name":"acquired.bin","resource_presence":"PRESENT",` +
			`"introduced_at_generation":3,"last_confirmed_generation":3}],"ambiguous":false}`))
	}))
	t.Cleanup(server.Close)

	client, err := indexcore.NewClient(server.URL)
	if err != nil {
		t.Fatalf("indexcore.NewClient() error = %v", err)
	}
	adapter, err := app.NewCanonicalResolveAdapter(client)
	if err != nil {
		t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
	}
	resolution, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
		RootID: "root-1", Path: "/library/acquired.bin",
	})
	if err != nil {
		t.Fatalf("ResolveCanonical() error = %v", err)
	}
	if len(resolution.Matches) != 1 || resolution.Matches[0].ResourceID != "res-1" ||
		resolution.Matches[0].Presence != acquisition.CanonicalPresencePresent {
		t.Fatalf("resolution = %+v", resolution)
	}

	mu.Lock()
	query, err := url.ParseQuery(rawQuery)
	mu.Unlock()
	if err != nil {
		t.Fatalf("parse recorded query %q: %v", rawQuery, err)
	}
	for _, forbidden := range []string{"include_removed", "include_deprecated_root", "include_deleted_root"} {
		if query.Has(forbidden) {
			t.Fatalf("resolve query %q carried %s; D-033 requires the default PRESENT-only visibility",
				rawQuery, forbidden)
		}
	}
	if query.Get("path") != "/library/acquired.bin" {
		t.Fatalf("resolve query %q did not carry the requested path", rawQuery)
	}
}

// --- 6: resolve error propagation --------------------------------------------

func TestCanonicalPortResolvePropagatesUpstreamFailure(t *testing.T) {
	fake := &canonicalResolveFake{err: errCanonicalUpstream}
	adapter, err := app.NewCanonicalResolveAdapter(fake)
	if err != nil {
		t.Fatalf("NewCanonicalResolveAdapter() error = %v", err)
	}
	resolution, err := adapter.ResolveCanonical(context.Background(), acquisition.CanonicalResolveRequest{
		RootID: "root-1", Path: "/library/acquired.bin",
	})
	if !errors.Is(err, errCanonicalUpstream) {
		t.Fatalf("ResolveCanonical() error = %v, want %v via errors.Is", err, errCanonicalUpstream)
	}
	if len(resolution.Matches) != 0 || resolution.Ambiguous {
		t.Fatalf("resolution = %+v, want the zero value on failure", resolution)
	}
}

// --- 7: projector forwarding and mapping -------------------------------------

func TestCanonicalPortProjectorForwardsAndMaps(t *testing.T) {
	fake := &canonicalProjectorFake{result: projector.Result{
		PreviousCursor: 4, CurrentCursor: 9, EventsRead: 7, Mutations: 3,
	}}
	adapter, err := app.NewCanonicalProjectorAdapter(fake)
	if err != nil {
		t.Fatalf("NewCanonicalProjectorAdapter() error = %v", err)
	}
	projection, err := adapter.ProjectOnce(context.Background(), storage.BindingID("binding-1"), 100)
	if err != nil {
		t.Fatalf("ProjectOnce() error = %v", err)
	}
	if projection != (acquisition.CanonicalProjection{EventsRead: 7, Mutations: 3}) {
		t.Fatalf("projection = %+v, want EventsRead 7 and Mutations 3", projection)
	}
	if fake.calls != 1 {
		t.Fatalf("upstream ProjectOnce calls = %d, want exactly 1", fake.calls)
	}
	if len(fake.bindings) != 1 || fake.bindings[0] != storage.BindingID("binding-1") {
		t.Fatalf("upstream binding IDs = %v, want the exact forwarded binding", fake.bindings)
	}
	if len(fake.limits) != 1 || fake.limits[0] != 100 {
		t.Fatalf("upstream limits = %v, want the exact forwarded limit", fake.limits)
	}

	t.Run("upstream error propagates", func(t *testing.T) {
		failing := &canonicalProjectorFake{err: errCanonicalUpstream}
		failingAdapter, err := app.NewCanonicalProjectorAdapter(failing)
		if err != nil {
			t.Fatalf("NewCanonicalProjectorAdapter() error = %v", err)
		}
		if _, err := failingAdapter.ProjectOnce(context.Background(), storage.BindingID("binding-1"), 100); !errors.Is(err, errCanonicalUpstream) {
			t.Fatalf("ProjectOnce() error = %v, want %v via errors.Is", err, errCanonicalUpstream)
		}
	})
}

// --- 8: classifier forwarding ------------------------------------------------

func TestCanonicalPortClassifierForwardsAndReturnsCopy(t *testing.T) {
	t.Run("forwards identity and returns the resulting Copy", func(t *testing.T) {
		binder := &canonicalBinderFake{result: catalog.BindCopyResult{
			Copy:    catalog.Copy{ID: "copy-1", VariantID: pointerTo(catalog.VariantID("variant-1"))},
			Changed: true,
		}}
		service, err := catalog.NewClassificationService(binder)
		if err != nil {
			t.Fatalf("NewClassificationService() error = %v", err)
		}
		adapter, err := app.NewCanonicalClassifierAdapter(service)
		if err != nil {
			t.Fatalf("NewCanonicalClassifierAdapter() error = %v", err)
		}

		copyRecord, err := adapter.Bind(context.Background(), catalog.CopyID("copy-1"), catalog.VariantID("variant-1"))
		if err != nil {
			t.Fatalf("Bind() error = %v", err)
		}
		if copyRecord.ID != catalog.CopyID("copy-1") || copyRecord.VariantID == nil ||
			*copyRecord.VariantID != catalog.VariantID("variant-1") {
			t.Fatalf("Copy = %+v, want the resulting bound Copy", copyRecord)
		}
		if binder.calls != 1 || binder.copyID != catalog.CopyID("copy-1") || binder.variantID != catalog.VariantID("variant-1") {
			t.Fatalf("binder saw calls=%d copy=%q variant=%q, want the exact forwarded identity",
				binder.calls, binder.copyID, binder.variantID)
		}
	})

	t.Run("upstream error propagates", func(t *testing.T) {
		binder := &canonicalBinderFake{err: errCanonicalUpstream}
		service, err := catalog.NewClassificationService(binder)
		if err != nil {
			t.Fatalf("NewClassificationService() error = %v", err)
		}
		adapter, err := app.NewCanonicalClassifierAdapter(service)
		if err != nil {
			t.Fatalf("NewCanonicalClassifierAdapter() error = %v", err)
		}
		if _, err := adapter.Bind(context.Background(), catalog.CopyID("copy-1"), catalog.VariantID("variant-1")); !errors.Is(err, errCanonicalUpstream) {
			t.Fatalf("Bind() error = %v, want %v via errors.Is", err, errCanonicalUpstream)
		}
	})

	// Acquisition, not the composition adapter, rejects an unbound or mis-bound
	// result. The adapter must pass the Copy through untouched instead of repairing
	// it, or a classification conflict would be hidden.
	t.Run("nil Variant is returned as-is", func(t *testing.T) {
		binder := &canonicalBinderFake{result: catalog.BindCopyResult{Copy: catalog.Copy{ID: "copy-1"}}}
		service, err := catalog.NewClassificationService(binder)
		if err != nil {
			t.Fatalf("NewClassificationService() error = %v", err)
		}
		adapter, err := app.NewCanonicalClassifierAdapter(service)
		if err != nil {
			t.Fatalf("NewCanonicalClassifierAdapter() error = %v", err)
		}
		copyRecord, err := adapter.Bind(context.Background(), catalog.CopyID("copy-1"), catalog.VariantID("variant-1"))
		if err != nil {
			t.Fatalf("Bind() error = %v", err)
		}
		if copyRecord.ID != catalog.CopyID("copy-1") || copyRecord.VariantID != nil {
			t.Fatalf("Copy = %+v, want the nil Variant returned as-is", copyRecord)
		}
	})

	t.Run("mismatched Variant is returned as-is", func(t *testing.T) {
		binder := &canonicalBinderFake{result: catalog.BindCopyResult{
			Copy: catalog.Copy{ID: "copy-1", VariantID: pointerTo(catalog.VariantID("variant-other"))},
		}}
		service, err := catalog.NewClassificationService(binder)
		if err != nil {
			t.Fatalf("NewClassificationService() error = %v", err)
		}
		adapter, err := app.NewCanonicalClassifierAdapter(service)
		if err != nil {
			t.Fatalf("NewCanonicalClassifierAdapter() error = %v", err)
		}
		copyRecord, err := adapter.Bind(context.Background(), catalog.CopyID("copy-1"), catalog.VariantID("variant-1"))
		if err != nil {
			t.Fatalf("Bind() error = %v", err)
		}
		if copyRecord.VariantID == nil || *copyRecord.VariantID != catalog.VariantID("variant-other") {
			t.Fatalf("Copy = %+v, want the mismatched Variant returned as-is rather than rewritten", copyRecord)
		}
	})
}

// --- 9: copy reader forwarding and ErrNotFound fidelity ----------------------

func TestCanonicalPortCopyReaderForwardsAndPreservesNotFound(t *testing.T) {
	t.Run("forwards identity and returns the Copy", func(t *testing.T) {
		fake := &canonicalCopyReaderFake{copy: catalog.Copy{
			ID: "copy-1", IndexCoreRootID: "root-1", IndexCoreResourceID: "res-1",
			Availability: catalog.CopyAvailabilityPresent,
		}}
		adapter, err := app.NewCanonicalCopyReaderAdapter(fake)
		if err != nil {
			t.Fatalf("NewCanonicalCopyReaderAdapter() error = %v", err)
		}
		copyRecord, err := adapter.GetCopyByPhysicalIdentity(context.Background(), "root-1", "res-1")
		if err != nil {
			t.Fatalf("GetCopyByPhysicalIdentity() error = %v", err)
		}
		if copyRecord.ID != catalog.CopyID("copy-1") || copyRecord.IndexCoreRootID != "root-1" ||
			copyRecord.IndexCoreResourceID != "res-1" {
			t.Fatalf("Copy = %+v, want the exact physical identity", copyRecord)
		}
		if len(fake.roots) != 1 || fake.roots[0] != "root-1" || len(fake.resources) != 1 || fake.resources[0] != "res-1" {
			t.Fatalf("upstream saw roots=%v resources=%v, want the exact forwarded identity", fake.roots, fake.resources)
		}
	})

	// The acquisition service branches on catalog.ErrNotFound to treat an
	// unprojected Copy as normal pending work, so the adapter must not swallow or
	// replace it.
	t.Run("catalog.ErrNotFound stays detectable", func(t *testing.T) {
		fake := &canonicalCopyReaderFake{err: fmt.Errorf("read catalog Copy: %w", catalog.ErrNotFound)}
		adapter, err := app.NewCanonicalCopyReaderAdapter(fake)
		if err != nil {
			t.Fatalf("NewCanonicalCopyReaderAdapter() error = %v", err)
		}
		copyRecord, err := adapter.GetCopyByPhysicalIdentity(context.Background(), "root-1", "res-1")
		if !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("error = %v, want catalog.ErrNotFound via errors.Is", err)
		}
		if copyRecord != (catalog.Copy{}) {
			t.Fatalf("Copy = %+v, want the zero value on failure", copyRecord)
		}
	})

	t.Run("other upstream errors propagate", func(t *testing.T) {
		fake := &canonicalCopyReaderFake{err: errCanonicalUpstream}
		adapter, err := app.NewCanonicalCopyReaderAdapter(fake)
		if err != nil {
			t.Fatalf("NewCanonicalCopyReaderAdapter() error = %v", err)
		}
		if _, err := adapter.GetCopyByPhysicalIdentity(context.Background(), "root-1", "res-1"); !errors.Is(err, errCanonicalUpstream) {
			t.Fatalf("error = %v, want %v via errors.Is", err, errCanonicalUpstream)
		}
	})
}

// --- 10: compile-time interface assertions -----------------------------------

func TestCanonicalPortCompileTimeInterfaceAssertions(t *testing.T) {
	// The production file carries these as package-level assertions; this test
	// restates them so a signature drift fails in the test build as well.
	var (
		_ acquisition.CanonicalResolvePort   = (*app.CanonicalResolveAdapter)(nil)
		_ acquisition.CanonicalProjectorPort = (*app.CanonicalProjectorAdapter)(nil)
		_ acquisition.CanonicalCopyReader    = (*app.CanonicalCopyReaderAdapter)(nil)
		_ acquisition.CanonicalClassifier    = (*app.CanonicalClassifierAdapter)(nil)

		// The concrete collaborators must keep satisfying the narrow source ports
		// the composition consumes.
		_ indexcore.ResolvePort          = (*indexcore.Client)(nil)
		_ app.CanonicalProjectorSource   = (*projector.Service)(nil)
		_ catalog.PhysicalIdentityReader = (catalog.Repository)(nil)
	)
}

// --- 11: exactly one upstream projection per invocation ----------------------

func TestCanonicalPortProjectorMakesExactlyOneUpstreamCall(t *testing.T) {
	fake := &canonicalProjectorFake{}
	adapter, err := app.NewCanonicalProjectorAdapter(fake)
	if err != nil {
		t.Fatalf("NewCanonicalProjectorAdapter() error = %v", err)
	}
	for invocation := 1; invocation <= 2; invocation++ {
		if _, err := adapter.ProjectOnce(context.Background(), storage.BindingID("binding-1"), 25); err != nil {
			t.Fatalf("ProjectOnce() invocation %d error = %v", invocation, err)
		}
		if fake.calls != invocation {
			t.Fatalf("upstream ProjectOnce calls after invocation %d = %d, want %d (no internal loop)",
				invocation, fake.calls, invocation)
		}
	}
}
