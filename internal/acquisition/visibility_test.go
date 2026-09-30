package acquisition_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/integrations/openlist"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var visibilityNow = time.Date(2026, 9, 30, 10, 30, 0, 0, time.UTC)

// visibilityPortDouble records exactly what the verifier asked for.
type visibilityPortDouble struct {
	mu        sync.Mutex
	calls     int
	requests  []openlist.StatRequest
	fact      openlist.VisibilityFact
	err       error
	transform func(openlist.StatRequest) openlist.VisibilityFact
}

func (port *visibilityPortDouble) Stat(_ context.Context, request openlist.StatRequest) (openlist.VisibilityFact, error) {
	port.mu.Lock()
	defer port.mu.Unlock()
	port.calls++
	port.requests = append(port.requests, request)
	if port.err != nil {
		return openlist.VisibilityFact{}, port.err
	}
	if port.transform != nil {
		return port.transform(request), nil
	}
	if port.fact.Mount == "" && port.fact.Path == "" {
		// Default: echo the request and report not visible.
		return openlist.VisibilityFact{
			Mount: request.Mount, Path: request.Path, Visible: false, ObservedAt: visibilityNow,
		}, nil
	}
	return port.fact, nil
}

func (port *visibilityPortDouble) snapshot() (int, []openlist.StatRequest) {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.calls, append([]openlist.StatRequest(nil), port.requests...)
}

type visibilityBindingReaderDouble struct {
	values map[storage.BindingID]storage.Binding
	err    error
	calls  int
}

func (reader *visibilityBindingReaderDouble) GetBinding(_ context.Context, id storage.BindingID) (storage.Binding, error) {
	reader.calls++
	if reader.err != nil {
		return storage.Binding{}, reader.err
	}
	binding, exists := reader.values[id]
	if !exists {
		return storage.Binding{}, storage.ErrNotFound
	}
	return binding, nil
}

const (
	visibilityManifestID = acquisition.ManifestID("e0000000-0000-4000-8000-000000000001")
	visibilityBindingID  = storage.BindingID("e0000000-0000-4000-8000-000000000002")
)

type visibilityFixture struct {
	verifier  *acquisition.VisibilityVerifier
	port      *visibilityPortDouble
	manifests *manifestReaderDouble
	bindings  *visibilityBindingReaderDouble
}

// newVisibilityFixture wires an AWAITING_VISIBILITY Manifest over an ACTIVE binding.
// mount and target are the persisted coordinates.
func newVisibilityFixture(t *testing.T, mount, target string, options ...func(*storage.Binding, *acquisition.Manifest)) *visibilityFixture {
	t.Helper()
	binding := storage.Binding{
		ID: visibilityBindingID, ConnectionID: executionConnID,
		ProviderScope:     stringPointer("provider-scope-should-not-matter"),
		OpenListMountPath: mount, IndexCoreRootID: "index-root-should-not-matter",
		Status: storage.BindingStatusActive, CreatedAt: visibilityNow, UpdatedAt: visibilityNow,
	}
	manifest := acquisition.Manifest{
		ID: visibilityManifestID, SourceType: "opaque-source", SourceRef: "opaque-ref",
		TargetStorageBindingID: visibilityBindingID, TargetPath: target,
		State: acquisition.StateAwaitingVisibility, CreatedAt: visibilityNow, UpdatedAt: visibilityNow,
	}
	for _, option := range options {
		option(&binding, &manifest)
	}
	manifests := &manifestReaderDouble{values: map[acquisition.ManifestID]acquisition.Manifest{
		visibilityManifestID: manifest,
	}}
	bindings := &visibilityBindingReaderDouble{values: map[storage.BindingID]storage.Binding{
		visibilityBindingID: binding,
	}}
	port := &visibilityPortDouble{}
	verifier, err := acquisition.NewVisibilityVerifier(manifests, bindings, port,
		acquisition.WithVisibilityClock(func() time.Time { return visibilityNow }))
	if err != nil {
		t.Fatalf("NewVisibilityVerifier() error = %v", err)
	}
	return &visibilityFixture{verifier: verifier, port: port, manifests: manifests, bindings: bindings}
}

func (fixture *visibilityFixture) verify() (acquisition.VisibilityResult, error) {
	return fixture.verifier.Verify(context.Background(), visibilityManifestID)
}

// --- 1-4: D-028 path mapping -------------------------------------------------

func TestVisibilityPathMappingFollowsD028(t *testing.T) {
	tests := []struct {
		name      string
		mount     string
		target    string
		wantMount string
		wantPath  string
	}{
		{name: "root mount", mount: "/", target: "/downloads/item", wantMount: "/", wantPath: "/downloads/item"},
		{name: "prefixed mount", mount: "/115", target: "/downloads/item", wantMount: "/115", wantPath: "/115/downloads/item"},
		{name: "nested mount", mount: "/cloud/115", target: "/a/b/c", wantMount: "/cloud/115", wantPath: "/cloud/115/a/b/c"},
		{name: "single component target", mount: "/115", target: "/item", wantMount: "/115", wantPath: "/115/item"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newVisibilityFixture(t, test.mount, test.target)
			result, err := fixture.verify()
			if err != nil {
				t.Fatalf("Verify() error = %v", err)
			}
			// 17: exactly one call carrying Panta port coordinates - the binding mount
			// and the binding-relative target, NOT the joined wire path.
			calls, requests := fixture.port.snapshot()
			if calls != 1 {
				t.Fatalf("Stat calls = %d, want exactly 1", calls)
			}
			if requests[0].Mount != test.wantMount || requests[0].Path != test.target {
				t.Fatalf("Stat request = %+v, want port coordinates mount %q target %q",
					requests[0], test.wantMount, test.target)
			}
			if requests[0].Path == test.wantPath && test.wantPath != test.target {
				t.Fatalf("Stat received the joined wire path %q instead of the binding-relative target %q",
					test.wantPath, test.target)
			}
			// The fact echoed port coordinates, and the joined wire path is recorded
			// separately for the OpenList boundary.
			if result.Mount != test.wantMount || result.TargetPath != test.target {
				t.Fatalf("result lost its port coordinates: %+v", result)
			}
			if result.OpenListPath != test.wantPath {
				t.Fatalf("OpenListPath = %q, want the D-028 join %q", result.OpenListPath, test.wantPath)
			}
			if result.Fact.Mount != test.wantMount || result.Fact.Path != test.target {
				t.Fatalf("fact did not echo port coordinates: %+v", result.Fact)
			}
			if result.State != acquisition.VisibilityNotVisible {
				t.Fatalf("state = %q, want NOT_VISIBLE", result.State)
			}
		})
	}
}

// TestVisibilityPathIgnoresProviderScopeAndIndexCoreRoot covers tests 3 and 4: the
// observation coordinates come only from the mount and the target path.
func TestVisibilityPathIgnoresProviderScopeAndIndexCoreRoot(t *testing.T) {
	fixture := newVisibilityFixture(t, "/115", "/downloads/item",
		func(binding *storage.Binding, _ *acquisition.Manifest) {
			scope := "magnet:?xt=urn:btih:provider-scope"
			binding.ProviderScope = &scope
			binding.IndexCoreRootID = "/downloads/item"
		})
	result, err := fixture.verify()
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if result.OpenListPath != "/115/downloads/item" {
		t.Fatalf("OpenList path = %q, want the D-028 join", result.OpenListPath)
	}
	if result.OpenListPath == *fixture.bindings.values[visibilityBindingID].ProviderScope {
		t.Fatal("OpenList path was derived from provider_scope")
	}
	_, requests := fixture.port.snapshot()
	if requests[0].Mount != "/115" || requests[0].Path != "/downloads/item" {
		t.Fatalf("Stat request = %+v, want port coordinates /115 + /downloads/item", requests[0])
	}
}

// TestVisibilityPathCannotEscapeMount covers test 3 for the mount-prefix case: a
// join that would leave the configured mount is rejected before any call.
func TestVisibilityPathCannotEscapeMount(t *testing.T) {
	// A persisted binding whose mount is not canonical must be rejected rather than
	// silently normalized into a different namespace.
	for _, mount := range []string{"115", "/115/", "//115", "/115/../other"} {
		t.Run(mount, func(t *testing.T) {
			fixture := newVisibilityFixture(t, mount, "/downloads/item")
			if _, err := fixture.verify(); !errors.Is(err, acquisition.ErrVisibilityPathInvalid) {
				t.Fatalf("Verify() error = %v, want ErrVisibilityPathInvalid", err)
			}
			if calls, _ := fixture.port.snapshot(); calls != 0 {
				t.Fatalf("Stat calls = %d, want 0 for an invalid mount", calls)
			}
		})
	}
	for _, target := range []string{"downloads/item", "/downloads/../other", "/a//b", "/a\x00b"} {
		t.Run("target "+target, func(t *testing.T) {
			fixture := newVisibilityFixture(t, "/115", target)
			if _, err := fixture.verify(); !errors.Is(err, acquisition.ErrVisibilityPathInvalid) {
				t.Fatalf("Verify() error = %v, want ErrVisibilityPathInvalid", err)
			}
			if calls, _ := fixture.port.snapshot(); calls != 0 {
				t.Fatalf("Stat calls = %d, want 0 for an invalid target", calls)
			}
		})
	}
}

// --- 15-16: preconditions ----------------------------------------------------

func TestVisibilityRequiresAwaitingVisibilityManifest(t *testing.T) {
	states := []acquisition.State{
		acquisition.StatePending, acquisition.StateActive, acquisition.StateReady,
		acquisition.StateFailed, acquisition.StateCanceled, acquisition.StateRecoveryRequired,
		acquisition.StateAwaitingCanonical,
	}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			fixture := newVisibilityFixture(t, "/115", "/downloads/item",
				func(_ *storage.Binding, manifest *acquisition.Manifest) { manifest.State = state })
			if _, err := fixture.verify(); !errors.Is(err, acquisition.ErrVisibilityManifestState) {
				t.Fatalf("Verify() error = %v, want ErrVisibilityManifestState", err)
			}
			// The binding must not even be read, let alone OpenList called.
			if calls, _ := fixture.port.snapshot(); calls != 0 {
				t.Fatalf("Stat calls = %d, want 0", calls)
			}
			if fixture.bindings.calls != 0 {
				t.Fatalf("binding reads = %d, want 0 for a wrong-state Manifest", fixture.bindings.calls)
			}
		})
	}
}

func TestVisibilityRejectsMissingOrDisabledBindingBeforeOpenList(t *testing.T) {
	t.Run("missing binding", func(t *testing.T) {
		fixture := newVisibilityFixture(t, "/115", "/downloads/item")
		fixture.bindings.values = map[storage.BindingID]storage.Binding{}
		if _, err := fixture.verify(); !errors.Is(err, acquisition.ErrVisibilityBindingMissing) {
			t.Fatalf("Verify() error = %v, want ErrVisibilityBindingMissing", err)
		}
		if calls, _ := fixture.port.snapshot(); calls != 0 {
			t.Fatalf("Stat calls = %d, want 0", calls)
		}
	})

	t.Run("disabled binding", func(t *testing.T) {
		fixture := newVisibilityFixture(t, "/115", "/downloads/item",
			func(binding *storage.Binding, _ *acquisition.Manifest) {
				binding.Status = storage.BindingStatusDisabled
			})
		if _, err := fixture.verify(); !errors.Is(err, acquisition.ErrVisibilityBindingState) {
			t.Fatalf("Verify() error = %v, want ErrVisibilityBindingState", err)
		}
		if calls, _ := fixture.port.snapshot(); calls != 0 {
			t.Fatalf("Stat calls = %d, want 0", calls)
		}
	})

	t.Run("binding identity mismatch", func(t *testing.T) {
		fixture := newVisibilityFixture(t, "/115", "/downloads/item",
			func(binding *storage.Binding, _ *acquisition.Manifest) {
				binding.ID = storage.BindingID("e0000000-0000-4000-8000-0000000000ff")
			})
		if _, err := fixture.verify(); !errors.Is(err, acquisition.ErrStorageTopologyMismatch) {
			t.Fatalf("Verify() error = %v, want ErrStorageTopologyMismatch", err)
		}
		if calls, _ := fixture.port.snapshot(); calls != 0 {
			t.Fatalf("Stat calls = %d, want 0", calls)
		}
	})
}

func TestVisibilityRejectsMissingManifestAndEmptyID(t *testing.T) {
	fixture := newVisibilityFixture(t, "/115", "/downloads/item")
	delete(fixture.manifests.values, visibilityManifestID)
	if _, err := fixture.verify(); !errors.Is(err, acquisition.ErrExecutionManifestNotFound) {
		t.Fatalf("Verify() error = %v, want ErrExecutionManifestNotFound", err)
	}
	if _, err := fixture.verifier.Verify(context.Background(), ""); !errors.Is(err, acquisition.ErrInvalidVisibilityRequest) {
		t.Fatalf("Verify(empty) error = %v, want ErrInvalidVisibilityRequest", err)
	}
}

// --- 18-21: results, identity, isolation -------------------------------------

func TestVisibilityVisibleResultIsReturnedWithoutMutation(t *testing.T) {
	fixture := newVisibilityFixture(t, "/115", "/downloads/item")
	before := fixture.manifests.values[visibilityManifestID]
	modified := time.Date(2026, 9, 30, 9, 0, 0, 0, time.UTC)
	fixture.port.transform = func(request openlist.StatRequest) openlist.VisibilityFact {
		return openlist.VisibilityFact{
			Mount: request.Mount, Path: request.Path, Visible: true,
			Name: "item", Directory: false, SizeBytes: 4096, ModifiedAt: &modified, ObservedAt: visibilityNow,
		}
	}

	result, err := fixture.verify()
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if result.State != acquisition.VisibilityVisible {
		t.Fatalf("state = %q, want VISIBLE", result.State)
	}
	if !result.Fact.Visible || result.Fact.Name != "item" || result.Fact.SizeBytes != 4096 {
		t.Fatalf("fact = %+v", result.Fact)
	}
	if result.ObservedAt != visibilityNow {
		t.Fatalf("observed at = %v", result.ObservedAt)
	}
	// 18: the Manifest is untouched.
	after := fixture.manifests.values[visibilityManifestID]
	if after.State != before.State || !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("Manifest mutated: %+v -> %+v", before, after)
	}
}

func TestVisibilityNotVisibleIsANormalResult(t *testing.T) {
	fixture := newVisibilityFixture(t, "/115", "/downloads/item")
	before := fixture.manifests.values[visibilityManifestID]

	result, err := fixture.verify()
	if err != nil {
		t.Fatalf("Verify() error = %v, want a normal not-visible observation", err)
	}
	if result.State != acquisition.VisibilityNotVisible || result.Fact.Visible {
		t.Fatalf("result = %+v, want NOT_VISIBLE", result)
	}
	// 19: still no mutation.
	after := fixture.manifests.values[visibilityManifestID]
	if after.State != before.State || !after.UpdatedAt.Equal(before.UpdatedAt) {
		t.Fatalf("Manifest mutated on a not-visible observation: %+v -> %+v", before, after)
	}
}

func TestVisibilityIntegrationFailureIsNeverNotVisible(t *testing.T) {
	fixture := newVisibilityFixture(t, "/115", "/downloads/item")
	fixture.port.err = &openlist.RemoteError{HTTPStatus: 200, EnvelopeCode: 500, Message: "storage not initialized"}

	result, err := fixture.verify()
	if err == nil {
		t.Fatalf("Verify() succeeded with %+v, want the integration error", result)
	}
	if result.State == acquisition.VisibilityNotVisible {
		t.Fatal("an integration failure was reported as NOT_VISIBLE")
	}
	if !errors.Is(err, openlist.ErrRemote) {
		t.Fatalf("error = %v, want ErrRemote", err)
	}
}

// TestVisibilityMismatchedFactIdentityFailsClosed covers test 20.
func TestVisibilityMismatchedFactIdentityFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		transform func(openlist.StatRequest) openlist.VisibilityFact
	}{
		{name: "wrong mount", transform: func(request openlist.StatRequest) openlist.VisibilityFact {
			return openlist.VisibilityFact{Mount: "/other", Path: request.Path, Visible: true}
		}},
		{name: "wrong path", transform: func(request openlist.StatRequest) openlist.VisibilityFact {
			return openlist.VisibilityFact{Mount: request.Mount, Path: "/other", Visible: true}
		}},
		{name: "empty identity", transform: func(openlist.StatRequest) openlist.VisibilityFact {
			return openlist.VisibilityFact{Visible: true}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newVisibilityFixture(t, "/115", "/downloads/item")
			fixture.port.transform = test.transform
			if _, err := fixture.verify(); !errors.Is(err, acquisition.ErrVisibilityFactIdentity) {
				t.Fatalf("Verify() error = %v, want ErrVisibilityFactIdentity", err)
			}
		})
	}
}

// TestVisibilityCallsPortExactlyOnceAndNothingElse covers tests 17 and 21: the
// verifier makes one Stat call and no other integration is reachable from it.
func TestVisibilityCallsPortExactlyOnceAndNothingElse(t *testing.T) {
	fixture := newVisibilityFixture(t, "/115", "/downloads/item")
	if _, err := fixture.verify(); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	calls, requests := fixture.port.snapshot()
	if calls != 1 {
		t.Fatalf("Stat calls = %d, want exactly 1", calls)
	}
	if len(requests) != 1 || requests[0].Mount != "/115" || requests[0].Path != "/downloads/item" {
		t.Fatalf("requests = %+v, want port coordinates /115 + /downloads/item", requests)
	}
	if fixture.bindings.calls != 1 {
		t.Fatalf("binding reads = %d, want exactly 1", fixture.bindings.calls)
	}
}

func TestNewVisibilityVerifierRequiresDependencies(t *testing.T) {
	fixture := newVisibilityFixture(t, "/115", "/downloads/item")
	if _, err := acquisition.NewVisibilityVerifier(nil, fixture.bindings, fixture.port); !errors.Is(err, acquisition.ErrVisibilityPortMissing) {
		t.Fatalf("nil manifests error = %v", err)
	}
	if _, err := acquisition.NewVisibilityVerifier(fixture.manifests, nil, fixture.port); !errors.Is(err, acquisition.ErrVisibilityPortMissing) {
		t.Fatalf("nil bindings error = %v", err)
	}
	if _, err := acquisition.NewVisibilityVerifier(fixture.manifests, fixture.bindings, nil); !errors.Is(err, acquisition.ErrVisibilityPortMissing) {
		t.Fatalf("nil port error = %v", err)
	}
	if _, err := acquisition.NewVisibilityVerifier(fixture.manifests, fixture.bindings, fixture.port,
		acquisition.WithVisibilityClock(nil)); !errors.Is(err, acquisition.ErrInvalidVisibilityRequest) {
		t.Fatalf("nil clock error = %v", err)
	}
}

func TestVisibilityHonoursContextCancellation(t *testing.T) {
	fixture := newVisibilityFixture(t, "/115", "/downloads/item")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fixture.verifier.Verify(ctx, visibilityManifestID); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify() error = %v, want context.Canceled", err)
	}
	if calls, _ := fixture.port.snapshot(); calls != 0 {
		t.Fatalf("Stat calls = %d, want 0", calls)
	}
}
