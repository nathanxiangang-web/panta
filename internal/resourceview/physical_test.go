package resourceview_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/resourceview"
)

func TestPhysicalResolveZeroMatches(t *testing.T) {
	index := &resolveReader{result: indexcore.ResolveResult{Matches: nil, Ambiguous: false}}
	service := newPhysicalService(t, index, newPhysicalCatalogReader())
	detail, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{
		RootID: "root-a", Path: "/known/file", ReadVisibility: indexcore.ReadVisibility{IncludeRemoved: true},
	})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if detail.Matches == nil || len(detail.Matches) != 0 || detail.Ambiguous {
		t.Fatalf("Resolve() = %#v, want non-nil empty unambiguous result", detail)
	}
	if index.calls != 1 || index.request.RootID != "root-a" || index.request.Path != "/known/file" || !index.request.IncludeRemoved {
		t.Fatalf("Q5 request = %#v, calls=%d", index.request, index.calls)
	}
}

func TestPhysicalResolveWithoutCopyIsPhysicalOnly(t *testing.T) {
	physical := physicalResource("root-a", "resource-a", indexcore.ResourcePresent)
	reader := newPhysicalCatalogReader()
	service := newPhysicalService(t, &resolveReader{result: resolved(physical)}, reader)
	detail, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{RootID: "root-a", Path: "/a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	match := onlyPhysicalMatch(t, detail)
	if match.Classification != resourceview.ClassificationPhysicalOnly || match.Copy != nil || match.Physical.ResourceID != "resource-a" {
		t.Fatalf("physical-only match = %#v", match)
	}
}

func TestPhysicalResolveUnresolvedCopy(t *testing.T) {
	physical := physicalResource("root-a", "resource-a", indexcore.ResourcePresent)
	reader := newPhysicalCatalogReader()
	reader.copies[physicalKey("root-a", "resource-a")] = catalog.Copy{
		ID: "copy-a", IndexCoreRootID: "root-a", IndexCoreResourceID: "resource-a",
		Availability: catalog.CopyAvailabilityPresent,
	}
	service := newPhysicalService(t, &resolveReader{result: resolved(physical)}, reader)
	detail, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{RootID: "root-a", Path: "/a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	match := onlyPhysicalMatch(t, detail)
	if match.Classification != resourceview.ClassificationUnresolvedCopy || match.Copy == nil ||
		match.Variant != nil || match.Release != nil || match.Asset != nil {
		t.Fatalf("unresolved match = %#v", match)
	}
}

func TestPhysicalResolveClassifiedLineage(t *testing.T) {
	physical := physicalResource("root-a", "resource-a", indexcore.ResourcePresent)
	reader := classifiedPhysicalCatalogReader("root-a", "resource-a")
	service := newPhysicalService(t, &resolveReader{result: resolved(physical)}, reader)
	detail, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{RootID: "root-a", Path: "/a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	match := onlyPhysicalMatch(t, detail)
	if match.Classification != resourceview.ClassificationClassified || match.Copy == nil ||
		match.Variant == nil || match.Variant.ID != "variant-a" ||
		match.Release == nil || match.Release.ID != "release-a" ||
		match.Asset == nil || match.Asset.ID != "asset-a" {
		t.Fatalf("classified match = %#v", match)
	}
}

func TestPhysicalResolvePreservesEveryAmbiguousMatchWithoutSelectingWinner(t *testing.T) {
	first := physicalResource("root-a", "resource-unclassified", indexcore.ResourcePresent)
	second := physicalResource("root-a", "resource-classified", indexcore.ResourcePresent)
	reader := classifiedPhysicalCatalogReader("root-a", "resource-classified")
	service := newPhysicalService(t, &resolveReader{result: indexcore.ResolveResult{
		Matches: []indexcore.ResourceContext{first, second}, Ambiguous: true,
	}}, reader)
	detail, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{RootID: "root-a", Path: "/ambiguous"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if !detail.Ambiguous || len(detail.Matches) != 2 ||
		detail.Matches[0].Physical.ResourceID != first.ResourceID ||
		detail.Matches[0].Classification != resourceview.ClassificationPhysicalOnly ||
		detail.Matches[1].Physical.ResourceID != second.ResourceID ||
		detail.Matches[1].Classification != resourceview.ClassificationClassified {
		t.Fatalf("ambiguous detail = %#v", detail)
	}
}

func TestPhysicalResolveUsesExactQ5IdentityForCopyLookup(t *testing.T) {
	physical := physicalResource("resolved-root", "resolved-resource", indexcore.ResourcePresent)
	reader := newPhysicalCatalogReader()
	service := newPhysicalService(t, &resolveReader{result: resolved(physical)}, reader)
	_, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{RootID: "request-root", Path: "/a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(reader.lookups) != 1 || reader.lookups[0] != physicalKey("resolved-root", "resolved-resource") {
		t.Fatalf("physical lookups = %#v", reader.lookups)
	}
}

func TestPhysicalResolveRejectsMissingClassifiedLineage(t *testing.T) {
	tests := []struct {
		name   string
		remove func(*physicalCatalogReader)
	}{
		{name: "missing Variant", remove: func(reader *physicalCatalogReader) { delete(reader.variants, "variant-a") }},
		{name: "missing Release", remove: func(reader *physicalCatalogReader) { delete(reader.releases, "release-a") }},
		{name: "missing Asset", remove: func(reader *physicalCatalogReader) { delete(reader.assets, "asset-a") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := classifiedPhysicalCatalogReader("root-a", "resource-a")
			test.remove(reader)
			service := newPhysicalService(t, &resolveReader{result: resolved(physicalResource("root-a", "resource-a", indexcore.ResourcePresent))}, reader)
			_, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{RootID: "root-a", Path: "/a"})
			if !errors.Is(err, resourceview.ErrInvalidCatalogState) {
				t.Fatalf("Resolve() error = %v, want ErrInvalidCatalogState", err)
			}
		})
	}
}

func TestPhysicalResolveRejectsCatalogOwnershipMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*physicalCatalogReader)
	}{
		{name: "Copy to Variant", mutate: func(reader *physicalCatalogReader) {
			reader.variants["variant-a"] = catalog.Variant{ID: "variant-other", ReleaseID: "release-a"}
		}},
		{name: "Variant to Release", mutate: func(reader *physicalCatalogReader) {
			reader.releases["release-a"] = catalog.Release{ID: "release-other", AssetID: "asset-a"}
		}},
		{name: "Release to Asset", mutate: func(reader *physicalCatalogReader) {
			reader.assets["asset-a"] = catalog.Asset{ID: "asset-other"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := classifiedPhysicalCatalogReader("root-a", "resource-a")
			test.mutate(reader)
			service := newPhysicalService(t, &resolveReader{result: resolved(physicalResource("root-a", "resource-a", indexcore.ResourcePresent))}, reader)
			_, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{RootID: "root-a", Path: "/a"})
			if !errors.Is(err, resourceview.ErrInvalidCatalogState) {
				t.Fatalf("Resolve() error = %v, want ErrInvalidCatalogState", err)
			}
		})
	}
}

func TestPhysicalPresenceIsIndependentFromCopyAvailability(t *testing.T) {
	physical := physicalResource("root-a", "resource-a", indexcore.ResourcePresent)
	reader := classifiedPhysicalCatalogReader("root-a", "resource-a")
	resourceCopy := reader.copies[physicalKey("root-a", "resource-a")]
	resourceCopy.Availability = catalog.CopyAvailabilityRemoved
	reader.copies[physicalKey("root-a", "resource-a")] = resourceCopy
	service := newPhysicalService(t, &resolveReader{result: resolved(physical)}, reader)
	detail, err := service.Resolve(context.Background(), resourceview.PhysicalRequest{RootID: "root-a", Path: "/a"})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	match := onlyPhysicalMatch(t, detail)
	if match.Physical.Presence != indexcore.ResourcePresent || match.Copy == nil || match.Copy.Availability != catalog.CopyAvailabilityRemoved {
		t.Fatalf("presence/availability were reconciled: %#v", match)
	}
}

type resolveReader struct {
	result  indexcore.ResolveResult
	err     error
	request indexcore.ResolveRequest
	calls   int
}

func (reader *resolveReader) Resolve(_ context.Context, request indexcore.ResolveRequest) (indexcore.ResolveResult, error) {
	reader.calls++
	reader.request = request
	return reader.result, reader.err
}

type physicalCatalogReader struct {
	copies   map[string]catalog.Copy
	variants map[catalog.VariantID]catalog.Variant
	releases map[catalog.ReleaseID]catalog.Release
	assets   map[catalog.AssetID]catalog.Asset
	lookups  []string
}

func newPhysicalCatalogReader() *physicalCatalogReader {
	return &physicalCatalogReader{
		copies: make(map[string]catalog.Copy), variants: make(map[catalog.VariantID]catalog.Variant),
		releases: make(map[catalog.ReleaseID]catalog.Release), assets: make(map[catalog.AssetID]catalog.Asset),
		lookups: make([]string, 0),
	}
}

func classifiedPhysicalCatalogReader(rootID, resourceID string) *physicalCatalogReader {
	reader := newPhysicalCatalogReader()
	variantID := catalog.VariantID("variant-a")
	reader.copies[physicalKey(rootID, resourceID)] = catalog.Copy{
		ID: "copy-a", VariantID: &variantID, IndexCoreRootID: rootID, IndexCoreResourceID: resourceID,
		Availability: catalog.CopyAvailabilityPresent,
	}
	reader.variants[variantID] = catalog.Variant{ID: variantID, ReleaseID: "release-a"}
	reader.releases["release-a"] = catalog.Release{ID: "release-a", AssetID: "asset-a"}
	reader.assets["asset-a"] = catalog.Asset{ID: "asset-a"}
	return reader
}

func (reader *physicalCatalogReader) GetCopyByPhysicalIdentity(_ context.Context, rootID, resourceID string) (catalog.Copy, error) {
	key := physicalKey(rootID, resourceID)
	reader.lookups = append(reader.lookups, key)
	value, ok := reader.copies[key]
	if !ok {
		return catalog.Copy{}, catalog.ErrNotFound
	}
	return value, nil
}

func (reader *physicalCatalogReader) GetVariant(_ context.Context, id catalog.VariantID) (catalog.Variant, error) {
	value, ok := reader.variants[id]
	if !ok {
		return catalog.Variant{}, catalog.ErrNotFound
	}
	return value, nil
}

func (reader *physicalCatalogReader) GetRelease(_ context.Context, id catalog.ReleaseID) (catalog.Release, error) {
	value, ok := reader.releases[id]
	if !ok {
		return catalog.Release{}, catalog.ErrNotFound
	}
	return value, nil
}

func (reader *physicalCatalogReader) GetAsset(_ context.Context, id catalog.AssetID) (catalog.Asset, error) {
	value, ok := reader.assets[id]
	if !ok {
		return catalog.Asset{}, catalog.ErrNotFound
	}
	return value, nil
}

func resolved(resources ...indexcore.ResourceContext) indexcore.ResolveResult {
	return indexcore.ResolveResult{Matches: resources}
}

func physicalResource(rootID, resourceID string, presence indexcore.ResourcePresence) indexcore.ResourceContext {
	return indexcore.ResourceContext{RootID: rootID, ResourceID: resourceID, Presence: presence}
}

func physicalKey(rootID, resourceID string) string { return rootID + "\x00" + resourceID }

func newPhysicalService(t *testing.T, index indexcore.ResolvePort, reader resourceview.PhysicalCatalogReader) *resourceview.PhysicalService {
	t.Helper()
	service, err := resourceview.NewPhysicalService(index, reader)
	if err != nil {
		t.Fatalf("NewPhysicalService() error = %v", err)
	}
	return service
}

func onlyPhysicalMatch(t *testing.T, detail resourceview.PhysicalDetail) resourceview.PhysicalMatch {
	t.Helper()
	if len(detail.Matches) != 1 {
		t.Fatalf("match count = %d, want 1", len(detail.Matches))
	}
	return detail.Matches[0]
}
