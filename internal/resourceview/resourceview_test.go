package resourceview_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/resourceview"
)

func TestAssetAbsenceAndEmptyHierarchyAreDistinct(t *testing.T) {
	reader := newCatalogReader()
	service := newService(t, reader)
	if _, err := service.Get(context.Background(), resourceview.Request{AssetID: "missing"}); !errors.Is(err, resourceview.ErrAssetNotFound) {
		t.Fatalf("missing asset error = %v, want ErrAssetNotFound", err)
	}

	reader.assets["asset-a"] = catalog.Asset{ID: "asset-a"}
	detail, err := service.Get(context.Background(), resourceview.Request{AssetID: "asset-a"})
	if err != nil {
		t.Fatalf("empty asset detail error = %v", err)
	}
	if detail.Asset.ID != "asset-a" || detail.Releases == nil || len(detail.Releases) != 0 ||
		detail.Variants == nil || detail.Copies == nil {
		t.Fatalf("empty asset detail = %#v", detail)
	}
}

func TestReleaseAndVariantSelectionsPreserveSiblingsAndValidateOwnership(t *testing.T) {
	reader := newCatalogReader()
	reader.assets["asset-a"] = catalog.Asset{ID: "asset-a"}
	reader.assets["asset-b"] = catalog.Asset{ID: "asset-b"}
	reader.releases["release-a1"] = catalog.Release{ID: "release-a1", AssetID: "asset-a"}
	reader.releases["release-a2"] = catalog.Release{ID: "release-a2", AssetID: "asset-a"}
	reader.releases["release-b"] = catalog.Release{ID: "release-b", AssetID: "asset-b"}
	reader.variants["variant-a1"] = catalog.Variant{ID: "variant-a1", ReleaseID: "release-a1"}
	reader.variants["variant-a2"] = catalog.Variant{ID: "variant-a2", ReleaseID: "release-a1"}
	reader.variants["variant-b"] = catalog.Variant{ID: "variant-b", ReleaseID: "release-b"}
	service := newService(t, reader)

	releaseID, variantID := catalog.ReleaseID("release-a1"), catalog.VariantID("variant-a2")
	detail, err := service.Get(context.Background(), resourceview.Request{
		AssetID: "asset-a", ReleaseID: &releaseID, VariantID: &variantID,
	})
	if err != nil {
		t.Fatalf("selected detail error = %v", err)
	}
	if len(detail.Releases) != 2 || detail.Releases[0].ID != "release-a1" || detail.Releases[1].ID != "release-a2" ||
		len(detail.Variants) != 2 || detail.Variants[0].ID != "variant-a1" || detail.Variants[1].ID != "variant-a2" ||
		detail.SelectedRelease == nil || detail.SelectedRelease.ID != releaseID ||
		detail.SelectedVariant == nil || detail.SelectedVariant.ID != variantID {
		t.Fatalf("selected detail = %#v", detail)
	}

	missingRelease := catalog.ReleaseID("missing")
	if _, err := service.Get(context.Background(), resourceview.Request{AssetID: "asset-a", ReleaseID: &missingRelease}); !errors.Is(err, resourceview.ErrReleaseNotFound) {
		t.Fatalf("missing release error = %v", err)
	}
	foreignRelease := catalog.ReleaseID("release-b")
	if _, err := service.Get(context.Background(), resourceview.Request{AssetID: "asset-a", ReleaseID: &foreignRelease}); !errors.Is(err, resourceview.ErrReleaseOwnershipMismatch) {
		t.Fatalf("foreign release error = %v", err)
	}
	missingVariant := catalog.VariantID("missing")
	if _, err := service.Get(context.Background(), resourceview.Request{AssetID: "asset-a", ReleaseID: &releaseID, VariantID: &missingVariant}); !errors.Is(err, resourceview.ErrVariantNotFound) {
		t.Fatalf("missing variant error = %v", err)
	}
	foreignVariant := catalog.VariantID("variant-b")
	if _, err := service.Get(context.Background(), resourceview.Request{AssetID: "asset-a", ReleaseID: &releaseID, VariantID: &foreignVariant}); !errors.Is(err, resourceview.ErrVariantOwnershipMismatch) {
		t.Fatalf("foreign variant error = %v", err)
	}
	if _, err := service.Get(context.Background(), resourceview.Request{AssetID: "asset-a", VariantID: &variantID}); !errors.Is(err, resourceview.ErrInvalidRequest) {
		t.Fatalf("variant without release error = %v", err)
	}
}

func TestSelectedVariantCopyAvailabilityAndUnresolvedIsolation(t *testing.T) {
	tests := []struct {
		name        string
		copies      []catalog.Copy
		wantCount   int
		wantPresent bool
	}{
		{name: "zero copies", wantCount: 0, wantPresent: false},
		{name: "removed only", copies: []catalog.Copy{classifiedCopy("removed", "variant-a", catalog.CopyAvailabilityRemoved)}, wantCount: 1, wantPresent: false},
		{name: "one present", copies: []catalog.Copy{classifiedCopy("present", "variant-a", catalog.CopyAvailabilityPresent)}, wantCount: 1, wantPresent: true},
		{name: "multiple present", copies: []catalog.Copy{
			classifiedCopy("present-a", "variant-a", catalog.CopyAvailabilityPresent),
			classifiedCopy("present-b", "variant-a", catalog.CopyAvailabilityPresent),
			classifiedCopy("removed", "variant-a", catalog.CopyAvailabilityRemoved),
		}, wantCount: 3, wantPresent: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := selectedCatalogReader()
			reader.copies = append(test.copies, catalog.Copy{ID: "unresolved", VariantID: nil, Availability: catalog.CopyAvailabilityPresent})
			service := newService(t, reader)
			releaseID, variantID := catalog.ReleaseID("release-a"), catalog.VariantID("variant-a")
			detail, err := service.Get(context.Background(), resourceview.Request{
				AssetID: "asset-a", ReleaseID: &releaseID, VariantID: &variantID,
			})
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if len(detail.Copies) != test.wantCount || detail.HasPresentCopy != test.wantPresent {
				t.Fatalf("copies/present = %#v/%v, want count=%d present=%v", detail.Copies, detail.HasPresentCopy, test.wantCount, test.wantPresent)
			}
			for _, resourceCopy := range detail.Copies {
				if resourceCopy.VariantID == nil || resourceCopy.ID == "unresolved" {
					t.Fatalf("unresolved Copy attached to logical Variant: %#v", resourceCopy)
				}
			}
		})
	}
}

type catalogReader struct {
	assets   map[catalog.AssetID]catalog.Asset
	releases map[catalog.ReleaseID]catalog.Release
	variants map[catalog.VariantID]catalog.Variant
	copies   []catalog.Copy
}

func newCatalogReader() *catalogReader {
	return &catalogReader{
		assets: make(map[catalog.AssetID]catalog.Asset), releases: make(map[catalog.ReleaseID]catalog.Release),
		variants: make(map[catalog.VariantID]catalog.Variant), copies: make([]catalog.Copy, 0),
	}
}

func selectedCatalogReader() *catalogReader {
	reader := newCatalogReader()
	reader.assets["asset-a"] = catalog.Asset{ID: "asset-a"}
	reader.releases["release-a"] = catalog.Release{ID: "release-a", AssetID: "asset-a"}
	reader.variants["variant-a"] = catalog.Variant{ID: "variant-a", ReleaseID: "release-a"}
	return reader
}

func (reader *catalogReader) GetAsset(_ context.Context, id catalog.AssetID) (catalog.Asset, error) {
	value, ok := reader.assets[id]
	if !ok {
		return catalog.Asset{}, catalog.ErrNotFound
	}
	return value, nil
}

func (reader *catalogReader) GetRelease(_ context.Context, id catalog.ReleaseID) (catalog.Release, error) {
	value, ok := reader.releases[id]
	if !ok {
		return catalog.Release{}, catalog.ErrNotFound
	}
	return value, nil
}

func (reader *catalogReader) GetVariant(_ context.Context, id catalog.VariantID) (catalog.Variant, error) {
	value, ok := reader.variants[id]
	if !ok {
		return catalog.Variant{}, catalog.ErrNotFound
	}
	return value, nil
}

func (reader *catalogReader) ListReleasesByAsset(_ context.Context, id catalog.AssetID) ([]catalog.Release, error) {
	result := make([]catalog.Release, 0)
	for _, releaseID := range []catalog.ReleaseID{"release-a1", "release-a2", "release-a", "release-b"} {
		if release, ok := reader.releases[releaseID]; ok && release.AssetID == id {
			result = append(result, release)
		}
	}
	return result, nil
}

func (reader *catalogReader) ListVariantsByRelease(_ context.Context, id catalog.ReleaseID) ([]catalog.Variant, error) {
	result := make([]catalog.Variant, 0)
	for _, variantID := range []catalog.VariantID{"variant-a1", "variant-a2", "variant-a", "variant-b"} {
		if variant, ok := reader.variants[variantID]; ok && variant.ReleaseID == id {
			result = append(result, variant)
		}
	}
	return result, nil
}

func (reader *catalogReader) ListCopiesByVariant(_ context.Context, id catalog.VariantID) ([]catalog.Copy, error) {
	result := make([]catalog.Copy, 0)
	for _, resourceCopy := range reader.copies {
		if resourceCopy.VariantID != nil && *resourceCopy.VariantID == id {
			result = append(result, resourceCopy)
		}
	}
	return result, nil
}

func newService(t *testing.T, reader catalog.HierarchyReader) *resourceview.Service {
	t.Helper()
	service, err := resourceview.NewService(reader)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func classifiedCopy(id string, variantID catalog.VariantID, availability string) catalog.Copy {
	return catalog.Copy{ID: catalog.CopyID(id), VariantID: &variantID, Availability: availability}
}
