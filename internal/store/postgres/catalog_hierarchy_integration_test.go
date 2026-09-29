package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestPostgresCatalogHierarchyReadsAreScopedOrderedAndPreserveCopyState(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 4 {
		t.Fatalf("Apply() = %#v, %v", status, err)
	}
	bindingID := storage.BindingID("17000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "gate-2-root")
	repository, err := NewCatalogRepository(pool)
	if err != nil {
		t.Fatalf("NewCatalogRepository() error = %v", err)
	}

	base := time.Date(2026, 9, 29, 2, 0, 0, 0, time.UTC)
	assetA := catalog.Asset{ID: "17000000-0000-4000-8000-000000000010", CanonicalName: "A", Category: "software", Status: "ACTIVE", CreatedAt: base, UpdatedAt: base}
	assetB := catalog.Asset{ID: "17000000-0000-4000-8000-000000000011", CanonicalName: "B", Category: "software", Status: "ACTIVE", CreatedAt: base, UpdatedAt: base}
	for _, asset := range []catalog.Asset{assetA, assetB} {
		if err := repository.CreateAsset(ctx, asset); err != nil {
			t.Fatalf("CreateAsset(%s) error = %v", asset.ID, err)
		}
	}
	releaseLater := catalog.Release{ID: "17000000-0000-4000-8000-000000000021", AssetID: assetA.ID, VersionRaw: "2", VersionScheme: catalog.VersionSchemeRevision, Channel: "stable", Status: "ACTIVE", CreatedAt: base.Add(time.Minute), UpdatedAt: base.Add(time.Minute)}
	releaseEarlier := catalog.Release{ID: "17000000-0000-4000-8000-000000000020", AssetID: assetA.ID, VersionRaw: "1", VersionScheme: catalog.VersionSchemeRevision, Channel: "stable", Status: "ACTIVE", CreatedAt: base, UpdatedAt: base}
	foreignRelease := catalog.Release{ID: "17000000-0000-4000-8000-000000000022", AssetID: assetB.ID, VersionRaw: "x", VersionScheme: catalog.VersionSchemeCustom, Channel: "other", Status: "ACTIVE", CreatedAt: base, UpdatedAt: base}
	for _, release := range []catalog.Release{releaseLater, foreignRelease, releaseEarlier} {
		if err := repository.CreateRelease(ctx, release); err != nil {
			t.Fatalf("CreateRelease(%s) error = %v", release.ID, err)
		}
	}

	variantZ := catalog.Variant{ID: "17000000-0000-4000-8000-000000000031", ReleaseID: releaseEarlier.ID, VariantKey: "z-linux", Attributes: json.RawMessage(`{"os":"linux"}`), Status: "ACTIVE", CreatedAt: base, UpdatedAt: base}
	variantA := catalog.Variant{ID: "17000000-0000-4000-8000-000000000030", ReleaseID: releaseEarlier.ID, VariantKey: "a-windows", Attributes: json.RawMessage(`{"os":"windows"}`), Status: "ACTIVE", CreatedAt: base, UpdatedAt: base}
	foreignVariant := catalog.Variant{ID: "17000000-0000-4000-8000-000000000032", ReleaseID: releaseLater.ID, VariantKey: "foreign", Attributes: json.RawMessage(`{}`), Status: "ACTIVE", CreatedAt: base, UpdatedAt: base}
	for _, variant := range []catalog.Variant{variantZ, foreignVariant, variantA} {
		if err := repository.CreateVariant(ctx, variant); err != nil {
			t.Fatalf("CreateVariant(%s) error = %v", variant.ID, err)
		}
	}

	removed := hierarchyCopy("17000000-0000-4000-8000-000000000040", variantA.ID, bindingID, "removed", catalog.CopyAvailabilityRemoved, base)
	present := hierarchyCopy("17000000-0000-4000-8000-000000000041", variantA.ID, bindingID, "present", catalog.CopyAvailabilityPresent, base.Add(time.Minute))
	otherVariant := hierarchyCopy("17000000-0000-4000-8000-000000000042", variantZ.ID, bindingID, "other", catalog.CopyAvailabilityPresent, base)
	unresolved := catalog.Copy{ID: "17000000-0000-4000-8000-000000000043", VariantID: nil, IndexCoreRootID: "gate-2-root", IndexCoreResourceID: "unresolved", StorageBindingID: catalog.StorageBindingID(bindingID), Availability: catalog.CopyAvailabilityPresent, CreatedAt: base, UpdatedAt: base}
	for _, resourceCopy := range []catalog.Copy{present, otherVariant, unresolved, removed} {
		if err := repository.CreateCopy(ctx, resourceCopy); err != nil {
			t.Fatalf("CreateCopy(%s) error = %v", resourceCopy.ID, err)
		}
	}

	releases, err := repository.ListReleasesByAsset(ctx, assetA.ID)
	if err != nil || len(releases) != 2 || releases[0].ID != releaseEarlier.ID || releases[1].ID != releaseLater.ID {
		t.Fatalf("ListReleasesByAsset() = %#v, %v", releases, err)
	}
	variants, err := repository.ListVariantsByRelease(ctx, releaseEarlier.ID)
	if err != nil || len(variants) != 2 || variants[0].ID != variantA.ID || variants[1].ID != variantZ.ID {
		t.Fatalf("ListVariantsByRelease() = %#v, %v", variants, err)
	}
	copies, err := repository.ListCopiesByVariant(ctx, variantA.ID)
	if err != nil || len(copies) != 2 || copies[0].ID != removed.ID || copies[1].ID != present.ID {
		t.Fatalf("ListCopiesByVariant() = %#v, %v", copies, err)
	}
	if copies[0].Availability != catalog.CopyAvailabilityRemoved || copies[1].Availability != catalog.CopyAvailabilityPresent {
		t.Fatalf("Copy availability did not round-trip: %#v", copies)
	}
	for _, resourceCopy := range copies {
		if resourceCopy.VariantID == nil || *resourceCopy.VariantID != variantA.ID || resourceCopy.ID == unresolved.ID || resourceCopy.ID == otherVariant.ID {
			t.Fatalf("logical Copy list leaked unresolved/foreign state: %#v", resourceCopy)
		}
	}
}

func hierarchyCopy(id string, variantID catalog.VariantID, bindingID storage.BindingID, resourceID, availability string, at time.Time) catalog.Copy {
	return catalog.Copy{
		ID: catalog.CopyID(id), VariantID: &variantID,
		IndexCoreRootID: "gate-2-root", IndexCoreResourceID: resourceID,
		StorageBindingID: catalog.StorageBindingID(bindingID), Availability: availability,
		CreatedAt: at, UpdatedAt: at,
	}
}
