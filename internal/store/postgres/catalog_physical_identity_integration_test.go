package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestPostgresCopyLookupByExactPhysicalIdentity(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	if _, err := migrator.Apply(ctx); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	bindingID := storage.BindingID("19000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "root-a")
	repository, err := NewCatalogRepository(pool)
	if err != nil {
		t.Fatalf("NewCatalogRepository() error = %v", err)
	}

	now := time.Date(2026, 9, 29, 8, 0, 0, 0, time.UTC)
	asset := catalog.Asset{ID: "19000000-0000-4000-8000-000000000010", CanonicalName: "A", Category: "software", Status: "ACTIVE", CreatedAt: now, UpdatedAt: now}
	release := catalog.Release{ID: "19000000-0000-4000-8000-000000000020", AssetID: asset.ID, VersionRaw: "1", VersionScheme: catalog.VersionSchemeRevision, Channel: "stable", Status: "ACTIVE", CreatedAt: now, UpdatedAt: now}
	variant := catalog.Variant{ID: "19000000-0000-4000-8000-000000000030", ReleaseID: release.ID, VariantKey: "default", Attributes: []byte(`{}`), Status: "ACTIVE", CreatedAt: now, UpdatedAt: now}
	if err := repository.CreateAsset(ctx, asset); err != nil {
		t.Fatalf("CreateAsset() error = %v", err)
	}
	if err := repository.CreateRelease(ctx, release); err != nil {
		t.Fatalf("CreateRelease() error = %v", err)
	}
	if err := repository.CreateVariant(ctx, variant); err != nil {
		t.Fatalf("CreateVariant() error = %v", err)
	}

	unresolved := catalog.Copy{
		ID: "19000000-0000-4000-8000-000000000040", IndexCoreRootID: "root-a", IndexCoreResourceID: "unresolved",
		StorageBindingID: catalog.StorageBindingID(bindingID), Availability: catalog.CopyAvailabilityPresent, CreatedAt: now, UpdatedAt: now,
	}
	classified := catalog.Copy{
		ID: "19000000-0000-4000-8000-000000000041", VariantID: &variant.ID,
		IndexCoreRootID: "root-a", IndexCoreResourceID: "classified",
		StorageBindingID: catalog.StorageBindingID(bindingID), Availability: catalog.CopyAvailabilityRemoved, CreatedAt: now, UpdatedAt: now,
	}
	for _, resourceCopy := range []catalog.Copy{unresolved, classified} {
		if err := repository.CreateCopy(ctx, resourceCopy); err != nil {
			t.Fatalf("CreateCopy(%s) error = %v", resourceCopy.ID, err)
		}
	}

	gotUnresolved, err := repository.GetCopyByPhysicalIdentity(ctx, "root-a", "unresolved")
	if err != nil || gotUnresolved.ID != unresolved.ID || gotUnresolved.VariantID != nil {
		t.Fatalf("unresolved lookup = %#v, %v", gotUnresolved, err)
	}
	gotClassified, err := repository.GetCopyByPhysicalIdentity(ctx, "root-a", "classified")
	if err != nil || gotClassified.ID != classified.ID || gotClassified.VariantID == nil || *gotClassified.VariantID != variant.ID || gotClassified.Availability != catalog.CopyAvailabilityRemoved {
		t.Fatalf("classified lookup = %#v, %v", gotClassified, err)
	}
	for _, identity := range [][2]string{{"root-a", "missing"}, {"root-other", "classified"}} {
		if _, err := repository.GetCopyByPhysicalIdentity(ctx, identity[0], identity[1]); !errors.Is(err, catalog.ErrNotFound) {
			t.Fatalf("missing lookup %v error = %v, want catalog.ErrNotFound", identity, err)
		}
	}

	duplicate := classified
	duplicate.ID = "19000000-0000-4000-8000-000000000042"
	if err := repository.CreateCopy(ctx, duplicate); err == nil {
		t.Fatal("CreateCopy() accepted duplicate (indexcore_root_id, indexcore_resource_id)")
	}
}
