package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/projector"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestPostgresCopyClassificationIsMonotonicConcurrentAndProjectorSafe(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 6 || status.LatestVersion != 6 {
		t.Fatalf("Apply() = %#v, %v", status, err)
	}

	bindingID := storage.BindingID("21000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "classification-root")
	repository, err := NewCatalogRepository(pool)
	if err != nil {
		t.Fatalf("NewCatalogRepository() error = %v", err)
	}
	service, err := catalog.NewClassificationService(repository)
	if err != nil {
		t.Fatalf("NewClassificationService() error = %v", err)
	}

	createdAt := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	asset := catalog.Asset{ID: "21000000-0000-4000-8000-000000000010", CanonicalName: "classification", Category: "software", Status: "ACTIVE", CreatedAt: createdAt, UpdatedAt: createdAt}
	release := catalog.Release{ID: "21000000-0000-4000-8000-000000000020", AssetID: asset.ID, VersionRaw: "1", VersionScheme: catalog.VersionSchemeRevision, Channel: "stable", Status: "ACTIVE", CreatedAt: createdAt, UpdatedAt: createdAt}
	variantA := catalog.Variant{ID: "21000000-0000-4000-8000-000000000030", ReleaseID: release.ID, VariantKey: "a", Attributes: []byte(`{}`), Status: "ACTIVE", CreatedAt: createdAt, UpdatedAt: createdAt}
	variantB := catalog.Variant{ID: "21000000-0000-4000-8000-000000000031", ReleaseID: release.ID, VariantKey: "b", Attributes: []byte(`{}`), Status: "ACTIVE", CreatedAt: createdAt, UpdatedAt: createdAt}
	if err := repository.CreateAsset(ctx, asset); err != nil {
		t.Fatalf("CreateAsset() error = %v", err)
	}
	if err := repository.CreateRelease(ctx, release); err != nil {
		t.Fatalf("CreateRelease() error = %v", err)
	}
	for _, variant := range []catalog.Variant{variantA, variantB} {
		if err := repository.CreateVariant(ctx, variant); err != nil {
			t.Fatalf("CreateVariant(%s) error = %v", variant.ID, err)
		}
	}

	baseCopy := classificationCopy("21000000-0000-4000-8000-000000000040", bindingID, "base", catalog.CopyAvailabilityRemoved, createdAt)
	competingCopy := classificationCopy("21000000-0000-4000-8000-000000000041", bindingID, "different-target", catalog.CopyAvailabilityPresent, createdAt)
	replayCopy := classificationCopy("21000000-0000-4000-8000-000000000042", bindingID, "same-target", catalog.CopyAvailabilityPresent, createdAt)
	projectedCopy := classificationCopy("21000000-0000-4000-8000-000000000043", bindingID, "projector", catalog.CopyAvailabilityPresent, createdAt)
	for _, resourceCopy := range []catalog.Copy{baseCopy, competingCopy, replayCopy, projectedCopy} {
		if err := repository.CreateCopy(ctx, resourceCopy); err != nil {
			t.Fatalf("CreateCopy(%s) error = %v", resourceCopy.ID, err)
		}
	}

	first, err := service.Bind(ctx, catalog.BindCopyRequest{CopyID: baseCopy.ID, VariantID: variantA.ID})
	if err != nil {
		t.Fatalf("Bind(first) error = %v", err)
	}
	if !first.Changed || first.Copy.VariantID == nil || *first.Copy.VariantID != variantA.ID {
		t.Fatalf("Bind(first) = %#v", first)
	}
	assertClassificationPreserved(t, first.Copy, baseCopy)

	replay, err := service.Bind(ctx, catalog.BindCopyRequest{CopyID: baseCopy.ID, VariantID: variantA.ID})
	if err != nil || replay.Changed {
		t.Fatalf("Bind(replay) = %#v, %v", replay, err)
	}
	if !replay.Copy.UpdatedAt.Equal(first.Copy.UpdatedAt) {
		t.Fatalf("same-target replay rewrote updated_at: first=%s replay=%s", first.Copy.UpdatedAt, replay.Copy.UpdatedAt)
	}
	assertClassificationPreserved(t, replay.Copy, baseCopy)
	if _, err := service.Bind(ctx, catalog.BindCopyRequest{CopyID: baseCopy.ID, VariantID: variantB.ID}); !errors.Is(err, catalog.ErrCopyAlreadyClassified) {
		t.Fatalf("Bind(different target) error = %v", err)
	}
	assertDurableVariant(t, ctx, repository, baseCopy.ID, variantA.ID)

	missingCopyID := catalog.CopyID("21000000-0000-4000-8000-000000000099")
	if _, err := service.Bind(ctx, catalog.BindCopyRequest{CopyID: missingCopyID, VariantID: variantA.ID}); !errors.Is(err, catalog.ErrClassificationCopyNotFound) {
		t.Fatalf("Bind(missing Copy) error = %v", err)
	}
	missingVariantID := catalog.VariantID("21000000-0000-4000-8000-000000000098")
	if _, err := service.Bind(ctx, catalog.BindCopyRequest{CopyID: competingCopy.ID, VariantID: missingVariantID}); !errors.Is(err, catalog.ErrClassificationVariantNotFound) {
		t.Fatalf("Bind(missing Variant) error = %v", err)
	}
	if durable, err := repository.GetCopy(ctx, competingCopy.ID); err != nil || durable.VariantID != nil {
		t.Fatalf("Copy changed after missing Variant: %#v, %v", durable, err)
	}

	differentResults := runConcurrentBindings(ctx, service, competingCopy.ID, variantA.ID, variantB.ID)
	changed, conflicts := 0, 0
	var winner catalog.VariantID
	for _, result := range differentResults {
		switch {
		case result.err == nil && result.result.Changed:
			changed++
			winner = *result.result.Copy.VariantID
		case errors.Is(result.err, catalog.ErrCopyAlreadyClassified):
			conflicts++
		default:
			t.Fatalf("different-target concurrent result = %#v", result)
		}
	}
	if changed != 1 || conflicts != 1 {
		t.Fatalf("different-target outcomes: changed=%d conflicts=%d results=%#v", changed, conflicts, differentResults)
	}
	assertDurableVariant(t, ctx, repository, competingCopy.ID, winner)

	sameResults := runConcurrentBindings(ctx, service, replayCopy.ID, variantA.ID, variantA.ID)
	changed, unchanged := 0, 0
	for _, result := range sameResults {
		if result.err != nil {
			t.Fatalf("same-target concurrent error = %v", result.err)
		}
		if result.result.Changed {
			changed++
		} else {
			unchanged++
		}
	}
	if changed != 1 || unchanged != 1 {
		t.Fatalf("same-target outcomes: changed=%d unchanged=%d results=%#v", changed, unchanged, sameResults)
	}
	assertDurableVariant(t, ctx, repository, replayCopy.ID, variantA.ID)

	if _, err := service.Bind(ctx, catalog.BindCopyRequest{CopyID: projectedCopy.ID, VariantID: variantB.ID}); err != nil {
		t.Fatalf("Bind(projector Copy) error = %v", err)
	}
	projectionStore, err := NewProjectionStore(pool)
	if err != nil {
		t.Fatalf("NewProjectionStore() error = %v", err)
	}
	if err := projectionStore.ApplyBatch(ctx, projector.Batch{
		StorageBindingID: bindingID, ExpectedCursor: 0, LastEventSeq: 1,
		Mutations: []projector.Mutation{{
			CopyID: "21000000-0000-4000-8000-000000000044", IndexCoreRootID: projectedCopy.IndexCoreRootID,
			IndexCoreResourceID: projectedCopy.IndexCoreResourceID, StorageBindingID: bindingID,
			Availability: projector.AvailabilityRemoved,
		}},
	}); err != nil {
		t.Fatalf("ApplyBatch(REMOVED) error = %v", err)
	}
	afterProjection, err := repository.GetCopy(ctx, projectedCopy.ID)
	if err != nil || afterProjection.VariantID == nil || *afterProjection.VariantID != variantB.ID || afterProjection.Availability != catalog.CopyAvailabilityRemoved {
		t.Fatalf("Copy after projector update = %#v, %v", afterProjection, err)
	}
	assertCopyIdentityPreserved(t, afterProjection, projectedCopy)
}

type bindingOutcome struct {
	result catalog.BindCopyResult
	err    error
}

func runConcurrentBindings(ctx context.Context, service *catalog.ClassificationService, copyID catalog.CopyID, targets ...catalog.VariantID) []bindingOutcome {
	start := make(chan struct{})
	results := make(chan bindingOutcome, len(targets))
	for _, target := range targets {
		target := target
		go func() {
			<-start
			result, err := service.Bind(ctx, catalog.BindCopyRequest{CopyID: copyID, VariantID: target})
			results <- bindingOutcome{result: result, err: err}
		}()
	}
	close(start)
	outcomes := make([]bindingOutcome, 0, len(targets))
	for range targets {
		outcomes = append(outcomes, <-results)
	}
	return outcomes
}

func classificationCopy(id string, bindingID storage.BindingID, resourceID, availability string, at time.Time) catalog.Copy {
	return catalog.Copy{
		ID: catalog.CopyID(id), IndexCoreRootID: "classification-root", IndexCoreResourceID: resourceID,
		StorageBindingID: catalog.StorageBindingID(bindingID), Availability: availability,
		CreatedAt: at, UpdatedAt: at,
	}
}

func assertClassificationPreserved(t *testing.T, got, original catalog.Copy) {
	t.Helper()
	assertCopyIdentityPreserved(t, got, original)
	if got.Availability != original.Availability {
		t.Fatalf("classification changed availability: got=%q original=%q", got.Availability, original.Availability)
	}
}

func assertCopyIdentityPreserved(t *testing.T, got, original catalog.Copy) {
	t.Helper()
	if got.ID != original.ID || got.IndexCoreRootID != original.IndexCoreRootID ||
		got.IndexCoreResourceID != original.IndexCoreResourceID || got.StorageBindingID != original.StorageBindingID ||
		!got.CreatedAt.Equal(original.CreatedAt) {
		t.Fatalf("classification changed physical Copy facts: got=%#v original=%#v", got, original)
	}
}

func assertDurableVariant(t *testing.T, ctx context.Context, repository *CatalogRepository, copyID catalog.CopyID, variantID catalog.VariantID) {
	t.Helper()
	resourceCopy, err := repository.GetCopy(ctx, copyID)
	if err != nil || resourceCopy.VariantID == nil || *resourceCopy.VariantID != variantID {
		t.Fatalf("durable Copy %s = %#v, %v; want Variant %s", copyID, resourceCopy, err, variantID)
	}
}
