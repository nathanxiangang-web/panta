package catalog_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/catalog"
)

func TestClassificationRejectsInvalidRequests(t *testing.T) {
	if _, err := catalog.NewClassificationService(nil); !errors.Is(err, catalog.ErrInvalidClassification) {
		t.Fatalf("NewClassificationService(nil) error = %v", err)
	}
	store := newMemoryBinder()
	service := newClassificationService(t, store)
	for _, request := range []catalog.BindCopyRequest{
		{}, {CopyID: "copy-a"}, {VariantID: "variant-a"},
	} {
		if _, err := service.Bind(context.Background(), request); !errors.Is(err, catalog.ErrInvalidClassification) {
			t.Fatalf("Bind(%#v) error = %v", request, err)
		}
	}
	if store.calls != 0 {
		t.Fatalf("invalid requests reached persistence %d times", store.calls)
	}
}

func TestClassificationReportsMissingCopyAndVariant(t *testing.T) {
	store := newMemoryBinder()
	store.variants["variant-a"] = struct{}{}
	service := newClassificationService(t, store)
	if _, err := service.Bind(context.Background(), catalog.BindCopyRequest{CopyID: "missing", VariantID: "variant-a"}); !errors.Is(err, catalog.ErrClassificationCopyNotFound) {
		t.Fatalf("missing Copy error = %v", err)
	}
	store.copies["copy-a"] = catalog.Copy{ID: "copy-a"}
	if _, err := service.Bind(context.Background(), catalog.BindCopyRequest{CopyID: "copy-a", VariantID: "missing"}); !errors.Is(err, catalog.ErrClassificationVariantNotFound) {
		t.Fatalf("missing Variant error = %v", err)
	}
}

func TestClassificationIsMonotonicIdempotentAndPreservesCopyFacts(t *testing.T) {
	createdAt := time.Date(2026, 9, 29, 9, 0, 0, 0, time.UTC)
	store := newMemoryBinder()
	store.variants["variant-a"] = struct{}{}
	store.variants["variant-b"] = struct{}{}
	original := catalog.Copy{
		ID: "copy-a", IndexCoreRootID: "root-a", IndexCoreResourceID: "resource-a",
		StorageBindingID: "binding-a", Availability: catalog.CopyAvailabilityRemoved,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	store.copies[original.ID] = original
	service := newClassificationService(t, store)

	first, err := service.Bind(context.Background(), catalog.BindCopyRequest{CopyID: original.ID, VariantID: "variant-a"})
	if err != nil {
		t.Fatalf("first Bind() error = %v", err)
	}
	if !first.Changed || first.Copy.VariantID == nil || *first.Copy.VariantID != "variant-a" {
		t.Fatalf("first Bind() = %#v", first)
	}
	assertPreservedCopyFacts(t, first.Copy, original)

	replay, err := service.Bind(context.Background(), catalog.BindCopyRequest{CopyID: original.ID, VariantID: "variant-a"})
	if err != nil {
		t.Fatalf("same-target replay error = %v", err)
	}
	if replay.Changed || replay.Copy.VariantID == nil || *replay.Copy.VariantID != "variant-a" {
		t.Fatalf("same-target replay = %#v", replay)
	}
	assertPreservedCopyFacts(t, replay.Copy, original)

	if _, err := service.Bind(context.Background(), catalog.BindCopyRequest{CopyID: original.ID, VariantID: "variant-b"}); !errors.Is(err, catalog.ErrCopyAlreadyClassified) {
		t.Fatalf("different-target Bind() error = %v", err)
	}
	durable := store.copies[original.ID]
	if durable.VariantID == nil || *durable.VariantID != "variant-a" {
		t.Fatalf("different-target attempt mutated Copy: %#v", durable)
	}
}

func assertPreservedCopyFacts(t *testing.T, got, original catalog.Copy) {
	t.Helper()
	if got.ID != original.ID || got.IndexCoreRootID != original.IndexCoreRootID ||
		got.IndexCoreResourceID != original.IndexCoreResourceID || got.StorageBindingID != original.StorageBindingID ||
		got.Availability != original.Availability || !got.CreatedAt.Equal(original.CreatedAt) {
		t.Fatalf("Copy facts changed: got=%#v original=%#v", got, original)
	}
}

type memoryBinder struct {
	copies   map[catalog.CopyID]catalog.Copy
	variants map[catalog.VariantID]struct{}
	calls    int
}

func newMemoryBinder() *memoryBinder {
	return &memoryBinder{copies: make(map[catalog.CopyID]catalog.Copy), variants: make(map[catalog.VariantID]struct{})}
}

func (store *memoryBinder) BindCopyToVariant(_ context.Context, copyID catalog.CopyID, variantID catalog.VariantID) (catalog.BindCopyResult, error) {
	store.calls++
	resourceCopy, ok := store.copies[copyID]
	if !ok {
		return catalog.BindCopyResult{}, catalog.ErrClassificationCopyNotFound
	}
	if resourceCopy.VariantID != nil {
		if *resourceCopy.VariantID != variantID {
			return catalog.BindCopyResult{}, catalog.ErrCopyAlreadyClassified
		}
		return catalog.BindCopyResult{Copy: resourceCopy}, nil
	}
	if _, ok := store.variants[variantID]; !ok {
		return catalog.BindCopyResult{}, catalog.ErrClassificationVariantNotFound
	}
	resourceCopy.VariantID = &variantID
	resourceCopy.UpdatedAt = resourceCopy.UpdatedAt.Add(time.Second)
	store.copies[copyID] = resourceCopy
	return catalog.BindCopyResult{Copy: resourceCopy, Changed: true}, nil
}

func newClassificationService(t *testing.T, binder catalog.CopyBinder) *catalog.ClassificationService {
	t.Helper()
	service, err := catalog.NewClassificationService(binder)
	if err != nil {
		t.Fatalf("NewClassificationService() error = %v", err)
	}
	return service
}
