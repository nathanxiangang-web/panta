package resourceview

import (
	"context"
	"errors"
	"fmt"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
)

// ClassificationState describes only Panta's optional Catalog attachment. It
// does not replace or reinterpret IndexCore's physical presence state.
type ClassificationState string

const (
	ClassificationPhysicalOnly   ClassificationState = "PHYSICAL_ONLY"
	ClassificationUnresolvedCopy ClassificationState = "UNRESOLVED_COPY"
	ClassificationClassified     ClassificationState = "CLASSIFIED"
)

// PhysicalRequest identifies the known IndexCore root and canonical path sent
// to Q5 Resolve. Visibility is forwarded without modification.
type PhysicalRequest struct {
	RootID string
	Path   string
	indexcore.ReadVisibility
}

// PhysicalMatch preserves one Q5 match and, when available, its complete and
// validated Panta Catalog lineage.
type PhysicalMatch struct {
	Physical       indexcore.ResourceContext
	Classification ClassificationState
	Copy           *catalog.Copy
	Variant        *catalog.Variant
	Release        *catalog.Release
	Asset          *catalog.Asset
}

// PhysicalDetail preserves every Q5 match and IndexCore's ambiguity decision.
type PhysicalDetail struct {
	Matches   []PhysicalMatch
	Ambiguous bool
}

// PhysicalCatalogReader is deliberately narrower than the Catalog repository:
// the physical read path cannot create or mutate logical entities.
type PhysicalCatalogReader interface {
	catalog.PhysicalIdentityReader
	GetVariant(context.Context, catalog.VariantID) (catalog.Variant, error)
	GetRelease(context.Context, catalog.ReleaseID) (catalog.Release, error)
	GetAsset(context.Context, catalog.AssetID) (catalog.Asset, error)
}

type PhysicalService struct {
	index   indexcore.ResolvePort
	catalog PhysicalCatalogReader
}

func NewPhysicalService(index indexcore.ResolvePort, reader PhysicalCatalogReader) (*PhysicalService, error) {
	if index == nil || reader == nil {
		return nil, ErrInvalidRequest
	}
	return &PhysicalService{index: index, catalog: reader}, nil
}

// Resolve starts from IndexCore Q5 physical truth and optionally attaches a
// Catalog lineage. Missing Copies are normal. Once a Copy claims a Variant,
// however, an incomplete or inconsistent lineage is rejected rather than
// silently downgraded to a less classified state.
func (service *PhysicalService) Resolve(ctx context.Context, request PhysicalRequest) (PhysicalDetail, error) {
	if request.RootID == "" || request.Path == "" {
		return PhysicalDetail{}, ErrInvalidRequest
	}

	resolved, err := service.index.Resolve(ctx, indexcore.ResolveRequest{
		RootID:         request.RootID,
		Path:           request.Path,
		ReadVisibility: request.ReadVisibility,
	})
	if err != nil {
		return PhysicalDetail{}, fmt.Errorf("resolve physical resource: %w", err)
	}

	detail := PhysicalDetail{
		Matches:   make([]PhysicalMatch, 0, len(resolved.Matches)),
		Ambiguous: resolved.Ambiguous,
	}
	for _, physical := range resolved.Matches {
		match, err := service.attachCatalog(ctx, physical)
		if err != nil {
			return PhysicalDetail{}, err
		}
		detail.Matches = append(detail.Matches, match)
	}
	return detail, nil
}

func (service *PhysicalService) attachCatalog(ctx context.Context, physical indexcore.ResourceContext) (PhysicalMatch, error) {
	match := PhysicalMatch{Physical: physical, Classification: ClassificationPhysicalOnly}
	resourceCopy, err := service.catalog.GetCopyByPhysicalIdentity(ctx, physical.RootID, physical.ResourceID)
	if errors.Is(err, catalog.ErrNotFound) {
		return match, nil
	}
	if err != nil {
		return PhysicalMatch{}, fmt.Errorf("load Copy for physical identity %s/%s: %w", physical.RootID, physical.ResourceID, err)
	}
	match.Copy = &resourceCopy
	if resourceCopy.IndexCoreRootID != physical.RootID || resourceCopy.IndexCoreResourceID != physical.ResourceID {
		return PhysicalMatch{}, invalidCatalogState("Copy %s physical identity mismatch", resourceCopy.ID)
	}
	if resourceCopy.VariantID == nil {
		match.Classification = ClassificationUnresolvedCopy
		return match, nil
	}

	variant, err := service.catalog.GetVariant(ctx, *resourceCopy.VariantID)
	if err != nil {
		return PhysicalMatch{}, lineageReadError("Variant", string(*resourceCopy.VariantID), err)
	}
	if variant.ID != *resourceCopy.VariantID {
		return PhysicalMatch{}, invalidCatalogState("Copy %s points to Variant %s but reader returned %s", resourceCopy.ID, *resourceCopy.VariantID, variant.ID)
	}
	release, err := service.catalog.GetRelease(ctx, variant.ReleaseID)
	if err != nil {
		return PhysicalMatch{}, lineageReadError("Release", string(variant.ReleaseID), err)
	}
	if release.ID != variant.ReleaseID {
		return PhysicalMatch{}, invalidCatalogState("Variant %s points to Release %s but reader returned %s", variant.ID, variant.ReleaseID, release.ID)
	}
	asset, err := service.catalog.GetAsset(ctx, release.AssetID)
	if err != nil {
		return PhysicalMatch{}, lineageReadError("Asset", string(release.AssetID), err)
	}
	if asset.ID != release.AssetID {
		return PhysicalMatch{}, invalidCatalogState("Release %s points to Asset %s but reader returned %s", release.ID, release.AssetID, asset.ID)
	}

	match.Classification = ClassificationClassified
	match.Variant = &variant
	match.Release = &release
	match.Asset = &asset
	return match, nil
}

func lineageReadError(entity, id string, err error) error {
	if errors.Is(err, catalog.ErrNotFound) {
		return invalidCatalogState("classified %s %s is missing", entity, id)
	}
	return fmt.Errorf("load classified %s %s: %w", entity, id, err)
}

func invalidCatalogState(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidCatalogState, fmt.Sprintf(format, args...))
}
