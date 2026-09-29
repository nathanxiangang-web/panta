// Package resourceview composes Panta-owned logical Catalog facts. It does not
// implement search, physical observation, storage scanning, or persistence.
package resourceview

import (
	"context"
	"errors"
	"fmt"

	"github.com/nathanxiangang-web/panta/internal/catalog"
)

var (
	ErrInvalidRequest           = errors.New("invalid logical resource request")
	ErrAssetNotFound            = errors.New("logical resource asset not found")
	ErrReleaseNotFound          = errors.New("logical resource release not found")
	ErrVariantNotFound          = errors.New("logical resource variant not found")
	ErrReleaseOwnershipMismatch = errors.New("release does not belong to requested asset")
	ErrVariantOwnershipMismatch = errors.New("variant does not belong to requested release")
	ErrInvalidCatalogState      = errors.New("invalid logical catalog state")
)

// Request selects an Asset and optionally drills down through one Release and
// one Variant. A Variant selection requires an explicit Release selection.
type Request struct {
	AssetID   catalog.AssetID
	ReleaseID *catalog.ReleaseID
	VariantID *catalog.VariantID
}

// Detail is a deterministic logical drill-down. Child slices are always
// non-nil: an empty slice is valid absence, while a missing selected entity is
// returned as a typed error.
type Detail struct {
	Asset           catalog.Asset
	Releases        []catalog.Release
	SelectedRelease *catalog.Release
	Variants        []catalog.Variant
	SelectedVariant *catalog.Variant
	Copies          []catalog.Copy
	HasPresentCopy  bool
}

type Service struct {
	catalog catalog.HierarchyReader
}

func NewService(reader catalog.HierarchyReader) (*Service, error) {
	if reader == nil {
		return nil, ErrInvalidRequest
	}
	return &Service{catalog: reader}, nil
}

// Get composes only logical Catalog state. It validates every caller-supplied
// parent/child relationship and fails closed rather than combining unrelated
// immutable IDs.
func (service *Service) Get(ctx context.Context, request Request) (Detail, error) {
	detail := Detail{
		Releases: make([]catalog.Release, 0),
		Variants: make([]catalog.Variant, 0),
		Copies:   make([]catalog.Copy, 0),
	}
	if request.AssetID == "" || (request.VariantID != nil && request.ReleaseID == nil) {
		return Detail{}, ErrInvalidRequest
	}

	asset, err := service.catalog.GetAsset(ctx, request.AssetID)
	if errors.Is(err, catalog.ErrNotFound) {
		return Detail{}, fmt.Errorf("%w: %s", ErrAssetNotFound, request.AssetID)
	}
	if err != nil {
		return Detail{}, fmt.Errorf("load asset %s: %w", request.AssetID, err)
	}
	detail.Asset = asset
	detail.Releases, err = service.catalog.ListReleasesByAsset(ctx, request.AssetID)
	if err != nil {
		return Detail{}, fmt.Errorf("list releases for asset %s: %w", request.AssetID, err)
	}
	for _, release := range detail.Releases {
		if release.AssetID != request.AssetID {
			return Detail{}, fmt.Errorf("%w: release %s", ErrInvalidCatalogState, release.ID)
		}
	}
	if request.ReleaseID == nil {
		return detail, nil
	}

	release, err := service.catalog.GetRelease(ctx, *request.ReleaseID)
	if errors.Is(err, catalog.ErrNotFound) {
		return Detail{}, fmt.Errorf("%w: %s", ErrReleaseNotFound, *request.ReleaseID)
	}
	if err != nil {
		return Detail{}, fmt.Errorf("load release %s: %w", *request.ReleaseID, err)
	}
	if release.AssetID != request.AssetID {
		return Detail{}, fmt.Errorf("%w: release %s asset %s", ErrReleaseOwnershipMismatch, release.ID, release.AssetID)
	}
	detail.SelectedRelease = &release
	detail.Variants, err = service.catalog.ListVariantsByRelease(ctx, release.ID)
	if err != nil {
		return Detail{}, fmt.Errorf("list variants for release %s: %w", release.ID, err)
	}
	for _, variant := range detail.Variants {
		if variant.ReleaseID != release.ID {
			return Detail{}, fmt.Errorf("%w: variant %s", ErrInvalidCatalogState, variant.ID)
		}
	}
	if request.VariantID == nil {
		return detail, nil
	}

	variant, err := service.catalog.GetVariant(ctx, *request.VariantID)
	if errors.Is(err, catalog.ErrNotFound) {
		return Detail{}, fmt.Errorf("%w: %s", ErrVariantNotFound, *request.VariantID)
	}
	if err != nil {
		return Detail{}, fmt.Errorf("load variant %s: %w", *request.VariantID, err)
	}
	if variant.ReleaseID != release.ID {
		return Detail{}, fmt.Errorf("%w: variant %s release %s", ErrVariantOwnershipMismatch, variant.ID, variant.ReleaseID)
	}
	detail.SelectedVariant = &variant
	detail.Copies, err = service.catalog.ListCopiesByVariant(ctx, variant.ID)
	if err != nil {
		return Detail{}, fmt.Errorf("list copies for variant %s: %w", variant.ID, err)
	}
	for _, resourceCopy := range detail.Copies {
		if resourceCopy.VariantID == nil || *resourceCopy.VariantID != variant.ID {
			return Detail{}, fmt.Errorf("%w: copy %s", ErrInvalidCatalogState, resourceCopy.ID)
		}
		if resourceCopy.Availability == catalog.CopyAvailabilityPresent {
			detail.HasPresentCopy = true
		}
	}
	return detail, nil
}
