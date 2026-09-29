package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nathanxiangang-web/panta/internal/catalog"
)

type catalogDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type CatalogRepository struct {
	db catalogDB
}

var _ catalog.Repository = (*CatalogRepository)(nil)

func NewCatalogRepository(db catalogDB) (*CatalogRepository, error) {
	if db == nil {
		return nil, errors.New("catalog database is required")
	}
	return &CatalogRepository{db: db}, nil
}

func (repository *CatalogRepository) CreateAsset(ctx context.Context, asset catalog.Asset) error {
	_, err := repository.db.Exec(ctx, `
INSERT INTO assets (asset_id, canonical_name, category, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6)`,
		string(asset.ID), asset.CanonicalName, asset.Category, asset.Status, asset.CreatedAt, asset.UpdatedAt,
	)
	return wrapWriteError("create asset", err)
}

func (repository *CatalogRepository) GetAsset(ctx context.Context, id catalog.AssetID) (catalog.Asset, error) {
	var asset catalog.Asset
	var assetID string
	err := repository.db.QueryRow(ctx, `
SELECT asset_id::text, canonical_name, category, status, created_at, updated_at
FROM assets WHERE asset_id = $1`, string(id)).Scan(
		&assetID, &asset.CanonicalName, &asset.Category, &asset.Status, &asset.CreatedAt, &asset.UpdatedAt,
	)
	if err != nil {
		return catalog.Asset{}, wrapReadError("asset", string(id), err)
	}
	asset.ID = catalog.AssetID(assetID)
	return asset, nil
}

func (repository *CatalogRepository) CreateRelease(ctx context.Context, release catalog.Release) error {
	_, err := repository.db.Exec(ctx, `
INSERT INTO releases (
    release_id, asset_id, version_raw, version_normalized, version_scheme,
    channel, release_date, source_ref, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		string(release.ID), string(release.AssetID), release.VersionRaw, release.VersionNormalized,
		string(release.VersionScheme), release.Channel, release.ReleaseDate, release.SourceRef,
		release.Status, release.CreatedAt, release.UpdatedAt,
	)
	return wrapWriteError("create release", err)
}

func (repository *CatalogRepository) GetRelease(ctx context.Context, id catalog.ReleaseID) (catalog.Release, error) {
	var release catalog.Release
	var releaseID, assetID string
	var releaseDate sql.NullTime
	err := repository.db.QueryRow(ctx, `
SELECT release_id::text, asset_id::text, version_raw, version_normalized,
       version_scheme, channel, release_date, source_ref, status, created_at, updated_at
FROM releases WHERE release_id = $1`, string(id)).Scan(
		&releaseID, &assetID, &release.VersionRaw, &release.VersionNormalized,
		&release.VersionScheme, &release.Channel, &releaseDate, &release.SourceRef,
		&release.Status, &release.CreatedAt, &release.UpdatedAt,
	)
	if err != nil {
		return catalog.Release{}, wrapReadError("release", string(id), err)
	}
	release.ID = catalog.ReleaseID(releaseID)
	release.AssetID = catalog.AssetID(assetID)
	if releaseDate.Valid {
		value := releaseDate.Time
		release.ReleaseDate = &value
	}
	return release, nil
}

// ListReleasesByAsset returns only direct children of assetID. Ordering is
// stable across calls: creation time first, then immutable ID as a tie-breaker.
func (repository *CatalogRepository) ListReleasesByAsset(ctx context.Context, assetID catalog.AssetID) ([]catalog.Release, error) {
	rows, err := repository.db.Query(ctx, `
SELECT release_id::text, asset_id::text, version_raw, version_normalized,
       version_scheme, channel, release_date, source_ref, status, created_at, updated_at
FROM releases
WHERE asset_id = $1
ORDER BY created_at, release_id`, string(assetID))
	if err != nil {
		return nil, fmt.Errorf("list releases for asset %s: %w", assetID, err)
	}
	defer rows.Close()

	releases := make([]catalog.Release, 0)
	for rows.Next() {
		var release catalog.Release
		var releaseID, storedAssetID string
		var releaseDate sql.NullTime
		if err := rows.Scan(
			&releaseID, &storedAssetID, &release.VersionRaw, &release.VersionNormalized,
			&release.VersionScheme, &release.Channel, &releaseDate, &release.SourceRef,
			&release.Status, &release.CreatedAt, &release.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan release for asset %s: %w", assetID, err)
		}
		release.ID = catalog.ReleaseID(releaseID)
		release.AssetID = catalog.AssetID(storedAssetID)
		if releaseDate.Valid {
			value := releaseDate.Time
			release.ReleaseDate = &value
		}
		releases = append(releases, release)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate releases for asset %s: %w", assetID, err)
	}
	return releases, nil
}

func (repository *CatalogRepository) CreateVariant(ctx context.Context, variant catalog.Variant) error {
	_, err := repository.db.Exec(ctx, `
INSERT INTO variants (variant_id, release_id, variant_key, attributes, status, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		string(variant.ID), string(variant.ReleaseID), variant.VariantKey, string(variant.Attributes),
		variant.Status, variant.CreatedAt, variant.UpdatedAt,
	)
	return wrapWriteError("create variant", err)
}

func (repository *CatalogRepository) GetVariant(ctx context.Context, id catalog.VariantID) (catalog.Variant, error) {
	var variant catalog.Variant
	var variantID, releaseID string
	var attributes []byte
	err := repository.db.QueryRow(ctx, `
SELECT variant_id::text, release_id::text, variant_key, attributes, status, created_at, updated_at
FROM variants WHERE variant_id = $1`, string(id)).Scan(
		&variantID, &releaseID, &variant.VariantKey, &attributes,
		&variant.Status, &variant.CreatedAt, &variant.UpdatedAt,
	)
	if err != nil {
		return catalog.Variant{}, wrapReadError("variant", string(id), err)
	}
	variant.ID = catalog.VariantID(variantID)
	variant.ReleaseID = catalog.ReleaseID(releaseID)
	variant.Attributes = attributes
	return variant, nil
}

// ListVariantsByRelease returns direct children ordered by their stable product
// key and immutable ID. It never combines variants from another Release.
func (repository *CatalogRepository) ListVariantsByRelease(ctx context.Context, releaseID catalog.ReleaseID) ([]catalog.Variant, error) {
	rows, err := repository.db.Query(ctx, `
SELECT variant_id::text, release_id::text, variant_key, attributes, status, created_at, updated_at
FROM variants
WHERE release_id = $1
ORDER BY variant_key, variant_id`, string(releaseID))
	if err != nil {
		return nil, fmt.Errorf("list variants for release %s: %w", releaseID, err)
	}
	defer rows.Close()

	variants := make([]catalog.Variant, 0)
	for rows.Next() {
		var variant catalog.Variant
		var variantID, storedReleaseID string
		var attributes []byte
		if err := rows.Scan(
			&variantID, &storedReleaseID, &variant.VariantKey, &attributes,
			&variant.Status, &variant.CreatedAt, &variant.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan variant for release %s: %w", releaseID, err)
		}
		variant.ID = catalog.VariantID(variantID)
		variant.ReleaseID = catalog.ReleaseID(storedReleaseID)
		variant.Attributes = attributes
		variants = append(variants, variant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate variants for release %s: %w", releaseID, err)
	}
	return variants, nil
}

func (repository *CatalogRepository) CreateCopy(ctx context.Context, resourceCopy catalog.Copy) error {
	var variantID *string
	if resourceCopy.VariantID != nil {
		value := string(*resourceCopy.VariantID)
		variantID = &value
	}
	_, err := repository.db.Exec(ctx, `
INSERT INTO copies (
    copy_id, variant_id, indexcore_root_id, indexcore_resource_id,
    storage_binding_id, availability, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		string(resourceCopy.ID), variantID, resourceCopy.IndexCoreRootID, resourceCopy.IndexCoreResourceID,
		string(resourceCopy.StorageBindingID), resourceCopy.Availability,
		resourceCopy.CreatedAt, resourceCopy.UpdatedAt,
	)
	return wrapWriteError("create copy", err)
}

func (repository *CatalogRepository) GetCopy(ctx context.Context, id catalog.CopyID) (catalog.Copy, error) {
	var resourceCopy catalog.Copy
	var copyID, storageBindingID string
	var variantID sql.NullString
	err := repository.db.QueryRow(ctx, `
SELECT copy_id::text, variant_id::text, indexcore_root_id, indexcore_resource_id,
       storage_binding_id::text, availability, created_at, updated_at
FROM copies WHERE copy_id = $1`, string(id)).Scan(
		&copyID, &variantID, &resourceCopy.IndexCoreRootID, &resourceCopy.IndexCoreResourceID,
		&storageBindingID, &resourceCopy.Availability, &resourceCopy.CreatedAt, &resourceCopy.UpdatedAt,
	)
	if err != nil {
		return catalog.Copy{}, wrapReadError("copy", string(id), err)
	}
	resourceCopy.ID = catalog.CopyID(copyID)
	resourceCopy.StorageBindingID = catalog.StorageBindingID(storageBindingID)
	if variantID.Valid {
		value := catalog.VariantID(variantID.String)
		resourceCopy.VariantID = &value
	}
	return resourceCopy, nil
}

// GetCopyByPhysicalIdentity performs an exact lookup by the canonical identity
// supplied by IndexCore. It never falls back to path, name, or partial matching.
func (repository *CatalogRepository) GetCopyByPhysicalIdentity(ctx context.Context, rootID, resourceID string) (catalog.Copy, error) {
	var resourceCopy catalog.Copy
	var copyID, storageBindingID string
	var variantID sql.NullString
	err := repository.db.QueryRow(ctx, `
SELECT copy_id::text, variant_id::text, indexcore_root_id, indexcore_resource_id,
       storage_binding_id::text, availability, created_at, updated_at
FROM copies
WHERE indexcore_root_id = $1 AND indexcore_resource_id = $2`, rootID, resourceID).Scan(
		&copyID, &variantID, &resourceCopy.IndexCoreRootID, &resourceCopy.IndexCoreResourceID,
		&storageBindingID, &resourceCopy.Availability, &resourceCopy.CreatedAt, &resourceCopy.UpdatedAt,
	)
	if err != nil {
		return catalog.Copy{}, wrapReadError("copy physical identity", rootID+"/"+resourceID, err)
	}
	resourceCopy.ID = catalog.CopyID(copyID)
	resourceCopy.StorageBindingID = catalog.StorageBindingID(storageBindingID)
	if variantID.Valid {
		value := catalog.VariantID(variantID.String)
		resourceCopy.VariantID = &value
	}
	return resourceCopy, nil
}

// ListCopiesByVariant returns classified Copies only. Unresolved physical
// Copies have variant_id NULL and remain outside this logical hierarchy.
func (repository *CatalogRepository) ListCopiesByVariant(ctx context.Context, variantID catalog.VariantID) ([]catalog.Copy, error) {
	rows, err := repository.db.Query(ctx, `
SELECT copy_id::text, variant_id::text, indexcore_root_id, indexcore_resource_id,
       storage_binding_id::text, availability, created_at, updated_at
FROM copies
WHERE variant_id = $1
ORDER BY created_at, copy_id`, string(variantID))
	if err != nil {
		return nil, fmt.Errorf("list copies for variant %s: %w", variantID, err)
	}
	defer rows.Close()

	copies := make([]catalog.Copy, 0)
	for rows.Next() {
		var resourceCopy catalog.Copy
		var copyID, storedVariantID, storageBindingID string
		if err := rows.Scan(
			&copyID, &storedVariantID, &resourceCopy.IndexCoreRootID, &resourceCopy.IndexCoreResourceID,
			&storageBindingID, &resourceCopy.Availability, &resourceCopy.CreatedAt, &resourceCopy.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan copy for variant %s: %w", variantID, err)
		}
		resourceCopy.ID = catalog.CopyID(copyID)
		storedVariant := catalog.VariantID(storedVariantID)
		resourceCopy.VariantID = &storedVariant
		resourceCopy.StorageBindingID = catalog.StorageBindingID(storageBindingID)
		copies = append(copies, resourceCopy)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate copies for variant %s: %w", variantID, err)
	}
	return copies, nil
}

func wrapReadError(entity, id string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s %s", catalog.ErrNotFound, entity, id)
	}
	return fmt.Errorf("get %s %s: %w", entity, id, err)
}

func wrapWriteError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}
