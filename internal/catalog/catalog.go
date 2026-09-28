// Package catalog owns Panta's logical resource identity and persistence ports.
// It is independent from PostgreSQL, pgx, provider adapters, and IndexCore.
package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var ErrNotFound = errors.New("catalog entity not found")

type AssetID string
type ReleaseID string
type VariantID string
type CopyID string
type StorageBindingID string

type VersionScheme string

const (
	VersionSchemeSemver     VersionScheme = "SEMVER"
	VersionSchemeDate       VersionScheme = "DATE"
	VersionSchemeRevision   VersionScheme = "REVISION"
	VersionSchemeUpstreamID VersionScheme = "UPSTREAM_ID"
	VersionSchemeCustom     VersionScheme = "CUSTOM"
	VersionSchemeNone       VersionScheme = "NONE"
)

type Asset struct {
	ID            AssetID
	CanonicalName string
	Category      string
	Status        string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Release struct {
	ID                ReleaseID
	AssetID           AssetID
	VersionRaw        string
	VersionNormalized *string
	VersionScheme     VersionScheme
	Channel           string
	ReleaseDate       *time.Time
	SourceRef         *string
	Status            string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Variant struct {
	ID         VariantID
	ReleaseID  ReleaseID
	VariantKey string
	Attributes json.RawMessage
	Status     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// Copy records Panta's binding to a canonical physical identity reported by
// IndexCore. A nil VariantID represents an unresolved physical resource.
type Copy struct {
	ID                  CopyID
	VariantID           *VariantID
	IndexCoreRootID     string
	IndexCoreResourceID string
	StorageBindingID    StorageBindingID
	Availability        string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// Repository is the persistence port for the minimum logical resource model.
// Implementations live outside the catalog package.
type Repository interface {
	CreateAsset(context.Context, Asset) error
	GetAsset(context.Context, AssetID) (Asset, error)
	CreateRelease(context.Context, Release) error
	GetRelease(context.Context, ReleaseID) (Release, error)
	CreateVariant(context.Context, Variant) error
	GetVariant(context.Context, VariantID) (Variant, error)
	CreateCopy(context.Context, Copy) error
	GetCopy(context.Context, CopyID) (Copy, error)
}
