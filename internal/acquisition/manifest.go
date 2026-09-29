package acquisition

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

const (
	MaxSourceTypeLength   = 64
	MaxSourceRefLength    = 4096
	MaxExpectedNameLength = 512
	MaxTargetPathLength   = 2048
)

var (
	ErrInvalidArgument          = errors.New("invalid acquisition manifest argument")
	ErrInvalidTargetPath        = errors.New("invalid acquisition target path")
	ErrStorageBindingNotFound   = errors.New("acquisition StorageBinding not found")
	ErrStorageBindingDisabled   = errors.New("acquisition StorageBinding is disabled")
	ErrAssetNotFound            = errors.New("acquisition Asset not found")
	ErrReleaseNotFound          = errors.New("acquisition Release not found")
	ErrVariantNotFound          = errors.New("acquisition Variant not found")
	ErrReleaseOwnershipMismatch = errors.New("acquisition Release does not belong to Asset")
	ErrVariantOwnershipMismatch = errors.New("acquisition Variant does not belong to Release")
	ErrNotFound                 = errors.New("acquisition manifest not found")
	ErrConflict                 = errors.New("acquisition manifest conflicts with existing state")
	ErrInvalidReference         = errors.New("acquisition manifest references missing state")
	ErrPersistence              = errors.New("acquisition manifest persistence failure")
)

type ManifestID string
type UserID string
type JobID = jobs.JobID
type State string

const (
	StatePending            State = "PENDING"
	StateActive             State = "ACTIVE"
	StateAwaitingVisibility State = "AWAITING_VISIBILITY"
	StateAwaitingCanonical  State = "AWAITING_CANONICAL"
	StateReady              State = "READY"
	StateFailed             State = "FAILED"
	StateRecoveryRequired   State = "RECOVERY_REQUIRED"
	StateCanceled           State = "CANCELED"
)

func (state State) Valid() bool {
	switch state {
	case StatePending, StateActive, StateAwaitingVisibility, StateAwaitingCanonical,
		StateReady, StateFailed, StateRecoveryRequired, StateCanceled:
		return true
	default:
		return false
	}
}

// Manifest is durable provider-neutral acquisition intent. It deliberately
// contains no provider task reference or Job execution/lease state.
type Manifest struct {
	ID                     ManifestID
	UserID                 *UserID
	SourceType             string
	SourceRef              string
	ExpectedName           *string
	TargetStorageBindingID storage.BindingID
	TargetPath             string
	AssetID                *catalog.AssetID
	ReleaseID              *catalog.ReleaseID
	VariantID              *catalog.VariantID
	JobID                  *jobs.JobID
	State                  State
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

type CreateManifestRequest struct {
	ID                     ManifestID
	UserID                 *UserID
	SourceType             string
	SourceRef              string
	ExpectedName           *string
	TargetStorageBindingID storage.BindingID
	TargetPath             string
	AssetID                *catalog.AssetID
	ReleaseID              *catalog.ReleaseID
	VariantID              *catalog.VariantID
	JobID                  *jobs.JobID
}

type ManifestRepository interface {
	CreateManifest(context.Context, Manifest) error
	GetManifest(context.Context, ManifestID) (Manifest, error)
}

type StorageBindingReader interface {
	GetBinding(context.Context, storage.BindingID) (storage.Binding, error)
}

type CatalogIdentityReader interface {
	GetAsset(context.Context, catalog.AssetID) (catalog.Asset, error)
	GetRelease(context.Context, catalog.ReleaseID) (catalog.Release, error)
	GetVariant(context.Context, catalog.VariantID) (catalog.Variant, error)
}

type Clock func() time.Time
type Option func(*Service) error

func WithClock(clock Clock) Option {
	return func(service *Service) error {
		if clock == nil {
			return ErrInvalidArgument
		}
		service.now = clock
		return nil
	}
}

type Service struct {
	bindings  StorageBindingReader
	catalog   CatalogIdentityReader
	manifests ManifestRepository
	now       Clock
}

func NewService(bindings StorageBindingReader, identities CatalogIdentityReader, manifests ManifestRepository, options ...Option) (*Service, error) {
	if bindings == nil || identities == nil || manifests == nil {
		return nil, ErrInvalidArgument
	}
	service := &Service{bindings: bindings, catalog: identities, manifests: manifests, now: time.Now}
	for _, option := range options {
		if option != nil {
			if err := option(service); err != nil {
				return nil, err
			}
		}
	}
	return service, nil
}

// CreateManifest validates product-owned intent and persists one PENDING
// Manifest. It performs no network call, provider action, or identity creation.
func (service *Service) CreateManifest(ctx context.Context, request CreateManifestRequest) (Manifest, error) {
	targetPath, err := validateCreateRequest(request)
	if err != nil {
		return Manifest{}, err
	}
	binding, err := service.bindings.GetBinding(ctx, request.TargetStorageBindingID)
	if errors.Is(err, storage.ErrNotFound) {
		return Manifest{}, fmt.Errorf("%w: %s", ErrStorageBindingNotFound, request.TargetStorageBindingID)
	}
	if err != nil {
		return Manifest{}, fmt.Errorf("load StorageBinding: %w", err)
	}
	if binding.Status != storage.BindingStatusActive {
		return Manifest{}, fmt.Errorf("%w: %s", ErrStorageBindingDisabled, binding.ID)
	}
	if binding.ID != request.TargetStorageBindingID {
		return Manifest{}, fmt.Errorf("%w: requested %s, got %s", ErrStorageBindingNotFound, request.TargetStorageBindingID, binding.ID)
	}
	if err := service.validateLineage(ctx, request); err != nil {
		return Manifest{}, err
	}

	now := service.now().UTC()
	manifest := Manifest{
		ID: request.ID, UserID: request.UserID, SourceType: request.SourceType, SourceRef: request.SourceRef,
		ExpectedName: request.ExpectedName, TargetStorageBindingID: request.TargetStorageBindingID, TargetPath: targetPath,
		AssetID: request.AssetID, ReleaseID: request.ReleaseID, VariantID: request.VariantID, JobID: request.JobID,
		State: StatePending, CreatedAt: now, UpdatedAt: now,
	}
	if err := service.manifests.CreateManifest(ctx, manifest); err != nil {
		return Manifest{}, fmt.Errorf("create acquisition Manifest: %w", err)
	}
	return manifest, nil
}

func NormalizeTargetPath(value string) (string, error) {
	if value == "" || !utf8.ValidString(value) || !strings.HasPrefix(value, "/") ||
		strings.Contains(value, "\\") || strings.ContainsRune(value, '\x00') {
		return "", ErrInvalidTargetPath
	}
	for _, component := range strings.Split(value, "/") {
		if component == "." || component == ".." {
			return "", ErrInvalidTargetPath
		}
	}
	normalized := path.Clean(value)
	if normalized == "." || !strings.HasPrefix(normalized, "/") || utf8.RuneCountInString(normalized) > MaxTargetPathLength {
		return "", ErrInvalidTargetPath
	}
	return normalized, nil
}

func validateCreateRequest(request CreateManifestRequest) (string, error) {
	if request.ID == "" || request.TargetStorageBindingID == "" || !validRequiredText(request.SourceType, MaxSourceTypeLength) ||
		!validRequiredText(request.SourceRef, MaxSourceRefLength) {
		return "", ErrInvalidArgument
	}
	if request.ExpectedName != nil && !validRequiredText(*request.ExpectedName, MaxExpectedNameLength) {
		return "", ErrInvalidArgument
	}
	if request.UserID != nil && *request.UserID == "" || request.JobID != nil && *request.JobID == "" ||
		request.AssetID != nil && *request.AssetID == "" || request.ReleaseID != nil && *request.ReleaseID == "" ||
		request.VariantID != nil && *request.VariantID == "" {
		return "", ErrInvalidArgument
	}
	if request.ReleaseID != nil && request.AssetID == nil || request.VariantID != nil && request.ReleaseID == nil {
		return "", ErrInvalidArgument
	}
	targetPath, err := NormalizeTargetPath(request.TargetPath)
	if err != nil {
		return "", err
	}
	return targetPath, nil
}

func validRequiredText(value string, maximum int) bool {
	return utf8.ValidString(value) && strings.TrimSpace(value) != "" && !strings.ContainsRune(value, '\x00') && utf8.RuneCountInString(value) <= maximum
}

// ValidateManifest protects the repository port from invalid direct callers.
// It accepts every frozen milestone state but does not authorize transitions.
func ValidateManifest(manifest Manifest) error {
	if manifest.ID == "" || manifest.TargetStorageBindingID == "" || !validRequiredText(manifest.SourceType, MaxSourceTypeLength) ||
		!validRequiredText(manifest.SourceRef, MaxSourceRefLength) || !manifest.State.Valid() ||
		manifest.CreatedAt.IsZero() || manifest.UpdatedAt.IsZero() {
		return ErrInvalidArgument
	}
	if manifest.ExpectedName != nil && !validRequiredText(*manifest.ExpectedName, MaxExpectedNameLength) {
		return ErrInvalidArgument
	}
	if manifest.UserID != nil && *manifest.UserID == "" || manifest.JobID != nil && *manifest.JobID == "" ||
		manifest.AssetID != nil && *manifest.AssetID == "" || manifest.ReleaseID != nil && *manifest.ReleaseID == "" ||
		manifest.VariantID != nil && *manifest.VariantID == "" ||
		manifest.ReleaseID != nil && manifest.AssetID == nil || manifest.VariantID != nil && manifest.ReleaseID == nil {
		return ErrInvalidArgument
	}
	normalized, err := NormalizeTargetPath(manifest.TargetPath)
	if err != nil || normalized != manifest.TargetPath {
		return ErrInvalidTargetPath
	}
	return nil
}

func (service *Service) validateLineage(ctx context.Context, request CreateManifestRequest) error {
	if request.AssetID == nil {
		return nil
	}
	asset, err := service.catalog.GetAsset(ctx, *request.AssetID)
	if errors.Is(err, catalog.ErrNotFound) {
		return fmt.Errorf("%w: %s", ErrAssetNotFound, *request.AssetID)
	}
	if err != nil {
		return fmt.Errorf("load Asset: %w", err)
	}
	if asset.ID != *request.AssetID {
		return fmt.Errorf("%w: requested %s, got %s", ErrAssetNotFound, *request.AssetID, asset.ID)
	}
	if request.ReleaseID == nil {
		return nil
	}
	release, err := service.catalog.GetRelease(ctx, *request.ReleaseID)
	if errors.Is(err, catalog.ErrNotFound) {
		return fmt.Errorf("%w: %s", ErrReleaseNotFound, *request.ReleaseID)
	}
	if err != nil {
		return fmt.Errorf("load Release: %w", err)
	}
	if release.ID != *request.ReleaseID || release.AssetID != asset.ID {
		return fmt.Errorf("%w: Release %s", ErrReleaseOwnershipMismatch, *request.ReleaseID)
	}
	if request.VariantID == nil {
		return nil
	}
	variant, err := service.catalog.GetVariant(ctx, *request.VariantID)
	if errors.Is(err, catalog.ErrNotFound) {
		return fmt.Errorf("%w: %s", ErrVariantNotFound, *request.VariantID)
	}
	if err != nil {
		return fmt.Errorf("load Variant: %w", err)
	}
	if variant.ID != *request.VariantID || variant.ReleaseID != release.ID {
		return fmt.Errorf("%w: Variant %s", ErrVariantOwnershipMismatch, *request.VariantID)
	}
	return nil
}
