package acquisition

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/nathanxiangang-web/panta/internal/integrations/openlist"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var (
	ErrInvalidVisibilityRequest = errors.New("invalid acquisition visibility request")
	ErrVisibilityManifestState  = errors.New("acquisition Manifest is not awaiting visibility")
	ErrVisibilityBindingMissing = errors.New("acquisition visibility StorageBinding not found")
	ErrVisibilityBindingState   = errors.New("acquisition visibility StorageBinding is not active")
	ErrVisibilityPathInvalid    = errors.New("acquisition visibility path is invalid")
	ErrVisibilityFactIdentity   = errors.New("acquisition visibility fact identity mismatch")
	ErrVisibilityPortMissing    = errors.New("acquisition visibility port is required")
)

// VisibilityState is the provider-neutral outcome of one known-path observation.
type VisibilityState string

const (
	// VisibilityVisible means OpenList currently exposes the exact expected path.
	VisibilityVisible VisibilityState = "VISIBLE"
	// VisibilityNotVisible is a normal observation: OpenList does not currently
	// expose the exact expected path. It is not a provider failure and does not
	// authorize another provider task.
	VisibilityNotVisible VisibilityState = "NOT_VISIBLE"
)

// VisibilityResult is one provider-neutral visibility observation. It carries only
// Panta-owned identities, never OpenList-internal provider types or download URLs.
type VisibilityResult struct {
	ManifestID ManifestID
	BindingID  storage.BindingID
	State      VisibilityState
	// Mount and TargetPath echo the exact binding mount and binding-relative target
	// that were requested.
	Mount      string
	TargetPath string
	// OpenListPath is the derived absolute path per D-028.
	OpenListPath string
	// Fact is the observation itself. It is informational and is not persisted.
	Fact openlist.VisibilityFact
	// ObservedAt is when the observation was taken.
	ObservedAt time.Time
}

// VisibilityBindingReader is the narrow storage read port this verifier needs. It
// deliberately does not require provider scope or a provider identity, because
// visibility observation never selects a provider session.
type VisibilityBindingReader interface {
	GetBinding(context.Context, storage.BindingID) (storage.Binding, error)
}

// VisibilityClock supplies the observation timestamp.
type VisibilityClock func() time.Time

// VisibilityOption configures the verifier.
type VisibilityOption func(*VisibilityVerifier) error

// WithVisibilityClock injects a deterministic clock for tests.
func WithVisibilityClock(clock VisibilityClock) VisibilityOption {
	return func(verifier *VisibilityVerifier) error {
		if clock == nil {
			return ErrInvalidVisibilityRequest
		}
		verifier.now = clock
		return nil
	}
}

// VisibilityVerifier checks whether an AWAITING_VISIBILITY Manifest's expected
// result is visible through OpenList.
//
// It is observation only: it performs no Job mutation, no Manifest transition, no
// IndexCore call, and no provider call.
type VisibilityVerifier struct {
	manifests ManifestReader
	bindings  VisibilityBindingReader
	port      openlist.VisibilityPort
	now       VisibilityClock
}

func NewVisibilityVerifier(
	manifests ManifestReader,
	bindings VisibilityBindingReader,
	port openlist.VisibilityPort,
	options ...VisibilityOption,
) (*VisibilityVerifier, error) {
	if manifests == nil || bindings == nil || port == nil {
		return nil, ErrVisibilityPortMissing
	}
	verifier := &VisibilityVerifier{manifests: manifests, bindings: bindings, port: port, now: time.Now}
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(verifier); err != nil {
			return nil, err
		}
	}
	return verifier, nil
}

// Verify performs exactly one known-path observation for the Manifest.
func (verifier *VisibilityVerifier) Verify(ctx context.Context, manifestID ManifestID) (VisibilityResult, error) {
	if err := ctx.Err(); err != nil {
		return VisibilityResult{}, err
	}
	if manifestID == "" {
		return VisibilityResult{}, ErrInvalidVisibilityRequest
	}

	manifest, err := verifier.manifests.GetManifest(ctx, manifestID)
	if errors.Is(err, ErrNotFound) {
		return VisibilityResult{}, fmt.Errorf("%w: %s", ErrExecutionManifestNotFound, manifestID)
	}
	if err != nil {
		return VisibilityResult{}, fmt.Errorf("load acquisition Manifest: %w", err)
	}
	if manifest.ID != manifestID {
		return VisibilityResult{}, fmt.Errorf("%w: requested %s, got %s",
			ErrExecutionIdentityMismatch, manifestID, manifest.ID)
	}
	// Only an AWAITING_VISIBILITY Manifest is observable here. Nothing in this gate
	// transitions the Manifest, so this is a read-only precondition.
	if manifest.State != StateAwaitingVisibility {
		return VisibilityResult{}, fmt.Errorf("%w: %s", ErrVisibilityManifestState, manifest.State)
	}

	binding, err := verifier.bindings.GetBinding(ctx, manifest.TargetStorageBindingID)
	if errors.Is(err, storage.ErrNotFound) {
		return VisibilityResult{}, fmt.Errorf("%w: %s", ErrVisibilityBindingMissing, manifest.TargetStorageBindingID)
	}
	if err != nil {
		return VisibilityResult{}, fmt.Errorf("load StorageBinding: %w", err)
	}
	if binding.ID != manifest.TargetStorageBindingID {
		return VisibilityResult{}, fmt.Errorf("%w: requested binding %s, got %s",
			ErrStorageTopologyMismatch, manifest.TargetStorageBindingID, binding.ID)
	}
	if binding.Status != storage.BindingStatusActive {
		return VisibilityResult{}, fmt.Errorf("%w: %s", ErrVisibilityBindingState, binding.ID)
	}

	mount, targetPath, err := resolveOpenListCoordinates(binding, manifest)
	if err != nil {
		return VisibilityResult{}, err
	}
	// D-028: the OpenList address is the join of the mount and the binding-relative
	// target, and that joined absolute path is what is sent to OpenList. The
	// binding-relative target is still echoed separately in the result.
	openListPath := joinOpenListPath(mount, targetPath)
	if err := validateJoinedPath(openListPath, mount); err != nil {
		return VisibilityResult{}, err
	}
	request := openlist.StatRequest{Mount: mount, Path: openListPath}

	fact, err := verifier.port.Stat(ctx, request)
	if err != nil {
		// Authorization, storage, provider, transport, and malformed-response failures
		// stay errors and are never collapsed into a not-visible observation.
		return VisibilityResult{}, err
	}
	if err := verifyFactIdentity(request, fact); err != nil {
		return VisibilityResult{}, err
	}

	result := VisibilityResult{
		ManifestID:   manifest.ID,
		BindingID:    binding.ID,
		Mount:        mount,
		TargetPath:   targetPath,
		OpenListPath: openListPath,
		Fact:         fact,
		ObservedAt:   verifier.now().UTC(),
	}
	if fact.Visible {
		result.State = VisibilityVisible
	} else {
		result.State = VisibilityNotVisible
	}
	return result, nil
}

// resolveOpenListCoordinates validates the persisted mount and target path with the
// existing normalizers and returns them unchanged for observation.
func resolveOpenListCoordinates(binding storage.Binding, manifest Manifest) (string, string, error) {
	// The persisted mount is stored canonically; re-normalizing proves it has not
	// drifted and rejects a malformed row before any network call.
	normalizedMount, err := storage.NormalizeMountPath(binding.OpenListMountPath)
	if err != nil || normalizedMount != binding.OpenListMountPath {
		return "", "", fmt.Errorf("%w: binding %s mount %q",
			ErrVisibilityPathInvalid, binding.ID, binding.OpenListMountPath)
	}
	normalizedTarget, err := NormalizeTargetPath(manifest.TargetPath)
	if err != nil || normalizedTarget != manifest.TargetPath {
		return "", "", fmt.Errorf("%w: Manifest %s target %q",
			ErrVisibilityPathInvalid, manifest.ID, manifest.TargetPath)
	}
	// D-028: the mount is the mount coordinate and the target is binding-relative.
	// Neither provider_scope nor IndexCore root participates in this mapping.
	return normalizedMount, normalizedTarget, nil
}

// joinOpenListPath implements D-028 exactly: Join(mount, target) with the root mount
// collapsing to the target.
func joinOpenListPath(mount, target string) string {
	if mount == "/" {
		return target
	}
	return path.Join(mount, target)
}

// validateJoinedPath proves the join stays absolute, normalized, and beneath or
// equal to the configured mount.
func validateJoinedPath(joined, mount string) error {
	if joined == "" || !strings.HasPrefix(joined, "/") || path.Clean(joined) != joined {
		return fmt.Errorf("%w: joined openlist path %q is not canonical", ErrVisibilityPathInvalid, joined)
	}
	if mount == "/" {
		return nil
	}
	if joined != mount && !strings.HasPrefix(joined, mount+"/") {
		return fmt.Errorf("%w: joined openlist path %q escapes mount %q",
			ErrVisibilityPathInvalid, joined, mount)
	}
	return nil
}

// verifyFactIdentity rejects a fact that does not describe exactly what was asked.
func verifyFactIdentity(request openlist.StatRequest, fact openlist.VisibilityFact) error {
	if fact.Mount != request.Mount || fact.Path != request.Path {
		return fmt.Errorf("%w: requested %s%s, observed %s%s",
			ErrVisibilityFactIdentity, request.Mount, request.Path, fact.Mount, fact.Path)
	}
	return nil
}
