package app

import (
	"context"
	"fmt"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/projector"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

// Package app is the composition boundary, so this is where the concrete IndexCore
// Q5 resolver, the bounded Journal projector, the catalog Copy reader, and the
// catalog classification service are adapted to the acquisition-owned canonical
// ports. Without these adapters the two capabilities would satisfy their own tests
// while the real process could never be wired: acquisition would ask for canonical
// ports that no production type provides.
//
// The adapters carry no policy of their own beyond the closed presence translation.
// Each collaborator keeps its own enforcement: IndexCore keeps the query contract,
// the projector keeps its bounded page validation, the catalog keeps the
// classification rules, and acquisition keeps the D-033 canonical decision.
//
// D-033 visibility: nothing here may widen the IndexCore read. The resolve adapter
// builds its request from root and path only, so ReadVisibility stays at its zero
// value and IndexCore keeps its default PRESENT-only behavior.

// Compile-time proof of the four capabilities the composition has to bridge, plus
// the concrete collaborators that must keep satisfying the narrow source ports. If
// any assertion stops holding, the build fails here instead of at runtime wiring.
var (
	_ acquisition.CanonicalResolvePort   = (*CanonicalResolveAdapter)(nil)
	_ acquisition.CanonicalProjectorPort = (*CanonicalProjectorAdapter)(nil)
	_ acquisition.CanonicalCopyReader    = (*CanonicalCopyReaderAdapter)(nil)
	_ acquisition.CanonicalClassifier    = (*CanonicalClassifierAdapter)(nil)

	_ indexcore.ResolvePort          = (*indexcore.Client)(nil)
	_ CanonicalProjectorSource       = (*projector.Service)(nil)
	_ catalog.PhysicalIdentityReader = (catalog.Repository)(nil)
)

// CanonicalResolveAdapter adapts an IndexCore Q5 resolver to the acquisition-owned
// CanonicalResolvePort.
type CanonicalResolveAdapter struct {
	resolver indexcore.ResolvePort
}

// NewCanonicalResolveAdapter builds the production adapter around an IndexCore Q5
// resolver. It fails closed on a missing dependency so an unwired resolver can never
// be placed on the canonical path.
func NewCanonicalResolveAdapter(resolver indexcore.ResolvePort) (*CanonicalResolveAdapter, error) {
	if resolver == nil {
		return nil, fmt.Errorf("compose canonical resolve adapter: IndexCore resolve port is required")
	}
	return &CanonicalResolveAdapter{resolver: resolver}, nil
}

// ResolveCanonical implements acquisition.CanonicalResolvePort by translating the
// acquisition-owned request into one IndexCore Q5 resolution and back.
//
// Visibility is deliberately left at its zero value: D-033 requires the default
// PRESENT-only visibility, so the adapter must never set an include-removed or
// include-deprecated override.
func (adapter *CanonicalResolveAdapter) ResolveCanonical(
	ctx context.Context,
	request acquisition.CanonicalResolveRequest,
) (acquisition.CanonicalResolution, error) {
	if adapter == nil || adapter.resolver == nil {
		return acquisition.CanonicalResolution{}, fmt.Errorf(
			"compose canonical resolve adapter: adapter is not configured")
	}
	result, err := adapter.resolver.Resolve(ctx, indexcore.ResolveRequest{
		RootID: request.RootID,
		Path:   request.Path,
		// ReadVisibility intentionally unset: the zero value is the frozen
		// PRESENT-only default. Do not add a visibility override here.
	})
	if err != nil {
		// The typed IndexCore failure crosses the boundary unchanged so callers keep
		// the distinct not-found/protocol/transport classification.
		return acquisition.CanonicalResolution{}, err
	}
	matches := make([]acquisition.CanonicalResource, 0, len(result.Matches))
	for _, match := range result.Matches {
		presence, err := canonicalPresenceFromIndexCore(match.Presence)
		if err != nil {
			return acquisition.CanonicalResolution{}, err
		}
		matches = append(matches, acquisition.CanonicalResource{
			RootID:        match.RootID,
			ResourceID:    match.ResourceID,
			CanonicalPath: canonicalPathOrEmpty(match.CanonicalPath),
			Presence:      presence,
		})
	}
	return acquisition.CanonicalResolution{Matches: matches, Ambiguous: result.Ambiguous}, nil
}

// canonicalPresenceFromIndexCore is the closed translation between the two presence
// vocabularies. An unknown value fails closed rather than being guessed, because a
// presence this boundary cannot name is not usable physical identity.
func canonicalPresenceFromIndexCore(presence indexcore.ResourcePresence) (acquisition.CanonicalPresence, error) {
	switch presence {
	case indexcore.ResourcePresent:
		return acquisition.CanonicalPresencePresent, nil
	case indexcore.ResourceRemoved:
		return acquisition.CanonicalPresenceRemoved, nil
	default:
		return "", fmt.Errorf("%w: IndexCore reported unknown presence %q",
			acquisition.ErrCanonicalIdentity, presence)
	}
}

// canonicalPathOrEmpty dereferences the nullable canonical path. A missing path is
// reported as the empty string so acquisition rejects the match: this boundary never
// fabricates a path from another field such as the resource name.
func canonicalPathOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// CanonicalProjectorSource is the narrow view of the concrete projector service the
// composition consumes. It is deliberately one bounded call: the acquisition port
// cannot loop and the adapter must not add one.
type CanonicalProjectorSource interface {
	ProjectOnce(context.Context, storage.BindingID, int) (projector.Result, error)
}

// CanonicalProjectorAdapter adapts the bounded projector service to the
// acquisition-owned CanonicalProjectorPort.
type CanonicalProjectorAdapter struct {
	projector CanonicalProjectorSource
}

// NewCanonicalProjectorAdapter builds the production adapter around the projector
// service. It fails closed on a missing dependency.
func NewCanonicalProjectorAdapter(source CanonicalProjectorSource) (*CanonicalProjectorAdapter, error) {
	if source == nil {
		return nil, fmt.Errorf("compose canonical projector adapter: projector service is required")
	}
	return &CanonicalProjectorAdapter{projector: source}, nil
}

// ProjectOnce implements acquisition.CanonicalProjectorPort by forwarding exactly one
// bounded projection invocation and reporting what it read and wrote.
func (adapter *CanonicalProjectorAdapter) ProjectOnce(
	ctx context.Context,
	bindingID storage.BindingID,
	limit int,
) (acquisition.CanonicalProjection, error) {
	if adapter == nil || adapter.projector == nil {
		return acquisition.CanonicalProjection{}, fmt.Errorf(
			"compose canonical projector adapter: adapter is not configured")
	}
	// Exactly one upstream call: the adapter never retries or loops, because the
	// acquisition layer already owns the bounded-attempt and retry policy.
	result, err := adapter.projector.ProjectOnce(ctx, bindingID, limit)
	if err != nil {
		// The projector failure crosses the boundary unchanged so its typed
		// classification stays available to diagnostics.
		return acquisition.CanonicalProjection{}, err
	}
	return acquisition.CanonicalProjection{
		EventsRead: result.EventsRead,
		Mutations:  result.Mutations,
	}, nil
}

// CanonicalCopyReaderAdapter adapts the catalog physical-identity reader to the
// acquisition-owned CanonicalCopyReader.
type CanonicalCopyReaderAdapter struct {
	reader catalog.PhysicalIdentityReader
}

// NewCanonicalCopyReaderAdapter builds the production adapter around the catalog
// physical-identity reader. It fails closed on a missing dependency.
func NewCanonicalCopyReaderAdapter(reader catalog.PhysicalIdentityReader) (*CanonicalCopyReaderAdapter, error) {
	if reader == nil {
		return nil, fmt.Errorf("compose canonical copy reader adapter: catalog physical-identity reader is required")
	}
	return &CanonicalCopyReaderAdapter{reader: reader}, nil
}

// GetCopyByPhysicalIdentity implements acquisition.CanonicalCopyReader by forwarding
// the exact physical identity unchanged.
//
// The error is returned exactly as the catalog produced it, because the acquisition
// service branches on catalog.ErrNotFound to treat a not-yet-projected Copy as
// normal pending work. Wrapping or replacing it here would silently change that
// decision.
func (adapter *CanonicalCopyReaderAdapter) GetCopyByPhysicalIdentity(
	ctx context.Context,
	rootID string,
	resourceID string,
) (catalog.Copy, error) {
	if adapter == nil || adapter.reader == nil {
		return catalog.Copy{}, fmt.Errorf(
			"compose canonical copy reader adapter: adapter is not configured")
	}
	return adapter.reader.GetCopyByPhysicalIdentity(ctx, rootID, resourceID)
}

// CanonicalClassifierAdapter adapts the catalog classification service to the
// acquisition-owned CanonicalClassifier.
type CanonicalClassifierAdapter struct {
	classifier *catalog.ClassificationService
}

// NewCanonicalClassifierAdapter builds the production adapter around the catalog
// classification service. It fails closed on a missing dependency.
func NewCanonicalClassifierAdapter(classifier *catalog.ClassificationService) (*CanonicalClassifierAdapter, error) {
	if classifier == nil {
		return nil, fmt.Errorf("compose canonical classifier adapter: catalog classification service is required")
	}
	return &CanonicalClassifierAdapter{classifier: classifier}, nil
}

// Bind implements acquisition.CanonicalClassifier by adapting the positional
// acquisition port to the catalog request struct and returning the resulting Copy.
//
// The resolved Copy is returned as-is. This adapter never repairs a nil or
// mismatched Variant, because acquisition owns the decision to reject it
// (ErrCanonicalCopyClassified); rewriting it here would hide a classification
// conflict.
func (adapter *CanonicalClassifierAdapter) Bind(
	ctx context.Context,
	copyID catalog.CopyID,
	variantID catalog.VariantID,
) (catalog.Copy, error) {
	if adapter == nil || adapter.classifier == nil {
		return catalog.Copy{}, fmt.Errorf(
			"compose canonical classifier adapter: adapter is not configured")
	}
	result, err := adapter.classifier.Bind(ctx, catalog.BindCopyRequest{
		CopyID:    copyID,
		VariantID: variantID,
	})
	if err != nil {
		return catalog.Copy{}, fmt.Errorf("compose catalog classification: %w", err)
	}
	return result.Copy, nil
}
