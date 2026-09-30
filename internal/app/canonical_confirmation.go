package app

// Gate 3.10 production composition for the canonical confirmation path.
//
// internal/acquisition owns CanonicalConfirmation and the four canonical ports it
// depends on, but it may not import a concrete integration or persistence package.
// internal/app/canonical.go owns the four adapters that bridge the real IndexCore Q5
// resolver, the bounded Journal projector, the catalog Copy reader, and the catalog
// classification service to those ports. Each piece satisfied only its own tests,
// though: acquisition.NewCanonicalConfirmation had no production caller, so the
// running process could never assemble a canonical path. A Gate 3.9 review rejected
// exactly that shape - production composition existing only in tests - so this file
// is the single production entry point that ties the adapters and the service
// together.
//
// This composition carries no policy of its own. The adapters keep the closed
// presence translation, the default PRESENT-only visibility, and the exact upstream
// error propagation; the acquisition service keeps the D-033 canonical decision.
// This function only refuses to hand out a partially wired path.

import (
	"fmt"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/projector"
)

// The concrete production collaborators accepted below must keep satisfying these
// narrow source ports. canonical.go asserts the same packages, and this file
// restates the assertions at the composition that consumes them: if a collaborator
// stops satisfying a port, the build fails here rather than at a runtime wiring
// call site.
var (
	_ indexcore.ResolvePort          = (*indexcore.Client)(nil)
	_ CanonicalProjectorSource       = (*projector.Service)(nil)
	_ catalog.PhysicalIdentityReader = (catalog.Repository)(nil)
)

// Compile-time proof that this composition supplies exactly the eight
// acquisition-owned ports, in the constructor's order, and returns the
// acquisition-owned service. If the constructor's shape drifts, the build fails
// here instead of at a future wiring call site.
var _ func(
	acquisition.ManifestReader,
	acquisition.JobReader,
	acquisition.RefreshBindingReader,
	acquisition.CanonicalResolvePort,
	acquisition.CanonicalProjectorPort,
	acquisition.CanonicalCopyReader,
	acquisition.CanonicalClassifier,
	acquisition.CanonicalStageStore,
) (*acquisition.CanonicalConfirmation, error) = acquisition.NewCanonicalConfirmation

// NewCanonicalConfirmation builds the four production adapters and the
// acquisition-owned canonical confirmation service they feed.
//
// The parameters are the concrete production collaborators, accepted through the
// narrow ports they already satisfy: *indexcore.Client as the Q5 resolver,
// *projector.Service as the bounded projector, the PostgreSQL *postgres.CatalogRepository
// (via catalog.Repository) or any catalog.PhysicalIdentityReader as the Copy
// reader, and *catalog.ClassificationService as the classifier. The Manifest, Job,
// StorageBinding, and canonical stage store stay acquisition-owned ports, so this
// package never depends on the concrete store.
//
// Every dependency is validated before any adapter is built, and every adapter
// result is validated before the service is built, so a nil or failed collaborator
// can never produce a usable canonical path.
func NewCanonicalConfirmation(
	resolver indexcore.ResolvePort,
	projectors CanonicalProjectorSource,
	copies catalog.PhysicalIdentityReader,
	classifier *catalog.ClassificationService,
	manifests acquisition.ManifestReader,
	jobStore acquisition.JobReader,
	bindings acquisition.RefreshBindingReader,
	store acquisition.CanonicalStageStore,
) (*acquisition.CanonicalConfirmation, error) {
	switch {
	case resolver == nil:
		return nil, fmt.Errorf("%w: IndexCore canonical resolve port is required", acquisition.ErrInvalidCanonicalRequest)
	case projectors == nil:
		return nil, fmt.Errorf("%w: canonical projector source is required", acquisition.ErrInvalidCanonicalRequest)
	case copies == nil:
		return nil, fmt.Errorf("%w: catalog physical-identity reader is required", acquisition.ErrInvalidCanonicalRequest)
	case classifier == nil:
		return nil, fmt.Errorf("%w: catalog classification service is required", acquisition.ErrInvalidCanonicalRequest)
	case manifests == nil:
		return nil, fmt.Errorf("%w: acquisition Manifest reader is required", acquisition.ErrInvalidCanonicalRequest)
	case jobStore == nil:
		return nil, fmt.Errorf("%w: acquisition Job reader is required", acquisition.ErrInvalidCanonicalRequest)
	case bindings == nil:
		return nil, fmt.Errorf("%w: acquisition StorageBinding reader is required", acquisition.ErrInvalidCanonicalRequest)
	case store == nil:
		return nil, fmt.Errorf("%w: acquisition canonical stage store is required", acquisition.ErrInvalidCanonicalRequest)
	}

	// Every adapter is built through its production constructor - never a struct
	// literal - so the constructor's own dependency validation is exercised and a
	// future change cannot be bypassed at this composition.
	resolveAdapter, err := NewCanonicalResolveAdapter(resolver)
	if err != nil {
		return nil, fmt.Errorf("compose canonical resolve adapter: %w", err)
	}
	if resolveAdapter == nil {
		return nil, fmt.Errorf("%w: canonical resolve adapter is required", acquisition.ErrInvalidCanonicalRequest)
	}

	projectorAdapter, err := NewCanonicalProjectorAdapter(projectors)
	if err != nil {
		return nil, fmt.Errorf("compose canonical projector adapter: %w", err)
	}
	if projectorAdapter == nil {
		return nil, fmt.Errorf("%w: canonical projector adapter is required", acquisition.ErrInvalidCanonicalRequest)
	}

	copyAdapter, err := NewCanonicalCopyReaderAdapter(copies)
	if err != nil {
		return nil, fmt.Errorf("compose canonical copy reader adapter: %w", err)
	}
	if copyAdapter == nil {
		return nil, fmt.Errorf("%w: canonical copy reader adapter is required", acquisition.ErrInvalidCanonicalRequest)
	}

	classifierAdapter, err := NewCanonicalClassifierAdapter(classifier)
	if err != nil {
		return nil, fmt.Errorf("compose canonical classifier adapter: %w", err)
	}
	if classifierAdapter == nil {
		return nil, fmt.Errorf("%w: canonical classifier adapter is required", acquisition.ErrInvalidCanonicalRequest)
	}

	confirmation, err := acquisition.NewCanonicalConfirmation(
		manifests,
		jobStore,
		bindings,
		resolveAdapter,
		projectorAdapter,
		copyAdapter,
		classifierAdapter,
		store,
	)
	if err != nil {
		return nil, fmt.Errorf("compose canonical confirmation: %w", err)
	}
	if confirmation == nil {
		return nil, fmt.Errorf("%w: canonical confirmation is required", acquisition.ErrInvalidCanonicalRequest)
	}
	return confirmation, nil
}
