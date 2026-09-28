// Package indexcore defines the Panta-owned boundary to IndexCore. Concrete
// clients belong below this port and must never expose IndexCore database types.
package indexcore

import (
	"context"
	"time"
)

// ResolveRequest addresses a physical resource directly by root and path. It
// deliberately requires no search contract.
type ResolveRequest struct {
	RootID string
	Path   string
}

// BrowseRequest addresses the children of a known root/path.
type BrowseRequest struct {
	RootID string
	Path   string
}

// ResourceContext contains the physical facts Panta needs from IndexCore.
type ResourceContext struct {
	ResourceID    string
	RootID        string
	CanonicalPath string
	Name          string
	SizeBytes     int64
	ModifiedAt    time.Time
	Directory     bool
	Present       bool
}

// ReadPort resolves and browses known physical paths.
type ReadPort interface {
	Resolve(context.Context, ResolveRequest) (ResourceContext, error)
	Browse(context.Context, BrowseRequest) ([]ResourceContext, error)
}

// ScopedRefreshRequest limits a refresh to a known root/path boundary.
type ScopedRefreshRequest struct {
	RootID string
	Path   string
}

// RefreshReference is an opaque reference to an accepted refresh request.
type RefreshReference struct {
	Value string
}

// ScopedRefreshPort requests bounded refresh through a supported IndexCore
// integration path. It does not grant database access.
type ScopedRefreshPort interface {
	RequestScopedRefresh(context.Context, ScopedRefreshRequest) (RefreshReference, error)
}
