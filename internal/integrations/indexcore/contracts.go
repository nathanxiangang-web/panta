// Package indexcore defines the Panta-owned boundary to IndexCore. Concrete
// clients belong below this port and must never expose IndexCore database types.
package indexcore

import (
	"context"
	"encoding/json"
	"time"
)

// ResourcePresence is IndexCore's consumer-visible physical presence state.
type ResourcePresence string

const (
	ResourcePresent ResourcePresence = "PRESENT"
	ResourceRemoved ResourcePresence = "REMOVED"
)

func (p ResourcePresence) valid() bool { return p == ResourcePresent || p == ResourceRemoved }

// RootLifecycleState is the closed lifecycle state exposed by IndexCore.
type RootLifecycleState string

const (
	RootNew        RootLifecycleState = "NEW"
	RootActive     RootLifecycleState = "ACTIVE"
	RootDeprecated RootLifecycleState = "DEPRECATED"
	RootDeleted    RootLifecycleState = "DELETED"
)

func (s RootLifecycleState) valid() bool {
	switch s {
	case RootNew, RootActive, RootDeprecated, RootDeleted:
		return true
	default:
		return false
	}
}

// ReadVisibility preserves the explicit visibility controls of the IndexCore
// query contract. Zero values keep the default visible-only behavior.
type ReadVisibility struct {
	IncludeRemoved        bool
	IncludeDeprecatedRoot bool
	IncludeDeletedRoot    bool
}

// ResolveRequest addresses a physical resource by root and canonical path.
type ResolveRequest struct {
	RootID string
	Path   string
	ReadVisibility
}

// ResolveResult retains every matching physical resource and IndexCore's
// ambiguity decision. Consumers must not silently choose a winner.
type ResolveResult struct {
	Matches   []ResourceContext
	Ambiguous bool
}

// BrowseRequest addresses one hierarchy level. A nil ParentResourceID means
// root-level children; Cursor is an opaque value that must be reused unchanged.
type BrowseRequest struct {
	RootID           string
	ParentResourceID *string
	Cursor           string
	Limit            int
	ReadVisibility
}

// ResourcePage is one hierarchy page and its optional opaque continuation.
type ResourcePage struct {
	Items      []ResourceContext
	NextCursor string
}

// ResourceContext contains the physical facts Panta consumes from IndexCore.
// Pointer fields preserve nullable/omitted values in the accepted wire contract.
type ResourceContext struct {
	ResourceID              string
	RootID                  string
	CanonicalPath           *string
	ParentResourceID        *string
	Name                    *string
	Directory               *bool
	SizeBytes               *int64
	ModifiedAt              *time.Time
	Presence                ResourcePresence
	IntroducedAtGeneration  int64
	LastConfirmedGeneration int64
}

// ResolvePort is the narrow Q5 read boundary for canonical path resolution.
type ResolvePort interface {
	Resolve(context.Context, ResolveRequest) (ResolveResult, error)
}

// ReadPort resolves canonical paths and browses hierarchy children.
type ReadPort interface {
	ResolvePort
	Browse(context.Context, BrowseRequest) (ResourcePage, error)
}

// JournalEventType is the closed canonical IndexCore journal event set.
type JournalEventType string

const (
	EventResourceAdded   JournalEventType = "resource-added"
	EventResourceUpdated JournalEventType = "resource-updated"
	EventResourceRenamed JournalEventType = "resource-renamed"
	EventResourceMoved   JournalEventType = "resource-moved"
	EventResourceRemoved JournalEventType = "resource-removed"
	EventRootDeprecated  JournalEventType = "root-deprecated"
	EventRootDeleted     JournalEventType = "root-deleted"
)

func (t JournalEventType) valid() bool {
	switch t {
	case EventResourceAdded, EventResourceUpdated, EventResourceRenamed,
		EventResourceMoved, EventResourceRemoved, EventRootDeprecated, EventRootDeleted:
		return true
	default:
		return false
	}
}

// JournalRequest reads canonical events for one root. AfterSeq is transmitted
// exactly as supplied; callers continue with the last EventSeq actually seen.
type JournalRequest struct {
	RootID   string
	AfterSeq int64
	Limit    int
}

// JournalEvent is a Panta-owned representation of an IndexCore journal event.
// Payload is opaque JSON owned by this integration boundary.
type JournalEvent struct {
	EventSeq           int64
	GenerationNumber   int64
	IntraGenerationSeq int32
	EventType          JournalEventType
	ResourceID         *string
	Payload            json.RawMessage
	CommittedAt        time.Time
}

// JournalReadPort reads per-root canonical journal events in server order.
type JournalReadPort interface {
	ReadJournal(context.Context, JournalRequest) ([]JournalEvent, error)
}

// RootStatusRequest reads a root's status with explicit lifecycle visibility.
type RootStatusRequest struct {
	RootID                string
	IncludeDeprecatedRoot bool
	IncludeDeletedRoot    bool
}

// RootStatus contains the narrow status facts needed by future consumers.
type RootStatus struct {
	RootID                  string
	LifecycleState          RootLifecycleState
	CurrentGeneration       int64
	LastAppliedAdmissionSeq *int64
}

// RootStatusReadPort reads status without exposing root mutation capabilities.
type RootStatusReadPort interface {
	RootStatus(context.Context, RootStatusRequest) (RootStatus, error)
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
