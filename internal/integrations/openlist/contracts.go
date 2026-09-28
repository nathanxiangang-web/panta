// Package openlist defines the Panta-owned boundary for storage visibility and
// access facts exposed by OpenList. It does not scan storage or access OpenList's
// database.
package openlist

import (
	"context"
	"time"
)

// StatRequest identifies a known mount/path.
type StatRequest struct {
	Mount string
	Path  string
}

// VisibilityFact reports whether OpenList currently exposes a known path.
type VisibilityFact struct {
	Mount      string
	Path       string
	Visible    bool
	Directory  bool
	SizeBytes  int64
	ObservedAt time.Time
}

// AccessRequest identifies a visible path for access resolution.
type AccessRequest struct {
	Mount string
	Path  string
}

// AccessTarget is a potentially time-bounded OpenList access location.
type AccessTarget struct {
	Location  string
	ExpiresAt time.Time
}

// VisibilityPort reads visibility facts for known paths.
type VisibilityPort interface {
	Stat(context.Context, StatRequest) (VisibilityFact, error)
}

// AccessPort resolves access for a known visible path.
type AccessPort interface {
	Access(context.Context, AccessRequest) (AccessTarget, error)
}
