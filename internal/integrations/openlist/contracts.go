// Package openlist defines the Panta-owned boundary for storage visibility and
// access facts exposed by OpenList, and the concrete HTTP read client behind it.
//
// This package performs exact known-path lookups only: it never lists, walks, or
// searches storage, and it never touches the OpenList database.
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
//
// A fact with Visible=false is a normal observation: OpenList does not currently
// expose that exact path. It is never a transport, authorization, storage, or
// provider failure, which are reported as errors instead.
type VisibilityFact struct {
	Mount      string
	Path       string
	Visible    bool
	Name       string
	Directory  bool
	SizeBytes  int64
	ModifiedAt *time.Time
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
