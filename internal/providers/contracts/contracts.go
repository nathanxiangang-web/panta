// Package contracts defines provider-neutral capabilities and ports owned by
// Panta. Provider adapters depend on this package; core packages never depend
// on provider implementations.
package contracts

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

// ErrUnsupportedCapability lets an adapter report that an optional operation
// is not available without leaking provider-specific error types.
var ErrUnsupportedCapability = errors.New("unsupported provider capability")

// ProviderID identifies a configured provider implementation without exposing
// provider-specific identifiers in core contracts.
type ProviderID string

const MaxProviderIDLength = 128

func (id ProviderID) Valid() bool {
	value := string(id)
	trimmed := strings.TrimSpace(value)
	return utf8.ValidString(value) && trimmed != "" && trimmed == value && !strings.ContainsRune(value, '\x00') &&
		utf8.RuneCountInString(value) <= MaxProviderIDLength
}

// CapabilitySet advertises the product capabilities implemented by a provider.
type CapabilitySet struct {
	Storage    bool
	Downloader bool
	Sharing    bool
}

// Descriptor identifies a provider and the capabilities it exposes to Panta.
type Descriptor struct {
	ID           ProviderID
	DisplayName  string
	Capabilities CapabilitySet
}

// TargetPath identifies a location inside a product-owned target scope.
type TargetPath struct {
	Scope string
	Path  string
}

// SourceReference is an opaque acquisition source understood by an adapter.
// Scheme describes the source class; Value is never interpreted by the domain.
type SourceReference struct {
	Scheme string
	Value  string
}

// StorageObjectReference identifies an object through provider-neutral scope
// and path concepts.
type StorageObjectReference struct {
	Scope string
	Path  string
}

// StorageObject contains the facts needed to verify a stored result.
type StorageObject struct {
	Reference StorageObjectReference
	Name      string
	SizeBytes int64
	Directory bool
}

// AccessTarget is a time-bounded or indirect location from which a resource can
// be accessed. Consumers must not assume it is a permanent URL.
type AccessTarget struct {
	Location  string
	ExpiresAt time.Time
}

// DownloadRequest describes a provider-neutral acquisition request.
type DownloadRequest struct {
	Source SourceReference
	Target TargetPath
}

// TaskReference is an opaque reference returned by a provider operation.
type TaskReference struct {
	Value string
}

// TaskState represents only the externally observable provider task lifecycle.
type TaskState string

const (
	TaskStatePending   TaskState = "pending"
	TaskStateRunning   TaskState = "running"
	TaskStateSucceeded TaskState = "succeeded"
	TaskStateFailed    TaskState = "failed"
	TaskStateCanceled  TaskState = "canceled"
)

// DownloadResult is the optional provider-neutral identity of an acquired
// top-level object.
//
// It carries exactly one direct-child name. It deliberately excludes provider file
// IDs, provider directory IDs, OpenList paths, and IndexCore resource IDs: this is
// descriptive identity only, and it proves no canonical presence.
type DownloadResult struct {
	Name string
}

// TaskStatus reports provider task progress without defining Panta job state.
//
// Result is optional: pending, running, failed, and canceled tasks may omit it. A
// succeeded task reports the exact top-level acquired object name when the provider
// exposes one.
type TaskStatus struct {
	Reference TaskReference
	State     TaskState
	Message   string
	Result    *DownloadResult
}

// ShareRequest identifies the stored object to share.
type ShareRequest struct {
	Object StorageObjectReference
}

// ShareReference is an opaque provider share reference.
type ShareReference struct {
	Value string
}

// ShareDetails contains the provider-neutral facts returned when inspecting a
// share. A zero expiry means the provider did not report an expiry.
type ShareDetails struct {
	Reference ShareReference
	Object    StorageObjectReference
	Active    bool
	CreatedAt time.Time
	ExpiresAt time.Time
}

// DescribedProvider exposes product-level provider identity and capabilities.
type DescribedProvider interface {
	Descriptor() Descriptor
}

// StorageProvider reads provider storage facts and access targets.
type StorageProvider interface {
	DescribedProvider
	Stat(context.Context, TargetPath) (StorageObject, error)
	Access(context.Context, StorageObjectReference) (AccessTarget, error)
}

// DownloaderProvider starts and observes provider acquisition tasks.
type DownloaderProvider interface {
	DescribedProvider
	StartDownload(context.Context, DownloadRequest) (TaskReference, error)
	DownloadStatus(context.Context, TaskReference) (TaskStatus, error)
	CancelDownload(context.Context, TaskReference) error
}

// ShareProvider creates, inspects, resolves, and revokes provider shares.
type ShareProvider interface {
	DescribedProvider
	CreateShare(context.Context, ShareRequest) (ShareReference, error)
	InspectShare(context.Context, ShareReference) (ShareDetails, error)
	ShareAccess(context.Context, ShareReference) (AccessTarget, error)
	RevokeShare(context.Context, ShareReference) error
}
