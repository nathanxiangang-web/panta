package acquisition

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// Opaque provider task reference and identity bounds. These values are stored
// exactly as the provider returned them and are never interpreted by Panta.
const (
	MaxProviderTaskRefLength = 1024
	MaxProviderIDLength      = contracts.MaxProviderIDLength
)

var (
	ErrInvalidProviderTask        = errors.New("invalid acquisition provider task")
	ErrProviderTaskNotFound       = errors.New("acquisition provider task not found")
	ErrProviderTaskConflict       = errors.New("acquisition provider task conflicts with existing state")
	ErrProviderTaskPersistence    = errors.New("acquisition provider task persistence failure")
	ErrProviderTaskIdentityChange = errors.New("acquisition provider task identity change")
)

// ProviderTask is the durable linkage between one Manifest, its ACQUISITION Job,
// and exactly one opaque external provider task. It carries no provider status:
// the provider stays authoritative for that.
type ProviderTask struct {
	ManifestID      ManifestID
	JobID           jobs.JobID
	ProviderID      contracts.ProviderID
	ProviderTaskRef string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ValidateProviderTask protects the store port from invalid direct callers.
func ValidateProviderTask(task ProviderTask) error {
	if task.ManifestID == "" || task.JobID == "" || !task.ProviderID.Valid() ||
		!ValidProviderTaskRef(task.ProviderTaskRef) || task.CreatedAt.IsZero() || task.UpdatedAt.IsZero() {
		return ErrInvalidProviderTask
	}
	if task.UpdatedAt.Before(task.CreatedAt) {
		return ErrInvalidProviderTask
	}
	return nil
}

// ValidProviderTaskRef reports whether an opaque provider task reference is
// non-empty, bounded, NUL-free text. The value itself is never interpreted.
func ValidProviderTaskRef(value string) bool {
	return utf8.ValidString(value) && strings.TrimSpace(value) != "" && !strings.ContainsRune(value, '\x00') &&
		utf8.RuneCountInString(value) <= MaxProviderTaskRefLength
}

// ProviderTaskStore persists the durable provider-task linkage.
//
// StoreProviderTask must fail closed with ErrProviderTaskIdentityChange when a
// row already exists for the Manifest or the Job and the recorded identity does
// not match the supplied task. It must never overwrite a known task reference.
type ProviderTaskStore interface {
	GetProviderTask(context.Context, ManifestID) (ProviderTask, error)
	StoreProviderTask(context.Context, ProviderTask) error
}
