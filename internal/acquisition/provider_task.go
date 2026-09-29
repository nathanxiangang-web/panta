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
	// ErrProviderTaskContention reports that the durable fence lock could not be
	// held in time, so no authorization to start an external side effect was
	// granted. Callers must treat it as fail-closed, not as permission.
	ErrProviderTaskContention = errors.New("acquisition provider task fence contention")
)

// ProviderTaskState records side-effect certainty only. It never mirrors provider
// task lifecycle status, which stays authoritative at the provider.
type ProviderTaskState string

const (
	// ProviderTaskStartReserved records that one execution claimed the right to
	// call StartDownload for this Manifest and Job. The opaque reference is not
	// yet known, so any later execution must fail closed instead of starting a
	// second external task.
	ProviderTaskStartReserved ProviderTaskState = "START_RESERVED"
	// ProviderTaskReferenceKnown records a durably committed opaque reference.
	ProviderTaskReferenceKnown ProviderTaskState = "REFERENCE_KNOWN"
)

func (state ProviderTaskState) Valid() bool {
	return state == ProviderTaskStartReserved || state == ProviderTaskReferenceKnown
}

// ProviderTask is the durable linkage between one Manifest, its ACQUISITION Job,
// and exactly one opaque external provider task. It carries no provider status:
// the provider stays authoritative for that.
type ProviderTask struct {
	ManifestID      ManifestID
	JobID           jobs.JobID
	ProviderID      contracts.ProviderID
	ProviderTaskRef string
	State           ProviderTaskState
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

// ReferenceKnown reports whether a durable opaque reference is available.
func (task ProviderTask) ReferenceKnown() bool {
	return task.State == ProviderTaskReferenceKnown && task.ProviderTaskRef != ""
}

// ValidateProviderTask protects the store port from invalid direct callers.
func ValidateProviderTask(task ProviderTask) error {
	if task.ManifestID == "" || task.JobID == "" || !task.ProviderID.Valid() || !task.State.Valid() ||
		task.CreatedAt.IsZero() || task.UpdatedAt.IsZero() || task.UpdatedAt.Before(task.CreatedAt) {
		return ErrInvalidProviderTask
	}
	switch task.State {
	case ProviderTaskStartReserved:
		if task.ProviderTaskRef != "" {
			return ErrInvalidProviderTask
		}
	case ProviderTaskReferenceKnown:
		if !ValidProviderTaskRef(task.ProviderTaskRef) {
			return ErrInvalidProviderTask
		}
	}
	return nil
}

// ValidProviderTaskRef reports whether an opaque provider task reference is
// non-empty, bounded, NUL-free text. The value itself is never interpreted.
func ValidProviderTaskRef(value string) bool {
	return utf8.ValidString(value) && strings.TrimSpace(value) != "" && !strings.ContainsRune(value, '\x00') &&
		utf8.RuneCountInString(value) <= MaxProviderTaskRefLength
}

// ProviderTaskClaimRequest asks the store to fence the external side effect.
//
// Reference must be empty for a start claim and valid for a reference commit.
type ProviderTaskClaimRequest struct {
	ManifestID ManifestID
	JobID      jobs.JobID
	ProviderID contracts.ProviderID
	Reference  string
	Now        time.Time
}

// ProviderTaskClaimResult reports what the durable store decided.
type ProviderTaskClaimResult struct {
	// Task is the durable row that now exists for the Manifest.
	Task ProviderTask
	// ClaimedStart is true only when this call exclusively reserved the right to
	// call StartDownload. Exactly one caller can observe this for a Manifest.
	ClaimedStart bool
	// CommittedReference is true when Reference was newly committed durably.
	CommittedReference bool
}

// ProviderTaskStore persists the durable provider-task linkage and fences the
// external side effect.
//
// ClaimProviderTask is the only way to obtain permission to call StartDownload.
// It must be atomic with respect to concurrent callers for the same Manifest and
// must enforce these semantics:
//
//	no row                              -> persist START_RESERVED, ClaimedStart=true
//	START_RESERVED, Reference valid     -> persist the reference as
//	                                       REFERENCE_KNOWN, CommittedReference=true
//	REFERENCE_KNOWN, Reference empty    -> return the known task, ClaimedStart=false
//	REFERENCE_KNOWN, different reference-> ErrProviderTaskIdentityChange
//	START_RESERVED, Reference empty     -> ErrInvalidProviderTask
//
// A start claim whose reference is never committed must remain durable as
// START_RESERVED so that later executions fail closed rather than starting a
// second external task.
type ProviderTaskStore interface {
	GetProviderTask(context.Context, ManifestID) (ProviderTask, error)
	ClaimProviderTask(context.Context, ProviderTaskClaimRequest) (ProviderTaskClaimResult, error)
}
