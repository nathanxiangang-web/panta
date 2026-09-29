package p115

import (
	"errors"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// ProviderID is the frozen provider identity for the 115 adapter. It must equal
// the StorageConnection provider_type used by the Gate 3.5 session registry.
const ProviderID contracts.ProviderID = "115"

// 115 offline-task status codes, frozen by D-025.
const (
	statusTodo    = 0
	statusRunning = 1
	statusDone    = 2
	statusFailed  = -1
)

var (
	ErrInvalidDownloadRequest = errors.New("115 download request is invalid")
	ErrUnsupportedSource      = errors.New("115 source scheme is not supported")
	ErrInvalidTaskReference   = errors.New("115 task reference is invalid")
	ErrTaskReferenceMissing   = errors.New("115 offline task reference was not returned")
	ErrTaskReferenceAmbiguous = errors.New("115 offline task reference is ambiguous")
	ErrTaskNotFound           = errors.New("115 offline task not found")
	ErrTaskStateUnknown       = errors.New("115 offline task state is unknown")
	ErrPaginationUnbounded    = errors.New("115 offline task pagination is malformed or not progressing")
	ErrBackendStart           = errors.New("115 offline task submission failed")
	ErrBackendList            = errors.New("115 offline task listing failed")
	ErrBackendDelete          = errors.New("115 offline task deletion failed")
)

// mapTaskStatus maps only the frozen 115 status set. An unknown code is an
// explicit adapter error rather than a guessed Panta state.
//
// Note there is no 115 code for a canceled offline task: CancelDownload removes
// the task, so a removed task surfaces as ErrTaskNotFound rather than as a
// canceled state.
func mapTaskStatus(status int) (contracts.TaskState, error) {
	switch status {
	case statusTodo:
		return contracts.TaskStatePending, nil
	case statusRunning:
		return contracts.TaskStateRunning, nil
	case statusDone:
		return contracts.TaskStateSucceeded, nil
	case statusFailed:
		return contracts.TaskStateFailed, nil
	default:
		return "", ErrTaskStateUnknown
	}
}
