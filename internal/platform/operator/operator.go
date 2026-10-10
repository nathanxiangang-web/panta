// Package operator exposes two distinct, explicit process actions over the
// accepted runtime graph. Preflight constructs and closes but never runs it.
package operator

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"time"

	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/platform/bootstrap"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
	"github.com/nathanxiangang-web/panta/internal/runtime"
)

type Action string

const (
	Preflight Action = "preflight-acquisition"
	Start     Action = "start-acquisition"
)

var ErrInvalidAction = errors.New("invalid acquisition operator action or configuration")
var ErrAlreadyStarting = errors.New("acquisition operator worker already starting or running")
var startGuard atomic.Bool

// Builder is an integration-test seam. The stock command passes nil and always
// uses the production D-038 constructor. An override cannot originate in CLI
// arguments or the operator JSON file.
type Builder func(context.Context, config.Config, runtime.RuntimeDependencies) (*runtime.Runtime, error)

// Run bounds bootstrap, constructs exactly one graph, and closes it on every
// path. Only Start invokes the worker; Preflight cannot ClaimNext or submit Hint.
func Run(ctx context.Context, action Action, cfg config.Config, bootstrapPath string, builder Builder) error {
	if action != Preflight && action != Start || !cfg.AcquisitionWorker.Enabled ||
		bootstrapPath == "" {
		return ErrInvalidAction
	}
	if err := cfg.Validate(); err != nil {
		return ErrInvalidAction
	}
	if action == Start {
		if !startGuard.CompareAndSwap(false, true) {
			return ErrAlreadyStarting
		}
		defer startGuard.Store(false)
	}
	startupCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	deps, err := bootstrap.Load(startupCtx, bootstrapPath)
	if err != nil {
		return bootstrap.Category(err)
	}
	if action == Start {
		deps.WorkerOptions = append(deps.WorkerOptions, app.WithWorkerEventSink(func(event app.WorkerEvent) {
			if event.Kind == app.WorkerTransientError ||
				(event.Kind == app.WorkerStage && event.DiagnosticCategory != "") {
				log.Printf("acquisition worker event=%s job_id=%s claim=%d stage=%s error_kind=%s first_error_category=%s elapsed_ms=%d cause_type=%s http_status=%d parser_stage=%s response_shape=%s",
					event.Kind, event.JobID, event.ClaimAttempts, event.Stage, event.ErrorKind,
					event.DiagnosticCategory, event.DiagnosticElapsedMS, event.DiagnosticCauseType, event.DiagnosticHTTPStatus, event.DiagnosticStage, event.DiagnosticResponseShape)
			}
		}))
	}
	if builder == nil {
		builder = runtime.NewRuntime
	}
	service, err := builder(startupCtx, cfg, deps)
	if err != nil {
		return err
	}
	if service == nil {
		return ErrInvalidAction
	}
	defer service.Close()
	if action == Preflight {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return service.Run(ctx)
}
