// Package app is the composition boundary for the Panta modular monolith.
package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/nathanxiangang-web/panta/internal/platform/config"
)

// Application owns process-level lifecycle wiring. Module wiring will be added
// here only as its gate is accepted.
type Application struct {
	config config.Config
	worker *AcquisitionWorker
}

var ErrAcquisitionWorkerUnwired = errors.New("acquisition worker enabled without complete runtime dependencies")

// New validates process configuration without constructing real integrations.
func New(cfg config.Config) (*Application, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}
	if cfg.AcquisitionWorker.Enabled {
		return nil, ErrAcquisitionWorkerUnwired
	}
	return &Application{config: cfg}, nil
}

// NewWithAcquisitionWorker is the explicit test/future-composition seam. The
// ordinary cmd/panta path calls New and therefore fails closed if enabled.
func NewWithAcquisitionWorker(cfg config.Config, worker *AcquisitionWorker) (*Application, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}
	if !cfg.AcquisitionWorker.Enabled || worker == nil || worker.policy != cfg.AcquisitionWorker {
		return nil, ErrAcquisitionWorkerUnwired
	}
	return &Application{config: cfg, worker: worker}, nil
}

// Run keeps the skeleton process alive until its context is cancelled.
func (a *Application) Run(ctx context.Context) error {
	if a.worker != nil {
		return a.worker.Run(ctx)
	}
	<-ctx.Done()
	return nil
}
