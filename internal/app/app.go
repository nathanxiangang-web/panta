// Package app is the composition boundary for the Panta modular monolith.
package app

import (
	"context"
	"fmt"

	"github.com/nathanxiangang-web/panta/internal/platform/config"
)

// Application owns process-level lifecycle wiring. Module wiring will be added
// here only as its gate is accepted.
type Application struct {
	config config.Config
}

// New validates process configuration without constructing real integrations.
func New(cfg config.Config) (*Application, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}
	return &Application{config: cfg}, nil
}

// Run keeps the skeleton process alive until its context is cancelled.
func (a *Application) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}
