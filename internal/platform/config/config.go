// Package config defines process configuration independently of transport and
// provider implementations.
package config

import (
	"errors"
	"os"
	"strings"
)

const defaultEnvironment = "development"

// Config contains the process settings needed by the Gate 0.1 skeleton.
type Config struct {
	Environment string
}

// Load reads configuration from environment variables and applies safe local
// defaults. It does not read provider credentials.
func Load() (Config, error) {
	environment := strings.TrimSpace(os.Getenv("PANTA_ENV"))
	if environment == "" {
		environment = defaultEnvironment
	}

	cfg := Config{Environment: environment}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks process-level invariants.
func (c Config) Validate() error {
	if strings.TrimSpace(c.Environment) == "" {
		return errors.New("environment is required")
	}
	return nil
}
