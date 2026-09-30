// Package config defines process configuration independently of transport and
// provider implementations.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const defaultEnvironment = "development"

const (
	MinAcquisitionWorkerInterval     = 100 * time.Millisecond
	MaxAcquisitionWorkerInterval     = 5 * time.Minute
	MinAcquisitionTickTimeout        = time.Second
	MaxAcquisitionTickTimeout        = 10 * time.Minute
	AcquisitionLeaseSafetyMargin     = 5 * time.Second
	MaxAcquisitionLeaseDuration      = time.Hour
	MaxAcquisitionRecoveryLimit      = 100
	MaxAcquisitionProjectorPageLimit = 1000
)

// AcquisitionWorker configures only the lifecycle. It contains no provider
// credentials or live integration endpoints, and is disabled unless opted in.
type AcquisitionWorker struct {
	Enabled            bool
	OwnerPrefix        string
	Interval           time.Duration
	TickTimeout        time.Duration
	LeaseDuration      time.Duration
	RetryDelay         time.Duration
	RecoveryLimit      int
	ProjectorPageLimit int
}

func (worker AcquisitionWorker) Validate() error {
	if !worker.Enabled {
		return nil
	}
	if worker.OwnerPrefix == "" || len(worker.OwnerPrefix) > 64 ||
		!isSafeOwnerPrefix(worker.OwnerPrefix) {
		return errors.New("acquisition worker owner prefix must be 1-64 ASCII letters, digits, hyphens or underscores")
	}
	if worker.Interval < MinAcquisitionWorkerInterval || worker.Interval > MaxAcquisitionWorkerInterval ||
		worker.TickTimeout < MinAcquisitionTickTimeout || worker.TickTimeout > MaxAcquisitionTickTimeout ||
		worker.LeaseDuration > MaxAcquisitionLeaseDuration ||
		worker.LeaseDuration < worker.TickTimeout+AcquisitionLeaseSafetyMargin ||
		worker.RetryDelay < worker.Interval || worker.RetryDelay > MaxAcquisitionWorkerInterval ||
		worker.RecoveryLimit < 1 || worker.RecoveryLimit > MaxAcquisitionRecoveryLimit ||
		worker.ProjectorPageLimit < 1 || worker.ProjectorPageLimit > MaxAcquisitionProjectorPageLimit {
		return errors.New("invalid acquisition worker interval, timeout, lease, retry or batch limits")
	}
	return nil
}

func isSafeOwnerPrefix(value string) bool {
	for _, char := range value {
		if char > unicode.MaxASCII || !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char == '-' || char == '_') {
			return false
		}
	}
	return true
}

// Config contains the process settings needed by the Gate 0.1 skeleton.
type Config struct {
	Environment       string
	DatabaseURL       string
	IndexCoreBaseURL  string
	AcquisitionWorker AcquisitionWorker
}

// Load reads configuration from environment variables and applies safe local
// defaults. It does not read provider credentials.
func Load() (Config, error) {
	environment := strings.TrimSpace(os.Getenv("PANTA_ENV"))
	if environment == "" {
		environment = defaultEnvironment
	}

	cfg := Config{
		Environment:      environment,
		DatabaseURL:      strings.TrimSpace(os.Getenv("PANTA_DATABASE_URL")),
		IndexCoreBaseURL: strings.TrimSpace(os.Getenv("PANTA_INDEXCORE_BASE_URL")),
	}
	enabled, err := strconv.ParseBool(strings.TrimSpace(os.Getenv("PANTA_ACQUISITION_WORKER_ENABLED")))
	if err != nil && strings.TrimSpace(os.Getenv("PANTA_ACQUISITION_WORKER_ENABLED")) != "" {
		return Config{}, fmt.Errorf("invalid PANTA_ACQUISITION_WORKER_ENABLED: %w", err)
	}
	if enabled {
		cfg.AcquisitionWorker = AcquisitionWorker{
			Enabled: true, OwnerPrefix: envOrDefault("PANTA_ACQUISITION_WORKER_OWNER_PREFIX", "panta-acquisition"),
			Interval: 5 * time.Second, TickTimeout: 30 * time.Second, LeaseDuration: 2 * time.Minute,
			RetryDelay: 10 * time.Second, RecoveryLimit: 10, ProjectorPageLimit: 100,
		}
		for _, setting := range []struct {
			key    string
			target *time.Duration
		}{
			{"PANTA_ACQUISITION_WORKER_INTERVAL", &cfg.AcquisitionWorker.Interval},
			{"PANTA_ACQUISITION_WORKER_TICK_TIMEOUT", &cfg.AcquisitionWorker.TickTimeout},
			{"PANTA_ACQUISITION_WORKER_LEASE_DURATION", &cfg.AcquisitionWorker.LeaseDuration},
			{"PANTA_ACQUISITION_WORKER_RETRY_DELAY", &cfg.AcquisitionWorker.RetryDelay},
		} {
			if value := strings.TrimSpace(os.Getenv(setting.key)); value != "" {
				parsed, parseErr := time.ParseDuration(value)
				if parseErr != nil {
					return Config{}, fmt.Errorf("invalid %s: %w", setting.key, parseErr)
				}
				*setting.target = parsed
			}
		}
		for _, setting := range []struct {
			key    string
			target *int
		}{
			{"PANTA_ACQUISITION_WORKER_RECOVERY_LIMIT", &cfg.AcquisitionWorker.RecoveryLimit},
			{"PANTA_ACQUISITION_WORKER_PROJECTOR_PAGE_LIMIT", &cfg.AcquisitionWorker.ProjectorPageLimit},
		} {
			if value := strings.TrimSpace(os.Getenv(setting.key)); value != "" {
				parsed, parseErr := strconv.Atoi(value)
				if parseErr != nil {
					return Config{}, fmt.Errorf("invalid %s: %w", setting.key, parseErr)
				}
				*setting.target = parsed
			}
		}
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// Validate checks process-level invariants.
func (c Config) Validate() error {
	if strings.TrimSpace(c.Environment) == "" {
		return errors.New("environment is required")
	}
	return c.AcquisitionWorker.Validate()
}
