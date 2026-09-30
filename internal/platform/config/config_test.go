package config

import (
	"testing"
	"time"
)

func TestLoadReadsIndexCoreBaseURL(t *testing.T) {
	t.Setenv("PANTA_ENV", "test")
	t.Setenv("PANTA_DATABASE_URL", "postgres://example")
	t.Setenv("PANTA_INDEXCORE_BASE_URL", " http://indexcore.internal:8080/base ")

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.IndexCoreBaseURL != "http://indexcore.internal:8080/base" {
		t.Fatalf("IndexCoreBaseURL = %q", got.IndexCoreBaseURL)
	}
}

func TestAcquisitionWorkerDisabledByDefault(t *testing.T) {
	t.Setenv("PANTA_ACQUISITION_WORKER_ENABLED", "")
	cfg, err := Load()
	if err != nil || cfg.AcquisitionWorker.Enabled {
		t.Fatalf("default worker = %#v, %v", cfg.AcquisitionWorker, err)
	}
}

func TestAcquisitionWorkerConfigurationBounds(t *testing.T) {
	worker := AcquisitionWorker{Enabled: true, OwnerPrefix: "worker", Interval: MinAcquisitionWorkerInterval,
		TickTimeout: MinAcquisitionTickTimeout, LeaseDuration: MinAcquisitionTickTimeout + AcquisitionLeaseSafetyMargin,
		RetryDelay: MinAcquisitionWorkerInterval, RecoveryLimit: 1, ProjectorPageLimit: 1}
	if err := worker.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*AcquisitionWorker){
		func(c *AcquisitionWorker) { c.OwnerPrefix = "" },
		func(c *AcquisitionWorker) { c.OwnerPrefix = "bad owner" },
		func(c *AcquisitionWorker) { c.Interval = 0 },
		func(c *AcquisitionWorker) { c.Interval = time.Millisecond },
		func(c *AcquisitionWorker) { c.TickTimeout = 0 },
		func(c *AcquisitionWorker) { c.LeaseDuration = c.TickTimeout },
		func(c *AcquisitionWorker) { c.RetryDelay = 0 },
		func(c *AcquisitionWorker) { c.RecoveryLimit = 0 },
		func(c *AcquisitionWorker) { c.ProjectorPageLimit = MaxAcquisitionProjectorPageLimit + 1 },
	} {
		invalid := worker
		mutate(&invalid)
		if err := invalid.Validate(); err == nil {
			t.Fatalf("invalid worker accepted: %#v", invalid)
		}
	}
}

func TestLoadEnabledAcquisitionWorkerSettings(t *testing.T) {
	t.Setenv("PANTA_ACQUISITION_WORKER_ENABLED", "true")
	t.Setenv("PANTA_ACQUISITION_WORKER_OWNER_PREFIX", "node-a")
	t.Setenv("PANTA_ACQUISITION_WORKER_INTERVAL", "2s")
	t.Setenv("PANTA_ACQUISITION_WORKER_TICK_TIMEOUT", "20s")
	t.Setenv("PANTA_ACQUISITION_WORKER_LEASE_DURATION", "1m")
	t.Setenv("PANTA_ACQUISITION_WORKER_RETRY_DELAY", "3s")
	t.Setenv("PANTA_ACQUISITION_WORKER_RECOVERY_LIMIT", "5")
	t.Setenv("PANTA_ACQUISITION_WORKER_PROJECTOR_PAGE_LIMIT", "25")
	cfg, err := Load()
	if err != nil || !cfg.AcquisitionWorker.Enabled || cfg.AcquisitionWorker.OwnerPrefix != "node-a" ||
		cfg.AcquisitionWorker.Interval != 2*time.Second || cfg.AcquisitionWorker.RecoveryLimit != 5 ||
		cfg.AcquisitionWorker.ProjectorPageLimit != 25 {
		t.Fatalf("loaded worker = %#v, %v", cfg.AcquisitionWorker, err)
	}
	t.Setenv("PANTA_ACQUISITION_WORKER_INTERVAL", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("malformed interval was accepted")
	}
}
