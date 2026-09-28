package config

import "testing"

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
