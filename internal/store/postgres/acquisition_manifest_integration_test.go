package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestPostgresAcquisitionManifestRoundTripAndConstraints(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 12 || status.LatestVersion != 12 || len(status.Applied) != 12 {
		t.Fatalf("Apply() = %#v, %v", status, err)
	}

	bindingID := storage.BindingID("23000000-0000-4000-8000-000000000001")
	seedStorageBinding(t, ctx, pool, bindingID, "acquisition-root")
	catalogRepository, err := NewCatalogRepository(pool)
	if err != nil {
		t.Fatalf("NewCatalogRepository() error = %v", err)
	}
	now := time.Date(2026, 9, 29, 13, 0, 0, 0, time.UTC)
	asset := catalog.Asset{ID: "23000000-0000-4000-8000-000000000010", CanonicalName: "acquisition", Category: "software", Status: "ACTIVE", CreatedAt: now, UpdatedAt: now}
	release := catalog.Release{ID: "23000000-0000-4000-8000-000000000020", AssetID: asset.ID, VersionRaw: "1", VersionScheme: catalog.VersionSchemeRevision, Channel: "stable", Status: "ACTIVE", CreatedAt: now, UpdatedAt: now}
	variant := catalog.Variant{ID: "23000000-0000-4000-8000-000000000030", ReleaseID: release.ID, VariantKey: "default", Attributes: json.RawMessage(`{}`), Status: "ACTIVE", CreatedAt: now, UpdatedAt: now}
	if err := catalogRepository.CreateAsset(ctx, asset); err != nil {
		t.Fatalf("CreateAsset() error = %v", err)
	}
	if err := catalogRepository.CreateRelease(ctx, release); err != nil {
		t.Fatalf("CreateRelease() error = %v", err)
	}
	if err := catalogRepository.CreateVariant(ctx, variant); err != nil {
		t.Fatalf("CreateVariant() error = %v", err)
	}
	jobRepository, err := NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}
	job, err := jobRepository.Create(ctx, jobs.CreateRequest{
		ID: "23000000-0000-4000-8000-000000000040", Type: "future-acquisition", Payload: json.RawMessage(`{}`), MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("Create(job) error = %v", err)
	}

	repository, err := NewAcquisitionManifestRepository(pool)
	if err != nil {
		t.Fatalf("NewAcquisitionManifestRepository() error = %v", err)
	}
	userID := acquisition.UserID("23000000-0000-4000-8000-000000000050")
	expectedName := "package.iso"
	jobID := job.ID
	full := acquisition.Manifest{
		ID: "23000000-0000-4000-8000-000000000060", UserID: &userID,
		SourceType: "opaque-source", SourceRef: "opaque://provider-owned-value", ExpectedName: &expectedName,
		TargetStorageBindingID: bindingID, TargetPath: "/downloads/package.iso",
		AssetID: &asset.ID, ReleaseID: &release.ID, VariantID: &variant.ID, JobID: &jobID,
		State: acquisition.StateActive, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateManifest(ctx, full); err != nil {
		t.Fatalf("CreateManifest(full) error = %v", err)
	}
	got, err := repository.GetManifest(ctx, full.ID)
	if err != nil {
		t.Fatalf("GetManifest(full) error = %v", err)
	}
	assertManifestRoundTrip(t, got, full)

	minimal := acquisition.Manifest{
		ID: "23000000-0000-4000-8000-000000000061", SourceType: "direct", SourceRef: "unclassified",
		TargetStorageBindingID: bindingID, TargetPath: "/", State: acquisition.StatePending,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateManifest(ctx, minimal); err != nil {
		t.Fatalf("CreateManifest(minimal) error = %v", err)
	}
	got, err = repository.GetManifest(ctx, minimal.ID)
	if err != nil {
		t.Fatalf("GetManifest(minimal) error = %v", err)
	}
	assertManifestRoundTrip(t, got, minimal)
	if got.UserID != nil || got.ExpectedName != nil || got.AssetID != nil || got.ReleaseID != nil || got.VariantID != nil || got.JobID != nil {
		t.Fatalf("nullable fields did not remain nil: %#v", got)
	}

	if _, err := repository.GetManifest(ctx, "23000000-0000-4000-8000-000000000099"); !errors.Is(err, acquisition.ErrNotFound) {
		t.Fatalf("GetManifest(missing) error = %v", err)
	}

	missingReferences := []struct {
		name   string
		mutate func(*acquisition.Manifest)
	}{
		{name: "StorageBinding", mutate: func(value *acquisition.Manifest) {
			value.TargetStorageBindingID = "23000000-0000-4000-8000-000000000090"
			value.JobID = nil
			value.State = acquisition.StatePending
		}},
		{name: "Asset", mutate: func(value *acquisition.Manifest) {
			missing := catalog.AssetID("23000000-0000-4000-8000-000000000091")
			value.AssetID, value.ReleaseID, value.VariantID, value.JobID = &missing, nil, nil, nil
			value.State = acquisition.StatePending
		}},
		{name: "Release", mutate: func(value *acquisition.Manifest) {
			missing := catalog.ReleaseID("23000000-0000-4000-8000-000000000092")
			value.ReleaseID, value.VariantID, value.JobID = &missing, nil, nil
			value.State = acquisition.StatePending
		}},
		{name: "Variant", mutate: func(value *acquisition.Manifest) {
			missing := catalog.VariantID("23000000-0000-4000-8000-000000000093")
			value.VariantID, value.JobID = &missing, nil
			value.State = acquisition.StatePending
		}},
		{name: "Job", mutate: func(value *acquisition.Manifest) {
			missing := jobs.JobID("23000000-0000-4000-8000-000000000094")
			value.JobID = &missing
		}},
	}
	for index, test := range missingReferences {
		t.Run("missing "+test.name, func(t *testing.T) {
			candidate := full
			candidate.ID = acquisition.ManifestID([]string{
				"23000000-0000-4000-8000-000000000070", "23000000-0000-4000-8000-000000000071",
				"23000000-0000-4000-8000-000000000072", "23000000-0000-4000-8000-000000000073",
				"23000000-0000-4000-8000-000000000074",
			}[index])
			test.mutate(&candidate)
			if err := repository.CreateManifest(ctx, candidate); !errors.Is(err, acquisition.ErrInvalidReference) {
				t.Fatalf("CreateManifest() error = %v, want ErrInvalidReference", err)
			}
		})
	}

	duplicateJob := minimal
	duplicateJob.ID = "23000000-0000-4000-8000-000000000080"
	duplicateJob.JobID = &jobID
	duplicateJob.State = acquisition.StateActive
	if err := repository.CreateManifest(ctx, duplicateJob); !errors.Is(err, acquisition.ErrConflict) {
		t.Fatalf("duplicate job_id error = %v", err)
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id,
    target_path, state, created_at, updated_at
) VALUES ($1, 'direct', 'opaque', $2, '/', 'UNKNOWN', $3, $3)`,
		"23000000-0000-4000-8000-000000000081", bindingID, now,
	); err == nil {
		t.Fatal("database accepted invalid Manifest state")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id,
    target_path, state, created_at, updated_at
) VALUES ($1, 'direct', 'opaque', $2, '/bad//path', 'PENDING', $3, $3)`,
		"23000000-0000-4000-8000-000000000082", bindingID, now,
	); err == nil {
		t.Fatal("database accepted non-normalized target_path")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id,
    target_path, job_id, state, created_at, updated_at
) VALUES ($1, 'direct', 'opaque', $2, '/', $3, 'PENDING', $4, $4)`,
		"23000000-0000-4000-8000-000000000083", bindingID, jobID, now,
	); err == nil {
		t.Fatal("database accepted PENDING Manifest with Job link")
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, source_type, source_ref, target_storage_binding_id,
    target_path, state, created_at, updated_at
) VALUES ($1, 'direct', 'opaque', $2, '/', 'ACTIVE', $3, $3)`,
		"23000000-0000-4000-8000-000000000084", bindingID, now,
	); err == nil {
		t.Fatal("database accepted ACTIVE Manifest without Job link")
	}

	var manifestIDDefault sql.NullString
	if err := pool.QueryRow(ctx, `
SELECT column_default
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'acquisition_manifests' AND column_name = 'manifest_id'`).Scan(&manifestIDDefault); err != nil {
		t.Fatalf("inspect manifest_id default: %v", err)
	}
	if manifestIDDefault.Valid {
		t.Fatalf("manifest_id has database default %q", manifestIDDefault.String)
	}
}

func assertManifestRoundTrip(t *testing.T, got, want acquisition.Manifest) {
	t.Helper()
	if got.ID != want.ID || got.SourceType != want.SourceType || got.SourceRef != want.SourceRef ||
		got.TargetStorageBindingID != want.TargetStorageBindingID || got.TargetPath != want.TargetPath ||
		got.State != want.State || !got.CreatedAt.Equal(want.CreatedAt) || !got.UpdatedAt.Equal(want.UpdatedAt) ||
		optionalValue(got.UserID) != optionalValue(want.UserID) || optionalValue(got.ExpectedName) != optionalValue(want.ExpectedName) ||
		optionalValue(got.AssetID) != optionalValue(want.AssetID) || optionalValue(got.ReleaseID) != optionalValue(want.ReleaseID) ||
		optionalValue(got.VariantID) != optionalValue(want.VariantID) || optionalValue(got.JobID) != optionalValue(want.JobID) {
		t.Fatalf("Manifest round-trip mismatch: got=%#v want=%#v", got, want)
	}
}

func optionalValue[T ~string](value *T) string {
	if value == nil {
		return ""
	}
	return string(*value)
}
