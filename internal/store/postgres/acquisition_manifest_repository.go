package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

const acquisitionManifestColumns = `
manifest_id::text, user_id::text, source_type, source_ref, expected_name,
target_storage_binding_id::text, target_path, asset_id::text, release_id::text,
variant_id::text, job_id::text, state, created_at, updated_at`

type acquisitionManifestDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type AcquisitionManifestRepository struct {
	db acquisitionManifestDB
}

var _ acquisition.ManifestRepository = (*AcquisitionManifestRepository)(nil)

func NewAcquisitionManifestRepository(db acquisitionManifestDB) (*AcquisitionManifestRepository, error) {
	if db == nil {
		return nil, errors.New("acquisition manifest database is required")
	}
	return &AcquisitionManifestRepository{db: db}, nil
}

func (repository *AcquisitionManifestRepository) CreateManifest(ctx context.Context, manifest acquisition.Manifest) error {
	if err := acquisition.ValidateManifest(manifest); err != nil {
		return err
	}
	_, err := repository.db.Exec(ctx, `
INSERT INTO acquisition_manifests (
    manifest_id, user_id, source_type, source_ref, expected_name,
    target_storage_binding_id, target_path, asset_id, release_id, variant_id,
    job_id, state, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
		string(manifest.ID), optionalID(manifest.UserID), manifest.SourceType, manifest.SourceRef, manifest.ExpectedName,
		string(manifest.TargetStorageBindingID), manifest.TargetPath, optionalID(manifest.AssetID),
		optionalID(manifest.ReleaseID), optionalID(manifest.VariantID), optionalID(manifest.JobID),
		string(manifest.State), manifest.CreatedAt, manifest.UpdatedAt,
	)
	if err == nil {
		return nil
	}
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		switch postgresError.Code {
		case "23505":
			return fmt.Errorf("%w: %s", acquisition.ErrConflict, postgresError.ConstraintName)
		case "23503":
			return fmt.Errorf("%w: %s", acquisition.ErrInvalidReference, postgresError.ConstraintName)
		case "23514", "22P02":
			return fmt.Errorf("%w: %s", acquisition.ErrInvalidArgument, postgresError.ConstraintName)
		}
	}
	return manifestPersistenceError("create Manifest", err)
}

func (repository *AcquisitionManifestRepository) GetManifest(ctx context.Context, id acquisition.ManifestID) (acquisition.Manifest, error) {
	if id == "" {
		return acquisition.Manifest{}, acquisition.ErrInvalidArgument
	}
	manifest, err := scanAcquisitionManifest(repository.db.QueryRow(ctx, `SELECT `+acquisitionManifestColumns+`
FROM acquisition_manifests
WHERE manifest_id = $1`, string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		return acquisition.Manifest{}, fmt.Errorf("%w: %s", acquisition.ErrNotFound, id)
	}
	if err != nil {
		return acquisition.Manifest{}, manifestPersistenceError("get Manifest", err)
	}
	return manifest, nil
}

func scanAcquisitionManifest(row pgx.Row) (acquisition.Manifest, error) {
	var manifest acquisition.Manifest
	var manifestID, bindingID string
	var userID, assetID, releaseID, variantID, jobID sql.NullString
	err := row.Scan(
		&manifestID, &userID, &manifest.SourceType, &manifest.SourceRef, &manifest.ExpectedName,
		&bindingID, &manifest.TargetPath, &assetID, &releaseID, &variantID, &jobID,
		&manifest.State, &manifest.CreatedAt, &manifest.UpdatedAt,
	)
	if err != nil {
		return acquisition.Manifest{}, err
	}
	manifest.ID = acquisition.ManifestID(manifestID)
	manifest.TargetStorageBindingID = storage.BindingID(bindingID)
	if userID.Valid {
		value := acquisition.UserID(userID.String)
		manifest.UserID = &value
	}
	if assetID.Valid {
		value := catalog.AssetID(assetID.String)
		manifest.AssetID = &value
	}
	if releaseID.Valid {
		value := catalog.ReleaseID(releaseID.String)
		manifest.ReleaseID = &value
	}
	if variantID.Valid {
		value := catalog.VariantID(variantID.String)
		manifest.VariantID = &value
	}
	if jobID.Valid {
		value := jobs.JobID(jobID.String)
		manifest.JobID = &value
	}
	return manifest, nil
}

func optionalID[T ~string](value *T) *string {
	if value == nil {
		return nil
	}
	converted := string(*value)
	return &converted
}

func manifestPersistenceError(operation string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w: %s: %v", acquisition.ErrPersistence, operation, err)
}
