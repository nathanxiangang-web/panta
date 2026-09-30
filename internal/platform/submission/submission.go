// Package submission is the explicit, enqueue-only operator adapter. It does
// not construct a provider session or start the acquisition worker.
package submission

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/platform/bootstrap"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
	"github.com/nathanxiangang-web/panta/internal/storage"
	"github.com/nathanxiangang-web/panta/internal/store/postgres"
)

var ErrInvalidRequestFile = errors.New("invalid protected acquisition submission request")
var ErrDatabaseUnavailable = errors.New("Panta submission database unavailable")

const maxRequestBytes = 16 << 10

type fileRequest struct {
	ManifestID             string  `json:"manifest_id"`
	JobID                  string  `json:"job_id"`
	MaxAttempts            int     `json:"max_attempts"`
	Source                 string  `json:"source_ref"`
	UserID                 *string `json:"user_id"`
	ExpectedName           *string `json:"expected_name"`
	TargetStorageBindingID string  `json:"target_storage_binding_id"`
	TargetPath             string  `json:"target_path"`
	AssetID                *string `json:"asset_id"`
	ReleaseID              *string `json:"release_id"`
	VariantID              *string `json:"variant_id"`
}

// Run reads one owner-protected request, checks schema v12, and submits intent.
// It never loads 115 secrets, contacts external services, or starts a worker.
func Run(ctx context.Context, cfg config.Config, requestPath string) (acquisition.SubmitResult, error) {
	content, err := bootstrap.ReadProtectedFile(requestPath, maxRequestBytes)
	if err != nil {
		return acquisition.SubmitResult{}, ErrInvalidRequestFile
	}
	defer clear(content)
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var input fileRequest
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		return acquisition.SubmitResult{}, ErrInvalidRequestFile
	}
	request := acquisition.SubmitRequest{
		ManifestID: acquisition.ManifestID(input.ManifestID), JobID: jobs.JobID(input.JobID),
		MaxAttempts: input.MaxAttempts, Source: input.Source,
		TargetStorageBindingID: storage.BindingID(input.TargetStorageBindingID), TargetPath: input.TargetPath,
		ExpectedName: input.ExpectedName,
	}
	if input.UserID != nil {
		value := acquisition.UserID(*input.UserID)
		request.UserID = &value
	}
	if input.AssetID != nil {
		value := catalog.AssetID(*input.AssetID)
		request.AssetID = &value
	}
	if input.ReleaseID != nil {
		value := catalog.ReleaseID(*input.ReleaseID)
		request.ReleaseID = &value
	}
	if input.VariantID != nil {
		value := catalog.VariantID(*input.VariantID)
		request.VariantID = &value
	}
	if _, err := acquisition.ResolveSource(input.Source); err != nil {
		return acquisition.SubmitResult{}, acquisition.ErrInvalidSubmission
	}
	if cfg.DatabaseURL == "" {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	pool, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	defer pool.Close()
	migrator, err := postgres.NewMigrator(pool)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	status, err := migrator.Status(ctx)
	if err != nil || !status.Compatible || status.CurrentVersion != 12 || status.LatestVersion != 12 {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	bindings, err := postgres.NewStorageRepository(pool)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	identities, err := postgres.NewCatalogRepository(pool)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	manifests, err := postgres.NewAcquisitionManifestRepository(pool)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	activationStore, err := postgres.NewAcquisitionActivationRepository(pool)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	jobStore, err := postgres.NewJobRepository(pool)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	creator, err := acquisition.NewService(bindings, identities, manifests)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	activator, err := acquisition.NewActivationService(activationStore)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	service, err := acquisition.NewSubmissionService(creator, manifests, activator, jobStore)
	if err != nil {
		return acquisition.SubmitResult{}, ErrDatabaseUnavailable
	}
	return service.Submit(ctx, request)
}

func clear(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
