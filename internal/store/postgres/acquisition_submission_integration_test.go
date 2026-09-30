package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestPostgresAcquisitionSubmissionExactSourceAndPendingRetry(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)
	applyIntegrationMigrations(t, ctx, pool)
	bindingID := storage.BindingID("25000000-0000-4000-8000-000000000201")
	seedStorageBinding(t, ctx, pool, bindingID, "submission-root")
	if _, err := pool.Exec(ctx, `UPDATE storage_bindings SET provider_scope = 'test-scope' WHERE storage_binding_id = $1`, string(bindingID)); err != nil {
		t.Fatal(err)
	}
	bindings, _ := NewStorageRepository(pool)
	identities, _ := NewCatalogRepository(pool)
	manifests, _ := NewAcquisitionManifestRepository(pool)
	activationStore, _ := NewAcquisitionActivationRepository(pool)
	jobStore, _ := NewJobRepository(pool)
	creator, _ := acquisition.NewService(bindings, identities, manifests)
	activator, _ := acquisition.NewActivationService(activationStore)
	service, _ := acquisition.NewSubmissionService(creator, manifests, activator, jobStore)
	request := acquisition.SubmitRequest{
		ManifestID: "25000000-0000-4000-8000-000000000202",
		JobID:      jobs.JobID("25000000-0000-4000-8000-000000000203"), MaxAttempts: 3,
		Source:                 `magnet:?xt=urn:btih:ABC&dn=A%20B&tr=https%3A%2F%2Ftracker.example%2Fa&x=keep`,
		TargetStorageBindingID: bindingID, TargetPath: "/downloads",
	}
	activationStore.afterJobInsert = func(context.Context, pgx.Tx) error { return errors.New("injected rollback") }
	first, err := service.Submit(ctx, request)
	if !errors.Is(err, acquisition.ErrActivationPending) || first.Status != acquisition.SubmissionPendingActivation {
		t.Fatalf("failed activation = %#v, %v", first, err)
	}
	pending, err := manifests.GetManifest(ctx, request.ManifestID)
	if err != nil || pending.State != acquisition.StatePending || pending.JobID != nil || pending.SourceRef != request.Source {
		t.Fatalf("PENDING intent lost: %#v, %v", pending, err)
	}
	assertJobCount(t, ctx, pool, 0, request.JobID)
	activationStore.afterJobInsert = nil
	second, err := service.Submit(ctx, request)
	if err != nil || second.Status != acquisition.SubmissionReplay {
		t.Fatalf("same-ID retry = %#v, %v", second, err)
	}
	active, err := manifests.GetManifest(ctx, request.ManifestID)
	if err != nil || active.State != acquisition.StateActive || active.SourceType != "magnet" ||
		active.SourceRef != request.Source || active.JobID == nil || *active.JobID != request.JobID {
		t.Fatalf("active Manifest = %#v, %v", active, err)
	}
	resolver, _ := acquisition.NewExecutionInputResolver(manifests, bindings)
	input, err := resolver.Resolve(ctx, request.ManifestID)
	if err != nil || input.Download.Source.Value != request.Source || input.Download.Source.Scheme != "magnet" {
		t.Fatalf("execution source changed: %#v, %v", input.Download.Source, err)
	}
	replay, err := service.Submit(ctx, request)
	if err != nil || replay.Status != acquisition.SubmissionReplay {
		t.Fatalf("replay = %#v, %v", replay, err)
	}
	conflict := request
	conflict.Source += "&different=1"
	if _, err := service.Submit(ctx, conflict); !errors.Is(err, acquisition.ErrSubmissionConflict) {
		t.Fatalf("conflicting source error = %v", err)
	}
	conflict = request
	conflict.JobID = "25000000-0000-4000-8000-000000000204"
	if _, err := service.Submit(ctx, conflict); !errors.Is(err, acquisition.ErrSubmissionConflict) {
		t.Fatalf("conflicting Job ID error = %v", err)
	}
	assertJobCount(t, ctx, pool, 1, request.JobID, conflict.JobID)
}
