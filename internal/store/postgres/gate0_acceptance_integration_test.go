package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/providers/registry"
	"github.com/nathanxiangang-web/panta/internal/providers/testprovider"
)

type knownPathIndexCore struct {
	resource indexcore.ResourceContext
}

func (client knownPathIndexCore) Resolve(_ context.Context, request indexcore.ResolveRequest) (indexcore.ResourceContext, error) {
	resource := client.resource
	resource.RootID = request.RootID
	resource.CanonicalPath = request.Path
	return resource, nil
}

func (knownPathIndexCore) Browse(context.Context, indexcore.BrowseRequest) ([]indexcore.ResourceContext, error) {
	return nil, nil
}

var _ indexcore.ReadPort = knownPathIndexCore{}

// TestGateZeroAcceptance composes the accepted Gate 0 boundaries without a
// real provider, search service, agent, worker loop, or external integration.
func TestGateZeroAcceptance(t *testing.T) {
	ctx := context.Background()
	pool := integrationPool(t, ctx)
	resetTestSchema(t, ctx, pool)

	migrator, err := NewMigrator(pool)
	if err != nil {
		t.Fatalf("NewMigrator() error = %v", err)
	}
	status, err := migrator.Apply(ctx)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	assertCurrentSchema(t, status)
	if len(status.Applied) != 3 || status.Applied[0].Version != 1 || status.Applied[1].Version != 2 || status.Applied[2].Version != 3 {
		t.Fatalf("applied migrations = %#v, want ordered 0001 + 0002 + 0003", status.Applied)
	}

	mock := testprovider.New()
	providers := registry.New()
	if err := providers.Register(registry.Entry{
		Descriptor: mock.Descriptor(), Storage: mock, Downloader: mock, Share: mock,
	}); err != nil {
		t.Fatalf("Register(mock provider) error = %v", err)
	}
	if _, err := providers.Get(testprovider.ID); err != nil {
		t.Fatalf("Get(mock provider) error = %v", err)
	}

	resolveRequest := indexcore.ResolveRequest{RootID: "root-gate0", Path: "/known/artifact.bin"}
	physical, err := (knownPathIndexCore{resource: indexcore.ResourceContext{
		ResourceID: "resource-gate0", Name: "artifact.bin", SizeBytes: 4096, Present: true,
	}}).Resolve(ctx, resolveRequest)
	if err != nil || physical.RootID != resolveRequest.RootID || physical.CanonicalPath != resolveRequest.Path || !physical.Present {
		t.Fatalf("Resolve(known path) = %#v, %v", physical, err)
	}

	catalogRepository, err := NewCatalogRepository(pool)
	if err != nil {
		t.Fatalf("NewCatalogRepository() error = %v", err)
	}
	now := time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC)
	asset := catalog.Asset{
		ID: "a0000000-0000-4000-8000-000000000001", CanonicalName: "Gate 0 Asset",
		Category: "artifact", Status: "ACTIVE", CreatedAt: now, UpdatedAt: now,
	}
	if err := catalogRepository.CreateAsset(ctx, asset); err != nil {
		t.Fatalf("CreateAsset() error = %v", err)
	}
	release := catalog.Release{
		ID: "a0000000-0000-4000-8000-000000000002", AssetID: asset.ID,
		VersionRaw: "unversioned", VersionScheme: catalog.VersionSchemeNone,
		Channel: "default", Status: "ACTIVE", CreatedAt: now, UpdatedAt: now,
	}
	if err := catalogRepository.CreateRelease(ctx, release); err != nil {
		t.Fatalf("CreateRelease() error = %v", err)
	}
	variant := catalog.Variant{
		ID: "a0000000-0000-4000-8000-000000000003", ReleaseID: release.ID,
		VariantKey: "default", Attributes: json.RawMessage(`{}`),
		Status: "ACTIVE", CreatedAt: now, UpdatedAt: now,
	}
	if err := catalogRepository.CreateVariant(ctx, variant); err != nil {
		t.Fatalf("CreateVariant() error = %v", err)
	}
	resourceCopy := catalog.Copy{
		ID: "a0000000-0000-4000-8000-000000000004", VariantID: &variant.ID,
		IndexCoreRootID: physical.RootID, IndexCoreResourceID: physical.ResourceID,
		StorageBindingID: "a0000000-0000-4000-8000-000000000005",
		Availability:     "PRESENT", CreatedAt: now, UpdatedAt: now,
	}
	if err := catalogRepository.CreateCopy(ctx, resourceCopy); err != nil {
		t.Fatalf("CreateCopy() error = %v", err)
	}
	storedCopy, err := catalogRepository.GetCopy(ctx, resourceCopy.ID)
	if err != nil || storedCopy.IndexCoreRootID != physical.RootID || storedCopy.IndexCoreResourceID != physical.ResourceID {
		t.Fatalf("GetCopy() = %#v, %v", storedCopy, err)
	}

	jobRepository, err := NewJobRepository(pool)
	if err != nil {
		t.Fatalf("NewJobRepository() error = %v", err)
	}
	job, err := jobRepository.Create(ctx, jobs.CreateRequest{
		ID: "b0000000-0000-4000-8000-000000000001", Type: "GATE0_GENERIC",
		Payload: json.RawMessage(`{"boundary":"provider-neutral"}`), MaxAttempts: 2,
	})
	if err != nil {
		t.Fatalf("Create(job) error = %v", err)
	}
	claimed, err := jobRepository.ClaimNext(ctx, jobs.ClaimRequest{Owner: "gate0-worker", Now: now, LeaseDuration: time.Minute})
	if err != nil || claimed.ID != job.ID || claimed.State != jobs.StateRunning {
		t.Fatalf("ClaimNext() = %#v, %v", claimed, err)
	}
	completed, err := jobRepository.Succeed(ctx, jobs.LeaseRequest{
		ID: job.ID, Owner: "gate0-worker", ExpectedAttempt: claimed.AttemptCount, Now: now.Add(time.Second),
	})
	if err != nil || completed.State != jobs.StateSucceeded {
		t.Fatalf("Succeed() = %#v, %v", completed, err)
	}
}
