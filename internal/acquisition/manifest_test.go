package acquisition_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestCreateManifestRejectsInvalidIntentBeforePersistence(t *testing.T) {
	service, _, _, manifests := manifestService(t)
	valid := validManifestRequest()
	tests := []struct {
		name   string
		mutate func(*acquisition.CreateManifestRequest)
		want   error
	}{
		{name: "empty Manifest ID", mutate: func(value *acquisition.CreateManifestRequest) { value.ID = "" }, want: acquisition.ErrInvalidArgument},
		{name: "empty source type", mutate: func(value *acquisition.CreateManifestRequest) { value.SourceType = "" }, want: acquisition.ErrInvalidArgument},
		{name: "empty source ref", mutate: func(value *acquisition.CreateManifestRequest) { value.SourceRef = " " }, want: acquisition.ErrInvalidArgument},
		{name: "long source type", mutate: func(value *acquisition.CreateManifestRequest) {
			value.SourceType = strings.Repeat("x", acquisition.MaxSourceTypeLength+1)
		}, want: acquisition.ErrInvalidArgument},
		{name: "empty binding", mutate: func(value *acquisition.CreateManifestRequest) { value.TargetStorageBindingID = "" }, want: acquisition.ErrInvalidArgument},
		{name: "empty target", mutate: func(value *acquisition.CreateManifestRequest) { value.TargetPath = "" }, want: acquisition.ErrInvalidTargetPath},
		{name: "relative target", mutate: func(value *acquisition.CreateManifestRequest) { value.TargetPath = "downloads/a" }, want: acquisition.ErrInvalidTargetPath},
		{name: "backslash target", mutate: func(value *acquisition.CreateManifestRequest) { value.TargetPath = `/downloads\a` }, want: acquisition.ErrInvalidTargetPath},
		{name: "dot target", mutate: func(value *acquisition.CreateManifestRequest) { value.TargetPath = "/downloads/./a" }, want: acquisition.ErrInvalidTargetPath},
		{name: "dotdot target", mutate: func(value *acquisition.CreateManifestRequest) { value.TargetPath = "/downloads/../a" }, want: acquisition.ErrInvalidTargetPath},
		{name: "Release without Asset", mutate: func(value *acquisition.CreateManifestRequest) {
			release := catalog.ReleaseID("release-a")
			value.ReleaseID = &release
		}, want: acquisition.ErrInvalidArgument},
		{name: "Variant without Release", mutate: func(value *acquisition.CreateManifestRequest) {
			variant := catalog.VariantID("variant-a")
			value.VariantID = &variant
		}, want: acquisition.ErrInvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid
			test.mutate(&request)
			if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, test.want) {
				t.Fatalf("CreateManifest() error = %v, want %v", err, test.want)
			}
		})
	}
	if manifests.calls != 0 {
		t.Fatalf("invalid intents reached persistence %d times", manifests.calls)
	}
}

func TestNormalizeTargetPath(t *testing.T) {
	tests := map[string]string{
		"/":                    "/",
		"//downloads///linux/": "/downloads/linux",
		"/downloads/file.iso":  "/downloads/file.iso",
	}
	for input, want := range tests {
		got, err := acquisition.NormalizeTargetPath(input)
		if err != nil || got != want {
			t.Fatalf("NormalizeTargetPath(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
}

func TestCreateManifestRequiresActiveStorageBinding(t *testing.T) {
	service, bindings, _, manifests := manifestService(t)
	request := validManifestRequest()
	delete(bindings.values, request.TargetStorageBindingID)
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrStorageBindingNotFound) {
		t.Fatalf("missing binding error = %v", err)
	}
	bindings.values[request.TargetStorageBindingID] = storage.Binding{ID: request.TargetStorageBindingID, Status: storage.BindingStatusDisabled}
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrStorageBindingDisabled) {
		t.Fatalf("disabled binding error = %v", err)
	}
	if manifests.calls != 0 {
		t.Fatalf("invalid binding reached persistence %d times", manifests.calls)
	}
}

func TestCreateManifestRequiresAcquisitionCapableStorageTopology(t *testing.T) {
	service, topology, _, manifests := manifestService(t)
	request := validManifestRequest()
	binding := topology.values[request.TargetStorageBindingID]
	binding.ProviderScope = nil
	topology.values[request.TargetStorageBindingID] = binding
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrStorageBindingNotAcquisitionCapable) {
		t.Fatalf("observation-only binding error = %v", err)
	}

	scope := "opaque-provider-scope"
	binding.ProviderScope = &scope
	topology.values[request.TargetStorageBindingID] = binding
	connection := topology.connections[binding.ConnectionID]
	connection.Status = storage.ConnectionStatusDisabled
	topology.connections[binding.ConnectionID] = connection
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrStorageConnectionDisabled) {
		t.Fatalf("disabled connection error = %v", err)
	}
	if manifests.calls != 0 {
		t.Fatalf("invalid acquisition topology reached persistence %d times", manifests.calls)
	}
	connection.Status = storage.ConnectionStatusActive
	topology.connections[binding.ConnectionID] = connection
	created, err := service.CreateManifest(context.Background(), request)
	if err != nil || created.TargetStorageBindingID != binding.ID || manifests.calls != 1 {
		t.Fatalf("active acquisition topology result = %#v, %v; persistence calls=%d", created, err, manifests.calls)
	}
}

func TestCreateManifestAllowsOptionalAndValidatedLogicalIdentity(t *testing.T) {
	service, _, identities, manifests := manifestService(t)
	request := validManifestRequest()
	created, err := service.CreateManifest(context.Background(), request)
	if err != nil {
		t.Fatalf("CreateManifest(no identity) error = %v", err)
	}
	if created.AssetID != nil || created.ReleaseID != nil || created.VariantID != nil || created.State != acquisition.StatePending || created.TargetPath != "/downloads/linux" {
		t.Fatalf("physical-only Manifest = %#v", created)
	}

	assetID := catalog.AssetID("asset-a")
	request.ID = "manifest-asset"
	request.AssetID = &assetID
	created, err = service.CreateManifest(context.Background(), request)
	if err != nil || created.AssetID == nil || *created.AssetID != assetID {
		t.Fatalf("CreateManifest(asset only) = %#v, %v", created, err)
	}

	releaseID := catalog.ReleaseID("release-a")
	variantID := catalog.VariantID("variant-a")
	request.ID = "manifest-full"
	request.ReleaseID = &releaseID
	request.VariantID = &variantID
	created, err = service.CreateManifest(context.Background(), request)
	if err != nil || created.State != acquisition.StatePending || created.ID != request.ID || !created.CreatedAt.Equal(created.UpdatedAt) {
		t.Fatalf("CreateManifest(full) = %#v, %v", created, err)
	}
	if len(manifests.values) != 3 || identities.assetCalls != 2 || identities.releaseCalls != 1 || identities.variantCalls != 1 {
		t.Fatalf("service calls: manifests=%d assets=%d releases=%d variants=%d", len(manifests.values), identities.assetCalls, identities.releaseCalls, identities.variantCalls)
	}
}

func TestCreateManifestCannotPrelinkExecutionState(t *testing.T) {
	service, _, _, manifests := manifestService(t)
	created, err := service.CreateManifest(context.Background(), validManifestRequest())
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}
	if created.State != acquisition.StatePending || created.JobID != nil {
		t.Fatalf("created Manifest = %#v, want PENDING with no Job link", created)
	}
	durable := manifests.values[created.ID]
	if durable.State != acquisition.StatePending || durable.JobID != nil {
		t.Fatalf("persisted Manifest = %#v, want PENDING with no Job link", durable)
	}
}

func TestValidateManifestEnforcesActivationLinkInvariant(t *testing.T) {
	service, _, _, _ := manifestService(t)
	manifest, err := service.CreateManifest(context.Background(), validManifestRequest())
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}
	jobID := jobs.JobID("job-a")

	pendingLinked := manifest
	pendingLinked.JobID = &jobID
	if err := acquisition.ValidateManifest(pendingLinked); !errors.Is(err, acquisition.ErrInvalidArgument) {
		t.Fatalf("ValidateManifest(PENDING linked) error = %v", err)
	}
	activeUnlinked := manifest
	activeUnlinked.State = acquisition.StateActive
	if err := acquisition.ValidateManifest(activeUnlinked); !errors.Is(err, acquisition.ErrInvalidArgument) {
		t.Fatalf("ValidateManifest(ACTIVE unlinked) error = %v", err)
	}
	activeLinked := activeUnlinked
	activeLinked.JobID = &jobID
	if err := acquisition.ValidateManifest(activeLinked); err != nil {
		t.Fatalf("ValidateManifest(ACTIVE linked) error = %v", err)
	}
}

func TestCreateManifestFailsClosedOnLogicalOwnershipMismatch(t *testing.T) {
	service, _, identities, manifests := manifestService(t)
	assetID := catalog.AssetID("asset-a")
	releaseID := catalog.ReleaseID("release-a")
	variantID := catalog.VariantID("variant-a")
	request := validManifestRequest()
	request.AssetID, request.ReleaseID, request.VariantID = &assetID, &releaseID, &variantID

	identities.releases[releaseID] = catalog.Release{ID: releaseID, AssetID: "asset-other"}
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrReleaseOwnershipMismatch) {
		t.Fatalf("Release ownership error = %v", err)
	}
	identities.releases[releaseID] = catalog.Release{ID: releaseID, AssetID: assetID}
	identities.variants[variantID] = catalog.Variant{ID: variantID, ReleaseID: "release-other"}
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrVariantOwnershipMismatch) {
		t.Fatalf("Variant ownership error = %v", err)
	}
	if manifests.calls != 0 {
		t.Fatalf("ownership mismatch reached persistence %d times", manifests.calls)
	}
}

func TestCreateManifestReportsMissingLogicalIdentity(t *testing.T) {
	service, _, identities, manifests := manifestService(t)
	assetID := catalog.AssetID("asset-a")
	releaseID := catalog.ReleaseID("release-a")
	variantID := catalog.VariantID("variant-a")
	request := validManifestRequest()
	request.AssetID, request.ReleaseID, request.VariantID = &assetID, &releaseID, &variantID

	delete(identities.assets, assetID)
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrAssetNotFound) {
		t.Fatalf("missing Asset error = %v", err)
	}
	identities.assets[assetID] = catalog.Asset{ID: assetID}
	delete(identities.releases, releaseID)
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrReleaseNotFound) {
		t.Fatalf("missing Release error = %v", err)
	}
	identities.releases[releaseID] = catalog.Release{ID: releaseID, AssetID: assetID}
	delete(identities.variants, variantID)
	if _, err := service.CreateManifest(context.Background(), request); !errors.Is(err, acquisition.ErrVariantNotFound) {
		t.Fatalf("missing Variant error = %v", err)
	}
	if manifests.calls != 0 {
		t.Fatalf("missing logical identity reached persistence %d times", manifests.calls)
	}
}

type bindingReader struct {
	values      map[storage.BindingID]storage.Binding
	connections map[storage.ConnectionID]storage.Connection
}

func (reader *bindingReader) GetBinding(_ context.Context, id storage.BindingID) (storage.Binding, error) {
	value, ok := reader.values[id]
	if !ok {
		return storage.Binding{}, storage.ErrNotFound
	}
	return value, nil
}

func (reader *bindingReader) GetConnection(_ context.Context, id storage.ConnectionID) (storage.Connection, error) {
	value, ok := reader.connections[id]
	if !ok {
		return storage.Connection{}, storage.ErrNotFound
	}
	return value, nil
}

type identityReader struct {
	assets       map[catalog.AssetID]catalog.Asset
	releases     map[catalog.ReleaseID]catalog.Release
	variants     map[catalog.VariantID]catalog.Variant
	assetCalls   int
	releaseCalls int
	variantCalls int
}

func (reader *identityReader) GetAsset(_ context.Context, id catalog.AssetID) (catalog.Asset, error) {
	reader.assetCalls++
	value, ok := reader.assets[id]
	if !ok {
		return catalog.Asset{}, catalog.ErrNotFound
	}
	return value, nil
}

func (reader *identityReader) GetRelease(_ context.Context, id catalog.ReleaseID) (catalog.Release, error) {
	reader.releaseCalls++
	value, ok := reader.releases[id]
	if !ok {
		return catalog.Release{}, catalog.ErrNotFound
	}
	return value, nil
}

func (reader *identityReader) GetVariant(_ context.Context, id catalog.VariantID) (catalog.Variant, error) {
	reader.variantCalls++
	value, ok := reader.variants[id]
	if !ok {
		return catalog.Variant{}, catalog.ErrNotFound
	}
	return value, nil
}

type manifestRepository struct {
	values map[acquisition.ManifestID]acquisition.Manifest
	calls  int
}

func (repository *manifestRepository) CreateManifest(_ context.Context, manifest acquisition.Manifest) error {
	repository.calls++
	repository.values[manifest.ID] = manifest
	return nil
}

func (repository *manifestRepository) GetManifest(_ context.Context, id acquisition.ManifestID) (acquisition.Manifest, error) {
	value, ok := repository.values[id]
	if !ok {
		return acquisition.Manifest{}, acquisition.ErrNotFound
	}
	return value, nil
}

func manifestService(t *testing.T) (*acquisition.Service, *bindingReader, *identityReader, *manifestRepository) {
	t.Helper()
	bindingID := storage.BindingID("binding-a")
	connectionID := storage.ConnectionID("connection-a")
	providerScope := "opaque-provider-scope"
	bindings := &bindingReader{values: map[storage.BindingID]storage.Binding{
		bindingID: {ID: bindingID, ConnectionID: connectionID, ProviderScope: &providerScope, Status: storage.BindingStatusActive},
	}, connections: map[storage.ConnectionID]storage.Connection{
		connectionID: {ID: connectionID, ProviderType: "provider-a", Status: storage.ConnectionStatusActive},
	}}
	identities := &identityReader{
		assets:   map[catalog.AssetID]catalog.Asset{"asset-a": {ID: "asset-a"}},
		releases: map[catalog.ReleaseID]catalog.Release{"release-a": {ID: "release-a", AssetID: "asset-a"}},
		variants: map[catalog.VariantID]catalog.Variant{"variant-a": {ID: "variant-a", ReleaseID: "release-a"}},
	}
	manifests := &manifestRepository{values: make(map[acquisition.ManifestID]acquisition.Manifest)}
	fixed := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	service, err := acquisition.NewService(bindings, identities, manifests, acquisition.WithClock(func() time.Time { return fixed }))
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service, bindings, identities, manifests
}

func validManifestRequest() acquisition.CreateManifestRequest {
	return acquisition.CreateManifestRequest{
		ID: "manifest-a", SourceType: "opaque", SourceRef: " source-ref-kept-opaque ",
		TargetStorageBindingID: "binding-a", TargetPath: "//downloads///linux/",
	}
}
