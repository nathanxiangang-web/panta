package acquisition_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/jobs"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

func TestExecutionInputResolverBuildsExactProviderNeutralInput(t *testing.T) {
	resolver, manifests, topology, manifest := executionResolverFixture(t)
	scope := "provider://opaque//target?root=%2Fnot-openlist"
	binding := topology.values[manifest.TargetStorageBindingID]
	binding.ProviderScope = &scope
	binding.OpenListMountPath = "/unrelated/openlist/mount"
	binding.IndexCoreRootID = "unrelated-index-root"
	topology.values[binding.ID] = binding
	credentialRef := "secret-store://connections/acquisition"
	connection := topology.connections[binding.ConnectionID]
	connection.CredentialRef = &credentialRef
	topology.connections[connection.ID] = connection
	manifest.SourceRef = "  magnet:?xt=urn:opaque&dn=unchanged  "
	manifests.values[manifest.ID] = manifest

	input, err := resolver.Resolve(context.Background(), manifest.ID)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if input.ManifestID != manifest.ID || input.ProviderID != "provider-a" || input.ConnectionID != connection.ID ||
		input.CredentialRef == nil || *input.CredentialRef != credentialRef ||
		input.Download.Source.Scheme != manifest.SourceType || input.Download.Source.Value != manifest.SourceRef ||
		input.Download.Target.Scope != scope || input.Download.Target.Path != manifest.TargetPath {
		t.Fatalf("ExecutionInput = %#v", input)
	}
	if input.Download.Target.Scope == binding.OpenListMountPath || input.Download.Target.Scope == binding.IndexCoreRootID {
		t.Fatalf("provider scope was derived from observation coordinates: %#v", input.Download.Target)
	}
}

func TestExecutionInputResolverRejectsManifestStatesAndMissingLink(t *testing.T) {
	for _, state := range []acquisition.State{
		acquisition.StatePending, acquisition.StateAwaitingVisibility, acquisition.StateAwaitingCanonical,
		acquisition.StateReady, acquisition.StateFailed, acquisition.StateRecoveryRequired, acquisition.StateCanceled,
	} {
		t.Run(string(state), func(t *testing.T) {
			resolver, manifests, _, manifest := executionResolverFixture(t)
			manifest.State = state
			manifests.values[manifest.ID] = manifest
			if _, err := resolver.Resolve(context.Background(), manifest.ID); !errors.Is(err, acquisition.ErrExecutionManifestState) {
				t.Fatalf("Resolve() error = %v", err)
			}
		})
	}

	resolver, manifests, _, manifest := executionResolverFixture(t)
	manifest.JobID = nil
	manifests.values[manifest.ID] = manifest
	if _, err := resolver.Resolve(context.Background(), manifest.ID); !errors.Is(err, acquisition.ErrExecutionManifestLinkage) {
		t.Fatalf("ACTIVE without Job error = %v", err)
	}
}

func TestExecutionInputResolverFailsClosedOnMissingOrInconsistentTopology(t *testing.T) {
	tests := []struct {
		name   string
		want   error
		mutate func(*manifestRepository, *bindingReader, acquisition.Manifest)
	}{
		{name: "missing binding", want: acquisition.ErrStorageBindingNotFound, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			delete(topology.values, manifest.TargetStorageBindingID)
		}},
		{name: "disabled binding", want: acquisition.ErrStorageBindingDisabled, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			binding := topology.values[manifest.TargetStorageBindingID]
			binding.Status = storage.BindingStatusDisabled
			topology.values[manifest.TargetStorageBindingID] = binding
		}},
		{name: "missing provider scope", want: acquisition.ErrStorageBindingNotAcquisitionCapable, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			binding := topology.values[manifest.TargetStorageBindingID]
			binding.ProviderScope = nil
			topology.values[manifest.TargetStorageBindingID] = binding
		}},
		{name: "invalid provider scope", want: acquisition.ErrStorageBindingNotAcquisitionCapable, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			binding := topology.values[manifest.TargetStorageBindingID]
			invalid := " "
			binding.ProviderScope = &invalid
			topology.values[manifest.TargetStorageBindingID] = binding
		}},
		{name: "binding identity mismatch", want: acquisition.ErrStorageTopologyMismatch, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			binding := topology.values[manifest.TargetStorageBindingID]
			binding.ID = "binding-other"
			topology.values[manifest.TargetStorageBindingID] = binding
		}},
		{name: "missing connection", want: acquisition.ErrStorageConnectionNotFound, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			binding := topology.values[manifest.TargetStorageBindingID]
			delete(topology.connections, binding.ConnectionID)
		}},
		{name: "disabled connection", want: acquisition.ErrStorageConnectionDisabled, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			binding := topology.values[manifest.TargetStorageBindingID]
			connection := topology.connections[binding.ConnectionID]
			connection.Status = storage.ConnectionStatusDisabled
			topology.connections[binding.ConnectionID] = connection
		}},
		{name: "connection identity mismatch", want: acquisition.ErrStorageTopologyMismatch, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			binding := topology.values[manifest.TargetStorageBindingID]
			connection := topology.connections[binding.ConnectionID]
			connection.ID = "connection-other"
			topology.connections[binding.ConnectionID] = connection
		}},
		{name: "invalid provider identity", want: acquisition.ErrInvalidProviderIdentity, mutate: func(_ *manifestRepository, topology *bindingReader, manifest acquisition.Manifest) {
			binding := topology.values[manifest.TargetStorageBindingID]
			connection := topology.connections[binding.ConnectionID]
			connection.ProviderType = " "
			topology.connections[binding.ConnectionID] = connection
		}},
		{name: "Manifest identity mismatch", want: acquisition.ErrExecutionIdentityMismatch, mutate: func(manifests *manifestRepository, _ *bindingReader, manifest acquisition.Manifest) {
			manifest.ID = "manifest-other"
			manifests.values["manifest-a"] = manifest
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver, manifests, topology, manifest := executionResolverFixture(t)
			test.mutate(manifests, topology, manifest)
			if _, err := resolver.Resolve(context.Background(), "manifest-a"); !errors.Is(err, test.want) {
				t.Fatalf("Resolve() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestExecutionInputResolverRejectsInvalidOrMissingManifest(t *testing.T) {
	resolver, _, _, _ := executionResolverFixture(t)
	if _, err := resolver.Resolve(context.Background(), ""); !errors.Is(err, acquisition.ErrInvalidExecutionInputRequest) {
		t.Fatalf("empty Manifest ID error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), "missing"); !errors.Is(err, acquisition.ErrExecutionManifestNotFound) {
		t.Fatalf("missing Manifest error = %v", err)
	}
}

func executionResolverFixture(t *testing.T) (*acquisition.ExecutionInputResolver, *manifestRepository, *bindingReader, acquisition.Manifest) {
	t.Helper()
	jobID := jobs.JobID("job-a")
	manifest := acquisition.Manifest{
		ID: "manifest-a", SourceType: "opaque-source", SourceRef: "opaque-ref",
		TargetStorageBindingID: "binding-a", TargetPath: "/downloads/item",
		JobID: &jobID, State: acquisition.StateActive,
	}
	scope := "provider-scope-a"
	topology := &bindingReader{
		values: map[storage.BindingID]storage.Binding{
			"binding-a": {
				ID: "binding-a", ConnectionID: "connection-a", ProviderScope: &scope,
				OpenListMountPath: "/openlist-a", IndexCoreRootID: "root-a", Status: storage.BindingStatusActive,
			},
		},
		connections: map[storage.ConnectionID]storage.Connection{
			"connection-a": {ID: "connection-a", ProviderType: "provider-a", Status: storage.ConnectionStatusActive},
		},
	}
	manifests := &manifestRepository{values: map[acquisition.ManifestID]acquisition.Manifest{manifest.ID: manifest}}
	resolver, err := acquisition.NewExecutionInputResolver(manifests, topology)
	if err != nil {
		t.Fatalf("NewExecutionInputResolver() error = %v", err)
	}
	return resolver, manifests, topology, manifest
}
