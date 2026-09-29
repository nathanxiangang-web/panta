package registry_test

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/providers/registry"
	"github.com/nathanxiangang-web/panta/internal/providers/testprovider"
)

// storageOnlyProvider is a minimal storage-only provider double used to prove
// the lookup reports no downloader when a provider does not implement one.
type storageOnlyProvider struct{}

const storageOnlyID contracts.ProviderID = "storage-only"

func (storageOnlyProvider) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{
		ID: storageOnlyID, DisplayName: "storage only", Capabilities: contracts.CapabilitySet{Storage: true},
	}
}

func (storageOnlyProvider) Stat(context.Context, contracts.TargetPath) (contracts.StorageObject, error) {
	return contracts.StorageObject{}, errors.New("not implemented")
}

func (storageOnlyProvider) Access(context.Context, contracts.StorageObjectReference) (contracts.AccessTarget, error) {
	return contracts.AccessTarget{}, errors.New("not implemented")
}

func TestRegistryLookupDownloaderReturnsRegisteredBinding(t *testing.T) {
	provider := testprovider.New()
	registered := registry.New()
	if err := registered.Register(registry.Entry{
		Descriptor: provider.Descriptor(),
		Storage:    provider,
		Downloader: provider,
		Share:      provider,
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lookup, err := registered.LookupDownloader(testprovider.ID)
	if err != nil {
		t.Fatalf("LookupDownloader() error = %v", err)
	}
	if lookup.Descriptor.ID != testprovider.ID || lookup.Downloader == nil {
		t.Fatalf("LookupDownloader() = %#v", lookup)
	}
	if lookup.Downloader.Descriptor().ID != testprovider.ID {
		t.Fatalf("downloader identity = %s", lookup.Downloader.Descriptor().ID)
	}
	// The result type is the shared provider-neutral contract, not a registry-local
	// alias, which is what lets a consumer port match this method exactly.
	var binding contracts.DownloaderBinding = lookup
	if binding.Descriptor.ID != testprovider.ID {
		t.Fatalf("contracts.DownloaderBinding = %#v", binding)
	}
}

func TestRegistryLookupDownloaderIsEmptyForStorageOnlyProvider(t *testing.T) {
	registered := registry.New()
	if err := registered.Register(registry.Entry{Descriptor: storageOnlyProvider{}.Descriptor(), Storage: storageOnlyProvider{}}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	lookup, err := registered.LookupDownloader(storageOnlyID)
	if err != nil {
		t.Fatalf("LookupDownloader() error = %v", err)
	}
	if lookup.Downloader != nil {
		t.Fatalf("Downloader = %#v, want nil for storage-only provider", lookup.Downloader)
	}
	if lookup.Descriptor.Capabilities.Downloader {
		t.Fatal("storage-only descriptor must not advertise downloader capability")
	}
}
