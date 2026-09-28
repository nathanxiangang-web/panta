package testprovider_test

import (
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/providers/contracttest"
	"github.com/nathanxiangang-web/panta/internal/providers/registry"
	"github.com/nathanxiangang-web/panta/internal/providers/testprovider"
)

func TestProviderConformance(t *testing.T) {
	cases := contractCases()
	contracttest.Run(t, newPorts(), cases)
}

func TestUnsupportedOptionalCancellationUsesNeutralError(t *testing.T) {
	factory := func(t *testing.T) contracttest.Ports {
		t.Helper()
		provider := testprovider.New(testprovider.WithoutDownloadCancellation())
		return contracttest.Ports{Storage: provider, Downloader: provider, Share: provider}
	}
	contracttest.RunUnsupportedDownloadCancellation(t, factory, contractCases().DownloadRequest)
}

func TestRegistryAcceptsFullProviderAndRejectsContradiction(t *testing.T) {
	provider := testprovider.New()
	descriptor := provider.Descriptor()
	entry := registry.Entry{
		Descriptor: descriptor,
		Storage:    provider,
		Downloader: provider,
		Share:      provider,
	}
	providers := registry.New()
	if err := providers.Register(entry); err != nil {
		t.Fatalf("Register(full provider) error = %v", err)
	}
	registered, err := providers.Get(testprovider.ID)
	if err != nil || registered.Descriptor != descriptor || registered.Storage == nil || registered.Downloader == nil || registered.Share == nil {
		t.Fatalf("Get() = %#v, %v", registered, err)
	}

	contradictory := entry
	contradictory.Descriptor.Capabilities.Sharing = false
	if err := registry.New().Register(contradictory); !errors.Is(err, registry.ErrInvalidEntry) {
		t.Fatalf("Register(contradictory) error = %v, want ErrInvalidEntry", err)
	}
}

func newPorts() contracttest.Factory {
	return func(t *testing.T) contracttest.Ports {
		t.Helper()
		provider := testprovider.New()
		return contracttest.Ports{Storage: provider, Downloader: provider, Share: provider}
	}
}

func contractCases() contracttest.Cases {
	return contracttest.Cases{
		KnownTarget: testprovider.KnownTarget(),
		DownloadRequest: contracts.DownloadRequest{
			Source: contracts.SourceReference{Scheme: "opaque-test", Value: "source::unchanged"},
			Target: contracts.TargetPath{Scope: "library", Path: "/downloads"},
		},
	}
}
