package session_test

import (
	"context"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/providers/registry"
	"github.com/nathanxiangang-web/panta/internal/providers/session"
	"github.com/nathanxiangang-web/panta/internal/providers/testprovider"
)

// TestRegistryPortFlowsIntoSessionRegistration mirrors the intended production
// composition: a provider port is obtained from the provider registry and then
// bound to an exact connection/credential identity in the session registry, which
// is what the execution service actually resolves against.
func TestRegistryPortFlowsIntoSessionRegistration(t *testing.T) {
	provider := testprovider.New()
	providers := registry.New()
	if err := providers.Register(registry.Entry{
		Descriptor: provider.Descriptor(),
		Storage:    provider,
		Downloader: provider,
		Share:      provider,
	}); err != nil {
		t.Fatalf("registry.Register() error = %v", err)
	}

	// Step 1: the provider registry supplies the downloader port.
	lookup, err := providers.LookupDownloader(testprovider.ID)
	if err != nil {
		t.Fatalf("LookupDownloader() error = %v", err)
	}
	if lookup.Downloader == nil {
		t.Fatal("registry returned no downloader port")
	}

	// Step 2: composition binds that port to one connection and credential.
	credential := "secret-ref://connections/scoped"
	sessions := session.New()
	if err := sessions.Register(session.RegistrationRequest{
		ProviderID:    lookup.Descriptor.ID,
		ConnectionID:  string(connectionA),
		CredentialRef: &credential,
		Downloader:    lookup.Downloader,
	}); err != nil {
		t.Fatalf("session.Register() error = %v", err)
	}

	// Step 3: execution resolves by the complete connection identity, never by
	// ProviderID alone.
	binding, err := sessions.ResolveDownloader(context.Background(), acquisition.DownloaderSessionRequest{
		ProviderID: testprovider.ID, ConnectionID: connectionA, CredentialRef: &credential,
	})
	if err != nil {
		t.Fatalf("ResolveDownloader() error = %v", err)
	}
	if binding.Descriptor.ID != testprovider.ID || binding.Downloader == nil {
		t.Fatalf("resolved binding = %#v", binding)
	}
	// A different connection with the same provider is not served, because no
	// session was registered for it.
	if _, err := sessions.ResolveDownloader(context.Background(), acquisition.DownloaderSessionRequest{
		ProviderID: testprovider.ID, ConnectionID: connectionB, CredentialRef: &credential,
	}); err == nil {
		t.Fatal("unregistered connection resolved a session, want fail-closed error")
	}
}
