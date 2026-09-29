package session_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/providers/session"
	"github.com/nathanxiangang-web/panta/internal/providers/testprovider"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

const (
	connectionA = storage.ConnectionID("a1000000-0000-4000-8000-000000000001")
	connectionB = storage.ConnectionID("a1000000-0000-4000-8000-000000000002")
)

func credentialRef(value string) *string { return &value }

// TestSameProviderResolvesDistinctSessionsPerConnection is the core D-024 proof:
// one ProviderID backing two StorageConnections with different credential
// references must resolve to different downloader sessions.
func TestSameProviderResolvesDistinctSessionsPerConnection(t *testing.T) {
	registry := session.New()
	sharedProvider := testprovider.New()
	connectionAProvider := testprovider.New()
	connectionBProvider := testprovider.New()

	// The same provider identity is registered twice, once per connection, each
	// with a different credential reference.
	if err := registry.Register(session.RegistrationRequest{
		ProviderID: sharedProvider.Descriptor().ID, ConnectionID: string(connectionA),
		CredentialRef: credentialRef("secret-ref://connections/a"), Downloader: connectionAProvider,
	}); err != nil {
		t.Fatalf("Register(connection A) error = %v", err)
	}
	if err := registry.Register(session.RegistrationRequest{
		ProviderID: sharedProvider.Descriptor().ID, ConnectionID: string(connectionB),
		CredentialRef: credentialRef("secret-ref://connections/b"), Downloader: connectionBProvider,
	}); err != nil {
		t.Fatalf("Register(connection B) error = %v", err)
	}

	ctx := context.Background()
	a, err := registry.ResolveDownloader(ctx, acquisition.DownloaderSessionRequest{
		ProviderID: sharedProvider.Descriptor().ID, ConnectionID: connectionA,
		CredentialRef: credentialRef("secret-ref://connections/a"),
	})
	if err != nil {
		t.Fatalf("ResolveDownloader(A) error = %v", err)
	}
	b, err := registry.ResolveDownloader(ctx, acquisition.DownloaderSessionRequest{
		ProviderID: sharedProvider.Descriptor().ID, ConnectionID: connectionB,
		CredentialRef: credentialRef("secret-ref://connections/b"),
	})
	if err != nil {
		t.Fatalf("ResolveDownloader(B) error = %v", err)
	}
	if a.Downloader == nil || b.Downloader == nil {
		t.Fatal("both connections must resolve a downloader")
	}
	// Different sessions, not the same port twice.
	if a.Downloader == b.Downloader {
		t.Fatal("different connections resolved the identical downloader port")
	}
	if a.Descriptor.ID != sharedProvider.Descriptor().ID || b.Descriptor.ID != sharedProvider.Descriptor().ID {
		t.Fatalf("descriptor identity drifted: %s / %s", a.Descriptor.ID, b.Descriptor.ID)
	}
}

func TestResolveDownloaderFailsClosedOnIdentityMismatch(t *testing.T) {
	registry := session.New()
	provider := testprovider.New()
	const registeredRef = "secret-ref://connections/a"
	if err := registry.Register(session.RegistrationRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: string(connectionA),
		CredentialRef: credentialRef(registeredRef), Downloader: provider,
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	ctx := context.Background()

	tests := []struct {
		name    string
		request acquisition.DownloaderSessionRequest
	}{
		{name: "wrong ConnectionID", request: acquisition.DownloaderSessionRequest{
			ProviderID: provider.Descriptor().ID, ConnectionID: connectionB, CredentialRef: credentialRef(registeredRef),
		}},
		{name: "wrong CredentialRef", request: acquisition.DownloaderSessionRequest{
			ProviderID: provider.Descriptor().ID, ConnectionID: connectionA, CredentialRef: credentialRef("secret-ref://connections/other"),
		}},
		{name: "missing required CredentialRef", request: acquisition.DownloaderSessionRequest{
			ProviderID: provider.Descriptor().ID, ConnectionID: connectionA,
		}},
		{name: "unknown ProviderID", request: acquisition.DownloaderSessionRequest{
			ProviderID: "not-registered", ConnectionID: connectionA, CredentialRef: credentialRef(registeredRef),
		}},
		{name: "empty ConnectionID", request: acquisition.DownloaderSessionRequest{
			ProviderID: provider.Descriptor().ID, CredentialRef: credentialRef(registeredRef),
		}},
		{name: "invalid ProviderID", request: acquisition.DownloaderSessionRequest{
			ProviderID: "  ", ConnectionID: connectionA, CredentialRef: credentialRef(registeredRef),
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := registry.ResolveDownloader(ctx, test.request); err == nil {
				t.Fatal("ResolveDownloader() succeeded, want fail-closed error")
			} else if errors.Is(err, contracts.ErrSecretUnavailable) {
				t.Fatalf("unexpected secret resolution: %v", err)
			}
		})
	}
}

// TestCredentialMisMatchErrorNamesNoCredentialValue keeps errors safe to log.
func TestCredentialMisMatchErrorNamesNoCredentialValue(t *testing.T) {
	registry := session.New()
	provider := testprovider.New()
	const registeredRef = "secret-ref://connections/a"
	const attemptedRef = "secret-ref://connections/leaky-value"
	if err := registry.Register(session.RegistrationRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: string(connectionA),
		CredentialRef: credentialRef(registeredRef), Downloader: provider,
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	_, err := registry.ResolveDownloader(context.Background(), acquisition.DownloaderSessionRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: connectionA, CredentialRef: credentialRef(attemptedRef),
	})
	if err == nil {
		t.Fatal("ResolveDownloader() succeeded, want ErrCredentialMismatch")
	}
	if !errors.Is(err, session.ErrCredentialMismatch) {
		t.Fatalf("error = %v, want ErrCredentialMismatch", err)
	}
	if strings.Contains(err.Error(), attemptedRef) || strings.Contains(err.Error(), registeredRef) {
		t.Fatalf("error %q leaks a credential reference value", err)
	}
}

// TestCredentialLessProviderResolves proves an explicitly credential-less
// registration works and still refuses a request that supplies a credential.
func TestCredentialLessProviderResolves(t *testing.T) {
	registry := session.New()
	provider := testprovider.New()
	if err := registry.Register(session.RegistrationRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: string(connectionA),
		Downloader: provider,
	}); err != nil {
		t.Fatalf("Register(credential-less) error = %v", err)
	}
	ctx := context.Background()

	binding, err := registry.ResolveDownloader(ctx, acquisition.DownloaderSessionRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: connectionA,
	})
	if err != nil {
		t.Fatalf("ResolveDownloader(credential-less) error = %v", err)
	}
	if binding.Downloader == nil {
		t.Fatal("credential-less registration must still resolve a downloader")
	}
	if _, err := registry.ResolveDownloader(ctx, acquisition.DownloaderSessionRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: connectionA, CredentialRef: credentialRef("unexpected"),
	}); !errors.Is(err, session.ErrCredentialMismatch) {
		t.Fatalf("credential supplied for credential-less binding error = %v, want ErrCredentialMismatch", err)
	}
}

func TestRegisterRejectsInvalidRegistration(t *testing.T) {
	provider := testprovider.New()
	storageOnly := storageOnlyProvider{}
	tests := []struct {
		name    string
		request session.RegistrationRequest
	}{
		{name: "empty provider", request: session.RegistrationRequest{
			ConnectionID: string(connectionA), Downloader: provider,
		}},
		{name: "empty connection", request: session.RegistrationRequest{
			ProviderID: provider.Descriptor().ID, Downloader: provider,
		}},
		{name: "nil downloader", request: session.RegistrationRequest{
			ProviderID: provider.Descriptor().ID, ConnectionID: string(connectionA),
		}},
		{name: "downloader identity mismatch", request: session.RegistrationRequest{
			ProviderID: "other-provider", ConnectionID: string(connectionA), Downloader: provider,
		}},
		{name: "provider without downloader capability", request: session.RegistrationRequest{
			ProviderID: storageOnly.Descriptor().ID, ConnectionID: string(connectionA), Downloader: nil,
		}},
		{name: "empty credential reference", request: session.RegistrationRequest{
			ProviderID: provider.Descriptor().ID, ConnectionID: string(connectionA),
			CredentialRef: credentialRef(""), Downloader: provider,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := session.New()
			if err := registry.Register(test.request); !errors.Is(err, session.ErrInvalidRegistration) {
				t.Fatalf("Register() error = %v, want ErrInvalidRegistration", err)
			}
		})
	}
}

func TestRegisterIsIdempotentOnlyForIdenticalIdentity(t *testing.T) {
	registry := session.New()
	provider := testprovider.New()
	request := session.RegistrationRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: string(connectionA),
		CredentialRef: credentialRef("secret-ref://connections/a"), Downloader: provider,
	}
	if err := registry.Register(request); err != nil {
		t.Fatalf("first Register() error = %v", err)
	}
	// Identical identity is deliberately idempotent.
	if err := registry.Register(request); err != nil {
		t.Fatalf("identical re-register error = %v, want nil", err)
	}
	// A different credential reference for the same provider/connection is a
	// conflicting duplicate and must be rejected.
	conflicting := request
	conflicting.CredentialRef = credentialRef("secret-ref://connections/other")
	if err := registry.Register(conflicting); !errors.Is(err, session.ErrDuplicateBinding) {
		t.Fatalf("conflicting re-register error = %v, want ErrDuplicateBinding", err)
	}
	// Dropping the credential is also a conflict.
	conflicting = request
	conflicting.CredentialRef = nil
	if err := registry.Register(conflicting); !errors.Is(err, session.ErrDuplicateBinding) {
		t.Fatalf("credential-less re-register error = %v, want ErrDuplicateBinding", err)
	}
	// The original binding survived all rejected registrations.
	binding, err := registry.ResolveDownloader(context.Background(), acquisition.DownloaderSessionRequest{
		ProviderID: provider.Descriptor().ID, ConnectionID: connectionA,
		CredentialRef: credentialRef("secret-ref://connections/a"),
	})
	if err != nil {
		t.Fatalf("ResolveDownloader() after conflicts error = %v", err)
	}
	if binding.Downloader != provider {
		t.Fatal("resolved downloader is not the originally registered port")
	}
}

// storageOnlyProvider is a descriptor-only double used to prove a provider that
// does not advertise downloader capability is rejected at registration.
type storageOnlyProvider struct{}

func (storageOnlyProvider) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{
		ID: "storage-only-session", DisplayName: "storage only",
		Capabilities: contracts.CapabilitySet{Storage: true},
	}
}

func (storageOnlyProvider) Stat(context.Context, contracts.TargetPath) (contracts.StorageObject, error) {
	return contracts.StorageObject{}, errors.New("not implemented")
}

func (storageOnlyProvider) Access(context.Context, contracts.StorageObjectReference) (contracts.AccessTarget, error) {
	return contracts.AccessTarget{}, errors.New("not implemented")
}
