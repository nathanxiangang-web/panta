// Package session is the provider-session composition boundary. It binds one
// downloader port to an exact provider/connection credential identity so that
// provider execution is scoped to a configured StorageConnection rather than to a
// provider identity alone.
//
// It deliberately depends on the acquisition port and on the opaque storage
// ConnectionID. It never depends on persistence, OpenList, IndexCore, Search, the
// Agent runtime, authentication implementations, or any concrete adapter.
package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

var (
	ErrInvalidRegistration = errors.New("invalid provider session registration")
	ErrDuplicateBinding    = errors.New("provider session binding already registered")
	ErrSessionNotFound     = errors.New("provider session binding not found")
	ErrCredentialMismatch  = errors.New("provider session credential mismatch")
)

// Binding is one explicit downloader registration for an exact provider and
// connection identity. It contains a port, never secret material: the port is
// responsible for holding any resolved session internally.
type Binding struct {
	Downloader contracts.DownloaderProvider
	// CredentialRef is nil for a provider explicitly registered without
	// credentials. A non-nil value must match the CredentialRef of a request
	// exactly for this binding to resolve.
	CredentialRef *string
}

// RegistrationRequest is the explicit input used to register one binding.
type RegistrationRequest struct {
	ProviderID    contracts.ProviderID
	ConnectionID  string
	CredentialRef *string
	Downloader    contracts.DownloaderProvider
}

type bindingKey struct {
	providerID   contracts.ProviderID
	connectionID string
}

// Registry resolves downloader ports by exact provider and connection identity.
// The same ProviderID may back several StorageConnections, each with its own
// credential binding.
type Registry struct {
	bindings map[bindingKey]Binding
}

var _ acquisition.DownloaderSessionResolver = (*Registry)(nil)

func New() *Registry {
	return &Registry{bindings: map[bindingKey]Binding{}}
}

// Register adds one explicit binding. Re-registering the same provider and
// connection is rejected unless the identity is byte-identical in every respect,
// in which case it is deliberately idempotent.
func (registry *Registry) Register(request RegistrationRequest) error {
	if registry == nil {
		return ErrInvalidRegistration
	}
	if registry.bindings == nil {
		registry.bindings = map[bindingKey]Binding{}
	}
	if err := validateRegistration(request); err != nil {
		return err
	}
	key := bindingKey{providerID: request.ProviderID, connectionID: string(request.ConnectionID)}
	candidate := Binding{Downloader: request.Downloader, CredentialRef: cloneString(request.CredentialRef)}
	if existing, exists := registry.bindings[key]; exists {
		if !sameCredential(existing.CredentialRef, candidate.CredentialRef) {
			return fmt.Errorf("%w: provider %s connection %s", ErrDuplicateBinding, request.ProviderID, request.ConnectionID)
		}
		return nil
	}
	registry.bindings[key] = candidate
	return nil
}

// ResolveDownloader returns the downloader bound to the exact provider,
// connection, and credential identity of the request. It fails closed on any
// mismatch and performs no secret lookup.
func (registry *Registry) ResolveDownloader(
	_ context.Context,
	request acquisition.DownloaderSessionRequest,
) (contracts.DownloaderBinding, error) {
	if registry == nil || !request.ProviderID.Valid() || request.ConnectionID == "" {
		return contracts.DownloaderBinding{}, fmt.Errorf("%w: %s", ErrSessionNotFound, request.ProviderID)
	}
	binding, exists := registry.bindings[bindingKey{
		providerID: request.ProviderID, connectionID: string(request.ConnectionID),
	}]
	if !exists {
		return contracts.DownloaderBinding{}, fmt.Errorf("%w: provider %s connection %s",
			ErrSessionNotFound, request.ProviderID, request.ConnectionID)
	}
	if !sameCredential(binding.CredentialRef, request.CredentialRef) {
		// The message intentionally names the identity but never a credential
		// value, so it stays safe to log.
		return contracts.DownloaderBinding{}, fmt.Errorf("%w: provider %s connection %s",
			ErrCredentialMismatch, request.ProviderID, request.ConnectionID)
	}
	if binding.Downloader == nil {
		return contracts.DownloaderBinding{}, fmt.Errorf("%w: provider %s connection %s has no downloader port",
			ErrSessionNotFound, request.ProviderID, request.ConnectionID)
	}
	return contracts.DownloaderBinding{
		Descriptor: binding.Downloader.Descriptor(),
		Downloader: binding.Downloader,
	}, nil
}

func validateRegistration(request RegistrationRequest) error {
	if !request.ProviderID.Valid() || request.ConnectionID == "" || request.Downloader == nil {
		return fmt.Errorf("%w: provider, connection, and downloader are required", ErrInvalidRegistration)
	}
	descriptor := request.Downloader.Descriptor()
	if descriptor.ID != request.ProviderID {
		return fmt.Errorf("%w: downloader identity %s does not match %s",
			ErrInvalidRegistration, descriptor.ID, request.ProviderID)
	}
	if !descriptor.Capabilities.Downloader {
		return fmt.Errorf("%w: provider %s does not advertise downloader capability",
			ErrInvalidRegistration, request.ProviderID)
	}
	if request.CredentialRef != nil && *request.CredentialRef == "" {
		return fmt.Errorf("%w: credential reference must be nil or non-empty", ErrInvalidRegistration)
	}
	return nil
}

func sameCredential(existing, requested *string) bool {
	if existing == nil || requested == nil {
		return existing == nil && requested == nil
	}
	return *existing == *requested
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
