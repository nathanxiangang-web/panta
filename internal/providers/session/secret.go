package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// StaticSecretResolver is a controlled composition double for the opaque secret
// port. It holds secret material outside product state and outside the domain,
// returns a fresh copy per resolution so callers cannot mutate the source, and
// never exposes the material through errors or any other channel.
//
// It is intentionally not a real secret backend: Gate 3.5 freezes the boundary
// only.
type StaticSecretResolver struct {
	mu      sync.Mutex
	secrets map[contracts.CredentialRef][]byte
}

var _ contracts.SecretResolver = (*StaticSecretResolver)(nil)

func NewStaticSecretResolver() *StaticSecretResolver {
	return &StaticSecretResolver{secrets: map[contracts.CredentialRef][]byte{}}
}

// Register stores one secret. The caller's slice is copied, so later mutation of
// the argument cannot change what this resolver returns.
func (resolver *StaticSecretResolver) Register(reference contracts.CredentialRef, secret []byte) error {
	if resolver == nil {
		return ErrInvalidRegistration
	}
	if reference == "" {
		return fmt.Errorf("%w: credential reference is required", ErrInvalidRegistration)
	}
	if len(secret) == 0 {
		return fmt.Errorf("%w: secret material is required", ErrInvalidRegistration)
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if resolver.secrets == nil {
		resolver.secrets = map[contracts.CredentialRef][]byte{}
	}
	resolver.secrets[reference] = append([]byte(nil), secret...)
	return nil
}

// ResolveSecret returns a copy of the secret material. The error carries only the
// opaque reference, never secret content.
func (resolver *StaticSecretResolver) ResolveSecret(_ context.Context, reference contracts.CredentialRef) ([]byte, error) {
	if resolver == nil {
		return nil, contracts.ErrSecretUnavailable
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	secret, exists := resolver.secrets[reference]
	if !exists {
		return nil, fmt.Errorf("%w: %s", contracts.ErrSecretUnavailable, reference)
	}
	return append([]byte(nil), secret...), nil
}
