package contracts

import (
	"context"
	"errors"
)

// CredentialRef is an opaque reference to secret or session material owned by an
// external secret boundary. It is safe to persist and to carry through domain
// state because it never contains the secret itself.
type CredentialRef string

// ErrSecretUnavailable reports that no secret material is available for a
// reference. It carries no secret content so it stays safe to log.
var ErrSecretUnavailable = errors.New("provider secret unavailable")

// SecretResolver is the frozen opaque secret lookup port. Concrete composition
// provides the implementation and owns all secret material.
//
// Gate 3.5 defines this boundary only: there is no real secret backend yet. An
// implementation must never log secret material, and callers must not store the
// returned bytes in product state or Job payloads.
type SecretResolver interface {
	ResolveSecret(context.Context, CredentialRef) ([]byte, error)
}
