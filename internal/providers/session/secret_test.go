package session_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/providers/session"
)

func TestStaticSecretResolverReturnsCopiesAndHidesContent(t *testing.T) {
	resolver := session.NewStaticSecretResolver()
	const reference contracts.CredentialRef = "secret-ref://connections/a"
	original := []byte("super-secret-token-value")
	if err := resolver.Register(reference, original); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	// Mutating the caller's slice must not change stored material.
	original[0] = 'X'

	ctx := context.Background()
	first, err := resolver.ResolveSecret(ctx, reference)
	if err != nil {
		t.Fatalf("ResolveSecret() error = %v", err)
	}
	if string(first) != "super-secret-token-value" {
		t.Fatalf("ResolveSecret() = %q, want the registered value", first)
	}
	// Mutating a resolved copy must not change stored material.
	first[0] = 'X'
	second, err := resolver.ResolveSecret(ctx, reference)
	if err != nil {
		t.Fatalf("second ResolveSecret() error = %v", err)
	}
	if string(second) != "super-secret-token-value" {
		t.Fatalf("second ResolveSecret() = %q, stored material was mutated", second)
	}
}

func TestStaticSecretResolverMissingReferenceIsSafeToLog(t *testing.T) {
	resolver := session.NewStaticSecretResolver()
	const missing contracts.CredentialRef = "secret-ref://connections/missing"
	_, err := resolver.ResolveSecret(context.Background(), missing)
	if !errors.Is(err, contracts.ErrSecretUnavailable) {
		t.Fatalf("ResolveSecret(missing) error = %v, want ErrSecretUnavailable", err)
	}
	if !strings.Contains(err.Error(), string(missing)) {
		t.Fatalf("error %v should name the opaque reference for diagnosis", err)
	}
}

func TestStaticSecretResolverRejectsInvalidRegistrations(t *testing.T) {
	resolver := session.NewStaticSecretResolver()
	if err := resolver.Register("", []byte("value")); !errors.Is(err, session.ErrInvalidRegistration) {
		t.Fatalf("Register(empty reference) error = %v, want ErrInvalidRegistration", err)
	}
	if err := resolver.Register("secret-ref://connections/a", nil); !errors.Is(err, session.ErrInvalidRegistration) {
		t.Fatalf("Register(nil secret) error = %v, want ErrInvalidRegistration", err)
	}
}
