package bootstrap

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

const testCookie = "synthetic-cookie-value"
const testHintToken = "0123456789abcdef0123456789abcdef"

func protectedFixture(t *testing.T) (string, string) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("protected mount requires Linux ownership semantics")
	}
	base := t.TempDir()
	if err := os.Chmod(base, 0700); err != nil {
		t.Fatal(err)
	}
	mount := filepath.Join(base, "secrets")
	if err := os.Mkdir(mount, 0700); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]string{"hint.token": testHintToken, "connection.cookie": testCookie} {
		if err := os.WriteFile(filepath.Join(mount, name), []byte(value), 0600); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(base, "bootstrap.json")
	writeConfig(t, path, mount, `"sessions":[{"provider_id":"115","connection_id":"connection-a","credential_ref":"opaque-ref","secret_file":"connection.cookie"}]`)
	return path, mount
}

func writeConfig(t *testing.T, path, mount, tail string) {
	t.Helper()
	content := `{"secret_dir":"` + mount + `","hint_base_url":"http://127.0.0.1:19100","hint_token_file":"hint.token",` + tail + `}`
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestProtectedBootstrapLoadsFreshSecretsAndRotation(t *testing.T) {
	path, mount := protectedFixture(t)
	deps, err := Load(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if deps.HintToken != testHintToken || len(deps.Sessions) != 1 {
		t.Fatalf("invalid bootstrap metadata")
	}
	first, err := deps.Secrets.ResolveSecret(context.Background(), contracts.CredentialRef("opaque-ref"))
	if err != nil || string(first) != testCookie {
		t.Fatal("first secret resolution failed")
	}
	first[0] = 'X'
	second, err := deps.Secrets.ResolveSecret(context.Background(), contracts.CredentialRef("opaque-ref"))
	if err != nil || string(second) != testCookie {
		t.Fatal("resolver returned shared mutable bytes")
	}
	if err := os.WriteFile(filepath.Join(mount, "connection.cookie"), []byte("rotated-synthetic-cookie"), 0600); err != nil {
		t.Fatal(err)
	}
	third, err := deps.Secrets.ResolveSecret(context.Background(), contracts.CredentialRef("opaque-ref"))
	if err != nil || string(third) != "rotated-synthetic-cookie" {
		t.Fatal("new resolution did not observe rotation")
	}
	if _, err := deps.Secrets.ResolveSecret(context.Background(), contracts.CredentialRef("other-ref")); !errors.Is(err, contracts.ErrSecretUnavailable) {
		t.Fatalf("unknown ref = %v", err)
	}
}

func TestProtectedBootstrapRejectsUnsafeSourcesWithoutDisclosure(t *testing.T) {
	for _, scenario := range []string{"missing file", "world readable cookie", "executable cookie", "unreadable cookie", "world readable token",
		"world readable config", "world readable mount", "symlink cookie", "symlink config",
		"path traversal", "duplicate session", "unknown plaintext field", "malformed JSON", "oversized secret", "relative config", "symlink mount"} {
		t.Run(scenario, func(t *testing.T) {
			path, mount := protectedFixture(t)
			switch scenario {
			case "missing file":
				if err := os.Remove(filepath.Join(mount, "connection.cookie")); err != nil {
					t.Fatal(err)
				}
			case "world readable cookie":
				if err := os.Chmod(filepath.Join(mount, "connection.cookie"), 0644); err != nil {
					t.Fatal(err)
				}
			case "executable cookie":
				if err := os.Chmod(filepath.Join(mount, "connection.cookie"), 0700); err != nil {
					t.Fatal(err)
				}
			case "unreadable cookie":
				if err := os.Chmod(filepath.Join(mount, "connection.cookie"), 0000); err != nil {
					t.Fatal(err)
				}
			case "world readable token":
				if err := os.Chmod(filepath.Join(mount, "hint.token"), 0644); err != nil {
					t.Fatal(err)
				}
			case "world readable config":
				if err := os.Chmod(path, 0644); err != nil {
					t.Fatal(err)
				}
			case "world readable mount":
				if err := os.Chmod(mount, 0755); err != nil {
					t.Fatal(err)
				}
			case "symlink cookie":
				if err := os.Remove(filepath.Join(mount, "connection.cookie")); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(filepath.Join(mount, "hint.token"), filepath.Join(mount, "connection.cookie")); err != nil {
					t.Fatal(err)
				}
			case "symlink config":
				original := filepath.Join(filepath.Dir(path), "original.json")
				if err := os.Rename(path, original); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(original, path); err != nil {
					t.Fatal(err)
				}
			case "path traversal":
				writeConfig(t, path, mount, `"sessions":[{"provider_id":"115","connection_id":"a","credential_ref":"opaque-ref","secret_file":"../connection.cookie"}]`)
			case "duplicate session":
				writeConfig(t, path, mount, `"sessions":[{"provider_id":"115","connection_id":"a","credential_ref":"opaque-ref","secret_file":"connection.cookie"},{"provider_id":"115","connection_id":"a","credential_ref":"opaque-ref","secret_file":"connection.cookie"}]`)
			case "unknown plaintext field":
				writeConfig(t, path, mount, `"cookie":"`+testCookie+`","sessions":[]`)
			case "malformed JSON":
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			case "oversized secret":
				if err := os.WriteFile(filepath.Join(mount, "connection.cookie"), []byte(strings.Repeat("x", maxSecretBytes+1)), 0600); err != nil {
					t.Fatal(err)
				}
			case "relative config":
				path = "bootstrap.json"
			case "symlink mount":
				link := filepath.Join(filepath.Dir(path), "linked-secrets")
				if err := os.Symlink(mount, link); err != nil {
					t.Fatal(err)
				}
				writeConfig(t, path, link, `"sessions":[{"provider_id":"115","connection_id":"a","credential_ref":"opaque-ref","secret_file":"connection.cookie"}]`)
			}
			_, err := Load(context.Background(), path)
			if !errors.Is(err, ErrUnsafeBootstrap) || strings.Contains(err.Error(), testCookie) || strings.Contains(err.Error(), mount) {
				t.Fatalf("unsafe bootstrap disclosed data or passed: %v", err)
			}
		})
	}
}
