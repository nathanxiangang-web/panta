// Package bootstrap loads operator-protected acquisition runtime inputs. It
// never persists secret bytes or places them in errors, logs, or Job state.
package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/runtime"
	"github.com/nathanxiangang-web/panta/internal/storage"
)

var ErrUnsafeBootstrap = errors.New("unsafe acquisition bootstrap configuration or secret mount")

const maxConfigBytes = 32 << 10
const maxSecretBytes = 16 << 10

// ReadProtectedFile is shared by explicit operator inputs. It returns bytes
// only to the caller; paths and contents are never included in its errors.
func ReadProtectedFile(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || limit < 1 {
		return nil, ErrUnsafeBootstrap
	}
	return readMountedFile(filepath.Dir(path), filepath.Base(path), limit)
}

type fileConfig struct {
	SecretDir     string        `json:"secret_dir"`
	HintBaseURL   string        `json:"hint_base_url"`
	HintTokenFile string        `json:"hint_token_file"`
	Sessions      []fileSession `json:"sessions"`
}
type fileSession struct {
	ProviderID    string `json:"provider_id"`
	ConnectionID  string `json:"connection_id"`
	CredentialRef string `json:"credential_ref"`
	SecretFile    string `json:"secret_file"`
}

// FileResolver resolves only explicitly listed opaque references from a
// protected mount. Every call re-opens the file and returns fresh bytes; a
// changed secret becomes effective only after constructing a new runtime.
type FileResolver struct {
	root  string
	files map[contracts.CredentialRef]string
}

var _ contracts.SecretResolver = (*FileResolver)(nil)

func (resolver *FileResolver) ResolveSecret(ctx context.Context, ref contracts.CredentialRef) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resolver == nil {
		return nil, contracts.ErrSecretUnavailable
	}
	name, found := resolver.files[ref]
	if !found {
		return nil, contracts.ErrSecretUnavailable
	}
	value, err := readMountedFile(resolver.root, name, maxSecretBytes)
	if err != nil || len(value) == 0 {
		return nil, contracts.ErrSecretUnavailable
	}
	if err := ctx.Err(); err != nil {
		zero(value)
		return nil, err
	}
	return value, nil
}

// Load validates a protected configuration file and every required mounted
// secret. It returns the accepted runtime's injected dependencies; the caller
// retains no raw provider secret bytes. Hint token bytes exist only in memory.
func Load(ctx context.Context, configPath string) (runtime.RuntimeDependencies, error) {
	if err := ctx.Err(); err != nil {
		return runtime.RuntimeDependencies{}, err
	}
	content, err := ReadProtectedFile(configPath, maxConfigBytes)
	if err != nil {
		return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
	}
	defer zero(content)
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	var cfg fileConfig
	if err := decoder.Decode(&cfg); err != nil {
		return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
	}
	if decoder.Decode(new(any)) != io.EOF {
		return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
	}
	if cfg.HintBaseURL == "" || len(cfg.Sessions) == 0 || !safeLeaf(cfg.HintTokenFile) {
		return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
	}
	if err := validateRoot(cfg.SecretDir); err != nil {
		return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
	}
	tokenBytes, err := readMountedFile(cfg.SecretDir, cfg.HintTokenFile, maxSecretBytes)
	if err != nil || len(tokenBytes) == 0 {
		return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
	}
	defer zero(tokenBytes)
	resolver := &FileResolver{root: cfg.SecretDir, files: make(map[contracts.CredentialRef]string, len(cfg.Sessions))}
	deps := runtime.RuntimeDependencies{Secrets: resolver, HintBaseURL: cfg.HintBaseURL, HintToken: string(tokenBytes)}
	seenConnections := make(map[storage.ConnectionID]bool, len(cfg.Sessions))
	for _, entry := range cfg.Sessions {
		ref := contracts.CredentialRef(entry.CredentialRef)
		id := storage.ConnectionID(entry.ConnectionID)
		if entry.ProviderID != "115" || id == "" || ref == "" || !safeLeaf(entry.SecretFile) ||
			seenConnections[id] || strings.TrimSpace(string(ref)) != string(ref) {
			return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
		}
		if existing, found := resolver.files[ref]; found && existing != entry.SecretFile {
			return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
		}
		resolver.files[ref] = entry.SecretFile
		seenConnections[id] = true
		deps.Sessions = append(deps.Sessions, runtime.RuntimeSession{
			ProviderID: contracts.ProviderID(entry.ProviderID), ConnectionID: id, CredentialRef: ref})
		value, err := resolver.ResolveSecret(ctx, ref)
		if err != nil {
			return runtime.RuntimeDependencies{}, ErrUnsafeBootstrap
		}
		zero(value)
	}
	return deps, nil
}

func safeLeaf(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name &&
		!strings.ContainsAny(name, "/\\\x00")
}

func validateRoot(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrUnsafeBootstrap
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil || resolved != path {
		return ErrUnsafeBootstrap
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 || info.Mode().Perm()&0700 != 0700 ||
		!ownedByCurrentUser(info) {
		return ErrUnsafeBootstrap
	}
	return nil
}

func readMountedFile(rootPath, name string, limit int64) ([]byte, error) {
	if !safeLeaf(name) || validateRoot(rootPath) != nil {
		return nil, ErrUnsafeBootstrap
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, ErrUnsafeBootstrap
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 ||
		info.Mode().Perm()&0400 == 0 || info.Mode().Perm()&0100 != 0 || info.Size() < 1 || info.Size() > limit ||
		!ownedByCurrentUser(info) {
		return nil, ErrUnsafeBootstrap
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, ErrUnsafeBootstrap
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || !opened.Mode().IsRegular() ||
		opened.Mode().Perm()&0077 != 0 || opened.Mode().Perm()&0100 != 0 || opened.Size() > limit {
		return nil, ErrUnsafeBootstrap
	}
	content, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || len(content) == 0 || int64(len(content)) > limit {
		return nil, ErrUnsafeBootstrap
	}
	return content, nil
}

func zero(value []byte) {
	for i := range value {
		value[i] = 0
	}
}

// Category avoids leaking path or secret text while preserving error identity.
func Category(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return fmt.Errorf("%w", ErrUnsafeBootstrap)
}
