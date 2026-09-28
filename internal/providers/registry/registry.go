// Package registry binds provider-neutral ports to provider identifiers at the
// application composition boundary.
package registry

import (
	"errors"
	"fmt"
	"sync"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

var (
	ErrNotFound  = errors.New("provider not found")
	ErrDuplicate = errors.New("provider already registered")
)

// Entry groups the independently replaceable ports exposed by one provider.
type Entry struct {
	Descriptor contracts.Descriptor
	Storage    contracts.StorageProvider
	Downloader contracts.DownloaderProvider
	Share      contracts.ShareProvider
}

// Registry stores provider ports without importing concrete adapters.
type Registry struct {
	mu      sync.RWMutex
	entries map[contracts.ProviderID]Entry
}

func New() *Registry {
	return &Registry{entries: make(map[contracts.ProviderID]Entry)}
}

func (r *Registry) Register(entry Entry) error {
	if entry.Descriptor.ID == "" {
		return errors.New("provider id is required")
	}
	if entry.Storage == nil && entry.Downloader == nil && entry.Share == nil {
		return errors.New("at least one provider port is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[entry.Descriptor.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicate, entry.Descriptor.ID)
	}
	r.entries[entry.Descriptor.ID] = entry
	return nil
}

func (r *Registry) Get(id contracts.ProviderID) (Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, exists := r.entries[id]
	if !exists {
		return Entry{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	return entry, nil
}
