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
	ErrNotFound     = errors.New("provider not found")
	ErrDuplicate    = errors.New("provider already registered")
	ErrInvalidEntry = errors.New("invalid provider registry entry")
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
	if err := validateEntry(entry); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[entry.Descriptor.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicate, entry.Descriptor.ID)
	}
	r.entries[entry.Descriptor.ID] = entry
	return nil
}

func validateEntry(entry Entry) error {
	if entry.Descriptor.ID == "" {
		return fmt.Errorf("%w: provider id is required", ErrInvalidEntry)
	}

	expectedCapabilities := contracts.CapabilitySet{
		Storage:    entry.Storage != nil,
		Downloader: entry.Downloader != nil,
		Sharing:    entry.Share != nil,
	}
	if expectedCapabilities == (contracts.CapabilitySet{}) {
		return fmt.Errorf("%w: at least one provider port is required", ErrInvalidEntry)
	}
	if entry.Descriptor.Capabilities != expectedCapabilities {
		return fmt.Errorf("%w: descriptor capabilities do not match supplied ports", ErrInvalidEntry)
	}

	ports := []struct {
		name string
		port contracts.DescribedProvider
	}{
		{name: "storage", port: entry.Storage},
		{name: "downloader", port: entry.Downloader},
		{name: "share", port: entry.Share},
	}
	for _, candidate := range ports {
		if candidate.port == nil {
			continue
		}
		descriptor := candidate.port.Descriptor()
		if descriptor.ID != entry.Descriptor.ID {
			return fmt.Errorf("%w: %s port provider id %q does not match %q", ErrInvalidEntry, candidate.name, descriptor.ID, entry.Descriptor.ID)
		}
		if descriptor.Capabilities != entry.Descriptor.Capabilities {
			return fmt.Errorf("%w: %s port capabilities do not match entry descriptor", ErrInvalidEntry, candidate.name)
		}
	}

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
