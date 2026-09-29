package registry

import (
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// DownloaderLookup is a provider-neutral view of one registered provider: its
// descriptor plus its Downloader port when the provider implements one. It is a
// structural projection so the acquisition domain can define the matching port
// without depending on this package or on any adapter.
type DownloaderLookup struct {
	Descriptor contracts.Descriptor
	Downloader contracts.DownloaderProvider
}

// LookupDownloader resolves a provider identity to its downloader-capable
// registration. It returns ErrNotFound for an unknown provider.
func (r *Registry) LookupDownloader(id contracts.ProviderID) (DownloaderLookup, error) {
	entry, err := r.Get(id)
	if err != nil {
		return DownloaderLookup{}, err
	}
	return DownloaderLookup{Descriptor: entry.Descriptor, Downloader: entry.Downloader}, nil
}
