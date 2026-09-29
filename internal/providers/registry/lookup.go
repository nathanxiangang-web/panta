package registry

import (
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

// LookupDownloader resolves a provider identity to its downloader-capable
// registration. It returns ErrNotFound for an unknown provider.
//
// The result is the shared provider-neutral contracts.DownloaderBinding, so a
// consumer port that declares the same method signature is satisfied by this
// registry directly, with no adapter and no duplicated projection type.
func (r *Registry) LookupDownloader(id contracts.ProviderID) (contracts.DownloaderBinding, error) {
	entry, err := r.Get(id)
	if err != nil {
		return contracts.DownloaderBinding{}, err
	}
	return contracts.DownloaderBinding{Descriptor: entry.Descriptor, Downloader: entry.Downloader}, nil
}
