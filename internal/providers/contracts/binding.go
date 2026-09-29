package contracts

// DownloaderBinding is the provider-neutral projection of one registered
// provider: its descriptor identity plus its Downloader port when implemented.
//
// It lives in contracts so the provider registry and product domains can share
// one real type instead of two structurally identical named types. Go does not
// make distinct named types assignable to each other, so a duplicated projection
// would compile in isolation and still fail to wire the real registry to a
// consumer port.
type DownloaderBinding struct {
	Descriptor Descriptor
	Downloader DownloaderProvider
}
