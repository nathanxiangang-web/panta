//go:build !linux

package bootstrap

import "os"

// Protected file-mount bootstrap is currently supported only on Linux hosts.
// On other platforms the operator path fails closed rather than assuming that
// Unix permission bits carry the same meaning.
func ownedByCurrentUser(os.FileInfo) bool { return false }
