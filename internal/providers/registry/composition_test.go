// Package registry_test contains the compile-time contract assertion that wires
// the real provider registry to the acquisition execution port.
package registry_test

import (
	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/providers/registry"
)

// TestRegistrySatisfiesAcquisitionProviderCatalog pins the real composition at
// compile time. Earlier rounds of Gate 3.4 compiled the acquisition port against a
// local test double only, which hid that the registry's lookup method returned a
// distinct named type and therefore could not satisfy the port at all.
var _ acquisition.ProviderCatalog = (*registry.Registry)(nil)
