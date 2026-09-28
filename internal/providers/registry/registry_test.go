package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

type storageDouble struct{}

func (storageDouble) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Storage: true}}
}
func (storageDouble) Stat(context.Context, contracts.TargetPath) (contracts.StorageObject, error) {
	return contracts.StorageObject{}, nil
}
func (storageDouble) Access(context.Context, contracts.StorageObjectReference) (contracts.AccessTarget, error) {
	return contracts.AccessTarget{}, nil
}

func TestRegistryStoresPortsByProviderID(t *testing.T) {
	reg := New()
	entry := Entry{
		Descriptor: contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Storage: true}},
		Storage:    storageDouble{},
	}
	if err := reg.Register(entry); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	got, err := reg.Get("memory")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got.Storage == nil {
		t.Fatal("Get() returned no storage port")
	}
	if err := reg.Register(entry); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate Register() error = %v, want ErrDuplicate", err)
	}
}
