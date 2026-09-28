package registry

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

type storageDouble struct {
	descriptor contracts.Descriptor
}

func (double storageDouble) Descriptor() contracts.Descriptor {
	return double.descriptor
}
func (storageDouble) Stat(context.Context, contracts.TargetPath) (contracts.StorageObject, error) {
	return contracts.StorageObject{}, nil
}
func (storageDouble) Access(context.Context, contracts.StorageObjectReference) (contracts.AccessTarget, error) {
	return contracts.AccessTarget{}, nil
}

func TestRegistryStoresPortsByProviderID(t *testing.T) {
	reg := New()
	descriptor := contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Storage: true}}
	entry := Entry{
		Descriptor: descriptor,
		Storage:    storageDouble{descriptor: descriptor},
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

func TestRegistryRejectsContradictoryProviderIdentityAndCapabilities(t *testing.T) {
	storageCapability := contracts.CapabilitySet{Storage: true}

	tests := []struct {
		name  string
		entry Entry
	}{
		{
			name: "port provider id differs from entry",
			entry: Entry{
				Descriptor: contracts.Descriptor{ID: "entry", Capabilities: storageCapability},
				Storage:    storageDouble{descriptor: contracts.Descriptor{ID: "port", Capabilities: storageCapability}},
			},
		},
		{
			name: "advertised capability has no matching port",
			entry: Entry{
				Descriptor: contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Downloader: true}},
				Storage:    storageDouble{descriptor: contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Downloader: true}}},
			},
		},
		{
			name: "port capability differs from entry",
			entry: Entry{
				Descriptor: contracts.Descriptor{ID: "memory", Capabilities: storageCapability},
				Storage:    storageDouble{descriptor: contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Storage: true, Sharing: true}}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := New().Register(test.entry); !errors.Is(err, ErrInvalidEntry) {
				t.Fatalf("Register() error = %v, want ErrInvalidEntry", err)
			}
		})
	}
}
