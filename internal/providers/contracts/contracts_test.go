package contracts_test

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

type downloaderDouble struct{}

func (downloaderDouble) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Downloader: true}}
}
func (downloaderDouble) StartDownload(context.Context, contracts.DownloadRequest) (contracts.TaskReference, error) {
	return contracts.TaskReference{Value: "task-1"}, nil
}
func (downloaderDouble) DownloadStatus(context.Context, contracts.TaskReference) (contracts.TaskStatus, error) {
	return contracts.TaskStatus{State: contracts.TaskStateSucceeded}, nil
}
func (downloaderDouble) CancelDownload(context.Context, contracts.TaskReference) error {
	return nil
}

type unsupportedCancelDouble struct{ downloaderDouble }

func (unsupportedCancelDouble) CancelDownload(context.Context, contracts.TaskReference) error {
	return contracts.ErrUnsupportedCapability
}

type shareDouble struct{}

func (shareDouble) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Sharing: true}}
}
func (shareDouble) CreateShare(context.Context, contracts.ShareRequest) (contracts.ShareReference, error) {
	return contracts.ShareReference{Value: "share-1"}, nil
}
func (shareDouble) InspectShare(_ context.Context, reference contracts.ShareReference) (contracts.ShareDetails, error) {
	return contracts.ShareDetails{Reference: reference, Active: true}, nil
}
func (shareDouble) ShareAccess(context.Context, contracts.ShareReference) (contracts.AccessTarget, error) {
	return contracts.AccessTarget{Location: "memory://share-1"}, nil
}
func (shareDouble) RevokeShare(context.Context, contracts.ShareReference) error {
	return nil
}

var (
	_ contracts.StorageProvider    = storageDouble{}
	_ contracts.DownloaderProvider = downloaderDouble{}
	_ contracts.ShareProvider      = shareDouble{}
)

func TestProviderPortsAcceptInMemoryDoubles(t *testing.T) {
	ctx := context.Background()

	object, err := (storageDouble{}).Stat(ctx, contracts.TargetPath{Scope: "library", Path: "/known/path"})
	if err != nil {
		t.Fatalf("storage Stat() error = %v", err)
	}
	if _, err := (storageDouble{}).Access(ctx, object.Reference); err != nil {
		t.Fatalf("storage Access() error = %v", err)
	}
	task, err := (downloaderDouble{}).StartDownload(ctx, contracts.DownloadRequest{})
	if err != nil || task.Value == "" {
		t.Fatalf("StartDownload() = %#v, %v", task, err)
	}
	if _, err := (downloaderDouble{}).DownloadStatus(ctx, task); err != nil {
		t.Fatalf("DownloadStatus() error = %v", err)
	}
	if err := (downloaderDouble{}).CancelDownload(ctx, task); err != nil {
		t.Fatalf("CancelDownload() error = %v", err)
	}
	if err := (unsupportedCancelDouble{}).CancelDownload(ctx, task); !errors.Is(err, contracts.ErrUnsupportedCapability) {
		t.Fatalf("unsupported CancelDownload() error = %v", err)
	}
	share, err := (shareDouble{}).CreateShare(ctx, contracts.ShareRequest{})
	if err != nil || share.Value == "" {
		t.Fatalf("CreateShare() = %#v, %v", share, err)
	}
	details, err := (shareDouble{}).InspectShare(ctx, share)
	if err != nil || details.Reference != share || !details.Active {
		t.Fatalf("InspectShare() = %#v, %v", details, err)
	}
	if _, err := (shareDouble{}).ShareAccess(ctx, share); err != nil {
		t.Fatalf("ShareAccess() error = %v", err)
	}
	if err := (shareDouble{}).RevokeShare(ctx, share); err != nil {
		t.Fatalf("RevokeShare() error = %v", err)
	}
}
