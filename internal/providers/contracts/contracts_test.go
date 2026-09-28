package contracts_test

import (
	"context"
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

type shareDouble struct{}

func (shareDouble) Descriptor() contracts.Descriptor {
	return contracts.Descriptor{ID: "memory", Capabilities: contracts.CapabilitySet{Sharing: true}}
}
func (shareDouble) CreateShare(context.Context, contracts.ShareRequest) (contracts.ShareReference, error) {
	return contracts.ShareReference{Value: "share-1"}, nil
}
func (shareDouble) ShareAccess(context.Context, contracts.ShareReference) (contracts.AccessTarget, error) {
	return contracts.AccessTarget{Location: "memory://share-1"}, nil
}

var (
	_ contracts.StorageProvider    = storageDouble{}
	_ contracts.DownloaderProvider = downloaderDouble{}
	_ contracts.ShareProvider      = shareDouble{}
)

func TestProviderPortsAcceptInMemoryDoubles(t *testing.T) {
	ctx := context.Background()

	if _, err := (storageDouble{}).Stat(ctx, contracts.TargetPath{Scope: "library", Path: "/known/path"}); err != nil {
		t.Fatalf("storage Stat() error = %v", err)
	}
	task, err := (downloaderDouble{}).StartDownload(ctx, contracts.DownloadRequest{})
	if err != nil || task.Value == "" {
		t.Fatalf("StartDownload() = %#v, %v", task, err)
	}
	share, err := (shareDouble{}).CreateShare(ctx, contracts.ShareRequest{})
	if err != nil || share.Value == "" {
		t.Fatalf("CreateShare() = %#v, %v", share, err)
	}
}
