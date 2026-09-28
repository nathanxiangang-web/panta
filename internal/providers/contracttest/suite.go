// Package contracttest contains provider-neutral conformance tests reusable by
// the Gate 0 test provider and future provider adapters.
package contracttest

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

type Ports struct {
	Storage    contracts.StorageProvider
	Downloader contracts.DownloaderProvider
	Share      contracts.ShareProvider
}

type Factory func(*testing.T) Ports

type Cases struct {
	KnownTarget     contracts.TargetPath
	DownloadRequest contracts.DownloadRequest
}

func Run(t *testing.T, factory Factory, cases Cases) {
	t.Helper()
	t.Run("descriptor", func(t *testing.T) {
		ports := factory(t)
		descriptor := ports.Storage.Descriptor()
		if descriptor.ID == "" || descriptor.DisplayName == "" {
			t.Fatalf("descriptor identity = %#v", descriptor)
		}
		wantCapabilities := contracts.CapabilitySet{Storage: true, Downloader: true, Sharing: true}
		if descriptor.Capabilities != wantCapabilities {
			t.Fatalf("descriptor capabilities = %#v, want %#v", descriptor.Capabilities, wantCapabilities)
		}
		if ports.Downloader.Descriptor() != descriptor || ports.Share.Descriptor() != descriptor {
			t.Fatal("provider ports returned inconsistent descriptors")
		}
	})

	t.Run("storage stat and access", func(t *testing.T) {
		ports := factory(t)
		object, err := ports.Storage.Stat(context.Background(), cases.KnownTarget)
		if err != nil {
			t.Fatalf("Stat() error = %v", err)
		}
		if object.Reference.Scope != cases.KnownTarget.Scope || object.Reference.Path != cases.KnownTarget.Path || object.Name == "" {
			t.Fatalf("Stat() = %#v", object)
		}
		access, err := ports.Storage.Access(context.Background(), object.Reference)
		if err != nil || access.Location == "" {
			t.Fatalf("Access() = %#v, %v", access, err)
		}
	})

	t.Run("download start status cancel", func(t *testing.T) {
		ports := factory(t)
		reference, err := ports.Downloader.StartDownload(context.Background(), cases.DownloadRequest)
		if err != nil || reference.Value == "" {
			t.Fatalf("StartDownload() = %#v, %v", reference, err)
		}
		status, err := ports.Downloader.DownloadStatus(context.Background(), reference)
		if err != nil || status.Reference != reference || !validTaskState(status.State) {
			t.Fatalf("DownloadStatus() = %#v, %v", status, err)
		}
		if err := ports.Downloader.CancelDownload(context.Background(), reference); err != nil {
			t.Fatalf("CancelDownload() error = %v", err)
		}
		canceled, err := ports.Downloader.DownloadStatus(context.Background(), reference)
		if err != nil || canceled.Reference != reference || canceled.State != contracts.TaskStateCanceled {
			t.Fatalf("DownloadStatus() after cancel = %#v, %v", canceled, err)
		}
	})

	t.Run("share create inspect access revoke", func(t *testing.T) {
		ports := factory(t)
		object, err := ports.Storage.Stat(context.Background(), cases.KnownTarget)
		if err != nil {
			t.Fatalf("Stat() error = %v", err)
		}
		reference, err := ports.Share.CreateShare(context.Background(), contracts.ShareRequest{Object: object.Reference})
		if err != nil || reference.Value == "" {
			t.Fatalf("CreateShare() = %#v, %v", reference, err)
		}
		details, err := ports.Share.InspectShare(context.Background(), reference)
		if err != nil || details.Reference != reference || details.Object != object.Reference || !details.Active {
			t.Fatalf("InspectShare() = %#v, %v", details, err)
		}
		access, err := ports.Share.ShareAccess(context.Background(), reference)
		if err != nil || access.Location == "" {
			t.Fatalf("ShareAccess() = %#v, %v", access, err)
		}
		if err := ports.Share.RevokeShare(context.Background(), reference); err != nil {
			t.Fatalf("RevokeShare() error = %v", err)
		}
		revoked, err := ports.Share.InspectShare(context.Background(), reference)
		if err != nil || revoked.Reference != reference || revoked.Active {
			t.Fatalf("InspectShare() after revoke = %#v, %v", revoked, err)
		}
	})
}

func RunUnsupportedDownloadCancellation(t *testing.T, factory Factory, request contracts.DownloadRequest) {
	t.Helper()
	ports := factory(t)
	reference, err := ports.Downloader.StartDownload(context.Background(), request)
	if err != nil {
		t.Fatalf("StartDownload() error = %v", err)
	}
	if err := ports.Downloader.CancelDownload(context.Background(), reference); !errors.Is(err, contracts.ErrUnsupportedCapability) {
		t.Fatalf("CancelDownload() error = %v, want ErrUnsupportedCapability", err)
	}
}

func validTaskState(state contracts.TaskState) bool {
	switch state {
	case contracts.TaskStatePending, contracts.TaskStateRunning, contracts.TaskStateSucceeded,
		contracts.TaskStateFailed, contracts.TaskStateCanceled:
		return true
	default:
		return false
	}
}
