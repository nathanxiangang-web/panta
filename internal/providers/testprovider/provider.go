// Package testprovider provides a deterministic, provider-neutral in-memory
// implementation for contract tests and architecture acceptance. It is not
// wired into the Panta runtime.
package testprovider

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
)

const ID contracts.ProviderID = "gate0-memory"

var ErrNotFound = errors.New("test provider reference not found")

type Option func(*Provider)

// WithoutDownloadCancellation configures the optional cancellation operation
// to return the provider-neutral unsupported-capability error.
func WithoutDownloadCancellation() Option {
	return func(provider *Provider) {
		provider.downloadCancellationSupported = false
	}
}

type Provider struct {
	mu                            sync.Mutex
	descriptor                    contracts.Descriptor
	objects                       map[contracts.TargetPath]contracts.StorageObject
	tasks                         map[contracts.TaskReference]contracts.TaskStatus
	shares                        map[contracts.ShareReference]contracts.ShareDetails
	nextTask                      int
	nextShare                     int
	downloadCancellationSupported bool
}

var (
	_ contracts.StorageProvider    = (*Provider)(nil)
	_ contracts.DownloaderProvider = (*Provider)(nil)
	_ contracts.ShareProvider      = (*Provider)(nil)
)

func New(options ...Option) *Provider {
	knownTarget := KnownTarget()
	provider := &Provider{
		descriptor: contracts.Descriptor{
			ID:          ID,
			DisplayName: "Gate 0 in-memory provider",
			Capabilities: contracts.CapabilitySet{
				Storage: true, Downloader: true, Sharing: true,
			},
		},
		objects: map[contracts.TargetPath]contracts.StorageObject{
			knownTarget: {
				Reference: contracts.StorageObjectReference{Scope: knownTarget.Scope, Path: knownTarget.Path},
				Name:      "artifact.bin",
				SizeBytes: 4096,
			},
		},
		tasks:                         make(map[contracts.TaskReference]contracts.TaskStatus),
		shares:                        make(map[contracts.ShareReference]contracts.ShareDetails),
		downloadCancellationSupported: true,
	}
	for _, option := range options {
		option(provider)
	}
	return provider
}

func KnownTarget() contracts.TargetPath {
	return contracts.TargetPath{Scope: "library", Path: "/known/artifact.bin"}
}

func (provider *Provider) Descriptor() contracts.Descriptor {
	return provider.descriptor
}

func (provider *Provider) Stat(ctx context.Context, target contracts.TargetPath) (contracts.StorageObject, error) {
	if err := ctx.Err(); err != nil {
		return contracts.StorageObject{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	object, exists := provider.objects[target]
	if !exists {
		return contracts.StorageObject{}, ErrNotFound
	}
	return object, nil
}

func (provider *Provider) Access(ctx context.Context, reference contracts.StorageObjectReference) (contracts.AccessTarget, error) {
	if err := ctx.Err(); err != nil {
		return contracts.AccessTarget{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	for _, object := range provider.objects {
		if object.Reference == reference {
			return contracts.AccessTarget{
				Location:  fmt.Sprintf("memory://storage/%s%s", reference.Scope, reference.Path),
				ExpiresAt: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
			}, nil
		}
	}
	return contracts.AccessTarget{}, ErrNotFound
}

func (provider *Provider) StartDownload(ctx context.Context, request contracts.DownloadRequest) (contracts.TaskReference, error) {
	if err := ctx.Err(); err != nil {
		return contracts.TaskReference{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.nextTask++
	reference := contracts.TaskReference{Value: fmt.Sprintf("task-%04d", provider.nextTask)}
	provider.tasks[reference] = contracts.TaskStatus{Reference: reference, State: contracts.TaskStatePending}
	return reference, nil
}

func (provider *Provider) DownloadStatus(ctx context.Context, reference contracts.TaskReference) (contracts.TaskStatus, error) {
	if err := ctx.Err(); err != nil {
		return contracts.TaskStatus{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	status, exists := provider.tasks[reference]
	if !exists {
		return contracts.TaskStatus{}, ErrNotFound
	}
	return status, nil
}

func (provider *Provider) CancelDownload(ctx context.Context, reference contracts.TaskReference) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if !provider.downloadCancellationSupported {
		return contracts.ErrUnsupportedCapability
	}
	status, exists := provider.tasks[reference]
	if !exists {
		return ErrNotFound
	}
	status.State = contracts.TaskStateCanceled
	status.Message = "canceled by contract test"
	provider.tasks[reference] = status
	return nil
}

func (provider *Provider) CreateShare(ctx context.Context, request contracts.ShareRequest) (contracts.ShareReference, error) {
	if err := ctx.Err(); err != nil {
		return contracts.ShareReference{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	provider.nextShare++
	reference := contracts.ShareReference{Value: fmt.Sprintf("share-%04d", provider.nextShare)}
	provider.shares[reference] = contracts.ShareDetails{
		Reference: reference,
		Object:    request.Object,
		Active:    true,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	return reference, nil
}

func (provider *Provider) InspectShare(ctx context.Context, reference contracts.ShareReference) (contracts.ShareDetails, error) {
	if err := ctx.Err(); err != nil {
		return contracts.ShareDetails{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	details, exists := provider.shares[reference]
	if !exists {
		return contracts.ShareDetails{}, ErrNotFound
	}
	return details, nil
}

func (provider *Provider) ShareAccess(ctx context.Context, reference contracts.ShareReference) (contracts.AccessTarget, error) {
	if err := ctx.Err(); err != nil {
		return contracts.AccessTarget{}, err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	details, exists := provider.shares[reference]
	if !exists || !details.Active {
		return contracts.AccessTarget{}, ErrNotFound
	}
	return contracts.AccessTarget{
		Location:  "memory://share/" + reference.Value,
		ExpiresAt: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
	}, nil
}

func (provider *Provider) RevokeShare(ctx context.Context, reference contracts.ShareReference) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	details, exists := provider.shares[reference]
	if !exists {
		return ErrNotFound
	}
	details.Active = false
	provider.shares[reference] = details
	return nil
}
