package indexcore_test

import (
	"context"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
)

type readDouble struct{}

func (readDouble) Resolve(_ context.Context, request indexcore.ResolveRequest) (indexcore.ResourceContext, error) {
	return indexcore.ResourceContext{ResourceID: "resource-1", RootID: request.RootID, CanonicalPath: request.Path, Present: true}, nil
}
func (readDouble) Browse(context.Context, indexcore.BrowseRequest) ([]indexcore.ResourceContext, error) {
	return nil, nil
}

type refreshDouble struct{}

func (refreshDouble) RequestScopedRefresh(context.Context, indexcore.ScopedRefreshRequest) (indexcore.RefreshReference, error) {
	return indexcore.RefreshReference{Value: "refresh-1"}, nil
}

var (
	_ indexcore.ReadPort          = readDouble{}
	_ indexcore.ScopedRefreshPort = refreshDouble{}
)

func TestDirectPathResolutionDoesNotRequireSearch(t *testing.T) {
	request := indexcore.ResolveRequest{RootID: "root-1", Path: "/known/path/file.bin"}
	resource, err := (readDouble{}).Resolve(context.Background(), request)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if resource.RootID != request.RootID || resource.CanonicalPath != request.Path || !resource.Present {
		t.Fatalf("Resolve() = %#v", resource)
	}
}
