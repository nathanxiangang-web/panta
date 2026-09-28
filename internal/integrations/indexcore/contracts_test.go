package indexcore_test

import (
	"context"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
)

type readDouble struct{}

func (readDouble) Resolve(_ context.Context, request indexcore.ResolveRequest) (indexcore.ResolveResult, error) {
	return indexcore.ResolveResult{Matches: []indexcore.ResourceContext{{
		ResourceID: "resource-1", RootID: request.RootID, CanonicalPath: &request.Path,
		Presence: indexcore.ResourcePresent,
	}}}, nil
}
func (readDouble) Browse(context.Context, indexcore.BrowseRequest) (indexcore.ResourcePage, error) {
	return indexcore.ResourcePage{}, nil
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
	result, err := (readDouble{}).Resolve(context.Background(), request)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if len(result.Matches) != 1 || result.Matches[0].RootID != request.RootID ||
		result.Matches[0].CanonicalPath == nil || *result.Matches[0].CanonicalPath != request.Path ||
		result.Matches[0].Presence != indexcore.ResourcePresent {
		t.Fatalf("Resolve() = %#v", result)
	}
}
