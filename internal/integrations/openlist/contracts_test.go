package openlist_test

import (
	"context"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/integrations/openlist"
)

type visibilityDouble struct{}

func (visibilityDouble) Stat(_ context.Context, request openlist.StatRequest) (openlist.VisibilityFact, error) {
	return openlist.VisibilityFact{Mount: request.Mount, Path: request.Path, Visible: true}, nil
}

type accessDouble struct{}

func (accessDouble) Access(context.Context, openlist.AccessRequest) (openlist.AccessTarget, error) {
	return openlist.AccessTarget{Location: "memory://visible"}, nil
}

var (
	_ openlist.VisibilityPort = visibilityDouble{}
	_ openlist.AccessPort     = accessDouble{}
)

func TestOpenListPortsAcceptInMemoryDoubles(t *testing.T) {
	fact, err := (visibilityDouble{}).Stat(context.Background(), openlist.StatRequest{Mount: "library", Path: "/known/path"})
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if !fact.Visible {
		t.Fatal("Stat() returned a non-visible fact")
	}
}
