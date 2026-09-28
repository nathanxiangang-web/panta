package app

import (
	"context"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/platform/config"
)

func TestApplicationRunsWithoutProviderImplementation(t *testing.T) {
	application, err := New(config.Config{Environment: "test"})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := application.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}
