package main

import (
	"context"
	"errors"
	"testing"

	"github.com/nathanxiangang-web/panta/internal/platform/operator"
)

func TestDefaultCommandNeverLoadsRuntimeDependencies(t *testing.T) {
	t.Setenv("PANTA_ACQUISITION_WORKER_ENABLED", "false")
	t.Setenv("PANTA_DATABASE_URL", "not-a-database-url")
	t.Setenv("PANTA_ACQUISITION_BOOTSTRAP_FILE", "/missing/protected/bootstrap.json")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runWithContext(ctx, nil); err != nil {
		t.Fatalf("disabled process = %v", err)
	}
}

func TestOperatorCommandRejectsUnrequestedModesAndRawArguments(t *testing.T) {
	t.Setenv("PANTA_ACQUISITION_WORKER_ENABLED", "true")
	for _, args := range [][]string{{"--cookie=unsafe"}, {"start-acquisition", "--hint-token=unsafe"}, {"preflight-acquisition", "--start"}} {
		if err := runWithContext(context.Background(), args); !errors.Is(err, operator.ErrInvalidAction) {
			t.Fatalf("args %v accepted: %v", args, err)
		}
	}
	if err := runWithContext(context.Background(), nil); err == nil {
		t.Fatal("enabled default mode started without explicit operator action")
	}
	t.Setenv("PANTA_ACQUISITION_WORKER_ENABLED", "false")
	if err := runWithContext(context.Background(), []string{"preflight-acquisition"}); !errors.Is(err, operator.ErrInvalidAction) {
		t.Fatalf("disabled preflight = %v", err)
	}
}
