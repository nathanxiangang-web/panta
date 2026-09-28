package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nathanxiangang-web/panta/internal/platform/config"
	pgstore "github.com/nathanxiangang-web/panta/internal/store/postgres"
)

func main() {
	os.Exit(run())
}

func run() int {
	if len(os.Args) != 2 || (os.Args[1] != "migrate" && os.Args[1] != "status") {
		fmt.Fprintln(os.Stderr, "usage: panta-db <migrate|status>")
		return 2
	}

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "load configuration: %v\n", err)
		return 1
	}
	if cfg.DatabaseURL == "" {
		fmt.Fprintln(os.Stderr, "PANTA_DATABASE_URL is required")
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgstore.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer pool.Close()

	migrator, err := pgstore.NewMigrator(pool)
	if err != nil {
		fmt.Fprintf(os.Stderr, "initialize migrator: %v\n", err)
		return 1
	}

	var status pgstore.SchemaStatus
	switch os.Args[1] {
	case "migrate":
		status, err = migrator.Apply(ctx)
	case "status":
		status, err = migrator.Status(ctx)
	}
	if err != nil {
		if errors.Is(err, pgstore.ErrIncompatibleSchema) {
			fmt.Fprintln(os.Stderr, pgstore.ErrIncompatibleSchema)
		} else {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if encodeErr := encoder.Encode(status); encodeErr != nil {
		fmt.Fprintf(os.Stderr, "encode schema status: %v\n", encodeErr)
		return 1
	}
	if err != nil || !status.Compatible {
		return 2
	}
	return 0
}
