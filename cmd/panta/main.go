package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
	"github.com/nathanxiangang-web/panta/internal/platform/operator"
	"github.com/nathanxiangang-web/panta/internal/platform/submission"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Printf("panta stopped: %v", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return runWithContext(ctx, args)
}

func runWithContext(ctx context.Context, args []string) error {
	if len(args) > 1 {
		return operator.ErrInvalidAction
	}
	if len(args) == 1 && args[0] != string(operator.Preflight) && args[0] != string(operator.Start) && args[0] != "submit-acquisition" {
		return operator.ErrInvalidAction
	}
	cfg, err := config.Load()
	if err != nil {
		return errors.New("invalid process configuration")
	}
	if len(args) == 1 {
		if args[0] == "submit-acquisition" {
			result, err := submission.Run(ctx, cfg, os.Getenv("PANTA_ACQUISITION_SUBMISSION_FILE"))
			if result.Status != "" {
				log.Printf("acquisition submission status=%s manifest_id=%s job_id=%s", result.Status, result.ManifestID, result.JobID)
			}
			return err
		}
		err := operator.Run(ctx, operator.Action(args[0]), cfg, os.Getenv("PANTA_ACQUISITION_BOOTSTRAP_FILE"), nil)
		if err != nil {
			return err
		}
		if args[0] == string(operator.Preflight) {
			log.Print("acquisition preflight OK")
		}
		return nil
	}
	application, err := app.New(cfg)
	if err != nil {
		return err
	}

	log.Printf("panta started environment=%s", cfg.Environment)
	return application.Run(ctx)
}
