package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}

	application, err := app.New(cfg)
	if err != nil {
		log.Fatalf("initialize application: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("panta started environment=%s", cfg.Environment)
	if err := application.Run(ctx); err != nil {
		log.Fatalf("run application: %v", err)
	}
}
