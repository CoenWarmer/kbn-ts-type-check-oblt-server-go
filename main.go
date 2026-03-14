// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License
// 2.0; you may not use this file except in compliance with the Elastic License
// 2.0.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kbn-ts-type-check-oblt-server-go/internal/config"
	"kbn-ts-type-check-oblt-server-go/internal/ingestion"
	"kbn-ts-type-check-oblt-server-go/internal/server"
	"kbn-ts-type-check-oblt-server-go/internal/source"
	"kbn-ts-type-check-oblt-server-go/internal/storage"
)

// appLogger implements both server.Logger and ingestion.Logger (identical method sets).
type appLogger struct{}

func (l *appLogger) Info(msg string) { log.Printf("[oblt-cache] %s", msg) }
func (l *appLogger) Warn(msg string) { log.Printf("[oblt-cache] WARN %s", msg) }

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	lg := &appLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var store storage.ArtifactStore
	switch cfg.DestinationType {
	case config.DestinationLocal:
		store = storage.NewLocalStore(cfg.DestinationPathOrBucket)
	case config.DestinationGCS:
		gcsStore, err := storage.NewGCSStore(ctx, cfg.DestinationPathOrBucket)
		if err != nil {
			return fmt.Errorf("create GCS store: %w", err)
		}
		store = gcsStore
	default:
		return fmt.Errorf("unknown destination type %q", cfg.DestinationType)
	}

	src := &source.Config{
		Bucket: cfg.SourceBucket,
		Prefix: cfg.SourcePrefix,
	}

	srv := server.New(cfg.Port, store, src, lg)

	// Start polling for new archives in the background.
	ingestion.StartPolling(ctx, src, store, lg, cfg.PollInterval)

	// Start the HTTP server in a goroutine so we can listen for shutdown signals.
	serverErr := make(chan error, 1)
	go func() {
		lg.Info(fmt.Sprintf("server listening on http://localhost:%d", cfg.Port))
		lg.Info("  GET  /health    - health check")
		lg.Info("  POST /artifacts - request artifacts (body: {commitSha, projects?})")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// Wait for OS signal or server error.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		lg.Info(fmt.Sprintf("received signal %s, shutting down...", sig))
	case err := <-serverErr:
		return fmt.Errorf("server error: %w", err)
	}

	// Graceful shutdown: stop accepting new connections and wait for in-flight requests.
	cancel() // stop background polling

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown: %w", err)
	}

	lg.Info("shutdown complete")
	return nil
}
