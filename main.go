// Command mongo-elastic replicates one or more MongoDB collections to
// Elasticsearch in real-time using MongoDB Change Streams.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"

	"github.com/abasse/mongo-elastic/internal/config"
	"github.com/abasse/mongo-elastic/internal/replicator"
)

func main() {
	// ── Load .env file if present (no-op when the file doesn't exist) ─────────
	// Variables already set in the environment always take precedence, so this
	// is safe to leave enabled in any deployment: Docker / Kubernetes inject
	// vars directly and will simply never have a .env file on disk.
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		// A file was found but could not be parsed — treat that as a hard error.
		slog.Error("failed to parse .env file", "error", err)
		os.Exit(1)
	}

	// ── Structured JSON logger (goes to stdout for log aggregators) ───────────
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel(),
	}))
	slog.SetDefault(logger)

	// ── Load and validate configuration ───────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	// Build a human-readable "col→idx" summary for the startup log.
	collectionSummary := make([]string, 0, len(cfg.Collections))
	for _, m := range cfg.Collections {
		collectionSummary = append(collectionSummary, m.Collection+"→"+m.Index)
	}

	logger.Info("mongo-elastic replicator starting",
		"mongo_uri", cfg.MongoURI,
		"mongo_db", cfg.MongoDB,
		"collections", collectionSummary,
		"es_addresses", cfg.ESAddresses,
		"token_store", cfg.TokenStore,
		"max_retries", cfg.MaxRetries,
		"initial_backoff", cfg.InitialBackoff.String(),
		"max_backoff", cfg.MaxBackoff.String(),
		"token_save_interval", cfg.TokenSaveInterval,
		"startup_full_sync", cfg.StartupFullSync,
		"stale_token_resync", cfg.StaleTokenResync,
		"sync_batch_size", cfg.SyncBatchSize,
	)

	// ── Build replicator (dials both backends with retry) ─────────────────────
	rep, err := replicator.New(cfg, logger)
	if err != nil {
		logger.Error("failed to initialise replicator", "error", err)
		os.Exit(1)
	}
	defer rep.Close()

	// ── Graceful-shutdown context ─────────────────────────────────────────────
	// signal.NotifyContext cancels ctx on SIGINT or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Run ───────────────────────────────────────────────────────────────────
	if err := rep.Run(ctx); err != nil {
		logger.Error("replicator exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("replicator stopped gracefully")
}

// logLevel reads LOG_LEVEL from the environment (default: info).
func logLevel() slog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
