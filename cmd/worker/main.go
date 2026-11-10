package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/amh1k/soup-job-queue/internal/config"
	"github.com/amh1k/soup-job-queue/internal/database"
	"github.com/amh1k/soup-job-queue/internal/jobs"
	"github.com/amh1k/soup-job-queue/internal/worker"
	"github.com/amh1k/soup-job-queue/migrations"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if err := run(); err != nil {
		slog.Error("worker failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := database.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns, cfg.DBTimeout)
	if err != nil {
		return err
	}
	defer pool.Close()
	// Compose orders migrations before workers; this guard also protects local use.
	check, cancel := context.WithTimeout(ctx, cfg.DBTimeout)
	err = migrations.Check(check, pool)
	cancel()
	if err != nil {
		return err
	}
	store, err := jobs.NewStoreWithLease(pool, cfg.Options.LeaseDuration)
	if err != nil {
		return err
	}
	runtime, err := worker.New(store, cfg.Options, slog.Default())
	if err != nil {
		return err
	}
	return runtime.Run(ctx)
}
