package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tankard_workdir/internal/app"
	"tankard_workdir/internal/config"
	"tankard_workdir/lib/logger/sl"
)

func main() {
	cfg := config.MustLoad()

	log := sl.InitLogger(cfg.Env, os.Stdout)

	application, err := app.New(log, cfg)
	if err != nil {
		log.Error("failed to init app", sl.Err(err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := application.Run(); err != nil {
			log.Error("application stopped with error", sl.Err(err))
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := application.Stop(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", sl.Err(err))
	}
}
