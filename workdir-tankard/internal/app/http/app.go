package httpapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type App struct {
	log    *slog.Logger
	server *http.Server

	addr string
}

func New(
	log *slog.Logger,
	addr string,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	idleTimeout time.Duration,
	handler http.Handler,
) *App {

	server := &http.Server{
		Addr:         addr,
		Handler:      handler,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
	}

	return &App{
		log:    log,
		server: server,
		addr:   addr,
	}
}

func (a *App) Run() error {
	const op = "httpapp.Run"

	log := a.log.With("op", op)

	log.Info("HTTP server starting", "addr", a.server.Addr)

	err := a.server.ListenAndServe()

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (a *App) MustRun() {
	if err := a.Run(); err != nil {
		panic(err)
	}
}

func (a *App) Stop(ctx context.Context) error {
	const op = "httpapp.Stop"

	log := a.log.With("op", op)

	log.Info("stopping HTTP server")

	return a.server.Shutdown(ctx)
}
