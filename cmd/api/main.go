// Command api serves the notification HTTP API.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	apispec "github.com/buraksunlu/insider-notification/api"
	"github.com/buraksunlu/insider-notification/internal/api"
	"github.com/buraksunlu/insider-notification/internal/config"
	"github.com/buraksunlu/insider-notification/internal/observability"
	"github.com/buraksunlu/insider-notification/internal/store"
)

// shutdownTimeout bounds how long in-flight requests may finish on SIGTERM.
const shutdownTimeout = 15 * time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("api stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := observability.NewLogger(os.Stdout, cfg.LogLevel)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pool.Close()
	st := store.New(pool)

	reg := observability.NewRegistry()
	srv := &http.Server{
		Addr: cfg.HTTPAddr,
		Handler: api.NewRouter(api.Deps{
			Store:          st,
			Ready:          st.Ping,
			Logger:         log,
			Metrics:        observability.NewMetrics(reg),
			MetricsHandler: observability.MetricsHandler(reg),
			OpenAPISpec:    apispec.Spec,
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	errc := make(chan error, 1)
	go func() {
		log.Info("api listening", "addr", cfg.HTTPAddr)
		errc <- srv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down: draining in-flight requests")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
