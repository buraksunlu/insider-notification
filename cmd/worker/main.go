// Command worker relays due notifications to RabbitMQ, consumes the channel
// queues, delivers to the provider, and exposes Prometheus metrics.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"

	"github.com/buraksunlu/insider-notification/internal/config"
	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/observability"
	"github.com/buraksunlu/insider-notification/internal/provider"
	"github.com/buraksunlu/insider-notification/internal/queue"
	"github.com/buraksunlu/insider-notification/internal/store"
	"github.com/buraksunlu/insider-notification/internal/worker"
)

const (
	providerTimeout   = 5 * time.Second
	ratePerChannel    = 100 // messages per second, per channel
	collectInterval   = 5 * time.Second
	notifyChannel     = "new_notification" // see migrations/000001_init.up.sql
	amqpDialTimeout   = 30 * time.Second
	metricsShutdown   = 5 * time.Second
	metricsReadHeader = 5 * time.Second
)

func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "error", err)
		os.Exit(1) // the orchestrator restarts the worker
	}
}

func run() error {
	cfg, err := config.Load()
	if err == nil {
		err = cfg.ValidateWorker()
	}
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

	conn, err := dial(ctx, cfg.AMQPURL, log)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if err := setupTopology(conn); err != nil {
		return err
	}
	pub, err := queue.NewPublisher(conn)
	if err != nil {
		return err
	}
	defer func() { _ = pub.Close() }()
	depths := queue.NewDepthReader(conn)
	defer depths.Close()

	reg := observability.NewRegistry()
	metrics := observability.NewMetrics(reg)
	handler := &worker.Handler{
		Store:     st,
		Sender:    provider.NewClient(cfg.ProviderURL, providerTimeout),
		DLQ:       pub,
		Limiters:  worker.NewLimiters(ratePerChannel),
		Metrics:   metrics,
		Logger:    log,
		Now:       time.Now,
		Rand:      rand.Float64, //nolint:gosec // backoff jitter, not security sensitive
		RetryBase: cfg.RetryBase,
	}

	g, gctx := errgroup.WithContext(ctx)

	// A lost broker connection is not recovered in-process: the worker exits
	// non-zero and is restarted. Nothing is lost: rows stay in Postgres and
	// the sweeper recovers anything left queued or processing.
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	g.Go(func() error {
		select {
		case <-gctx.Done():
			return nil
		case err := <-closed:
			if err == nil { // closed without an error frame
				return errors.New("rabbitmq connection closed")
			}
			return fmt.Errorf("rabbitmq connection lost: %w", err)
		}
	})

	relay := &worker.Relay{
		Store:     st,
		Publisher: pub,
		Listen:    func(ctx context.Context) <-chan struct{} { return st.Listen(ctx, notifyChannel) },
		Logger:    log,
	}
	g.Go(func() error { return relay.Run(gctx) })

	sweeper := &worker.Sweeper{Store: st, Logger: log}
	g.Go(func() error { return sweeper.Run(gctx) })

	collector := &observability.Collector{
		Metrics: metrics, Depths: depths.Depths, Statuses: st.CountByStatus,
		Interval: collectInterval, Logger: log,
	}
	g.Go(func() error { return collector.Run(gctx) })

	for _, c := range domain.Channels {
		g.Go(func() error {
			if err := queue.Consume(gctx, conn, c, cfg.WorkerPrefetch, handler.Handle); err != nil {
				return fmt.Errorf("consume %s: %w", c, err)
			}
			return nil
		})
	}

	serveMetrics(gctx, g, cfg.MetricsAddr, observability.MetricsHandler(reg), log)

	log.Info("worker started", "provider_url", cfg.ProviderURL, "prefetch", cfg.WorkerPrefetch, "retry_base", cfg.RetryBase.String())
	err = g.Wait()
	log.Info("worker stopped")
	return err
}

// dial connects to RabbitMQ, retrying while the broker is still starting.
func dial(ctx context.Context, url string, log *slog.Logger) (*amqp.Connection, error) {
	deadline := time.Now().Add(amqpDialTimeout)
	for {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("rabbitmq: %w", err)
		}
		log.Warn("rabbitmq not reachable yet, retrying", "error", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func setupTopology(conn *amqp.Connection) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	if err := queue.Setup(ch); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}
	return nil
}

// serveMetrics runs the metrics and liveness endpoint until ctx is done.
func serveMetrics(ctx context.Context, g *errgroup.Group, addr string, metrics http.Handler, log *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: metricsReadHeader}

	g.Go(func() error {
		log.Info("metrics listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("metrics server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), metricsShutdown)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
}
