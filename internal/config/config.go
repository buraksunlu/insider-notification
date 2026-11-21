// Package config loads service configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the settings shared by the api and worker binaries.
type Config struct {
	DatabaseURL string
	AMQPURL     string
	ProviderURL string
	HTTPAddr    string
	MetricsAddr string
	LogLevel    string
	// RetryBase is the base delay of the exponential retry backoff.
	RetryBase time.Duration
	// WorkerPrefetch is how many messages per channel a worker delivers
	// concurrently. It must cover rate × provider latency (100/s × 200 ms
	// = 20) so the rate limiter, not concurrency, caps throughput.
	WorkerPrefetch int
}

// Load reads the configuration from the environment, applying defaults
// for optional values. DATABASE_URL is required by both binaries; the
// worker additionally calls ValidateWorker.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		AMQPURL:     os.Getenv("AMQP_URL"),
		ProviderURL: os.Getenv("PROVIDER_URL"),
		HTTPAddr:    getenv("HTTP_ADDR", ":8080"),
		MetricsAddr: getenv("METRICS_ADDR", ":9091"),
		LogLevel:    getenv("LOG_LEVEL", "info"),
	}

	var errs []error
	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}

	base, err := time.ParseDuration(getenv("RETRY_BASE", "2s"))
	if err != nil || base <= 0 {
		errs = append(errs, fmt.Errorf("RETRY_BASE must be a positive duration"))
	}
	cfg.RetryBase = base

	prefetch, err := strconv.Atoi(getenv("WORKER_PREFETCH", "50"))
	if err != nil || prefetch < 1 {
		errs = append(errs, fmt.Errorf("WORKER_PREFETCH must be a positive integer"))
	}
	cfg.WorkerPrefetch = prefetch

	return cfg, errors.Join(errs...)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ValidateWorker checks the settings only the worker needs.
func (c Config) ValidateWorker() error {
	var errs []error
	if c.AMQPURL == "" {
		errs = append(errs, errors.New("AMQP_URL is required"))
	}
	if c.ProviderURL == "" {
		errs = append(errs, errors.New("PROVIDER_URL is required"))
	}
	return errors.Join(errs...)
}
