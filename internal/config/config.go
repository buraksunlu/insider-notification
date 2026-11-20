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
// for optional values and failing when a required value is missing.
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
	for name, val := range map[string]string{
		"DATABASE_URL": cfg.DatabaseURL,
		"AMQP_URL":     cfg.AMQPURL,
		"PROVIDER_URL": cfg.ProviderURL,
	} {
		if val == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
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
