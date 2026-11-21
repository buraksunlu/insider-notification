package config

import (
	"strings"
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("AMQP_URL", "amqp://x")
	t.Setenv("PROVIDER_URL", "http://x")
}

func TestLoad_RequiresDatabaseURL(t *testing.T) {
	setRequired(t)
	t.Setenv("DATABASE_URL", "")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("expected error mentioning DATABASE_URL, got %v", err)
	}
}

func TestLoad_Defaults(t *testing.T) {
	setRequired(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr = %q, want :8080", cfg.HTTPAddr)
	}
	if cfg.MetricsAddr != ":9091" {
		t.Errorf("MetricsAddr = %q, want :9091", cfg.MetricsAddr)
	}
	if cfg.RetryBase != 2*time.Second {
		t.Errorf("RetryBase = %v, want 2s", cfg.RetryBase)
	}
	if cfg.WorkerPrefetch != 50 {
		t.Errorf("WorkerPrefetch = %d, want 50", cfg.WorkerPrefetch)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
	}
}

func TestLoad_InvalidRetryBase(t *testing.T) {
	setRequired(t)
	t.Setenv("RETRY_BASE", "soon")

	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RETRY_BASE") {
		t.Fatalf("expected error mentioning RETRY_BASE, got %v", err)
	}
}

func TestLoad_WorkerPrefetch(t *testing.T) {
	setRequired(t)
	t.Setenv("WORKER_PREFETCH", "25")
	cfg, err := Load()
	if err != nil || cfg.WorkerPrefetch != 25 {
		t.Fatalf("WorkerPrefetch = %d (err %v), want 25", cfg.WorkerPrefetch, err)
	}

	for _, bad := range []string{"0", "-1", "many"} {
		t.Setenv("WORKER_PREFETCH", bad)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "WORKER_PREFETCH") {
			t.Errorf("WORKER_PREFETCH=%q: expected error, got %v", bad, err)
		}
	}
}

// The api binary needs only the database; broker and provider are the
// worker's concern, checked by ValidateWorker.
func TestLoad_APIOnlyNeedsDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("AMQP_URL", "")
	t.Setenv("PROVIDER_URL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load without AMQP_URL/PROVIDER_URL: %v", err)
	}
	err = cfg.ValidateWorker()
	if err == nil || !strings.Contains(err.Error(), "AMQP_URL") || !strings.Contains(err.Error(), "PROVIDER_URL") {
		t.Fatalf("ValidateWorker = %v, want errors for AMQP_URL and PROVIDER_URL", err)
	}
}

func TestValidateWorker_OK(t *testing.T) {
	setRequired(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateWorker(); err != nil {
		t.Fatalf("ValidateWorker: %v", err)
	}
}
