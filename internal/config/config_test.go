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
