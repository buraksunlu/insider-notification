package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

func TestLogger_AddsCorrelationIDFromContext(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "info")

	log.InfoContext(WithCorrelationID(context.Background(), "corr-42"), "hello", "k", "v")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("not JSON: %q", buf.String())
	}
	if entry["correlation_id"] != "corr-42" || entry["msg"] != "hello" || entry["k"] != "v" {
		t.Fatalf("unexpected entry: %v", entry)
	}
}

func TestLogger_NoCorrelationIDWithoutContextValue(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, "info").InfoContext(context.Background(), "hello")

	var entry map[string]any
	_ = json.Unmarshal(buf.Bytes(), &entry)
	if _, ok := entry["correlation_id"]; ok {
		t.Fatalf("unexpected correlation_id: %v", entry)
	}
}

func TestLogger_Level(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger(&buf, "warn")
	log.Info("dropped")
	if buf.Len() != 0 {
		t.Fatalf("info logged at warn level: %q", buf.String())
	}
}

func TestCorrelationID_Empty(t *testing.T) {
	if got := CorrelationID(context.Background()); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}
