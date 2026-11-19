package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func ones() float64  { return 1 }
func zeros() float64 { return 0 }
func half() float64  { return 0.5 }

func TestBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		rnd     func() float64
		want    time.Duration
	}{
		{1, ones, 2 * time.Second},
		{2, ones, 4 * time.Second},
		{3, ones, 8 * time.Second},
		{3, half, 4 * time.Second},
		{20, ones, 5 * time.Minute},
		{1000, ones, 5 * time.Minute}, // no overflow
		{3, zeros, 0},
		{0, ones, 2 * time.Second}, // attempt < 1 treated as first attempt
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("attempt=%d", tc.attempt), func(t *testing.T) {
			if got := Backoff(tc.attempt, 2*time.Second, tc.rnd); got != tc.want {
				t.Fatalf("Backoff(%d) = %v, want %v", tc.attempt, got, tc.want)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name   string
		status int
		err    error
		want   Outcome
	}{
		{"200", 200, nil, OutcomeSent},
		{"202", 202, nil, OutcomeSent},
		{"204", 204, nil, OutcomeSent},
		{"301 leftover redirect", 301, nil, OutcomePermanent},
		{"400", 400, nil, OutcomePermanent},
		{"404", 404, nil, OutcomePermanent},
		{"408 request timeout", 408, nil, OutcomeRetry},
		{"429", 429, nil, OutcomeRetry},
		{"500", 500, nil, OutcomeRetry},
		{"503", 503, nil, OutcomeRetry},
		{"deadline exceeded", 0, context.DeadlineExceeded, OutcomeRetry},
		{"wrapped deadline", 0, fmt.Errorf("post: %w", context.DeadlineExceeded), OutcomeRetry},
		{"canceled on shutdown", 0, context.Canceled, OutcomeRetry},
		{"network error", 0, &net.OpError{Op: "dial", Err: errors.New("connection refused")}, OutcomeRetry},
		{"unknown transport error", 0, errors.New("boom"), OutcomeRetry},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Classify(tc.status, tc.err); got != tc.want {
				t.Fatalf("Classify(%d, %v) = %v, want %v", tc.status, tc.err, got, tc.want)
			}
		})
	}
}

func TestNextDelay(t *testing.T) {
	base := 2 * time.Second
	if got := NextDelay(1, base, 30*time.Second, ones); got != 30*time.Second {
		t.Errorf("Retry-After 30s: got %v", got)
	}
	if got := NextDelay(1, base, time.Hour, ones); got != 5*time.Minute {
		t.Errorf("Retry-After 1h should be capped: got %v", got)
	}
	if got := NextDelay(3, base, 0, ones); got != 8*time.Second {
		t.Errorf("no Retry-After falls back to backoff: got %v", got)
	}
}
