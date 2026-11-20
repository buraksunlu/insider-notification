package observability_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ptest "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/observability"
)

func newMetrics(t *testing.T) (*observability.Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return observability.NewMetrics(reg), reg
}

func TestDeliveryCounter(t *testing.T) {
	m, reg := newMetrics(t)
	m.Delivery(domain.ChannelSMS, observability.ResultSent)
	m.Delivery(domain.ChannelSMS, observability.ResultSent)
	m.Delivery(domain.ChannelEmail, observability.ResultRetry)

	want := `
# HELP deliveries_total Delivery outcomes per channel (sent, retry, failed, skipped).
# TYPE deliveries_total counter
deliveries_total{channel="email",result="retry"} 1
deliveries_total{channel="sms",result="sent"} 2
`
	if err := ptest.GatherAndCompare(reg, strings.NewReader(want), "deliveries_total"); err != nil {
		t.Fatal(err)
	}
}

func TestCreatedCounter(t *testing.T) {
	m, reg := newMetrics(t)
	m.Created(domain.ChannelPush)
	want := `
# HELP notifications_created_total Notifications accepted by the API (idempotent replays excluded).
# TYPE notifications_created_total counter
notifications_created_total{channel="push"} 1
`
	if err := ptest.GatherAndCompare(reg, strings.NewReader(want), "notifications_created_total"); err != nil {
		t.Fatal(err)
	}
}

func TestHistograms(t *testing.T) {
	m, reg := newMetrics(t)
	m.AttemptDuration(domain.ChannelSMS, 120*time.Millisecond)
	m.EndToEnd(domain.ChannelSMS, 90*time.Second)
	m.HTTPRequest(http.MethodGet, "/api/v1/notifications/{id}", 200, 3*time.Millisecond)

	for name, want := range map[string]int{
		"delivery_attempt_duration_seconds": 1,
		"notification_end_to_end_seconds":   1,
		"http_request_duration_seconds":     1,
	} {
		if got := seriesCount(t, reg, name); got != want {
			t.Errorf("%s series = %d, want %d", name, got, want)
		}
	}

	// 90 s must land in a finite bucket: retries make minutes-long paths normal.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != "notification_end_to_end_seconds" {
			continue
		}
		h := mf.GetMetric()[0].GetHistogram()
		var finite uint64
		for _, b := range h.GetBucket() {
			if b.GetUpperBound() >= 90 {
				finite = b.GetCumulativeCount()
				break
			}
		}
		if finite != 1 {
			t.Errorf("90s observation not in a finite end-to-end bucket")
		}
	}
}

func TestGaugesResetMissingLabels(t *testing.T) {
	m, reg := newMetrics(t)
	m.SetStatusCounts(map[domain.Status]int64{domain.StatusPending: 5, domain.StatusSent: 2})
	m.SetStatusCounts(map[domain.Status]int64{domain.StatusSent: 3})
	m.SetQueueDepths(map[string]int{"notifications.sms": 7})

	want := `
# HELP notifications_by_status Notifications currently in each status.
# TYPE notifications_by_status gauge
notifications_by_status{status="cancelled"} 0
notifications_by_status{status="failed"} 0
notifications_by_status{status="pending"} 0
notifications_by_status{status="processing"} 0
notifications_by_status{status="queued"} 0
notifications_by_status{status="sent"} 3
# HELP queue_depth Ready messages per RabbitMQ queue.
# TYPE queue_depth gauge
queue_depth{queue="notifications.sms"} 7
`
	if err := ptest.GatherAndCompare(reg, strings.NewReader(want), "notifications_by_status", "queue_depth"); err != nil {
		t.Fatal(err)
	}
}

func TestNilMetricsIsSafe(_ *testing.T) {
	var m *observability.Metrics
	m.Created(domain.ChannelSMS)
	m.Delivery(domain.ChannelSMS, observability.ResultSent)
	m.AttemptDuration(domain.ChannelSMS, time.Second)
	m.EndToEnd(domain.ChannelSMS, time.Second)
	m.HTTPRequest("GET", "/", 200, time.Second)
	m.SetQueueDepths(map[string]int{"q": 1})
	m.SetStatusCounts(map[domain.Status]int64{domain.StatusSent: 1})
}

func TestNewRegistryServesRuntimeMetrics(t *testing.T) {
	reg := observability.NewRegistry()
	observability.NewMetrics(reg).Delivery(domain.ChannelSMS, observability.ResultSent)

	rec := httptest.NewRecorder()
	observability.MetricsHandler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ := io.ReadAll(rec.Body)
	for _, want := range []string{"go_goroutines", "process_", `deliveries_total{channel="sms",result="sent"} 1`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}

func TestCollectorUpdatesGauges(t *testing.T) {
	m, reg := newMetrics(t)
	var depthCalls atomic.Int32
	c := &observability.Collector{
		Metrics: m,
		Depths: func(context.Context) (map[string]int, error) {
			depthCalls.Add(1)
			return map[string]int{"notifications.dlq": 4}, nil
		},
		Statuses: func(context.Context) (map[domain.Status]int64, error) {
			return map[domain.Status]int64{domain.StatusFailed: 4}, nil
		},
		Interval: 10 * time.Millisecond,
		Logger:   observability.NewLogger(io.Discard, "error"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	deadline := time.Now().Add(time.Second)
	for depthCalls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if depthCalls.Load() < 2 {
		t.Fatal("collector did not run on interval")
	}
	if got := gaugeValue(t, reg, "queue_depth", "notifications.dlq"); got != 4 {
		t.Errorf("queue_depth dlq = %v, want 4", got)
	}
	if got := gaugeValue(t, reg, "notifications_by_status", "failed"); got != 4 {
		t.Errorf("by_status failed = %v, want 4", got)
	}
}

func TestCollectorSurvivesSourceErrors(t *testing.T) {
	m, reg := newMetrics(t)
	m.SetQueueDepths(map[string]int{"notifications.sms": 9})
	var calls atomic.Int32
	c := &observability.Collector{
		Metrics: m,
		Depths: func(context.Context) (map[string]int, error) {
			calls.Add(1)
			return nil, errors.New("broker down")
		},
		Statuses: func(context.Context) (map[domain.Status]int64, error) { return nil, errors.New("db down") },
		Interval: 10 * time.Millisecond,
		Logger:   observability.NewLogger(io.Discard, "error"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if calls.Load() < 3 {
		t.Fatal("collector stopped after an error")
	}
	// Last known value is kept rather than reported as an empty queue.
	if got := gaugeValue(t, reg, "queue_depth", "notifications.sms"); got != 9 {
		t.Errorf("queue_depth sms = %v, want last known 9", got)
	}
}

func seriesCount(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return len(mf.GetMetric())
		}
	}
	return 0
}

func gaugeValue(t *testing.T, reg *prometheus.Registry, name, label string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, mt := range mf.GetMetric() {
			for _, lp := range mt.GetLabel() {
				if lp.GetValue() == label {
					return mt.GetGauge().GetValue()
				}
			}
		}
	}
	t.Fatalf("%s{%s} not found", name, label)
	return 0
}
