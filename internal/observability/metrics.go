package observability

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// Delivery results used as the result label of deliveries_total.
const (
	ResultSent    = "sent"
	ResultRetry   = "retry"
	ResultFailed  = "failed"
	ResultSkipped = "skipped" // claim guard dropped it: cancelled or duplicate
)

// allStatuses is every notification status, so a status that drops to zero
// rows is reported as 0 instead of keeping its last value.
var allStatuses = []domain.Status{
	domain.StatusPending, domain.StatusQueued, domain.StatusProcessing,
	domain.StatusSent, domain.StatusFailed, domain.StatusCancelled,
}

// Metrics holds the application's Prometheus metrics. Every method is safe
// on a nil *Metrics, so components work unchanged without metrics.
type Metrics struct {
	created    *prometheus.CounterVec
	deliveries *prometheus.CounterVec
	attempt    *prometheus.HistogramVec
	endToEnd   *prometheus.HistogramVec
	httpDur    *prometheus.HistogramVec
	queueDepth *prometheus.GaugeVec
	byStatus   *prometheus.GaugeVec

	mu         sync.Mutex
	seenQueues map[string]bool
}

// NewRegistry returns a registry with Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return reg
}

// MetricsHandler serves the metrics gathered by g in the exposition format.
func MetricsHandler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}

// NewMetrics creates the metrics and registers them on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		created: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Notifications accepted by the API (idempotent replays excluded).",
		}, []string{"channel"}),
		deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "deliveries_total",
			Help: "Delivery outcomes per channel (sent, retry, failed, skipped).",
		}, []string{"channel", "result"}),
		attempt: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "delivery_attempt_duration_seconds",
			Help: "Duration of one provider call, including timeouts.",
			// Up to the 5 s provider timeout.
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}, []string{"channel"}),
		endToEnd: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "notification_end_to_end_seconds",
			Help: "Time from a notification becoming due (created or scheduled_at) to sent, retries included.",
			// Retries stretch successful paths to minutes (backoff cap 5 min).
			Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}, []string{"channel"}),
		httpDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route", "code"}),
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Ready messages per RabbitMQ queue.",
		}, []string{"queue"}),
		byStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "notifications_by_status",
			Help: "Notifications currently in each status.",
		}, []string{"status"}),
		seenQueues: map[string]bool{},
	}
	reg.MustRegister(m.created, m.deliveries, m.attempt, m.endToEnd, m.httpDur, m.queueDepth, m.byStatus)
	return m
}

// Created counts a newly created notification.
func (m *Metrics) Created(c domain.Channel) {
	if m == nil {
		return
	}
	m.created.WithLabelValues(string(c)).Inc()
}

// Delivery counts one handled message by result (Result* constants).
func (m *Metrics) Delivery(c domain.Channel, result string) {
	if m == nil {
		return
	}
	m.deliveries.WithLabelValues(string(c), result).Inc()
}

// AttemptDuration observes the duration of one provider call.
func (m *Metrics) AttemptDuration(c domain.Channel, d time.Duration) {
	if m == nil {
		return
	}
	m.attempt.WithLabelValues(string(c)).Observe(d.Seconds())
}

// EndToEnd observes the time from due to sent.
func (m *Metrics) EndToEnd(c domain.Channel, d time.Duration) {
	if m == nil {
		return
	}
	m.endToEnd.WithLabelValues(string(c)).Observe(d.Seconds())
}

// HTTPRequest observes one HTTP request. route must be a pattern such as
// /api/v1/notifications/{id}, never a raw path, to bound cardinality.
func (m *Metrics) HTTPRequest(method, route string, code int, d time.Duration) {
	if m == nil {
		return
	}
	m.httpDur.WithLabelValues(method, route, strconv.Itoa(code)).Observe(d.Seconds())
}

// SetQueueDepths sets queue_depth from depths; a queue reported before but
// missing now is set to 0.
func (m *Metrics) SetQueueDepths(depths map[string]int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for q := range m.seenQueues {
		if _, ok := depths[q]; !ok {
			m.queueDepth.WithLabelValues(q).Set(0)
		}
	}
	for q, n := range depths {
		m.seenQueues[q] = true
		m.queueDepth.WithLabelValues(q).Set(float64(n))
	}
}

// SetStatusCounts sets notifications_by_status; statuses absent from
// counts are set to 0.
func (m *Metrics) SetStatusCounts(counts map[domain.Status]int64) {
	if m == nil {
		return
	}
	for _, s := range allStatuses {
		m.byStatus.WithLabelValues(string(s)).Set(float64(counts[s]))
	}
}
