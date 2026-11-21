// Command mockprovider is a stand-in for the external notification provider
// (webhook.site in the case). It answers like the spec:
//
//	POST / {"to","channel","content"} → 202 {"messageId","status":"accepted","timestamp"}
//
// Behaviour for demos and end-to-end tests:
//   - FAIL_RATE (0–1): share of requests answered with 500.
//   - LATENCY_MS: delay before every answer.
//   - content starting with "mock:<code>" (e.g. "mock:500", "mock:400",
//     "mock:429") is always answered with that status code.
//
// GET /received/{id} returns how many times a notification id (the
// Idempotency-Key header) was received, so tests can prove a cancelled
// notification was never sent and a sent one was sent once.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func main() {
	failRate, _ := strconv.ParseFloat(os.Getenv("FAIL_RATE"), 64)
	latencyMS, _ := strconv.Atoi(os.Getenv("LATENCY_MS"))
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	m := newMock(failRate, time.Duration(latencyMS)*time.Millisecond)
	srv := &http.Server{Addr: addr, Handler: m.routes(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("mock provider listening", "addr", addr, "fail_rate", failRate, "latency_ms", latencyMS)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("mock provider stopped", "error", err)
		os.Exit(1)
	}
}

type mock struct {
	failRate float64
	latency  time.Duration
	rand     func() float64

	mu       sync.Mutex
	received map[string]int
}

func newMock(failRate float64, latency time.Duration) *mock {
	return &mock{
		failRate: failRate,
		latency:  latency,
		rand:     rand.Float64, //nolint:gosec // simulated failures, not security sensitive
		received: map[string]int{},
	}
}

func (m *mock) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /", m.deliver)
	mux.HandleFunc("GET /received/{id}", m.count)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

type request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

func (m *mock) deliver(w http.ResponseWriter, r *http.Request) {
	var req request
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil || req.To == "" || req.Channel == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {to, channel, content}"})
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		m.mu.Lock()
		m.received[key]++
		m.mu.Unlock()
	}
	if m.latency > 0 {
		select {
		case <-time.After(m.latency):
		case <-r.Context().Done():
			return
		}
	}

	if code, ok := forcedStatus(req.Content); ok {
		writeJSON(w, code, map[string]string{"error": "forced by content"})
		return
	}
	if m.failRate > 0 && m.rand() < m.failRate {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "simulated failure"})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{
		"messageId": uuid.NewString(),
		"status":    "accepted",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

// forcedStatus reads "mock:<code>" at the start of content.
func forcedStatus(content string) (int, bool) {
	rest, ok := strings.CutPrefix(content, "mock:")
	if !ok || len(rest) < 3 {
		return 0, false
	}
	code, err := strconv.Atoi(rest[:3])
	if err != nil || code < 200 || code > 599 {
		return 0, false
	}
	return code, true
}

func (m *mock) count(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	n := m.received[r.PathValue("id")]
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]int{"count": n})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
