package provider_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/buraksunlu/insider-notification/internal/provider"
)

var smsReq = provider.Request{To: "+905551234567", Channel: "sms", Content: "Merhaba ş 👋"}

func newServer(t *testing.T, h http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func TestSendAcceptedWithBody(t *testing.T) {
	var (
		gotBody    map[string]any
		gotHeaders http.Header
		gotMethod  string
	)
	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotHeaders = r.Header.Clone()
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &gotBody); err != nil {
			t.Errorf("request body is not JSON: %v (%q)", err, raw)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"messageId":"abc-123","status":"accepted","timestamp":"2026-11-20T10:00:00Z"}`)
	})

	res, err := provider.NewClient(srv.URL, time.Second).Send(context.Background(), "idem-1", "corr-1", smsReq)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.StatusCode != http.StatusAccepted {
		t.Errorf("StatusCode = %d, want 202", res.StatusCode)
	}
	if res.MessageID == nil || *res.MessageID != "abc-123" {
		t.Errorf("MessageID = %v, want abc-123", res.MessageID)
	}
	if res.Latency <= 0 {
		t.Errorf("Latency = %v, want > 0", res.Latency)
	}

	if gotMethod != http.MethodPost {
		t.Errorf("method = %s, want POST", gotMethod)
	}
	want := map[string]any{"to": smsReq.To, "channel": smsReq.Channel, "content": smsReq.Content}
	if len(gotBody) != len(want) {
		t.Errorf("body keys = %v, want exactly to/channel/content", gotBody)
	}
	for k, v := range want {
		if gotBody[k] != v {
			t.Errorf("body[%q] = %v, want %v", k, gotBody[k], v)
		}
	}
	for h, v := range map[string]string{
		"Idempotency-Key":  "idem-1",
		"X-Correlation-ID": "corr-1",
		"Content-Type":     "application/json",
	} {
		if got := gotHeaders.Get(h); got != v {
			t.Errorf("header %s = %q, want %q", h, got, v)
		}
	}
}

func TestSendAcceptedWithoutUsableBody(t *testing.T) {
	for name, body := range map[string]string{
		"empty":         "",
		"not json":      "ok",
		"no message id": `{"status":"accepted"}`,
		"blank id":      `{"messageId":""}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusAccepted)
				_, _ = io.WriteString(w, body)
			})
			res, err := provider.NewClient(srv.URL, time.Second).Send(context.Background(), "k", "c", smsReq)
			if err != nil {
				t.Fatalf("Send: %v", err)
			}
			if res.StatusCode != http.StatusAccepted || res.MessageID != nil {
				t.Errorf("got status %d id %v, want 202 and nil id", res.StatusCode, res.MessageID)
			}
		})
	}
}

func TestSendNon2xxIsNotAnError(t *testing.T) {
	srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"messageId":"ignored"}`)
	})
	res, err := provider.NewClient(srv.URL, time.Second).Send(context.Background(), "k", "c", smsReq)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("StatusCode = %d, want 503", res.StatusCode)
	}
	if res.MessageID != nil {
		t.Errorf("MessageID = %v, want nil on non-2xx", *res.MessageID)
	}
}

func TestSendRetryAfter(t *testing.T) {
	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	cases := []struct {
		name   string
		header string
		min    time.Duration
		max    time.Duration
	}{
		{"seconds", "7", 7 * time.Second, 7 * time.Second},
		{"http date", future, 25 * time.Second, 30 * time.Second},
		{"absent", "", 0, 0},
		{"garbage", "soon", 0, 0},
		{"negative", "-3", 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
				if tc.header != "" {
					w.Header().Set("Retry-After", tc.header)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			})
			res, err := provider.NewClient(srv.URL, time.Second).Send(context.Background(), "k", "c", smsReq)
			if err != nil {
				t.Fatalf("Send: %v", err)
			}
			if res.RetryAfter < tc.min || res.RetryAfter > tc.max {
				t.Errorf("RetryAfter = %v, want in [%v, %v]", res.RetryAfter, tc.min, tc.max)
			}
		})
	}
}

func TestSendTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusAccepted)
	})
	defer close(release)

	start := time.Now()
	_, err := provider.NewClient(srv.URL, 100*time.Millisecond).Send(context.Background(), "k", "c", smsReq)
	if err == nil {
		t.Fatal("Send: want timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want errors.Is(err, context.DeadlineExceeded)", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Send took %v, timeout not enforced", elapsed)
	}
}

func TestSendCancelledContext(t *testing.T) {
	srv := newServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := provider.NewClient(srv.URL, time.Second).Send(ctx, "k", "c", smsReq)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestSendConnectionRefused(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	if _, err := provider.NewClient(url, time.Second).Send(context.Background(), "k", "c", smsReq); err == nil {
		t.Fatal("Send to closed server: want error, got nil")
	}
}

func TestSendOmitsEmptyCorrelationID(t *testing.T) {
	var present bool
	srv := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, present = r.Header["X-Correlation-Id"]
		w.WriteHeader(http.StatusAccepted)
	})
	if _, err := provider.NewClient(srv.URL, time.Second).Send(context.Background(), "k", "", smsReq); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if present {
		t.Error("X-Correlation-ID sent although empty")
	}
}
