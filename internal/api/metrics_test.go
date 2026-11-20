package api_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func counterValue(t *testing.T, reg *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	next:
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			for k, v := range labels {
				if got[k] != v {
					continue next
				}
			}
			if m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
			return float64(m.GetHistogram().GetSampleCount())
		}
	}
	return 0
}

func created(t *testing.T, h *harness, channel string) float64 {
	t.Helper()
	return counterValue(t, h.reg, "notifications_created_total", map[string]string{"channel": channel})
}

func TestMetrics_CreatedExcludesReplay(t *testing.T) {
	h := newHarness(t)
	h.do(t, "POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "k1")
	h.do(t, "POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "k1")
	if got := created(t, h, "sms"); got != 1 {
		t.Fatalf("created{sms} = %v, want 1 (replay must not count)", got)
	}
}

func TestMetrics_CreatedNotCountedOnValidationError(t *testing.T) {
	h := newHarness(t)
	h.do(t, "POST", "/api/v1/notifications", `{"channel":"sms","recipient":"123","content":""}`)
	if got := created(t, h, "sms"); got != 0 {
		t.Fatalf("created{sms} = %v, want 0", got)
	}
}

func TestMetrics_BatchCountsNewRowsPerChannel(t *testing.T) {
	h := newHarness(t)
	h.do(t, "POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "old")
	r := h.do(t, "POST", "/api/v1/notifications/batch", `{"notifications":[
		{"channel":"sms","recipient":"+905551234567","content":"hello","idempotency_key":"old"},
		{"channel":"sms","recipient":"+905551234567","content":"a"},
		{"channel":"email","recipient":"a@b.co","content":"b"}]}`)
	if r.code != http.StatusAccepted {
		t.Fatalf("batch code = %d, body %s", r.code, r.raw)
	}
	if got := created(t, h, "sms"); got != 2 {
		t.Errorf("created{sms} = %v, want 2 (1 single + 1 new in batch, reused key excluded)", got)
	}
	if got := created(t, h, "email"); got != 1 {
		t.Errorf("created{email} = %v, want 1", got)
	}
}

func TestMetrics_HTTPDurationUsesRoutePattern(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/notifications", validSMS)
	id, _ := r.body["id"].(string)
	h.do(t, "GET", "/api/v1/notifications/"+id, "")
	h.do(t, "GET", "/no/such/route", "")

	for _, tc := range []struct {
		route, code string
	}{
		{"/api/v1/notifications", "202"},
		{"/api/v1/notifications/{id}", "200"},
		{"unmatched", "404"},
	} {
		labels := map[string]string{"route": tc.route, "code": tc.code}
		if got := counterValue(t, h.reg, "http_request_duration_seconds", labels); got != 1 {
			t.Errorf("http_request_duration_seconds%v samples = %v, want 1", labels, got)
		}
	}
}

func TestMetrics_Endpoint(t *testing.T) {
	h := newHarness(t)
	h.do(t, "POST", "/api/v1/notifications", validSMS)
	r := h.do(t, "GET", "/metrics", "")
	if r.code != http.StatusOK {
		t.Fatalf("/metrics code = %d", r.code)
	}
	if !strings.Contains(r.raw, `notifications_created_total{channel="sms"} 1`) {
		t.Errorf("/metrics missing created counter:\n%s", r.raw)
	}
}
