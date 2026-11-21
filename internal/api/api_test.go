package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/buraksunlu/insider-notification/internal/api"
	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/observability"
	"github.com/buraksunlu/insider-notification/internal/store"
)

var fixedNow = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

type harness struct {
	now     time.Time // what the API sees as the current time
	store   *fakeStore
	handler http.Handler
	ready   error
	reg     *prometheus.Registry
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{now: fixedNow, store: newFakeStore(), reg: prometheus.NewRegistry()}
	h.handler = api.NewRouter(api.Deps{
		Store:          h.store,
		Ready:          func(context.Context) error { return h.ready },
		Logger:         observability.NewLogger(io.Discard, "error"),
		Now:            func() time.Time { return h.now },
		Metrics:        observability.NewMetrics(h.reg),
		MetricsHandler: observability.MetricsHandler(h.reg),
	})
	return h
}

type response struct {
	code   int
	header http.Header
	body   map[string]any
	raw    string
}

func (h *harness) do(t *testing.T, method, path, body string, headers ...string) response {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	r := response{code: rec.Code, header: rec.Header(), raw: rec.Body.String()}
	_ = json.Unmarshal(rec.Body.Bytes(), &r.body)
	return r
}

func errDetails(t *testing.T, r response) map[string]any {
	t.Helper()
	e, ok := r.body["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error object in %s", r.raw)
	}
	d, _ := e["details"].(map[string]any)
	return d
}

const validSMS = `{"channel":"sms","recipient":"+905551234567","content":"hello"}`

func TestCreate_Accepted(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/notifications", validSMS)

	if r.code != http.StatusAccepted {
		t.Fatalf("code = %d, body %s", r.code, r.raw)
	}
	id, _ := r.body["id"].(string)
	if r.header.Get("Location") != "/api/v1/notifications/"+id || r.body["status"] != "pending" || r.body["priority"] != "normal" {
		t.Fatalf("unexpected response: %v location=%q", r.body, r.header.Get("Location"))
	}
}

func TestCreate_IdempotentReplayReturns200(t *testing.T) {
	h := newHarness(t)
	first := h.do(t, "POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "k1")
	second := h.do(t, "POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "k1")

	if first.code != http.StatusAccepted || second.code != http.StatusOK || first.body["id"] != second.body["id"] {
		t.Fatalf("first=%d second=%d ids %v %v", first.code, second.code, first.body["id"], second.body["id"])
	}
	if second.body["idempotency_key"] != "k1" {
		t.Fatalf("idempotency_key not echoed: %v", second.body)
	}
}

func TestCreate_IdempotencyConflict409(t *testing.T) {
	h := newHarness(t)
	h.do(t, "POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "k1")
	r := h.do(t, "POST", "/api/v1/notifications", `{"channel":"sms","recipient":"+905551234567","content":"other"}`, "Idempotency-Key", "k1")

	if r.code != http.StatusConflict {
		t.Fatalf("code = %d, body %s", r.code, r.raw)
	}
}

func TestCreate_ValidationError422(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/notifications", `{"channel":"sms","recipient":"123","content":""}`)

	if r.code != http.StatusUnprocessableEntity {
		t.Fatalf("code = %d", r.code)
	}
	d := errDetails(t, r)
	if d["recipient"] == nil || d["content"] == nil {
		t.Fatalf("details = %v", d)
	}
}

func TestCreate_BadBodies(t *testing.T) {
	tests := []struct {
		name, body string
		want       int
	}{
		{"malformed json", `{"channel":`, http.StatusBadRequest},
		{"wrong type", `{"channel":5}`, http.StatusBadRequest},
		{"unknown field typo", `{"channel":"sms","recipent":"+905551234567","content":"x"}`, http.StatusUnprocessableEntity},
		{"idempotency key in single body", `{"channel":"sms","recipient":"+905551234567","content":"x","idempotency_key":"k"}`, http.StatusUnprocessableEntity},
		{"trailing data", validSMS + `{}`, http.StatusBadRequest},
		{"empty body", ``, http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			r := h.do(t, "POST", "/api/v1/notifications", tc.body)
			if r.code != tc.want {
				t.Fatalf("code = %d, want %d, body %s", r.code, tc.want, r.raw)
			}
			if _, ok := r.body["error"]; !ok {
				t.Fatalf("missing error body: %s", r.raw)
			}
		})
	}
}

func TestCreate_StoreFailureIs500WithoutLeakingDetails(t *testing.T) {
	h := newHarness(t)
	h.store.createErr = errors.New("pq: connection refused to 10.0.0.1")
	r := h.do(t, "POST", "/api/v1/notifications", validSMS)

	if r.code != http.StatusInternalServerError || strings.Contains(r.raw, "10.0.0.1") {
		t.Fatalf("code=%d body=%s", r.code, r.raw)
	}
}

func batchBody(items ...string) string {
	return `{"notifications":[` + strings.Join(items, ",") + `]}`
}

func TestBatch_Accepted(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(validSMS,
		`{"channel":"email","recipient":"a@b.co","content":"hi","idempotency_key":"e1"}`))

	if r.code != http.StatusAccepted {
		t.Fatalf("code = %d, body %s", r.code, r.raw)
	}
	ids, _ := r.body["ids"].([]any)
	batchID, _ := r.body["batch_id"].(string)
	if len(ids) != 2 || r.header.Get("Location") != "/api/v1/batches/"+batchID {
		t.Fatalf("unexpected: %v location=%q", r.body, r.header.Get("Location"))
	}
}

func TestBatch_SizeLimits(t *testing.T) {
	h := newHarness(t)
	items := make([]string, 1001)
	for i := range items {
		items[i] = validSMS
	}
	if r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(items...)); r.code != http.StatusUnprocessableEntity {
		t.Fatalf("1001 items: code = %d", r.code)
	}
	if r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody()); r.code != http.StatusUnprocessableEntity {
		t.Fatalf("0 items: code = %d", r.code)
	}
	if r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(items[:1000]...)); r.code != http.StatusAccepted {
		t.Fatalf("1000 items: code = %d", r.code)
	}
}

func TestBatch_InvalidItemReportsIndex(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(validSMS, validSMS, validSMS,
		`{"channel":"sms","recipient":"bad","content":"x"}`))

	if r.code != http.StatusUnprocessableEntity {
		t.Fatalf("code = %d", r.code)
	}
	item, _ := errDetails(t, r)["3"].(map[string]any)
	if item["recipient"] == nil {
		t.Fatalf("details = %v", errDetails(t, r))
	}
	if len(h.store.byID) != 0 {
		t.Fatal("nothing must be stored when an item is invalid")
	}
}

func TestBatch_DuplicateKeyInBatch(t *testing.T) {
	h := newHarness(t)
	item := `{"channel":"sms","recipient":"+905551234567","content":"x","idempotency_key":"dup"}`
	r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(item, validSMS, validSMS, validSMS, validSMS, item))

	if r.code != http.StatusUnprocessableEntity {
		t.Fatalf("code = %d, body %s", r.code, r.raw)
	}
	item5, _ := errDetails(t, r)["5"].(map[string]any)
	if item5["idempotency_key"] == nil {
		t.Fatalf("details = %v", errDetails(t, r))
	}
}

func TestBatch_StoreConflictReportsIndex(t *testing.T) {
	h := newHarness(t)
	h.store.createErr = &store.ItemError{Index: 1, Err: store.ErrIdempotencyConflict}
	r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(validSMS, validSMS))

	if r.code != http.StatusConflict || errDetails(t, r)["1"] == nil {
		t.Fatalf("code=%d body=%s", r.code, r.raw)
	}
}

func seed(h *harness, st domain.Status) domain.Notification {
	n := domain.NewNotification(domain.CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "x"}, "c", fixedNow)
	n.Status = st
	h.store.put(n)
	return n
}

func TestGet(t *testing.T) {
	h := newHarness(t)
	n := seed(h, domain.StatusSent)

	if r := h.do(t, "GET", "/api/v1/notifications/"+n.ID.String(), ""); r.code != 200 || r.body["status"] != "sent" {
		t.Fatalf("get: %d %s", r.code, r.raw)
	}
	if r := h.do(t, "GET", "/api/v1/notifications/"+uuid.NewString(), ""); r.code != http.StatusNotFound {
		t.Fatalf("unknown: %d", r.code)
	}
	if r := h.do(t, "GET", "/api/v1/notifications/not-a-uuid", ""); r.code != http.StatusBadRequest {
		t.Fatalf("bad id: %d", r.code)
	}
}

func TestCancel(t *testing.T) {
	h := newHarness(t)
	pending := seed(h, domain.StatusPending)
	sent := seed(h, domain.StatusSent)

	if r := h.do(t, "POST", "/api/v1/notifications/"+pending.ID.String()+"/cancel", ""); r.code != 200 || r.body["status"] != "cancelled" {
		t.Fatalf("cancel pending: %d %s", r.code, r.raw)
	}
	if r := h.do(t, "POST", "/api/v1/notifications/"+sent.ID.String()+"/cancel", ""); r.code != http.StatusConflict {
		t.Fatalf("cancel sent: %d", r.code)
	}
	if r := h.do(t, "POST", "/api/v1/notifications/"+uuid.NewString()+"/cancel", ""); r.code != http.StatusNotFound {
		t.Fatalf("cancel unknown: %d", r.code)
	}
}

func TestBatchGetAndCancel(t *testing.T) {
	h := newHarness(t)
	created := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(validSMS, validSMS))
	batchID, _ := created.body["batch_id"].(string)

	r := h.do(t, "GET", "/api/v1/batches/"+batchID, "")
	counts, _ := r.body["counts"].(map[string]any)
	data, _ := r.body["data"].([]any)
	if r.code != 200 || r.body["total"] != float64(2) || counts["pending"] != float64(2) || len(data) != 2 {
		t.Fatalf("get batch: %d %s", r.code, r.raw)
	}

	r = h.do(t, "POST", "/api/v1/batches/"+batchID+"/cancel", "")
	if r.code != 200 || r.body["cancelled"] != float64(2) {
		t.Fatalf("cancel batch: %d %s", r.code, r.raw)
	}
	if r := h.do(t, "GET", "/api/v1/batches/"+uuid.NewString(), ""); r.code != http.StatusNotFound {
		t.Fatalf("unknown batch: %d", r.code)
	}
	if r := h.do(t, "POST", "/api/v1/batches/"+uuid.NewString()+"/cancel", ""); r.code != http.StatusNotFound {
		t.Fatalf("cancel unknown batch: %d", r.code)
	}
}

func TestList_ParsesFilters(t *testing.T) {
	h := newHarness(t)
	seed(h, domain.StatusSent)
	r := h.do(t, "GET", "/api/v1/notifications?status=sent&channel=sms&from=2026-11-01T00:00:00Z&to=2026-11-30T00:00:00Z&limit=50&cursor=abc", "")

	if r.code != 200 || r.body["next_cursor"] != "next-cursor" {
		t.Fatalf("list: %d %s", r.code, r.raw)
	}
	if data, _ := r.body["data"].([]any); len(data) != 1 {
		t.Fatalf("data = %v", r.body["data"])
	}
	f := h.store.lastFilter
	if *f.Status != domain.StatusSent || *f.Channel != domain.ChannelSMS || f.Limit != 50 || f.Cursor != "abc" ||
		!f.From.Equal(time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)) || f.To == nil {
		t.Fatalf("filter = %+v", f)
	}
}

func TestList_EmptyDataIsArray(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "GET", "/api/v1/notifications", "")
	if r.code != 200 || !strings.Contains(r.raw, `"data":[]`) {
		t.Fatalf("list: %d %s", r.code, r.raw)
	}
}

func TestList_BadParams400(t *testing.T) {
	for _, q := range []string{
		"limit=0", "limit=500", "limit=abc",
		"from=2026-11-30T00:00:00Z&to=2026-11-01T00:00:00Z", "from=yesterday",
		"status=bogus", "channel=fax",
	} {
		t.Run(q, func(t *testing.T) {
			h := newHarness(t)
			if r := h.do(t, "GET", "/api/v1/notifications?"+q, ""); r.code != http.StatusBadRequest {
				t.Fatalf("code = %d, body %s", r.code, r.raw)
			}
		})
	}
}

func TestList_BadCursorFromStore400(t *testing.T) {
	h := newHarness(t)
	h.store.listErr = store.ErrBadCursor
	if r := h.do(t, "GET", "/api/v1/notifications?cursor=%25%25%25", ""); r.code != http.StatusBadRequest {
		t.Fatalf("code = %d", r.code)
	}
}

func TestCorrelationID(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "GET", "/healthz", "", "X-Correlation-ID", "abc-123")
	if r.header.Get("X-Correlation-ID") != "abc-123" {
		t.Fatalf("echo: %q", r.header.Get("X-Correlation-ID"))
	}

	r = h.do(t, "GET", "/healthz", "")
	if _, err := uuid.Parse(r.header.Get("X-Correlation-ID")); err != nil {
		t.Fatalf("generated id %q is not a UUID", r.header.Get("X-Correlation-ID"))
	}

	r = h.do(t, "GET", "/healthz", "", "X-Correlation-ID", strings.Repeat("x", 200))
	if _, err := uuid.Parse(r.header.Get("X-Correlation-ID")); err != nil {
		t.Fatalf("oversized id should be replaced, got %q", r.header.Get("X-Correlation-ID"))
	}
}

func TestCreate_StoresCorrelationID(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/notifications", validSMS, "X-Correlation-ID", "corr-9")
	if r.body["correlation_id"] != "corr-9" {
		t.Fatalf("correlation_id = %v", r.body["correlation_id"])
	}
}

func TestHealth(t *testing.T) {
	h := newHarness(t)
	if r := h.do(t, "GET", "/healthz", ""); r.code != 200 {
		t.Fatalf("healthz: %d", r.code)
	}
	if r := h.do(t, "GET", "/readyz", ""); r.code != 200 {
		t.Fatalf("readyz ok: %d", r.code)
	}
	h.ready = errors.New("db down")
	if r := h.do(t, "GET", "/readyz", ""); r.code != http.StatusServiceUnavailable {
		t.Fatalf("readyz down: %d", r.code)
	}
}

func TestBodyTooLarge413(t *testing.T) {
	h := newHarness(t)
	huge := fmt.Sprintf(`{"channel":"email","recipient":"a@b.co","content":%q}`, strings.Repeat("a", 17<<20))
	req := httptest.NewRequest("POST", "/api/v1/notifications", bytes.NewBufferString(huge))
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d", rec.Code)
	}
}

func TestUnknownRouteIsJSON404(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "GET", "/api/v1/nope", "")
	if r.code != http.StatusNotFound || r.body["error"] == nil {
		t.Fatalf("code=%d body=%s", r.code, r.raw)
	}
}

// scheduledSMS is due 5 s after fixedNow.
var scheduledSMS = `{"channel":"sms","recipient":"+905551234567","content":"hello","scheduled_at":"` +
	fixedNow.Add(5*time.Second).Format(time.RFC3339) + `"}`

// A client whose first response timed out retries with the same key after
// scheduled_at has passed: that is a replay, not a new invalid request.
func TestCreate_ScheduledReplayAfterDueReturns200(t *testing.T) {
	h := newHarness(t)
	first := h.do(t, "POST", "/api/v1/notifications", scheduledSMS, "Idempotency-Key", "k1")
	h.now = fixedNow.Add(10 * time.Second)
	second := h.do(t, "POST", "/api/v1/notifications", scheduledSMS, "Idempotency-Key", "k1")

	if first.code != http.StatusAccepted || second.code != http.StatusOK || first.body["id"] != second.body["id"] {
		t.Fatalf("first=%d second=%d (%s)", first.code, second.code, second.raw)
	}
}

func TestCreate_ScheduledReplayWithOtherPayloadIs409(t *testing.T) {
	h := newHarness(t)
	h.do(t, "POST", "/api/v1/notifications", scheduledSMS, "Idempotency-Key", "k1")
	h.now = fixedNow.Add(10 * time.Second)
	other := strings.Replace(scheduledSMS, `"hello"`, `"other"`, 1)
	if r := h.do(t, "POST", "/api/v1/notifications", other, "Idempotency-Key", "k1"); r.code != http.StatusConflict {
		t.Fatalf("code = %d, want 409 (%s)", r.code, r.raw)
	}
}

func TestCreate_PastScheduleWithUnknownKeyIs422(t *testing.T) {
	h := newHarness(t)
	h.now = fixedNow.Add(10 * time.Second)
	r := h.do(t, "POST", "/api/v1/notifications", scheduledSMS, "Idempotency-Key", "new-key")
	if r.code != http.StatusUnprocessableEntity || errDetails(t, r)["scheduled_at"] == nil {
		t.Fatalf("code = %d, want 422 on scheduled_at (%s)", r.code, r.raw)
	}
}

func TestBatch_ScheduledReplayAfterDueReusesRow(t *testing.T) {
	h := newHarness(t)
	item := strings.Replace(scheduledSMS, `}`, `,"idempotency_key":"b1"}`, 1)
	first := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(item))
	h.now = fixedNow.Add(10 * time.Second)
	second := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(item))

	if first.code != http.StatusAccepted || second.code != http.StatusAccepted {
		t.Fatalf("first=%d second=%d (%s)", first.code, second.code, second.raw)
	}
	firstIDs, _ := first.body["ids"].([]any)
	secondIDs, _ := second.body["ids"].([]any)
	if len(firstIDs) != 1 || len(secondIDs) != 1 || firstIDs[0] != secondIDs[0] {
		t.Fatalf("ids %v then %v, want the same row reused", firstIDs, secondIDs)
	}
}

func TestBatch_PastScheduleWithUnknownKeyIs422(t *testing.T) {
	h := newHarness(t)
	h.now = fixedNow.Add(10 * time.Second)
	item := strings.Replace(scheduledSMS, `}`, `,"idempotency_key":"never-seen"}`, 1)
	r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(item))
	if r.code != http.StatusUnprocessableEntity {
		t.Fatalf("code = %d, want 422 (%s)", r.code, r.raw)
	}
}

// Batch keys are trimmed like the header, so the same key written with
// stray spaces cannot create a second notification.
func TestBatch_IdempotencyKeyIsTrimmed(t *testing.T) {
	h := newHarness(t)
	b := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(
		`{"channel":"sms","recipient":"+905551234567","content":"hello","idempotency_key":"  order-1 "}`))
	single := h.do(t, "POST", "/api/v1/notifications", validSMS, "Idempotency-Key", "order-1")

	ids, _ := b.body["ids"].([]any)
	if b.code != http.StatusAccepted || single.code != http.StatusOK || len(ids) != 1 || ids[0] != single.body["id"] {
		t.Fatalf("batch=%d ids=%v single=%d id=%v, want the single create to replay the batch row",
			b.code, ids, single.code, single.body["id"])
	}
	if single.body["idempotency_key"] != "order-1" {
		t.Fatalf("stored key = %v, want trimmed order-1", single.body["idempotency_key"])
	}
}

func TestBatch_BlankIdempotencyKeyIs422(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(
		`{"channel":"sms","recipient":"+905551234567","content":"hello","idempotency_key":"   "}`))
	item, _ := errDetails(t, r)["0"].(map[string]any)
	if r.code != http.StatusUnprocessableEntity || item["idempotency_key"] == nil {
		t.Fatalf("code = %d body %s, want 422 on idempotency_key", r.code, r.raw)
	}
}

func TestBatch_TrimmedKeysDuplicateInBatch(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(
		`{"channel":"sms","recipient":"+905551234567","content":"a","idempotency_key":"k"}`,
		`{"channel":"sms","recipient":"+905551234567","content":"b","idempotency_key":" k"}`))
	item, _ := errDetails(t, r)["1"].(map[string]any)
	if r.code != http.StatusUnprocessableEntity || item["idempotency_key"] == nil {
		t.Fatalf("code = %d body %s, want 422 duplicate key on item 1", r.code, r.raw)
	}
}

func TestDocsServeSpecAndSwaggerUI(t *testing.T) {
	spec := []byte("openapi: 3.0.3\ninfo: {title: test, version: '1'}\n")
	handler := api.NewRouter(api.Deps{
		Store: newFakeStore(), Logger: observability.NewLogger(io.Discard, "error"), OpenAPISpec: spec,
	})
	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec
	}

	rec := get("/openapi.yaml")
	if rec.Code != http.StatusOK || rec.Body.String() != string(spec) || !strings.Contains(rec.Header().Get("Content-Type"), "yaml") {
		t.Fatalf("/openapi.yaml: code %d type %q body %q", rec.Code, rec.Header().Get("Content-Type"), rec.Body.String())
	}
	rec = get("/docs")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), "text/html") ||
		!strings.Contains(rec.Body.String(), "swagger-ui") || !strings.Contains(rec.Body.String(), "/openapi.yaml") {
		t.Fatalf("/docs: code %d type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
}

func TestDocsAbsentWithoutSpec(t *testing.T) {
	h := newHarness(t)
	if r := h.do(t, "GET", "/docs", ""); r.code != http.StatusNotFound {
		t.Fatalf("/docs without a spec: code %d, want 404", r.code)
	}
}
