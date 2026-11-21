//go:build e2e

// Package e2e drives the whole stack started by docker compose: API,
// worker, RabbitMQ, Postgres and the mock provider. Run with `make e2e`,
// which starts the stack with RETRY_BASE=50ms so retry scenarios finish fast.
package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

var (
	apiURL  = env("API_URL", "http://localhost:8080")
	mockURL = env("MOCK_URL", "http://localhost:8090")
	client  = &http.Client{Timeout: 10 * time.Second}
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func TestMain(m *testing.M) {
	// The stack may still be starting: wait for the API to be ready.
	deadline := time.Now().Add(90 * time.Second)
	for {
		resp, err := client.Get(apiURL + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			fmt.Fprintln(os.Stderr, "API not ready at", apiURL, "- is `docker compose up` running?")
			os.Exit(1)
		}
		time.Sleep(time.Second)
	}
	os.Exit(m.Run())
}

type notification struct {
	ID                string `json:"id"`
	Status            string `json:"status"`
	Attempts          int    `json:"attempts"`
	MaxAttempts       int    `json:"max_attempts"`
	LastError         string `json:"last_error"`
	ProviderMessageID string `json:"provider_message_id"`
}

func call(t *testing.T, method, path string, body any, headers ...string) (int, []byte) {
	t.Helper()
	var r io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		r = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, apiURL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func create(t *testing.T, body map[string]any, headers ...string) notification {
	t.Helper()
	code, raw := call(t, "POST", "/api/v1/notifications", body, headers...)
	if code != http.StatusAccepted {
		t.Fatalf("create: %d %s", code, raw)
	}
	var n notification
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatal(err)
	}
	return n
}

func get(t *testing.T, id string) notification {
	t.Helper()
	code, raw := call(t, "GET", "/api/v1/notifications/"+id, nil)
	if code != http.StatusOK {
		t.Fatalf("get %s: %d %s", id, code, raw)
	}
	var n notification
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatal(err)
	}
	return n
}

// waitFor polls a notification until it reaches a final status.
func waitFor(t *testing.T, id, want string, within time.Duration) notification {
	t.Helper()
	deadline := time.Now().Add(within)
	var n notification
	for time.Now().Before(deadline) {
		n = get(t, id)
		if n.Status == want {
			return n
		}
		if n.Status == "sent" || n.Status == "failed" || n.Status == "cancelled" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("notification %s: status %q (attempts %d, last_error %q), want %q", id, n.Status, n.Attempts, n.LastError, want)
	return n
}

// received is how many times the mock provider got notification id.
func received(t *testing.T, id string) int {
	t.Helper()
	resp, err := client.Get(mockURL + "/received/" + id)
	if err != nil {
		t.Fatalf("mock provider: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	var body struct {
		Count int `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body.Count
}

func sms(content string) map[string]any {
	return map[string]any{"channel": "sms", "recipient": "+905551234567", "content": content}
}

func TestDeliversToProvider(t *testing.T) {
	n := create(t, sms("Merhaba 👋 e2e"))
	got := waitFor(t, n.ID, "sent", 20*time.Second)
	if got.Attempts != 1 || got.ProviderMessageID == "" {
		t.Fatalf("sent with attempts %d, provider id %q; want 1 attempt and an id", got.Attempts, got.ProviderMessageID)
	}
	if c := received(t, n.ID); c != 1 {
		t.Fatalf("provider received it %d times, want 1", c)
	}
}

// Needs the worker started with a short RETRY_BASE (make e2e uses 50ms);
// with the default 2s base, eight attempts take about four minutes.
func TestRetriesThenFailsToDLQ(t *testing.T) {
	n := create(t, sms("mock:500 always fails"))
	got := waitFor(t, n.ID, "failed", 60*time.Second)
	if got.Attempts != got.MaxAttempts || !strings.Contains(got.LastError, "max attempts") {
		t.Fatalf("failed after %d/%d attempts, last_error %q; want all attempts used", got.Attempts, got.MaxAttempts, got.LastError)
	}
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	n := create(t, sms("mock:400 bad request"))
	got := waitFor(t, n.ID, "failed", 20*time.Second)
	if got.Attempts != 1 || !strings.Contains(got.LastError, "400") {
		t.Fatalf("failed after %d attempts, last_error %q; want 1 attempt with HTTP 400", got.Attempts, got.LastError)
	}
}

func TestCancelledScheduledNotificationIsNeverSent(t *testing.T) {
	body := sms("scheduled then cancelled")
	body["scheduled_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	n := create(t, body)

	code, raw := call(t, "POST", "/api/v1/notifications/"+n.ID+"/cancel", nil)
	if code != http.StatusOK {
		t.Fatalf("cancel: %d %s", code, raw)
	}
	time.Sleep(2 * time.Second) // a few relay ticks
	if got := get(t, n.ID); got.Status != "cancelled" || got.Attempts != 0 {
		t.Fatalf("status %q attempts %d, want cancelled with 0 attempts", got.Status, got.Attempts)
	}
	if c := received(t, n.ID); c != 0 {
		t.Fatalf("provider received a cancelled notification %d times", c)
	}
}

func TestBatchIsDeliveredAndSummarised(t *testing.T) {
	items := []map[string]any{
		sms("batch 1"),
		{"channel": "email", "recipient": "ada@example.com", "content": "batch 2", "priority": "high"},
		{"channel": "push", "recipient": "device-token-e2e", "content": "batch 3", "priority": "low"},
	}
	code, raw := call(t, "POST", "/api/v1/notifications/batch", map[string]any{"notifications": items})
	if code != http.StatusAccepted {
		t.Fatalf("batch: %d %s", code, raw)
	}
	var created struct {
		BatchID string   `json:"batch_id"`
		IDs     []string `json:"ids"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || len(created.IDs) != 3 {
		t.Fatalf("batch response %s (%v)", raw, err)
	}
	for _, id := range created.IDs {
		waitFor(t, id, "sent", 20*time.Second)
	}

	code, raw = call(t, "GET", "/api/v1/batches/"+created.BatchID, nil)
	var batch struct {
		Total  int            `json:"total"`
		Counts map[string]int `json:"counts"`
	}
	if code != http.StatusOK || json.Unmarshal(raw, &batch) != nil || batch.Total != 3 || batch.Counts["sent"] != 3 {
		t.Fatalf("batch summary: %d %s, want total 3 and 3 sent", code, raw)
	}
}

func TestIdempotentCreate(t *testing.T) {
	key := fmt.Sprintf("e2e-%d", time.Now().UnixNano())
	first := create(t, sms("idempotent"), "Idempotency-Key", key)
	code, raw := call(t, "POST", "/api/v1/notifications", sms("idempotent"), "Idempotency-Key", key)
	var second notification
	if code != http.StatusOK || json.Unmarshal(raw, &second) != nil || second.ID != first.ID {
		t.Fatalf("replay: %d %s, want 200 with id %s", code, raw, first.ID)
	}
	waitFor(t, first.ID, "sent", 20*time.Second)
	if c := received(t, first.ID); c != 1 {
		t.Fatalf("provider received it %d times, want 1", c)
	}
}

func TestTemplateIsRenderedAndDelivered(t *testing.T) {
	name := fmt.Sprintf("e2e-otp-%d", time.Now().UnixNano())
	code, raw := call(t, "POST", "/api/v1/templates", map[string]any{
		"name": name, "channel": "sms", "body": "Merhaba {{name}}, kodunuz {{code}}",
	})
	var tpl struct {
		ID string `json:"id"`
	}
	if code != http.StatusCreated || json.Unmarshal(raw, &tpl) != nil {
		t.Fatalf("create template: %d %s", code, raw)
	}

	code, raw = call(t, "POST", "/api/v1/notifications", map[string]any{
		"channel": "sms", "recipient": "+905551234567", "template_id": tpl.ID,
		"variables": map[string]string{"name": "Şule", "code": "4821"},
	})
	var n struct {
		ID         string `json:"id"`
		Content    string `json:"content"`
		TemplateID string `json:"template_id"`
	}
	if code != http.StatusAccepted || json.Unmarshal(raw, &n) != nil {
		t.Fatalf("create from template: %d %s", code, raw)
	}
	if n.Content != "Merhaba Şule, kodunuz 4821" || n.TemplateID != tpl.ID {
		t.Fatalf("content %q template_id %q", n.Content, n.TemplateID)
	}
	waitFor(t, n.ID, "sent", 20*time.Second)
}
