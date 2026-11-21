package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func post(t *testing.T, h http.Handler, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAcceptsWithSpecBody(t *testing.T) {
	h := newMock(0, 0).routes()
	rec := post(t, h, `{"to":"+905551234567","channel":"sms","content":"hi"}`, "id-1")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("code = %d, want 202", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["messageId"] == "" || body["status"] != "accepted" || body["timestamp"] == "" {
		t.Fatalf("body = %v, want messageId, status accepted, timestamp", body)
	}
}

func TestContentForcesStatus(t *testing.T) {
	h := newMock(0, 0).routes()
	for content, want := range map[string]int{"mock:500 x": 500, "mock:400": 400, "mock:429": 429, "mock:abc": 202, "hello mock:500": 202} {
		rec := post(t, h, `{"to":"+905551234567","channel":"sms","content":"`+content+`"}`, "")
		if rec.Code != want {
			t.Errorf("content %q: code %d, want %d", content, rec.Code, want)
		}
	}
}

func TestFailRate(t *testing.T) {
	m := newMock(0.5, 0)
	h := m.routes()
	m.rand = func() float64 { return 0.4 }
	if rec := post(t, h, `{"to":"a","channel":"push","content":"x"}`, ""); rec.Code != 500 {
		t.Fatalf("below fail rate: code %d, want 500", rec.Code)
	}
	m.rand = func() float64 { return 0.6 }
	if rec := post(t, h, `{"to":"a","channel":"push","content":"x"}`, ""); rec.Code != 202 {
		t.Fatalf("above fail rate: code %d, want 202", rec.Code)
	}
}

func TestRejectsBadBody(t *testing.T) {
	h := newMock(0, 0).routes()
	if rec := post(t, h, `not json`, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("code = %d, want 400", rec.Code)
	}
}

func TestCountsReceivedByIdempotencyKey(t *testing.T) {
	h := newMock(0, 0).routes()
	post(t, h, `{"to":"a","channel":"push","content":"x"}`, "id-7")
	post(t, h, `{"to":"a","channel":"push","content":"x"}`, "id-7")

	for id, want := range map[string]int{"id-7": 2, "never": 0} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/received/"+id, nil))
		var body map[string]int
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		if body["count"] != want {
			t.Errorf("received %s = %d, want %d", id, body["count"], want)
		}
	}
}
