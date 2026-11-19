package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

func ptrTime(t time.Time) *time.Time { return &t }

func TestValidate(t *testing.T) {
	tests := []struct {
		name      string
		in        CreateInput
		wantField string // "" means valid
	}{
		{"valid sms", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"}, ""},
		{"valid email", CreateInput{Channel: "email", Recipient: "ada@example.com", Content: "hi", Priority: "high"}, ""},
		{"valid push", CreateInput{Channel: "push", Recipient: "device-token-1", Content: "hi", Priority: "low"}, ""},
		{"sms phone without plus", CreateInput{Channel: "sms", Recipient: "905551234567", Content: "hi"}, "recipient"},
		{"sms phone too short", CreateInput{Channel: "sms", Recipient: "+90555", Content: "hi"}, "recipient"},
		{"email invalid", CreateInput{Channel: "email", Recipient: "not-an-email", Content: "hi"}, "recipient"},
		{"push token too long", CreateInput{Channel: "push", Recipient: strings.Repeat("a", 513), Content: "hi"}, "recipient"},
		{"push token empty", CreateInput{Channel: "push", Recipient: "", Content: "hi"}, "recipient"},
		{"sms 160 turkish runes ok", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: strings.Repeat("ş", 160)}, ""},
		{"sms 161 turkish runes too long", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: strings.Repeat("ş", 161)}, "content"},
		{"sms 160 emoji ok", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: strings.Repeat("😀", 160)}, ""},
		{"sms 161 emoji too long", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: strings.Repeat("😀", 161)}, "content"},
		{"push 257 too long", CreateInput{Channel: "push", Recipient: "tok", Content: strings.Repeat("a", 257)}, "content"},
		{"email 10000 ok", CreateInput{Channel: "email", Recipient: "a@b.co", Content: strings.Repeat("a", 10000)}, ""},
		{"email 10001 too long", CreateInput{Channel: "email", Recipient: "a@b.co", Content: strings.Repeat("a", 10001)}, "content"},
		{"empty content", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: ""}, "content"},
		{"whitespace content", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "  \n\t "}, "content"},
		{"unknown channel", CreateInput{Channel: "fax", Recipient: "x", Content: "hi"}, "channel"},
		{"unknown priority", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "hi", Priority: "urgent"}, "priority"},
		{"scheduled in past", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "hi", ScheduledAt: ptrTime(now.Add(-time.Minute))}, "scheduled_at"},
		{"scheduled beyond 30 days", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "hi", ScheduledAt: ptrTime(now.Add(31 * 24 * time.Hour))}, "scheduled_at"},
		{"scheduled in an hour ok", CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "hi", ScheduledAt: ptrTime(now.Add(time.Hour))}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.in, now)
			if tc.wantField == "" {
				if err != nil {
					t.Fatalf("expected valid, got %v", err)
				}
				return
			}
			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("expected *ValidationError, got %v", err)
			}
			if _, ok := verr.Fields[tc.wantField]; !ok {
				t.Fatalf("expected error on field %q, got %v", tc.wantField, verr.Fields)
			}
		})
	}
}

func TestValidate_ReportsAllFields(t *testing.T) {
	err := Validate(CreateInput{Channel: "sms", Recipient: "bad", Content: ""}, now)
	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("expected *ValidationError, got %v", err)
	}
	for _, f := range []string{"recipient", "content"} {
		if _, ok := verr.Fields[f]; !ok {
			t.Errorf("missing error for %q in %v", f, verr.Fields)
		}
	}
}

func TestNewNotification_Defaults(t *testing.T) {
	n := NewNotification(CreateInput{Channel: "sms", Recipient: "+905551234567", Content: "hi"}, "corr-1", now)

	if n.Status != StatusPending {
		t.Errorf("Status = %q, want pending", n.Status)
	}
	if n.Priority != PriorityNormal {
		t.Errorf("Priority = %q, want normal", n.Priority)
	}
	if n.MaxAttempts != 5 {
		t.Errorf("MaxAttempts = %d, want 5", n.MaxAttempts)
	}
	if !n.NextAttemptAt.Equal(now) {
		t.Errorf("NextAttemptAt = %v, want now", n.NextAttemptAt)
	}
	if n.CorrelationID != "corr-1" || n.ID.String() == "00000000-0000-0000-0000-000000000000" {
		t.Errorf("unexpected id/correlation: %v %q", n.ID, n.CorrelationID)
	}
}

func TestNewNotification_ScheduledSetsNextAttempt(t *testing.T) {
	at := now.Add(time.Hour)
	n := NewNotification(CreateInput{Channel: "email", Recipient: "a@b.co", Content: "hi", Priority: "high", ScheduledAt: &at}, "c", now)

	if !n.NextAttemptAt.Equal(at) {
		t.Errorf("NextAttemptAt = %v, want %v", n.NextAttemptAt, at)
	}
	if n.Priority != PriorityHigh {
		t.Errorf("Priority = %q, want high", n.Priority)
	}
}

func TestPriorityAMQP(t *testing.T) {
	want := map[Priority]uint8{PriorityHigh: 3, PriorityNormal: 2, PriorityLow: 1}
	for p, w := range want {
		if got := p.AMQP(); got != w {
			t.Errorf("%s.AMQP() = %d, want %d", p, got, w)
		}
	}
}
