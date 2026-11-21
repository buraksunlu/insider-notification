// Package provider is the HTTP client for the external notification
// provider (a webhook that accepts {to, channel, content}).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxBodyBytes bounds how much of a provider response is read, so a
// misbehaving provider cannot make the worker buffer an unbounded body.
const maxBodyBytes = 64 << 10

// Request is the payload sent to the provider.
type Request struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// Result describes the provider's answer. Any HTTP response, including
// 4xx and 5xx, is a Result; classifying it is the caller's job.
type Result struct {
	StatusCode int
	// MessageID is the provider's id from a 2xx body, nil when the body
	// is empty, not JSON or carries no id.
	MessageID *string
	// RetryAfter is the parsed Retry-After header, 0 when absent or invalid.
	RetryAfter time.Duration
	// Latency is the time from sending the request to reading the body.
	Latency time.Duration
}

// Client posts notifications to the provider.
type Client struct {
	url     string
	timeout time.Duration
	http    *http.Client
	now     func() time.Time
}

// NewClient returns a client that gives each Send at most timeout,
// covering connect, headers and body.
//
// Redirects are not followed: on 301/302/303 Go would resend the POST as a
// GET without a body, and a 2xx from the target would record an undelivered
// notification as sent. The 3xx is returned and classified as permanent.
func NewClient(url string, timeout time.Duration) *Client {
	return &Client{
		url:     url,
		timeout: timeout,
		http: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now: time.Now,
	}
}

// Send posts r to the provider. idemKey is sent as Idempotency-Key so a
// redelivered message can be deduplicated by the provider; correlationID
// is propagated as X-Correlation-ID when set.
//
// A non-2xx response is not an error. The error is non-nil only when no
// response was received: timeout (wraps context.DeadlineExceeded),
// cancellation (wraps context.Canceled) or a network failure.
func (c *Client) Send(ctx context.Context, idemKey, correlationID string, r Request) (Result, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return Result{}, fmt.Errorf("encode provider request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(payload))
	if err != nil {
		return Result{}, fmt.Errorf("build provider request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idemKey)
	if correlationID != "" {
		req.Header.Set("X-Correlation-ID", correlationID)
	}

	start := c.now()
	resp, err := c.http.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("provider request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return Result{}, fmt.Errorf("read provider response: %w", err)
	}

	res := Result{
		StatusCode: resp.StatusCode,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), c.now()),
		Latency:    c.now().Sub(start),
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		res.MessageID = messageID(body)
	}
	return res, nil
}

// messageID extracts "messageId" from a provider body, tolerating bodies
// that are empty or not JSON: the provider accepted the message either way.
func messageID(body []byte) *string {
	var v struct {
		MessageID string `json:"messageId"`
	}
	if json.Unmarshal(body, &v) != nil {
		return nil
	}
	id := strings.TrimSpace(v.MessageID)
	if id == "" {
		return nil
	}
	return &id
}

// parseRetryAfter accepts both forms of RFC 9110 Retry-After: delay in
// seconds or an HTTP date. Invalid, negative or past values yield 0.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}
