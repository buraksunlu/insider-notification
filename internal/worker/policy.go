// Package worker contains the delivery pipeline: retry policy, message
// handler, outbox relay and stuck-message sweeper.
package worker

import (
	"net/http"
	"time"
)

// MaxBackoff caps both the exponential backoff and a provider's Retry-After.
const MaxBackoff = 5 * time.Minute

// Outcome is the result category of a delivery attempt.
type Outcome int

// Delivery outcomes.
const (
	OutcomeSent      Outcome = iota // provider accepted the message
	OutcomeRetry                    // transient failure, try again later
	OutcomePermanent                // the request itself is wrong, never retry
)

func (o Outcome) String() string {
	switch o {
	case OutcomeSent:
		return "sent"
	case OutcomeRetry:
		return "retry"
	default:
		return "permanent"
	}
}

// Backoff returns the delay before the next attempt using exponential
// backoff with full jitter: rnd() * min(MaxBackoff, base * 2^(attempt-1)).
// attempt is 1-based (the attempt that just failed).
func Backoff(attempt int, base time.Duration, rnd func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// Stop doubling once the cap is reached so large attempts cannot overflow.
	ceiling := base
	for i := 1; i < attempt && ceiling < MaxBackoff; i++ {
		ceiling *= 2
	}
	return time.Duration(rnd() * float64(min(ceiling, MaxBackoff)))
}

// NextDelay honours a provider's Retry-After (capped at MaxBackoff) and
// otherwise falls back to Backoff.
func NextDelay(attempt int, base, retryAfter time.Duration, rnd func() float64) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter, MaxBackoff)
	}
	return Backoff(attempt, base, rnd)
}

// Classify maps a provider response to an outcome. A non-nil err means
// the request never got a response (timeout, network error, shutdown)
// and is always retryable: the message is not lost, only delayed.
func Classify(statusCode int, err error) Outcome {
	if err != nil {
		return OutcomeRetry
	}
	switch {
	case statusCode >= 200 && statusCode < 300:
		return OutcomeSent
	case statusCode == http.StatusRequestTimeout,
		statusCode == http.StatusTooManyRequests,
		statusCode >= 500:
		return OutcomeRetry
	default:
		// 3xx left over after the client followed redirects and other 4xx:
		// the request is wrong and retrying cannot fix it.
		return OutcomePermanent
	}
}
