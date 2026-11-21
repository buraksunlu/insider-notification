package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"golang.org/x/time/rate"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/observability"
	"github.com/buraksunlu/insider-notification/internal/provider"
	"github.com/buraksunlu/insider-notification/internal/queue"
	"github.com/buraksunlu/insider-notification/internal/store"
)

// State-write retry bounds. Once the provider has answered, its outcome
// must be written: a requeued message is skipped (the row is processing)
// and the sweeper then resets the row, so giving up early turns a delivered
// message into a duplicate send. Writes are therefore retried for up to
// DefaultStoreWriteTimeout, with pauses capped at maxStoreRetryPause.
const (
	DefaultStoreWriteTimeout = 30 * time.Second
	maxStoreRetryPause       = 2 * time.Second
)

// HandlerStore is the part of the store the consumer needs.
type HandlerStore interface {
	StartProcessing(ctx context.Context, id uuid.UUID) (domain.Notification, bool, error)
	MarkSent(ctx context.Context, id uuid.UUID, providerMsgID *string, at time.Time) error
	ScheduleRetry(ctx context.Context, id uuid.UUID, next time.Time, lastErr string) error
	MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error
	RecordAttempt(ctx context.Context, a domain.Attempt) error
}

// Sender delivers a notification to the provider.
type Sender interface {
	Send(ctx context.Context, idemKey, correlationID string, r provider.Request) (provider.Result, error)
}

// DLQPublisher records permanently failed notifications.
type DLQPublisher interface {
	PublishDLQ(ctx context.Context, n domain.Notification, reason string) error
}

// Handler processes one queue message: claim, rate-limit, deliver,
// classify, then mark sent, reschedule or fail to the DLQ.
type Handler struct {
	Store    HandlerStore
	Sender   Sender
	DLQ      DLQPublisher
	Limiters map[domain.Channel]*rate.Limiter
	Metrics  *observability.Metrics // nil disables metrics
	Logger   *slog.Logger
	Now      func() time.Time
	Rand     func() float64
	// RetryBase is the base of the delivery backoff.
	RetryBase time.Duration
	// StoreRetryBase is the first pause between state-write attempts; it
	// doubles each time. Zero means 100 ms.
	StoreRetryBase time.Duration
	// StoreWriteTimeout is how long a state write is retried before the
	// message is requeued. Zero means DefaultStoreWriteTimeout.
	StoreWriteTimeout time.Duration
}

// NewLimiters returns one token bucket per channel, perSecond tokens per
// second with an equal burst, so channels never throttle each other.
func NewLimiters(perSecond int) map[domain.Channel]*rate.Limiter {
	out := make(map[domain.Channel]*rate.Limiter, len(domain.Channels))
	for _, c := range domain.Channels {
		out[c] = rate.NewLimiter(rate.Limit(perSecond), perSecond)
	}
	return out
}

// Handle processes m. A nil return acks the message; an error requeues it.
// That happens only when the database could not be reached: before the
// claim nothing has been sent; after delivery the outcome write is retried
// for StoreWriteTimeout first, because a requeue then means the sweeper
// resets the row and the notification may be sent again (at-least-once).
func (h *Handler) Handle(ctx context.Context, m queue.Message) error {
	// Claim first: a cancelled or duplicate message is skipped without
	// spending a rate-limit token.
	n, ok, err := h.Store.StartProcessing(ctx, m.ID)
	if err != nil {
		return fmt.Errorf("start processing %s: %w", m.ID, err)
	}
	corr := m.CorrelationID
	if corr == "" {
		corr = n.CorrelationID
	}
	ctx = observability.WithCorrelationID(ctx, corr)
	log := h.Logger.With("notification_id", m.ID.String(), "channel", string(m.Channel))
	if !ok {
		log.DebugContext(ctx, "skip message: notification is not queued")
		h.Metrics.Delivery(m.Channel, observability.ResultSkipped)
		return nil
	}
	log = log.With("attempt", n.Attempts)

	res, sendErr := h.deliver(ctx, n, corr)
	h.recordAttempt(ctx, log, n, res, sendErr)

	switch outcome := Classify(res.StatusCode, sendErr); {
	case outcome == OutcomeSent:
		if res.MessageID == nil {
			log.WarnContext(ctx, "provider accepted without a message id", "status", res.StatusCode)
		}
		log.InfoContext(ctx, "notification sent", "status", res.StatusCode, "latency_ms", res.Latency.Milliseconds())
		now := h.Now()
		if err := h.write(ctx, log, func(ctx context.Context) error { return h.Store.MarkSent(ctx, n.ID, res.MessageID, now) }); err != nil {
			return err
		}
		h.Metrics.Delivery(n.Channel, observability.ResultSent)
		h.Metrics.EndToEnd(n.Channel, now.Sub(dueAt(n)))
		return nil

	case outcome == OutcomeRetry && n.Attempts < n.MaxAttempts:
		reason := failureReason(res, sendErr)
		delay := NextDelay(n.Attempts, h.RetryBase, res.RetryAfter, h.Rand)
		log.WarnContext(ctx, "delivery failed, retry scheduled", "reason", reason, "retry_in", delay.String())
		if err := h.write(ctx, log, func(ctx context.Context) error { return h.Store.ScheduleRetry(ctx, n.ID, h.Now().Add(delay), reason) }); err != nil {
			return err
		}
		h.Metrics.Delivery(n.Channel, observability.ResultRetry)
		return nil

	default:
		reason := failureReason(res, sendErr)
		if outcome == OutcomeRetry {
			reason = fmt.Sprintf("max attempts (%d) reached: %s", n.MaxAttempts, reason)
		}
		log.ErrorContext(ctx, "delivery failed permanently", "reason", reason)
		// The database is the source of truth: mark failed first, then
		// mirror to the DLQ. A DLQ failure is logged, the row stays failed.
		if err := h.write(ctx, log, func(ctx context.Context) error { return h.Store.MarkFailed(ctx, n.ID, reason) }); err != nil {
			return err
		}
		h.Metrics.Delivery(n.Channel, observability.ResultFailed)
		n.Status, n.LastError = domain.StatusFailed, &reason
		if err := h.DLQ.PublishDLQ(ctx, n, reason); err != nil {
			log.ErrorContext(ctx, "publish to DLQ failed", "error", err)
		}
		return nil
	}
}

func (h *Handler) deliver(ctx context.Context, n domain.Notification, corr string) (provider.Result, error) {
	if l := h.Limiters[n.Channel]; l != nil {
		if err := l.Wait(ctx); err != nil {
			return provider.Result{}, fmt.Errorf("rate limiter: %w", err)
		}
	}
	// Timed here rather than taken from Result.Latency so timeouts and
	// network errors are observed too.
	start := time.Now()
	res, err := h.Sender.Send(ctx, n.ID.String(), corr, provider.Request{
		To: n.Recipient, Channel: string(n.Channel), Content: n.Content,
	})
	h.Metrics.AttemptDuration(n.Channel, time.Since(start))
	return res, err
}

// dueAt is when a notification became deliverable: its scheduled time, or
// its creation for immediate ones. End-to-end latency is measured from here
// so a notification scheduled days ahead does not look days late.
func dueAt(n domain.Notification) time.Time {
	if n.ScheduledAt != nil && n.ScheduledAt.After(n.CreatedAt) {
		return *n.ScheduledAt
	}
	return n.CreatedAt
}

// recordAttempt writes the audit row. It is best effort: losing an audit
// row must not change the delivery outcome.
func (h *Handler) recordAttempt(ctx context.Context, log *slog.Logger, n domain.Notification, res provider.Result, sendErr error) {
	a := domain.Attempt{
		NotificationID: n.ID,
		Attempt:        n.Attempts,
		HTTPStatus:     res.StatusCode,
		LatencyMS:      res.Latency.Milliseconds(),
	}
	if sendErr != nil {
		msg := sendErr.Error()
		a.Error = &msg
	}
	if err := h.Store.RecordAttempt(ctx, a); err != nil {
		log.WarnContext(ctx, "record delivery attempt failed", "error", err)
	}
}

// write runs a state transition, retrying transient errors with a doubling
// pause until StoreWriteTimeout. It runs on a context detached from ctx's
// cancellation: on shutdown the delivery has already happened and its
// outcome must still be recorded. ErrStateConflict means the row has
// already moved on (e.g. reset by the sweeper) and is not retried.
func (h *Handler) write(ctx context.Context, log *slog.Logger, fn func(context.Context) error) error {
	timeout := h.StoreWriteTimeout
	if timeout <= 0 {
		timeout = DefaultStoreWriteTimeout
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	pause := h.StoreRetryBase
	if pause <= 0 {
		pause = 100 * time.Millisecond
	}
	for {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, store.ErrStateConflict) {
			log.WarnContext(ctx, "state transition skipped: row changed concurrently")
			return nil
		}
		select {
		case <-time.After(pause):
		case <-ctx.Done():
			log.ErrorContext(ctx, "state transition failed, message requeued", "error", err)
			return err
		}
		pause = min(pause*2, maxStoreRetryPause)
	}
}

func failureReason(res provider.Result, sendErr error) string {
	if sendErr != nil {
		return "transport error: " + sendErr.Error()
	}
	return fmt.Sprintf("provider returned HTTP %d", res.StatusCode)
}
