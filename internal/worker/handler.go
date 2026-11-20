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

// storeWriteAttempts is how often a final state write is tried before the
// message is requeued. A short database blip must not turn a delivered
// message into a duplicate send after the sweeper resets it.
const storeWriteAttempts = 3

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
	Logger   *slog.Logger
	Now      func() time.Time
	Rand     func() float64
	// RetryBase is the base of the delivery backoff.
	RetryBase time.Duration
	// StoreRetryBase is the first pause between state-write attempts; it
	// doubles each time. Zero means 100 ms.
	StoreRetryBase time.Duration
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

// Handle processes m. A nil return acks the message; an error requeues it
// (used only when the database could not be reached, so nothing is lost).
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
		return h.write(ctx, log, func() error { return h.Store.MarkSent(ctx, n.ID, res.MessageID, h.Now()) })

	case outcome == OutcomeRetry && n.Attempts < n.MaxAttempts:
		reason := failureReason(res, sendErr)
		delay := NextDelay(n.Attempts, h.RetryBase, res.RetryAfter, h.Rand)
		log.WarnContext(ctx, "delivery failed, retry scheduled", "reason", reason, "retry_in", delay.String())
		return h.write(ctx, log, func() error { return h.Store.ScheduleRetry(ctx, n.ID, h.Now().Add(delay), reason) })

	default:
		reason := failureReason(res, sendErr)
		if outcome == OutcomeRetry {
			reason = fmt.Sprintf("max attempts (%d) reached: %s", n.MaxAttempts, reason)
		}
		log.ErrorContext(ctx, "delivery failed permanently", "reason", reason)
		// The database is the source of truth: mark failed first, then
		// mirror to the DLQ. A DLQ failure is logged, the row stays failed.
		if err := h.write(ctx, log, func() error { return h.Store.MarkFailed(ctx, n.ID, reason) }); err != nil {
			return err
		}
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
	return h.Sender.Send(ctx, n.ID.String(), corr, provider.Request{
		To: n.Recipient, Channel: string(n.Channel), Content: n.Content,
	})
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

// write runs a state transition, retrying transient errors with a short
// doubling pause. ErrStateConflict means the row has already moved on
// (e.g. reset by the sweeper) and is not retried or requeued.
func (h *Handler) write(ctx context.Context, log *slog.Logger, fn func() error) error {
	pause := h.StoreRetryBase
	if pause <= 0 {
		pause = 100 * time.Millisecond
	}
	var err error
	for i := range storeWriteAttempts {
		if err = fn(); err == nil {
			return nil
		}
		if errors.Is(err, store.ErrStateConflict) {
			log.WarnContext(ctx, "state transition skipped: row changed concurrently")
			return nil
		}
		if i < storeWriteAttempts-1 {
			select {
			case <-time.After(pause):
			case <-ctx.Done():
				return ctx.Err()
			}
			pause *= 2
		}
	}
	log.ErrorContext(ctx, "state transition failed, message requeued", "error", err)
	return err
}

func failureReason(res provider.Result, sendErr error) string {
	if sendErr != nil {
		return "transport error: " + sendErr.Error()
	}
	return fmt.Sprintf("provider returned HTTP %d", res.StatusCode)
}
