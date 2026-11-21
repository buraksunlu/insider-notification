package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// RelayStore is the outbox side of the store.
type RelayStore interface {
	ClaimDue(ctx context.Context, c domain.Channel, limit int, publish func(domain.Notification) error) (int, error)
	QueuedByChannel(ctx context.Context) (map[domain.Channel]int, error)
}

// Publisher publishes a notification to its channel queue.
type Publisher interface {
	Publish(ctx context.Context, n domain.Notification) error
}

// Relay defaults, used when Interval or Batch is not set.
const (
	DefaultRelayInterval = time.Second
	DefaultRelayBatch    = 500
	// DefaultMaxQueued is how many messages per channel may wait in
	// RabbitMQ: about 100 s of work at 100 msg/s. It must stay well below
	// rate × DefaultQueuedAfter (60 000), or a message merely waiting its
	// turn would be taken for lost by the sweeper and published again.
	DefaultMaxQueued = 10000
)

// Relay moves due notifications from the database outbox to RabbitMQ.
// It wakes on a LISTEN signal (new inserts) and on every Interval (retries
// and scheduled notifications whose time has come).
type Relay struct {
	Store     RelayStore
	Publisher Publisher
	// Listen returns a channel signalled on new inserts; nil means poll only.
	Listen func(ctx context.Context) <-chan struct{}
	// Interval is the poll period; zero means DefaultRelayInterval.
	Interval time.Duration
	// Batch is the most rows claimed per transaction; zero means DefaultRelayBatch.
	Batch int
	// MaxQueued caps queued messages per channel; zero means DefaultMaxQueued.
	// A backlog stays in Postgres instead of piling up in RabbitMQ.
	MaxQueued int
	Logger    *slog.Logger
}

// Run relays until ctx is cancelled and then returns nil. Errors are logged
// and retried on the next tick: unpublished rows go back to pending.
func (r *Relay) Run(ctx context.Context) error {
	if r.Interval <= 0 {
		r.Interval = DefaultRelayInterval
	}
	if r.Batch <= 0 {
		r.Batch = DefaultRelayBatch
	}
	if r.MaxQueued <= 0 {
		r.MaxQueued = DefaultMaxQueued
	}
	var wake <-chan struct{}
	if r.Listen != nil {
		wake = r.Listen(ctx)
	}
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		r.drain(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case _, ok := <-wake:
			if !ok {
				wake = nil // listener gone: keep polling
			}
		}
	}
}

// drain claims batches per channel until every channel is either caught up
// (a short batch) or at MaxQueued, so a large backlog is published without
// waiting for the next tick but never floods RabbitMQ. A channel at its cap
// is refilled on the next tick, once consumers have made room.
func (r *Relay) drain(ctx context.Context) {
	publish := func(item domain.Notification) error { return r.Publisher.Publish(ctx, item) }
	for ctx.Err() == nil {
		queued, err := r.Store.QueuedByChannel(ctx)
		if err != nil {
			r.logError(ctx, err)
			return
		}
		more := false
		for _, c := range domain.Channels {
			room := r.MaxQueued - queued[c]
			if room <= 0 {
				continue
			}
			limit := min(r.Batch, room)
			n, err := r.Store.ClaimDue(ctx, c, limit, publish)
			if err != nil {
				r.logError(ctx, err)
				return
			}
			if n > 0 {
				r.Logger.DebugContext(ctx, "relayed notifications", "channel", string(c), "count", n)
			}
			more = more || n == limit
		}
		if !more {
			return
		}
	}
}

func (r *Relay) logError(ctx context.Context, err error) {
	if ctx.Err() == nil {
		r.Logger.ErrorContext(ctx, "relay tick failed", "error", err)
	}
}
