package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// RelayStore is the outbox side of the store.
type RelayStore interface {
	ClaimDue(ctx context.Context, limit int, publish func([]domain.Notification) error) (int, error)
}

// Publisher publishes a notification to its channel queue.
type Publisher interface {
	Publish(ctx context.Context, n domain.Notification) error
}

// Relay moves due notifications from the database outbox to RabbitMQ.
// It wakes on a LISTEN signal (new inserts) and on every Interval (retries
// and scheduled notifications whose time has come).
type Relay struct {
	Store     RelayStore
	Publisher Publisher
	// Listen returns a channel signalled on new inserts; nil means poll only.
	Listen   func(ctx context.Context) <-chan struct{}
	Interval time.Duration
	Batch    int
	Logger   *slog.Logger
}

// Run relays until ctx is cancelled and then returns nil. Errors are logged
// and retried on the next tick: unpublished rows simply stay pending.
func (r *Relay) Run(ctx context.Context) error {
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

// drain claims batches until one comes back short, so a large backlog is
// published without waiting for the next tick.
func (r *Relay) drain(ctx context.Context) {
	for ctx.Err() == nil {
		n, err := r.Store.ClaimDue(ctx, r.Batch, func(due []domain.Notification) error {
			for _, item := range due {
				if err := r.Publisher.Publish(ctx, item); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			if ctx.Err() == nil {
				r.Logger.ErrorContext(ctx, "relay tick failed", "error", err)
			}
			return
		}
		if n > 0 {
			r.Logger.DebugContext(ctx, "relayed notifications", "count", n)
		}
		if n < r.Batch {
			return
		}
	}
}
