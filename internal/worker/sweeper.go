package worker

import (
	"context"
	"log/slog"
	"time"
)

// Sweeper defaults, used when a field is not set.
const (
	DefaultSweepInterval = 30 * time.Second
	// DefaultStuckAfter must exceed the longest delivery (provider timeout
	// plus state-write retries) so a live delivery is never reset.
	DefaultStuckAfter = 2 * time.Minute
	// DefaultQueuedAfter must exceed the normal time a message waits in its
	// queue; resetting a row whose message is merely slow only republishes
	// it, and the duplicate is skipped, but it wastes work.
	DefaultQueuedAfter = 10 * time.Minute
)

// SweeperStore resets rows abandoned in processing or queued.
type SweeperStore interface {
	ResetStuck(ctx context.Context, olderThan time.Duration) (int64, error)
	ResetStaleQueued(ctx context.Context, olderThan time.Duration) (int64, error)
}

// Sweeper periodically returns abandoned notifications to pending so the
// relay publishes them again: rows stuck in processing (their worker
// crashed mid-delivery) and rows stuck in queued (their message never
// reached a queue).
type Sweeper struct {
	Store       SweeperStore
	Interval    time.Duration
	StuckAfter  time.Duration
	QueuedAfter time.Duration
	Logger      *slog.Logger
}

// Run sweeps once immediately, so rows left by a crashed predecessor are
// recovered on start, then every Interval until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) error {
	if s.Interval <= 0 {
		s.Interval = DefaultSweepInterval
	}
	if s.StuckAfter <= 0 {
		s.StuckAfter = DefaultStuckAfter
	}
	if s.QueuedAfter <= 0 {
		s.QueuedAfter = DefaultQueuedAfter
	}
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		s.sweep(ctx, "processing", s.StuckAfter, s.Store.ResetStuck)
		s.sweep(ctx, "queued", s.QueuedAfter, s.Store.ResetStaleQueued)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Sweeper) sweep(ctx context.Context, state string, after time.Duration, reset func(context.Context, time.Duration) (int64, error)) {
	n, err := reset(ctx, after)
	switch {
	case err != nil && ctx.Err() == nil:
		s.Logger.ErrorContext(ctx, "sweep failed", "state", state, "error", err)
	case n > 0:
		s.Logger.WarnContext(ctx, "reset abandoned notifications", "state", state, "count", n)
	}
}
