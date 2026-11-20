package worker

import (
	"context"
	"log/slog"
	"time"
)

// SweeperStore resets rows abandoned in processing.
type SweeperStore interface {
	ResetStuck(ctx context.Context, olderThan time.Duration) (int64, error)
}

// Sweeper periodically returns notifications stuck in processing (their
// worker crashed mid-delivery) to pending so the relay publishes them again.
type Sweeper struct {
	Store      SweeperStore
	Interval   time.Duration
	StuckAfter time.Duration
	Logger     *slog.Logger
}

// Run sweeps once immediately, so rows left by a crashed predecessor are
// recovered on start, then every Interval until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		n, err := s.Store.ResetStuck(ctx, s.StuckAfter)
		switch {
		case err != nil && ctx.Err() == nil:
			s.Logger.ErrorContext(ctx, "sweep failed", "error", err)
		case n > 0:
			s.Logger.WarnContext(ctx, "reset stuck notifications", "count", n)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
