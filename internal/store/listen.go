package store

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// listenRetry is the pause before re-subscribing after the LISTEN
// connection is lost. The relay keeps polling meanwhile.
const listenRetry = time.Second

// Listen subscribes to the Postgres notification channel and signals the
// returned channel (capacity 1, so bursts coalesce) on every notification.
// It also signals each time the subscription becomes active, covering
// inserts missed while disconnected. On connection loss it reconnects after
// a short pause. The channel is closed when ctx is cancelled.
func (s *Store) Listen(ctx context.Context, channel string) <-chan struct{} {
	out := make(chan struct{}, 1)
	signal := func() {
		select {
		case out <- struct{}{}:
		default:
		}
	}
	go func() {
		defer close(out)
		for ctx.Err() == nil {
			_ = s.listenOnce(ctx, channel, signal)
			select {
			case <-ctx.Done():
			case <-time.After(listenRetry):
			}
		}
	}()
	return out
}

func (s *Store) listenOnce(ctx context.Context, channel string, signal func()) error {
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	// Take the connection out of the pool: a LISTEN session must not be
	// handed to other queries, and it is closed rather than returned.
	conn := pc.Hijack()
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()

	if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return err
	}
	signal()
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		signal()
	}
}
