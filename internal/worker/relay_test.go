package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/queue"
	"github.com/buraksunlu/insider-notification/internal/store"
	"github.com/buraksunlu/insider-notification/internal/testutil"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeOutbox hands out queued rows in chunks of at most limit.
type fakeOutbox struct {
	mu      sync.Mutex
	due     []domain.Notification
	failErr error
	calls   int
}

func (f *fakeOutbox) ClaimDue(_ context.Context, limit int, publish func([]domain.Notification) error) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failErr != nil {
		return 0, f.failErr
	}
	n := min(limit, len(f.due))
	if n == 0 {
		return 0, nil
	}
	if err := publish(f.due[:n]); err != nil {
		return 0, err // rows stay due, like a rolled-back transaction
	}
	f.due = f.due[n:]
	return n, nil
}

func (f *fakeOutbox) remaining() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.due)
}

type fakePublisher struct {
	mu  sync.Mutex
	got []domain.Notification
	err error
}

func (p *fakePublisher) Publish(_ context.Context, n domain.Notification) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.got = append(p.got, n)
	return nil
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.got)
}

func rows(n int) []domain.Notification {
	out := make([]domain.Notification, n)
	for i := range out {
		out[i] = sms()
	}
	return out
}

func runInBackground(t *testing.T, run func(context.Context) error) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v, want nil on shutdown", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	return cancel
}

func eventually(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestRelayDrainsFullBatchesWithoutWaiting(t *testing.T) {
	outbox := &fakeOutbox{due: rows(1200)}
	pub := &fakePublisher{}
	r := &Relay{Store: outbox, Publisher: pub, Interval: time.Hour, Batch: 500, Logger: quiet}
	runInBackground(t, r.Run)

	// Interval is an hour: only the drain loop can publish all 1200 rows.
	eventually(t, time.Second, func() bool { return pub.count() == 1200 }, "relay did not drain 3 batches in one tick")
}

func TestRelayWakesOnListenSignal(t *testing.T) {
	outbox := &fakeOutbox{}
	pub := &fakePublisher{}
	wake := make(chan struct{}, 1)
	r := &Relay{
		Store: outbox, Publisher: pub, Interval: time.Hour, Batch: 500, Logger: quiet,
		Listen: func(context.Context) <-chan struct{} { return wake },
	}
	runInBackground(t, r.Run)

	eventually(t, time.Second, func() bool { outbox.mu.Lock(); defer outbox.mu.Unlock(); return outbox.calls >= 1 }, "no initial tick")
	outbox.mu.Lock()
	outbox.due = rows(3)
	outbox.mu.Unlock()
	wake <- struct{}{}
	eventually(t, time.Second, func() bool { return pub.count() == 3 }, "relay ignored the LISTEN signal")
}

func TestRelayPollsOnInterval(t *testing.T) {
	outbox := &fakeOutbox{}
	pub := &fakePublisher{}
	r := &Relay{Store: outbox, Publisher: pub, Interval: 20 * time.Millisecond, Batch: 500, Logger: quiet}
	runInBackground(t, r.Run)

	time.Sleep(30 * time.Millisecond)
	outbox.mu.Lock()
	outbox.due = rows(2)
	outbox.mu.Unlock()
	eventually(t, time.Second, func() bool { return pub.count() == 2 }, "relay did not poll")
}

func TestRelayKeepsRunningAfterErrors(t *testing.T) {
	outbox := &fakeOutbox{due: rows(2)}
	pub := &fakePublisher{err: errors.New("broker down")}
	r := &Relay{Store: outbox, Publisher: pub, Interval: 10 * time.Millisecond, Batch: 500, Logger: quiet}
	runInBackground(t, r.Run)

	time.Sleep(50 * time.Millisecond)
	if outbox.remaining() != 2 {
		t.Fatalf("rows claimed although publish failed")
	}
	pub.mu.Lock()
	pub.err = nil
	pub.mu.Unlock()
	eventually(t, time.Second, func() bool { return pub.count() == 2 }, "relay stopped after a publish error")
}

type fakeSweeper struct {
	calls     atomic.Int32
	olderThan atomic.Int64
}

func (f *fakeSweeper) ResetStuck(_ context.Context, d time.Duration) (int64, error) {
	f.calls.Add(1)
	f.olderThan.Store(int64(d))
	return 1, nil
}

func TestSweeperRunsImmediatelyAndOnInterval(t *testing.T) {
	st := &fakeSweeper{}
	s := &Sweeper{Store: st, Interval: 20 * time.Millisecond, StuckAfter: 2 * time.Minute, Logger: quiet}
	runInBackground(t, s.Run)

	eventually(t, time.Second, func() bool { return st.calls.Load() >= 1 }, "sweeper did not run on start")
	eventually(t, time.Second, func() bool { return st.calls.Load() >= 3 }, "sweeper did not run on interval")
	if got := time.Duration(st.olderThan.Load()); got != 2*time.Minute {
		t.Errorf("olderThan = %v, want 2m", got)
	}
}

// TestRelayPublishesDueRows runs the relay against real Postgres and
// RabbitMQ: a pending row is published to its channel queue and marked queued.
func TestRelayPublishesDueRows(t *testing.T) {
	pool := testutil.Postgres(t)
	conn := testutil.RabbitMQ(t)
	ctx := context.Background()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	if err := queue.Setup(ch); err != nil {
		t.Fatal(err)
	}
	purge := func() {
		for _, c := range domain.Channels {
			_, _ = ch.QueuePurge(queue.Name(c), false)
		}
	}
	purge()
	t.Cleanup(purge)

	st := store.New(pool)
	n := sms()
	n.NextAttemptAt = time.Now().Add(-time.Second)
	if _, _, err := st.Create(ctx, n); err != nil {
		t.Fatal(err)
	}
	pub, err := queue.NewPublisher(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	r := &Relay{Store: st, Publisher: pub, Interval: 50 * time.Millisecond, Batch: 500, Logger: quiet}
	runInBackground(t, r.Run)

	eventually(t, 5*time.Second, func() bool {
		got, err := st.Get(ctx, n.ID)
		return err == nil && got.Status == domain.StatusQueued
	}, "row was not marked queued")
	eventually(t, 5*time.Second, func() bool {
		d, err := queue.Depth(ch, queue.Name(domain.ChannelSMS))
		return err == nil && d == 1
	}, "message not in notifications.sms")
}
