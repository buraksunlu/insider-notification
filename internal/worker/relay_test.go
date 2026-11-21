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
	"github.com/buraksunlu/insider-notification/internal/provider"
	"github.com/buraksunlu/insider-notification/internal/queue"
	"github.com/buraksunlu/insider-notification/internal/store"
	"github.com/buraksunlu/insider-notification/internal/testutil"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fakeOutbox hands out due rows in chunks of at most limit and tracks how
// many published rows per channel are still queued (not yet consumed).
type fakeOutbox struct {
	mu      sync.Mutex
	due     []domain.Notification
	queued  map[domain.Channel]int
	failErr error
	calls   int
}

func (f *fakeOutbox) QueuedByChannel(context.Context) (map[domain.Channel]int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[domain.Channel]int{}
	for c, n := range f.queued {
		out[c] = n
	}
	return out, nil
}

func (f *fakeOutbox) ClaimDue(_ context.Context, c domain.Channel, limit int, publish func(domain.Notification) error) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failErr != nil {
		return 0, f.failErr
	}
	if f.queued == nil {
		f.queued = map[domain.Channel]int{}
	}
	claimed := 0
	for i := 0; i < len(f.due) && claimed < limit; {
		n := f.due[i]
		if n.Channel != c {
			i++
			continue
		}
		if err := publish(n); err != nil {
			return claimed, err // unpublished rows stay due, like the real release
		}
		f.due = append(f.due[:i], f.due[i+1:]...)
		claimed++
		f.queued[c]++
	}
	return claimed, nil
}

// consume simulates consumers taking n queued messages of channel c.
func (f *fakeOutbox) consume(c domain.Channel, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queued[c] -= n
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

// The relay keeps at most MaxQueued messages per channel in RabbitMQ, so
// during a backlog no message waits long enough for the sweeper to take it
// for lost and publish it again.
func TestRelayCapsQueuedPerChannel(t *testing.T) {
	outbox := &fakeOutbox{due: append(rows(50), pushRows(5)...)}
	pub := &fakePublisher{}
	r := &Relay{Store: outbox, Publisher: pub, Interval: 20 * time.Millisecond, Batch: 8, MaxQueued: 20, Logger: quiet}
	runInBackground(t, r.Run)

	eventually(t, time.Second, func() bool { return pub.count() == 25 }, "want 20 sms (the cap) and all 5 push published")
	time.Sleep(60 * time.Millisecond) // several ticks: still capped
	if got := pub.count(); got != 25 {
		t.Fatalf("published %d with sms at its cap, want 25", got)
	}

	outbox.consume(domain.ChannelSMS, 15)
	eventually(t, time.Second, func() bool { return pub.count() == 40 }, "relay did not refill sms after consumers made room")
}

func pushRows(n int) []domain.Notification {
	out := make([]domain.Notification, n)
	for i := range out {
		out[i] = sms()
		out[i].Channel = domain.ChannelPush
	}
	return out
}

func TestRelayDefaultsZeroBatchAndInterval(t *testing.T) {
	outbox := &fakeOutbox{due: rows(3)}
	pub := &fakePublisher{}
	r := &Relay{Store: outbox, Publisher: pub, Logger: quiet} // no Batch, no Interval
	runInBackground(t, r.Run)

	eventually(t, time.Second, func() bool { return pub.count() == 3 }, "relay with zero config did not publish")
	time.Sleep(50 * time.Millisecond)
	outbox.mu.Lock()
	calls := outbox.calls
	outbox.mu.Unlock()
	if calls > 2*len(domain.Channels) { // one claim per channel per drain
		t.Fatalf("ClaimDue called %d times in 50ms: relay is busy-looping", calls)
	}
}

type fakeSweeper struct {
	calls       atomic.Int32
	olderThan   atomic.Int64
	queuedCalls atomic.Int32
	queuedAfter atomic.Int64
}

func (f *fakeSweeper) ResetStuck(_ context.Context, d time.Duration) (int64, error) {
	f.calls.Add(1)
	f.olderThan.Store(int64(d))
	return 1, nil
}

func (f *fakeSweeper) ResetStaleQueued(_ context.Context, d time.Duration) (int64, error) {
	f.queuedCalls.Add(1)
	f.queuedAfter.Store(int64(d))
	return 1, nil
}

func TestSweeperRunsImmediatelyAndOnInterval(t *testing.T) {
	st := &fakeSweeper{}
	s := &Sweeper{Store: st, Interval: 20 * time.Millisecond, StuckAfter: 2 * time.Minute, QueuedAfter: 10 * time.Minute, Logger: quiet}
	runInBackground(t, s.Run)

	eventually(t, time.Second, func() bool { return st.calls.Load() >= 1 }, "sweeper did not run on start")
	eventually(t, time.Second, func() bool { return st.calls.Load() >= 3 }, "sweeper did not run on interval")
	if got := time.Duration(st.olderThan.Load()); got != 2*time.Minute {
		t.Errorf("olderThan = %v, want 2m", got)
	}
	eventually(t, time.Second, func() bool { return st.queuedCalls.Load() >= 3 }, "sweeper did not reset stale queued rows")
	if got := time.Duration(st.queuedAfter.Load()); got != 10*time.Minute {
		t.Errorf("queuedAfter = %v, want 10m", got)
	}
}

func TestSweeperDefaults(t *testing.T) {
	st := &fakeSweeper{}
	s := &Sweeper{Store: st, Logger: quiet} // no Interval: must not panic
	runInBackground(t, s.Run)

	eventually(t, time.Second, func() bool { return st.queuedCalls.Load() >= 1 }, "sweeper did not run")
	if got := time.Duration(st.olderThan.Load()); got != DefaultStuckAfter {
		t.Errorf("olderThan = %v, want %v", got, DefaultStuckAfter)
	}
	if got := time.Duration(st.queuedAfter.Load()); got != DefaultQueuedAfter {
		t.Errorf("queuedAfter = %v, want %v", got, DefaultQueuedAfter)
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

// countingSender accepts every notification and counts sends per id.
type countingSender struct {
	mu    sync.Mutex
	sends map[string]int
}

func (s *countingSender) Send(_ context.Context, key, _ string, _ provider.Request) (provider.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sends[key]++
	id := "pm-" + key
	return provider.Result{StatusCode: 202, MessageID: &id}, nil
}

// TestRelayAndConsumerDeliverEveryNotification runs relay, consumer and
// handler against real Postgres and RabbitMQ. It is the end-to-end
// regression test for the race where a consumer received a message before
// the relay committed its row as queued, skipped it, and left the row
// stuck in queued: every row must reach sent, each sent exactly once.
func TestRelayAndConsumerDeliverEveryNotification(t *testing.T) {
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
	const total = 50
	for range total {
		n := sms()
		n.NextAttemptAt = time.Now().Add(-time.Second)
		if _, _, err := st.Create(ctx, n); err != nil {
			t.Fatal(err)
		}
	}

	sender := &countingSender{sends: map[string]int{}}
	h := &Handler{
		Store: st, Sender: sender, DLQ: &fakeDLQ{}, Limiters: NewLimiters(1000),
		Logger: quiet, Now: time.Now, Rand: func() float64 { return 0 }, RetryBase: time.Second,
	}
	runInBackground(t, func(ctx context.Context) error {
		return queue.Consume(ctx, conn, domain.ChannelSMS, 20, h.Handle)
	})

	pub, err := queue.NewPublisher(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pub.Close() })
	// Small batches: many claim/publish/consume interleavings.
	r := &Relay{Store: st, Publisher: pub, Interval: 20 * time.Millisecond, Batch: 5, Logger: quiet}
	runInBackground(t, r.Run)

	eventually(t, 15*time.Second, func() bool {
		counts, err := st.CountByStatus(ctx)
		return err == nil && counts[domain.StatusSent] == total
	}, "not every notification reached sent (rows stuck in queued?)")

	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.sends) != total {
		t.Fatalf("provider saw %d distinct notifications, want %d", len(sender.sends), total)
	}
	for id, c := range sender.sends {
		if c != 1 {
			t.Errorf("notification %s sent %d times, want 1", id, c)
		}
	}
}
