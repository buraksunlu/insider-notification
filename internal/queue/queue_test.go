package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/queue"
	"github.com/buraksunlu/insider-notification/internal/testutil"
)

// setup returns a connection and a channel on a freshly declared topology.
func setup(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	conn := testutil.RabbitMQ(t)
	resetTopology(t, conn)
	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ch.Close() })
	if err := queue.Setup(ch); err != nil {
		t.Fatalf("setup: %v", err)
	}
	return conn, ch
}

// resetTopology deletes everything a previous test declared. Each delete uses
// its own channel because a failing AMQP call closes the channel.
func resetTopology(t *testing.T, conn *amqp.Connection) {
	t.Helper()
	del := func(f func(ch *amqp.Channel) error) {
		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}
		_ = f(ch)
		_ = ch.Close()
	}
	for _, c := range domain.Channels {
		name := queue.Name(c)
		del(func(ch *amqp.Channel) error { _, err := ch.QueueDelete(name, false, false, false); return err })
	}
	del(func(ch *amqp.Channel) error { _, err := ch.QueueDelete(queue.DLQ, false, false, false); return err })
	del(func(ch *amqp.Channel) error { return ch.ExchangeDelete(queue.Exchange, false, false) })
	del(func(ch *amqp.Channel) error { return ch.ExchangeDelete(queue.DLX, false, false) })
}

func notification(ch domain.Channel, prio domain.Priority) domain.Notification {
	return domain.Notification{
		ID:            uuid.New(),
		Channel:       ch,
		Recipient:     "+905551112233",
		Content:       "hello",
		Priority:      prio,
		Status:        domain.StatusQueued,
		Attempts:      3,
		MaxAttempts:   domain.DefaultMaxAttempts,
		CorrelationID: "corr-" + string(prio),
	}
}

func publisher(t *testing.T, conn *amqp.Connection) *queue.Publisher {
	t.Helper()
	p, err := queue.NewPublisher(conn)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// waitDepth polls until queue q holds want messages or the test times out.
func waitDepth(t *testing.T, ch *amqp.Channel, q string, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		got, err := queue.Depth(ch, q)
		if err != nil {
			t.Fatalf("depth %s: %v", q, err)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("depth %s = %d, want %d", q, got, want)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// consume runs queue.Consume in the background and returns a stop function
// that cancels it and returns its error.
func consume(t *testing.T, conn *amqp.Connection, c domain.Channel, prefetch int, h func(context.Context, queue.Message) error) func() error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- queue.Consume(ctx, conn, c, prefetch, h) }()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				err = errors.New("consume did not stop")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func TestSetupIsIdempotent(t *testing.T) {
	_, ch := setup(t)
	if err := queue.Setup(ch); err != nil {
		t.Fatalf("second setup: %v", err)
	}
}

func TestPriorityOrdering(t *testing.T) {
	conn, ch := setup(t)
	p := publisher(t, conn)
	low1 := notification(domain.ChannelSMS, domain.PriorityLow)
	low2 := notification(domain.ChannelSMS, domain.PriorityLow)
	high := notification(domain.ChannelSMS, domain.PriorityHigh)
	for _, n := range []domain.Notification{low1, low2, high} {
		if err := p.Publish(context.Background(), n); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitDepth(t, ch, queue.Name(domain.ChannelSMS), 3)

	got := make(chan queue.Message, 3)
	stop := consume(t, conn, domain.ChannelSMS, 1, func(_ context.Context, m queue.Message) error {
		got <- m
		return nil
	})
	var order []queue.Message
	for range 3 {
		select {
		case m := <-got:
			order = append(order, m)
		case <-time.After(10 * time.Second):
			t.Fatalf("received %d of 3 messages", len(order))
		}
	}
	if err := stop(); err != nil {
		t.Fatalf("consume: %v", err)
	}

	if order[0].ID != high.ID {
		t.Fatalf("first message = %s, want the high priority one %s", order[0].ID, high.ID)
	}
	if order[0].Channel != domain.ChannelSMS || order[0].CorrelationID != high.CorrelationID {
		t.Fatalf("message fields = %+v", order[0])
	}
	waitDepth(t, ch, queue.Name(domain.ChannelSMS), 0)
}

func TestMalformedMessageGoesToDLQ(t *testing.T) {
	conn, ch := setup(t)
	if err := ch.PublishWithContext(context.Background(), queue.Exchange, string(domain.ChannelEmail), false, false,
		amqp.Publishing{ContentType: "application/json", Body: []byte("not json")}); err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 1)
	consume(t, conn, domain.ChannelEmail, 10, func(context.Context, queue.Message) error {
		called <- struct{}{}
		return nil
	})
	waitDepth(t, ch, queue.DLQ, 1)
	select {
	case <-called:
		t.Fatal("handler called for a malformed message")
	default:
	}
}

func TestHandlerPoisonGoesToDLQ(t *testing.T) {
	conn, ch := setup(t)
	if err := publisher(t, conn).Publish(context.Background(), notification(domain.ChannelPush, domain.PriorityNormal)); err != nil {
		t.Fatal(err)
	}
	consume(t, conn, domain.ChannelPush, 10, func(context.Context, queue.Message) error {
		return errors.Join(errors.New("unknown notification"), queue.ErrPoison)
	})
	waitDepth(t, ch, queue.DLQ, 1)
	waitDepth(t, ch, queue.Name(domain.ChannelPush), 0)
}

func TestTransientErrorRequeues(t *testing.T) {
	conn, ch := setup(t)
	n := notification(domain.ChannelSMS, domain.PriorityNormal)
	if err := publisher(t, conn).Publish(context.Background(), n); err != nil {
		t.Fatal(err)
	}

	calls := make(chan uuid.UUID, 2)
	var mu sync.Mutex
	failed := false
	consume(t, conn, domain.ChannelSMS, 10, func(_ context.Context, m queue.Message) error {
		calls <- m.ID
		mu.Lock()
		defer mu.Unlock()
		if !failed {
			failed = true
			return errors.New("database unavailable")
		}
		return nil
	})
	for i := range 2 {
		select {
		case id := <-calls:
			if id != n.ID {
				t.Fatalf("call %d got id %s, want %s", i, id, n.ID)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("handler called %d times, want 2", i)
		}
	}
	waitDepth(t, ch, queue.Name(domain.ChannelSMS), 0)
	waitDepth(t, ch, queue.DLQ, 0)
}

func TestConsumeFinishesInFlightOnShutdown(t *testing.T) {
	conn, ch := setup(t)
	if err := publisher(t, conn).Publish(context.Background(), notification(domain.ChannelSMS, domain.PriorityNormal)); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	finished := make(chan struct{})
	stop := consume(t, conn, domain.ChannelSMS, 10, func(ctx context.Context, _ queue.Message) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		close(finished)
		return nil
	})
	<-started
	if err := stop(); err != nil {
		t.Fatalf("consume returned %v on shutdown, want nil", err)
	}
	select {
	case <-finished:
	default:
		t.Fatal("in-flight handler did not finish before Consume returned")
	}
	waitDepth(t, ch, queue.Name(domain.ChannelSMS), 0)
}

func TestPublishDLQ(t *testing.T) {
	conn, ch := setup(t)
	n := notification(domain.ChannelEmail, domain.PriorityHigh)
	if err := publisher(t, conn).PublishDLQ(context.Background(), n, "provider returned 400"); err != nil {
		t.Fatalf("publish dlq: %v", err)
	}
	waitDepth(t, ch, queue.DLQ, 1)

	d, ok, err := ch.Get(queue.DLQ, true)
	if err != nil || !ok {
		t.Fatalf("get dlq: ok=%v err=%v", ok, err)
	}
	if got := d.Headers["x-failure-reason"]; got != "provider returned 400" {
		t.Fatalf("x-failure-reason = %v", got)
	}
	if got, _ := d.Headers["x-attempts"].(int32); got != 3 {
		t.Fatalf("x-attempts = %v (%T), want 3", d.Headers["x-attempts"], d.Headers["x-attempts"])
	}
	if d.CorrelationId != n.CorrelationID {
		t.Fatalf("correlation id = %q, want %q", d.CorrelationId, n.CorrelationID)
	}
	var body struct {
		ID      uuid.UUID `json:"id"`
		Channel string    `json:"channel"`
		Content string    `json:"content"`
	}
	if err := json.Unmarshal(d.Body, &body); err != nil {
		t.Fatalf("dlq body is not json: %v", err)
	}
	if body.ID != n.ID || body.Channel != "email" || body.Content != n.Content {
		t.Fatalf("dlq body = %+v", body)
	}
}

func TestDepthReaderReportsAllQueues(t *testing.T) {
	conn, ch := setup(t)
	p := publisher(t, conn)
	for range 2 {
		if err := p.Publish(context.Background(), notification(domain.ChannelSMS, domain.PriorityNormal)); err != nil {
			t.Fatal(err)
		}
	}
	waitDepth(t, ch, queue.Name(domain.ChannelSMS), 2)

	r := queue.NewDepthReader(conn)
	t.Cleanup(r.Close)
	got, err := r.Depths(context.Background())
	if err != nil {
		t.Fatalf("Depths: %v", err)
	}
	want := map[string]int{
		"notifications.sms": 2, "notifications.email": 0, "notifications.push": 0, "notifications.dlq": 0,
	}
	if len(got) != len(want) {
		t.Fatalf("Depths = %v, want %v", got, want)
	}
	for q, n := range want {
		if got[q] != n {
			t.Errorf("depth %s = %d, want %d", q, got[q], n)
		}
	}
}

func TestDepthReaderRecoversClosedChannel(t *testing.T) {
	conn, ch := setup(t)
	r := queue.NewDepthReader(conn)
	t.Cleanup(r.Close)
	if _, err := r.Depths(context.Background()); err != nil {
		t.Fatalf("Depths: %v", err)
	}

	// A passive declare of a missing queue makes the broker close the channel.
	if _, err := ch.QueueDelete(queue.Name(domain.ChannelPush), false, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Depths(context.Background()); err == nil {
		t.Fatal("Depths with a missing queue: want error")
	}

	if err := queue.Setup(ch); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Depths(context.Background()); err != nil {
		t.Fatalf("Depths after the queue is back: %v (channel not reopened)", err)
	}
}

func TestPublisherRecoversClosedChannel(t *testing.T) {
	conn, ch := setup(t)
	p := publisher(t, conn)
	n := notification(domain.ChannelSMS, domain.PriorityNormal)

	// Publishing to a missing exchange makes the broker close the channel.
	if err := ch.ExchangeDelete(queue.DLX, false, false); err != nil {
		t.Fatal(err)
	}
	if err := p.PublishDLQ(context.Background(), n, "x"); err == nil {
		t.Fatal("PublishDLQ to a missing exchange: want error")
	}

	if err := queue.Setup(ch); err != nil {
		t.Fatal(err)
	}
	if err := p.Publish(context.Background(), n); err != nil {
		t.Fatalf("Publish after the channel closed: %v (channel not reopened)", err)
	}
	waitDepth(t, ch, queue.Name(domain.ChannelSMS), 1)
}
