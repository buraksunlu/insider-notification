package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// ErrPoison marks a message that can never be processed. Handlers wrap it
// (errors.Is) to send the message to the DLQ instead of requeueing it.
var ErrPoison = errors.New("poison message")

// ErrDeliveriesClosed means the broker or connection stopped the consumer;
// the caller should reconnect.
var ErrDeliveriesClosed = errors.New("delivery channel closed")

// requeueDelay slows a nack-requeue loop down while a dependency (usually
// the database) is unavailable. Delivery retries themselves live in the DB.
const requeueDelay = time.Second

// Consume delivers messages of channel c to handle until ctx is cancelled.
// Up to prefetch messages are handled concurrently. Outcome per message:
// nil → ack; ErrPoison or an undecodable body → nack without requeue (DLQ);
// any other error → nack with requeue after a short delay.
//
// On cancellation Consume stops receiving, lets in-flight handlers finish
// (their context is not cancelled) and returns nil. It returns
// ErrDeliveriesClosed if the broker closes the stream first.
func Consume(ctx context.Context, conn *amqp.Connection, c domain.Channel, prefetch int, handle func(context.Context, Message) error) error {
	if prefetch < 1 {
		prefetch = 1
	}
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()
	if err := ch.Qos(prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	tag := "consumer-" + string(c) + "-" + uuid.NewString()
	deliveries, err := ch.Consume(Name(c), tag, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume %s: %w", Name(c), err)
	}

	handlerCtx := context.WithoutCancel(ctx)
	sem := make(chan struct{}, prefetch)
	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			// Stop new deliveries; unacked prefetched ones are returned to the
			// queue by the broker when the channel closes.
			_ = ch.Cancel(tag, false)
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return ErrDeliveriesClosed
			}
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() { <-sem; wg.Done() }()
				settle(ctx, handlerCtx, d, handle)
			}()
		}
	}
}

func settle(loopCtx, handlerCtx context.Context, d amqp.Delivery, handle func(context.Context, Message) error) {
	var m Message
	if err := json.Unmarshal(d.Body, &m); err != nil || m.ID == uuid.Nil {
		_ = d.Nack(false, false)
		return
	}
	if m.CorrelationID == "" {
		m.CorrelationID = d.CorrelationId
	}

	err := handle(handlerCtx, m)
	switch {
	case err == nil:
		_ = d.Ack(false)
	case errors.Is(err, ErrPoison):
		_ = d.Nack(false, false)
	default:
		select {
		case <-time.After(requeueDelay):
		case <-loopCtx.Done():
		}
		_ = d.Nack(false, true)
	}
}
