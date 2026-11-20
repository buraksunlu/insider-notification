// Package queue is the RabbitMQ transport between the outbox relay and the
// delivery consumers: topology, a confirming publisher and a consumer loop.
package queue

import (
	"fmt"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// Topology names. Channel queues are bound to Exchange with the channel name
// as routing key; DLX fans everything out to DLQ.
const (
	Exchange = "notifications"
	DLX      = "notifications.dlx"
	DLQ      = "notifications.dlq"

	// maxPriority matches domain.Priority.AMQP (high=3, normal=2, low=1).
	maxPriority = 3
)

// Message is the body published for a notification. It carries only the id:
// the consumer reloads the row, so the database stays the source of truth.
type Message struct {
	ID            uuid.UUID      `json:"id"`
	Channel       domain.Channel `json:"channel"`
	CorrelationID string         `json:"correlation_id,omitempty"`
}

// Name is the queue that holds notifications of channel c.
func Name(c domain.Channel) string { return "notifications." + string(c) }

// Setup declares the exchanges and queues. It is idempotent, so every
// process may call it on start.
func Setup(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(Exchange, amqp.ExchangeDirect, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", Exchange, err)
	}
	if err := ch.ExchangeDeclare(DLX, amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange %s: %w", DLX, err)
	}
	if _, err := ch.QueueDeclare(DLQ, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare queue %s: %w", DLQ, err)
	}
	if err := ch.QueueBind(DLQ, "", DLX, false, nil); err != nil {
		return fmt.Errorf("bind %s: %w", DLQ, err)
	}

	args := amqp.Table{
		"x-max-priority":         int32(maxPriority),
		"x-dead-letter-exchange": DLX,
	}
	for _, c := range domain.Channels {
		name := Name(c)
		if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", name, err)
		}
		if err := ch.QueueBind(name, string(c), Exchange, false, nil); err != nil {
			return fmt.Errorf("bind %s: %w", name, err)
		}
	}
	return nil
}

// Depth returns how many ready messages queue q holds.
func Depth(ch *amqp.Channel, q string) (int, error) {
	info, err := ch.QueueDeclarePassive(q, true, false, false, false, nil)
	if err != nil {
		return 0, err
	}
	return info.Messages, nil
}
