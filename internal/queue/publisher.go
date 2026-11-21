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

// ErrNacked means the broker refused to take responsibility for a message.
var ErrNacked = errors.New("message nacked by broker")

// confirmTimeout bounds a publish when the caller's context has no deadline.
const confirmTimeout = 5 * time.Second

// Publisher publishes persistent messages and waits for the broker's
// confirmation, so a nil error means the message is stored by RabbitMQ.
// It is safe for concurrent use.
//
// The broker closes a channel on some errors (e.g. publishing to a missing
// exchange); the publisher then opens a new one on the next publish. A
// closed connection is not recovered here: the caller reconnects.
type Publisher struct {
	conn *amqp.Connection
	mu   sync.Mutex
	ch   *amqp.Channel
}

// NewPublisher opens a channel in confirm mode on conn.
func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	p := &Publisher{conn: conn}
	if err := p.open(); err != nil {
		return nil, err
	}
	return p, nil
}

// open replaces the channel with a new one in confirm mode. Callers hold
// p.mu, except NewPublisher, which owns p exclusively.
func (p *Publisher) open() error {
	ch, err := p.conn.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("confirm mode: %w", err)
	}
	p.ch = ch
	return nil
}

// Publish sends n to its channel queue with n's priority.
func (p *Publisher) Publish(ctx context.Context, n domain.Notification) error {
	body, err := json.Marshal(Message{ID: n.ID, Channel: n.Channel, CorrelationID: n.CorrelationID})
	if err != nil {
		return err
	}
	return p.publish(ctx, Exchange, string(n.Channel), amqp.Publishing{
		Priority:      n.Priority.AMQP(),
		CorrelationId: n.CorrelationID,
		MessageId:     n.ID.String(),
		Body:          body,
	})
}

// dlqRecord is the DLQ body: the whole notification, readable in the
// RabbitMQ management UI without a database lookup.
type dlqRecord struct {
	ID             uuid.UUID       `json:"id"`
	BatchID        *uuid.UUID      `json:"batch_id,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	Channel        domain.Channel  `json:"channel"`
	Recipient      string          `json:"recipient"`
	Content        string          `json:"content"`
	Priority       domain.Priority `json:"priority"`
	Status         domain.Status   `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	LastError      *string         `json:"last_error,omitempty"`
	CorrelationID  string          `json:"correlation_id,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// PublishDLQ records a permanently failed notification in the dead-letter
// queue, with the reason and attempt count as headers.
func (p *Publisher) PublishDLQ(ctx context.Context, n domain.Notification, reason string) error {
	body, err := json.Marshal(dlqRecord{
		ID: n.ID, BatchID: n.BatchID, IdempotencyKey: n.IdempotencyKey, Channel: n.Channel,
		Recipient: n.Recipient, Content: n.Content, Priority: n.Priority, Status: n.Status,
		Attempts: n.Attempts, MaxAttempts: n.MaxAttempts, LastError: n.LastError,
		CorrelationID: n.CorrelationID, CreatedAt: n.CreatedAt,
	})
	if err != nil {
		return err
	}
	return p.publish(ctx, DLX, string(n.Channel), amqp.Publishing{
		Headers: amqp.Table{
			"x-failure-reason": reason,
			"x-attempts":       int32(n.Attempts), //nolint:gosec // attempts is bounded by max_attempts
		},
		CorrelationId: n.CorrelationID,
		MessageId:     n.ID.String(),
		Body:          body,
	})
}

func (p *Publisher) publish(ctx context.Context, exchange, key string, msg amqp.Publishing) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, confirmTimeout)
		defer cancel()
	}
	msg.ContentType = "application/json"
	msg.DeliveryMode = amqp.Persistent
	msg.Timestamp = time.Now().UTC()

	// One publish at a time keeps the confirm bookkeeping trivial; the relay
	// publishes sequentially anyway.
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch.IsClosed() {
		if err := p.open(); err != nil {
			return err
		}
	}
	dc, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		return fmt.Errorf("publish to %s: %w", exchange, err)
	}
	ok, err := dc.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for confirm: %w", err)
	}
	if !ok {
		return ErrNacked
	}
	return nil
}

// Close closes the publisher's channel.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.ch.Close()
}
