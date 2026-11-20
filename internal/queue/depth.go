package queue

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// DepthReader reports the ready-message count of every channel queue and
// the DLQ, for the queue_depth metric. It owns one AMQP channel and reopens
// it when the broker has closed it (any failed declare closes a channel).
type DepthReader struct {
	conn *amqp.Connection
	mu   sync.Mutex
	ch   *amqp.Channel
}

// NewDepthReader returns a reader on conn; the channel is opened lazily.
func NewDepthReader(conn *amqp.Connection) *DepthReader { return &DepthReader{conn: conn} }

// Depths returns queue name → ready messages. ctx is accepted to fit the
// metrics collector; passive declares are not cancellable.
func (r *DepthReader) Depths(context.Context) (map[string]int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ch == nil || r.ch.IsClosed() {
		ch, err := r.conn.Channel()
		if err != nil {
			return nil, fmt.Errorf("open channel: %w", err)
		}
		r.ch = ch
	}

	names := make([]string, 0, len(domain.Channels)+1)
	for _, c := range domain.Channels {
		names = append(names, Name(c))
	}
	names = append(names, DLQ)

	out := make(map[string]int, len(names))
	for _, q := range names {
		n, err := Depth(r.ch, q)
		if err != nil {
			return nil, fmt.Errorf("depth %s: %w", q, err)
		}
		out[q] = n
	}
	return out, nil
}

// Close closes the reader's channel.
func (r *DepthReader) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ch != nil {
		_ = r.ch.Close()
	}
}
