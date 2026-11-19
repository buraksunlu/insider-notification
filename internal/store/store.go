// Package store is the PostgreSQL repository. The notifications table is
// both the source of truth and the outbox the worker relay publishes from.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// Errors returned by the store. Callers match them with errors.Is.
var (
	ErrNotFound            = errors.New("not found")
	ErrNotCancellable      = errors.New("notification can no longer be cancelled")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	ErrDuplicateKeyInBatch = errors.New("idempotency key repeated within the batch")
	ErrBadCursor           = errors.New("invalid pagination cursor")
	ErrStateConflict       = errors.New("notification is not in the expected state")
)

// ItemError ties a batch error to the index of the offending item.
type ItemError struct {
	Index int
	Err   error
}

func (e *ItemError) Error() string { return fmt.Sprintf("item %d: %v", e.Index, e.Err) }
func (e *ItemError) Unwrap() error { return e.Err }

// Store implements persistence on top of a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store backed by pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Ping checks database connectivity (used by the readiness probe).
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const columns = `id, batch_id, idempotency_key, channel, recipient, content, priority, status,
	attempts, max_attempts, next_attempt_at, scheduled_at, provider_message_id, last_error,
	correlation_id, created_at, updated_at, sent_at`

func scan(row pgx.Row) (domain.Notification, error) {
	var n domain.Notification
	err := row.Scan(&n.ID, &n.BatchID, &n.IdempotencyKey, &n.Channel, &n.Recipient, &n.Content,
		&n.Priority, &n.Status, &n.Attempts, &n.MaxAttempts, &n.NextAttemptAt, &n.ScheduledAt,
		&n.ProviderMessageID, &n.LastError, &n.CorrelationID, &n.CreatedAt, &n.UpdatedAt, &n.SentAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return n, ErrNotFound
	}
	return n, err
}

func scanAll(rows pgx.Rows) ([]domain.Notification, error) {
	defer rows.Close()
	var out []domain.Notification
	for rows.Next() {
		n, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
