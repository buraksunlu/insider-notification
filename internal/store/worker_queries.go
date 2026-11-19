package store

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// ClaimDue locks up to limit due pending notifications (highest priority
// first), hands them to publish and marks them queued. If publish fails the
// transaction rolls back and the rows stay pending for the next tick.
// SKIP LOCKED lets several relays run without claiming the same rows.
func (s *Store) ClaimDue(ctx context.Context, limit int, publish func([]domain.Notification) error) (int, error) {
	var claimed int
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT `+columns+` FROM notifications
			WHERE status = 'pending' AND next_attempt_at <= now()
			ORDER BY priority_rank DESC, next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED`, limit)
		if err != nil {
			return err
		}
		due, err := scanAll(rows)
		if err != nil || len(due) == 0 {
			return err
		}
		if err := publish(due); err != nil {
			return err
		}
		ids := make([]uuid.UUID, len(due))
		for i, n := range due {
			ids[i] = n.ID
		}
		if _, err := tx.Exec(ctx, `UPDATE notifications SET status = 'queued', updated_at = now()
			WHERE id = ANY($1)`, ids); err != nil {
			return err
		}
		claimed = len(due)
		return nil
	})
	return claimed, err
}

// StartProcessing moves a queued notification to processing and counts the
// attempt. ok=false means the message must be skipped: it was cancelled,
// already handled, or is a duplicate delivery.
func (s *Store) StartProcessing(ctx context.Context, id uuid.UUID) (domain.Notification, bool, error) {
	n, err := scan(s.pool.QueryRow(ctx, `UPDATE notifications
		SET status = 'processing', attempts = attempts + 1, updated_at = now()
		WHERE id = $1 AND status = 'queued'
		RETURNING `+columns, id))
	if errors.Is(err, ErrNotFound) {
		return domain.Notification{}, false, nil
	}
	return n, err == nil, err
}

// MarkSent records a successful delivery. A success is kept even when the
// sweeper has meanwhile reset the row, so a delivered message is not resent.
func (s *Store) MarkSent(ctx context.Context, id uuid.UUID, providerMsgID *string, at time.Time) error {
	return s.transition(ctx, `UPDATE notifications
		SET status = 'sent', provider_message_id = $2, sent_at = $3, last_error = NULL, updated_at = now()
		WHERE id = $1 AND status IN ('processing', 'pending', 'queued')`, id, providerMsgID, at)
}

// ScheduleRetry puts a processing notification back to pending, due at next.
func (s *Store) ScheduleRetry(ctx context.Context, id uuid.UUID, next time.Time, lastErr string) error {
	return s.transition(ctx, `UPDATE notifications
		SET status = 'pending', next_attempt_at = $2, last_error = $3, updated_at = now()
		WHERE id = $1 AND status = 'processing'`, id, next, lastErr)
}

// MarkFailed marks a processing notification as permanently failed.
func (s *Store) MarkFailed(ctx context.Context, id uuid.UUID, lastErr string) error {
	return s.transition(ctx, `UPDATE notifications
		SET status = 'failed', last_error = $2, updated_at = now()
		WHERE id = $1 AND status = 'processing'`, id, lastErr)
}

func (s *Store) transition(ctx context.Context, sql string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrStateConflict
	}
	return nil
}

// RecordAttempt appends a delivery attempt to the audit table.
func (s *Store) RecordAttempt(ctx context.Context, a domain.Attempt) error {
	var status *int
	if a.HTTPStatus != 0 {
		status = &a.HTTPStatus
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO delivery_attempts (notification_id, attempt, http_status, error, latency_ms)
		VALUES ($1, $2, $3, $4, $5)`, a.NotificationID, a.Attempt, status, a.Error, a.LatencyMS)
	return err
}

// ResetStuck returns notifications stuck in processing for longer than
// olderThan (e.g. their worker crashed) to pending so they are retried.
func (s *Store) ResetStuck(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE notifications
		SET status = 'pending', next_attempt_at = now(), updated_at = now()
		WHERE status = 'processing' AND updated_at < now() - make_interval(secs => $1)`, olderThan.Seconds())
	return tag.RowsAffected(), err
}

// CountByStatus returns how many notifications are in each status.
func (s *Store) CountByStatus(ctx context.Context) (map[domain.Status]int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM notifications GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[domain.Status]int64{}
	for rows.Next() {
		var st domain.Status
		var c int64
		if err := rows.Scan(&st, &c); err != nil {
			return nil, err
		}
		out[st] = c
	}
	return out, rows.Err()
}
