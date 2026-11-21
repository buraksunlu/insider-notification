package store

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

// ClaimDue marks up to limit due pending notifications (highest priority
// first) as queued, commits, and only then hands each to publish. The
// commit must come first: a consumer can receive a message the moment it is
// published, and it only claims rows that are already queued.
//
// If publish fails, the rows not yet published go back to pending and the
// error is returned with the count published so far. A row whose release
// also fails stays queued until the sweeper's stale-queued reset recovers it.
// SKIP LOCKED lets several relays run without claiming the same rows.
func (s *Store) ClaimDue(ctx context.Context, limit int, publish func(domain.Notification) error) (int, error) {
	var due []domain.Notification
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `UPDATE notifications SET status = 'queued', updated_at = now()
			WHERE id IN (
				SELECT id FROM notifications
				WHERE status = 'pending' AND next_attempt_at <= now()
				ORDER BY priority_rank DESC, next_attempt_at
				LIMIT $1
				FOR UPDATE SKIP LOCKED)
			RETURNING `+columns, limit)
		if err != nil {
			return err
		}
		due, err = scanAll(rows)
		return err
	})
	if err != nil {
		return 0, err
	}
	// UPDATE ... RETURNING does not keep the subquery's order.
	sort.SliceStable(due, func(i, j int) bool {
		if due[i].Priority.AMQP() != due[j].Priority.AMQP() {
			return due[i].Priority.AMQP() > due[j].Priority.AMQP()
		}
		return due[i].NextAttemptAt.Before(due[j].NextAttemptAt)
	})

	for i, n := range due {
		if err := publish(n); err != nil {
			s.release(ctx, due[i:])
			return i, err
		}
	}
	return len(due), nil
}

// release returns claimed but unpublished rows to pending. It is best
// effort: on failure the stale-queued sweep picks the rows up later.
func (s *Store) release(ctx context.Context, ns []domain.Notification) {
	ids := make([]uuid.UUID, len(ns))
	for i, n := range ns {
		ids[i] = n.ID
	}
	_, _ = s.pool.Exec(context.WithoutCancel(ctx), `UPDATE notifications SET status = 'pending', updated_at = now()
		WHERE id = ANY($1) AND status = 'queued'`, ids)
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
// sweeper has meanwhile reset the row (so a delivered message is not resent)
// and even when the reset row was then cancelled: the provider accepted the
// message, so sent is the truthful status.
func (s *Store) MarkSent(ctx context.Context, id uuid.UUID, providerMsgID *string, at time.Time) error {
	return s.transition(ctx, `UPDATE notifications
		SET status = 'sent', provider_message_id = $2, sent_at = $3, last_error = NULL, updated_at = now()
		WHERE id = $1 AND status IN ('processing', 'pending', 'queued', 'cancelled')`, id, providerMsgID, at)
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

// ResetStaleQueued returns notifications queued for longer than olderThan
// to pending so the relay publishes them again. It recovers rows whose
// message never reached a queue (dropped, purged, or a release that failed
// after a publish error). If the old message does turn up later, it is
// skipped: StartProcessing only claims a row once.
func (s *Store) ResetStaleQueued(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE notifications
		SET status = 'pending', updated_at = now()
		WHERE status = 'queued' AND updated_at < now() - make_interval(secs => $1)`, olderThan.Seconds())
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
