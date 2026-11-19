package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/buraksunlu/insider-notification/internal/domain"
)

const insertSQL = `INSERT INTO notifications (id, batch_id, idempotency_key, channel, recipient, content,
	priority, status, attempts, max_attempts, next_attempt_at, scheduled_at, correlation_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING ` + columns

func insertArgs(n domain.Notification) []any {
	return []any{n.ID, n.BatchID, n.IdempotencyKey, n.Channel, n.Recipient, n.Content, n.Priority,
		n.Status, n.Attempts, n.MaxAttempts, n.NextAttemptAt, n.ScheduledAt, n.CorrelationID,
		n.CreatedAt, n.UpdatedAt}
}

// Create stores a notification. When its idempotency key already exists the
// existing row is returned with created=false; if that row carries a
// different payload, ErrIdempotencyConflict is returned alongside it.
func (s *Store) Create(ctx context.Context, n domain.Notification) (domain.Notification, bool, error) {
	got, err := scan(s.pool.QueryRow(ctx, insertSQL, insertArgs(n)...))
	if err == nil {
		return got, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return domain.Notification{}, false, err
	}
	existing, err := s.getByKey(ctx, s.pool, *n.IdempotencyKey)
	if err != nil {
		return domain.Notification{}, false, err
	}
	if !samePayload(existing, n) {
		return existing, false, ErrIdempotencyConflict
	}
	return existing, false, nil
}

// CreateBatch stores all notifications in one transaction under a new batch.
// Items whose idempotency key already exists reuse the existing row (and stay
// linked to their original batch); the batch total counts new rows only.
// The returned ids are in input order.
func (s *Store) CreateBatch(ctx context.Context, ns []domain.Notification) (uuid.UUID, []uuid.UUID, error) {
	seen := make(map[string]bool, len(ns))
	for i, n := range ns {
		if n.IdempotencyKey == nil {
			continue
		}
		if seen[*n.IdempotencyKey] {
			return uuid.Nil, nil, &ItemError{Index: i, Err: ErrDuplicateKeyInBatch}
		}
		seen[*n.IdempotencyKey] = true
	}

	batchID := uuid.New()
	ids := make([]uuid.UUID, len(ns))
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO batches (id, total) VALUES ($1, 0)`, batchID); err != nil {
			return err
		}

		b := &pgx.Batch{}
		for _, n := range ns {
			n.BatchID = &batchID
			b.Queue(insertSQL, insertArgs(n)...)
		}
		results := tx.SendBatch(ctx, b)
		var reused []int
		for i := range ns {
			got, err := scan(results.QueryRow())
			switch {
			case err == nil:
				ids[i] = got.ID
			case errors.Is(err, ErrNotFound): // idempotency key already taken
				reused = append(reused, i)
			default:
				_ = results.Close() // the item error below is the one worth reporting
				return &ItemError{Index: i, Err: err}
			}
		}
		if err := results.Close(); err != nil {
			return err
		}

		for _, i := range reused {
			existing, err := s.getByKey(ctx, tx, *ns[i].IdempotencyKey)
			if err != nil {
				return &ItemError{Index: i, Err: err}
			}
			if !samePayload(existing, ns[i]) {
				return &ItemError{Index: i, Err: ErrIdempotencyConflict}
			}
			ids[i] = existing.ID
		}

		_, err := tx.Exec(ctx, `UPDATE batches SET total = $2 WHERE id = $1`, batchID, len(ns)-len(reused))
		return err
	})
	if err != nil {
		return uuid.Nil, nil, err
	}
	return batchID, ids, nil
}

// Get returns one notification by id.
func (s *Store) Get(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	return scan(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM notifications WHERE id = $1`, id))
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) getByKey(ctx context.Context, q querier, key string) (domain.Notification, error) {
	return scan(q.QueryRow(ctx, `SELECT `+columns+` FROM notifications WHERE idempotency_key = $1`, key))
}

// samePayload reports whether a replayed request matches the stored one.
func samePayload(a, b domain.Notification) bool {
	sameSchedule := (a.ScheduledAt == nil) == (b.ScheduledAt == nil) &&
		(a.ScheduledAt == nil || a.ScheduledAt.Truncate(time.Microsecond).Equal(b.ScheduledAt.Truncate(time.Microsecond)))
	return a.Channel == b.Channel && a.Recipient == b.Recipient && a.Content == b.Content &&
		a.Priority == b.Priority && sameSchedule
}

// Cancel moves a pending or queued notification to cancelled.
func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error) {
	got, err := scan(s.pool.QueryRow(ctx, `UPDATE notifications
		SET status = 'cancelled', updated_at = now()
		WHERE id = $1 AND status IN ('pending', 'queued')
		RETURNING `+columns, id))
	if !errors.Is(err, ErrNotFound) {
		return got, err
	}
	if _, err := s.Get(ctx, id); err != nil {
		return domain.Notification{}, err
	}
	return domain.Notification{}, ErrNotCancellable
}

// BatchSummary describes a batch and how its notifications are progressing.
type BatchSummary struct {
	ID        uuid.UUID
	Total     int
	Counts    map[domain.Status]int
	CreatedAt time.Time
}

// GetBatch returns the batch summary and one page of its notifications.
func (s *Store) GetBatch(ctx context.Context, id uuid.UUID, p Page) (BatchSummary, []domain.Notification, string, error) {
	sum := BatchSummary{ID: id, Counts: map[domain.Status]int{}}
	err := s.pool.QueryRow(ctx, `SELECT total, created_at FROM batches WHERE id = $1`, id).Scan(&sum.Total, &sum.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return sum, nil, "", ErrNotFound
	}
	if err != nil {
		return sum, nil, "", err
	}

	rows, err := s.pool.Query(ctx, `SELECT status, count(*) FROM notifications WHERE batch_id = $1 GROUP BY status`, id)
	if err != nil {
		return sum, nil, "", err
	}
	for rows.Next() {
		var st domain.Status
		var c int
		if err := rows.Scan(&st, &c); err != nil {
			rows.Close()
			return sum, nil, "", err
		}
		sum.Counts[st] = c
	}
	if err := rows.Err(); err != nil {
		return sum, nil, "", err
	}

	items, next, err := s.List(ctx, ListFilter{BatchID: &id, Page: p})
	return sum, items, next, err
}

// Page selects one page of a keyset-paginated listing.
type Page struct {
	Limit  int    // 1..MaxLimit, 0 means DefaultLimit
	Cursor string // opaque, from a previous page; empty for the first page
}

// Pagination bounds.
const (
	DefaultLimit = 20
	MaxLimit     = 100
)

// ListFilter narrows a listing; nil fields are ignored. From and To bound
// created_at inclusively.
type ListFilter struct {
	Status  *domain.Status
	Channel *domain.Channel
	BatchID *uuid.UUID
	From    *time.Time
	To      *time.Time
	Page
}

// List returns notifications newest first and the cursor of the next page
// ("" when there is none).
func (s *Store) List(ctx context.Context, f ListFilter) ([]domain.Notification, string, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}
	limit = min(limit, MaxLimit)

	var conds []string
	var args []any
	add := func(cond string, v ...any) {
		for _, a := range v {
			args = append(args, a)
			cond = strings.Replace(cond, "?", "$"+strconv.Itoa(len(args)), 1)
		}
		conds = append(conds, cond)
	}
	if f.Status != nil {
		add("status = ?", *f.Status)
	}
	if f.Channel != nil {
		add("channel = ?", *f.Channel)
	}
	if f.BatchID != nil {
		add("batch_id = ?", *f.BatchID)
	}
	if f.From != nil {
		add("created_at >= ?", *f.From)
	}
	if f.To != nil {
		add("created_at <= ?", *f.To)
	}
	if f.Cursor != "" {
		at, id, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", err
		}
		add("(created_at, id) < (?, ?)", at, id)
	}

	sql := `SELECT ` + columns + ` FROM notifications`
	if len(conds) > 0 {
		sql += " WHERE " + strings.Join(conds, " AND ")
	}
	sql += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT %d", limit+1)

	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, "", err
	}
	items, err := scanAll(rows)
	if err != nil {
		return nil, "", err
	}
	if len(items) <= limit {
		return items, "", nil
	}
	items = items[:limit]
	last := items[limit-1]
	return items, encodeCursor(last.CreatedAt, last.ID), nil
}

func encodeCursor(at time.Time, id uuid.UUID) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(at.UnixMicro(), 10) + "," + id.String()))
}

func decodeCursor(c string) (time.Time, uuid.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	micros, idStr, ok := strings.Cut(string(raw), ",")
	if !ok {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	us, err := strconv.ParseInt(micros, 10, 64)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	id, err := uuid.Parse(idStr)
	if err != nil {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	return time.UnixMicro(us), id, nil
}
