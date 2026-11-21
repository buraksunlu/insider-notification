package api

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/observability"
	"github.com/buraksunlu/insider-notification/internal/store"
)

// IdempotencyHeader lets clients safely retry single creates.
const IdempotencyHeader = "Idempotency-Key"

const maxIdempotencyKeyLen = 255

// MaxBatchSize is the largest batch accepted in one request.
const MaxBatchSize = 1000

func (s *server) createNotification(w http.ResponseWriter, r *http.Request) {
	var req createRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	in := req.input()
	key := strings.TrimSpace(r.Header.Get(IdempotencyHeader))
	if len(key) > maxIdempotencyKeyLen {
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", "invalid notification",
			map[string]any{IdempotencyHeader: fmt.Sprintf("must be at most %d characters", maxIdempotencyKeyLen)})
		return
	}
	if key != "" {
		in.IdempotencyKey = &key
	}

	now := s.now()
	err := s.validate(r, req, &in, now, templateCache{})
	var storeErr *lookupError
	if errors.As(err, &storeErr) {
		s.storeError(w, r, storeErr.err)
		return
	}
	if err != nil {
		replay, lookupErr := s.isReplay(r, in.IdempotencyKey, err)
		if lookupErr != nil {
			s.storeError(w, r, lookupErr)
			return
		}
		if !replay {
			writeValidation(w, err)
			return
		}
		// Create finds the existing key: 200 with the stored row, or 409.
	}

	n := domain.NewNotification(in, observability.CorrelationID(r.Context()), now)
	got, created, err := s.store.Create(r.Context(), n)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	status := http.StatusOK // idempotent replay
	if created {
		s.metrics.Created(got.Channel)
		status = http.StatusAccepted
		w.Header().Set("Location", "/api/v1/notifications/"+got.ID.String())
	}
	writeJSON(w, status, toDTO(got))
}

func (s *server) createBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Notifications) == 0 || len(req.Notifications) > MaxBatchSize {
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", "invalid batch",
			map[string]any{"notifications": fmt.Sprintf("must contain 1-%d items", MaxBatchSize)})
		return
	}

	now := s.now()
	corr := observability.CorrelationID(r.Context())
	templates := templateCache{} // a campaign batch reuses one template
	details := map[string]any{}
	firstByKey := map[string]int{}
	ns := make([]domain.Notification, len(req.Notifications))
	for i, item := range req.Notifications {
		in := item.input()
		if k := item.IdempotencyKey; k != nil {
			// Trimmed like the header, so " k" and "k" are one key.
			trimmed := strings.TrimSpace(*k)
			in.IdempotencyKey = &trimmed
		}
		fields := map[string]string{}

		err := s.validate(r, item.createRequest, &in, now, templates)
		var storeErr *lookupError
		if errors.As(err, &storeErr) {
			s.storeError(w, r, storeErr.err)
			return
		}
		var verr *domain.ValidationError
		if errors.As(err, &verr) {
			replay, lookupErr := s.isReplay(r, in.IdempotencyKey, err)
			if lookupErr != nil {
				s.storeError(w, r, lookupErr)
				return
			}
			if !replay { // a replay is resolved by CreateBatch reusing the row
				fields = verr.Fields
			}
		}
		if k := in.IdempotencyKey; k != nil {
			switch j, dup := firstByKey[*k]; {
			case len(*k) == 0 || len(*k) > maxIdempotencyKeyLen:
				fields["idempotency_key"] = fmt.Sprintf("must be 1-%d characters", maxIdempotencyKeyLen)
			case dup:
				fields["idempotency_key"] = fmt.Sprintf("duplicates item %d", j)
			default:
				firstByKey[*k] = i
			}
		}
		if len(fields) > 0 {
			details[strconv.Itoa(i)] = fields
			continue
		}
		ns[i] = domain.NewNotification(in, corr, now)
	}
	if len(details) > 0 {
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", "one or more items are invalid", details)
		return
	}

	batchID, ids, err := s.store.CreateBatch(r.Context(), ns)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	for i, id := range ids {
		if id == ns[i].ID { // otherwise an existing row was reused by idempotency key
			s.metrics.Created(ns[i].Channel)
		}
	}
	w.Header().Set("Location", "/api/v1/batches/"+batchID.String())
	writeJSON(w, http.StatusAccepted, batchCreatedResponse{BatchID: batchID, IDs: ids})
}

func (s *server) getNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	n, err := s.store.Get(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDTO(n))
}

func (s *server) cancelNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	n, err := s.store.Cancel(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toDTO(n))
}

func (s *server) listNotifications(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	details := map[string]any{}
	var f store.ListFilter

	page, ok := parsePage(q.Get("limit"), q.Get("cursor"), details)
	f.Page = page
	if v := q.Get("status"); v != "" {
		st := domain.Status(v)
		if !st.Valid() {
			details["status"] = "must be one of pending, queued, processing, sent, failed, cancelled"
		}
		f.Status = &st
	}
	if v := q.Get("channel"); v != "" {
		ch := domain.Channel(v)
		if !ch.Valid() {
			details["channel"] = "must be one of sms, email, push"
		}
		f.Channel = &ch
	}
	f.From = parseTime(q.Get("from"), "from", details)
	f.To = parseTime(q.Get("to"), "to", details)
	if f.From != nil && f.To != nil && f.From.After(*f.To) {
		details["to"] = "must not be before from"
	}
	if !ok || len(details) > 0 {
		writeError(w, http.StatusBadRequest, "invalid_query", "invalid query parameters", details)
		return
	}

	items, next, err := s.store.List(r.Context(), f)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, listResponse{Data: toDTOs(items), NextCursor: next})
}

func parseID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", "id must be a UUID", nil)
		return uuid.Nil, false
	}
	return id, true
}

// parsePage validates limit/cursor, recording problems in details.
func parsePage(limit, cursor string, details map[string]any) (store.Page, bool) {
	p := store.Page{Cursor: cursor}
	if limit == "" {
		return p, true
	}
	n, err := strconv.Atoi(limit)
	if err != nil || n < 1 || n > store.MaxLimit {
		details["limit"] = fmt.Sprintf("must be an integer between 1 and %d", store.MaxLimit)
		return p, false
	}
	p.Limit = n
	return p, true
}

func parseTime(v, name string, details map[string]any) *time.Time {
	if v == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		details[name] = "must be an RFC 3339 timestamp, e.g. 2026-11-19T12:00:00Z"
		return nil
	}
	return &t
}

func writeValidation(w http.ResponseWriter, err error) {
	writeValidationAs(w, err, "invalid notification")
}

func writeValidationAs(w http.ResponseWriter, err error, message string) {
	var verr *domain.ValidationError
	if !errors.As(err, &verr) {
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", err.Error(), nil)
		return
	}
	details := make(map[string]any, len(verr.Fields))
	for k, v := range verr.Fields {
		details[k] = v
	}
	writeError(w, http.StatusUnprocessableEntity, "validation_failed", message, details)
}

// isReplay reports whether a request that failed validation only because
// its scheduled_at has passed is a retry of an existing idempotency key. The
// time was valid when the notification was first created, so the retry must
// get the stored notification (or 409 for another payload), not 422.
func (s *server) isReplay(r *http.Request, key *string, validationErr error) (bool, error) {
	if key == nil || *key == "" || !domain.OnlyScheduledInPast(validationErr) {
		return false, nil
	}
	return s.store.HasIdempotencyKey(r.Context(), *key)
}
