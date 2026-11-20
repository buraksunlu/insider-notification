// Package api exposes the notification management HTTP API.
package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/store"
)

// NotificationStore is the persistence the API needs; *store.Store implements it.
type NotificationStore interface {
	Create(ctx context.Context, n domain.Notification) (domain.Notification, bool, error)
	CreateBatch(ctx context.Context, ns []domain.Notification) (uuid.UUID, []uuid.UUID, error)
	Get(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	GetBatch(ctx context.Context, id uuid.UUID, p store.Page) (store.BatchSummary, []domain.Notification, string, error)
	Cancel(ctx context.Context, id uuid.UUID) (domain.Notification, error)
	CancelBatch(ctx context.Context, id uuid.UUID) (int64, error)
	List(ctx context.Context, f store.ListFilter) ([]domain.Notification, string, error)
}

// Deps are the collaborators of the HTTP API.
type Deps struct {
	Store NotificationStore
	// Ready reports whether downstream dependencies are reachable (readiness probe).
	Ready  func(context.Context) error
	Logger *slog.Logger
	Now    func() time.Time
}

// MaxBodyBytes bounds request bodies: 1000 emails of 10 000 multibyte
// characters fit, anything larger is rejected with 413.
const MaxBodyBytes = 16 << 20

type server struct {
	store NotificationStore
	ready func(context.Context) error
	log   *slog.Logger
	now   func() time.Time
}

// NewRouter builds the HTTP handler with all routes and middleware.
func NewRouter(d Deps) http.Handler {
	if d.Now == nil {
		d.Now = time.Now
	}
	s := &server{store: d.Store, ready: d.Ready, log: d.Logger, now: d.Now}

	r := chi.NewRouter()
	r.Use(correlationID, s.requestLog, s.recoverer, limitBody(MaxBodyBytes))
	r.NotFound(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "route not found", nil)
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed", nil)
	})

	r.Get("/healthz", s.healthz)
	r.Get("/readyz", s.readyz)

	r.Route("/api/v1", func(r chi.Router) {
		r.Post("/notifications", s.createNotification)
		r.Post("/notifications/batch", s.createBatch)
		r.Get("/notifications", s.listNotifications)
		r.Get("/notifications/{id}", s.getNotification)
		r.Post("/notifications/{id}/cancel", s.cancelNotification)
		r.Get("/batches/{id}", s.getBatch)
		r.Post("/batches/{id}/cancel", s.cancelBatch)
	})
	return r
}
