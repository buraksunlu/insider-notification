// Package observability provides structured logging and metrics.
package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"
)

type correlationKey struct{}

// WithCorrelationID returns a context carrying the request's correlation id.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationKey{}, id)
}

// CorrelationID returns the correlation id stored in ctx, or "".
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(correlationKey{}).(string)
	return id
}

// NewLogger returns a JSON logger writing to w. Every record logged with a
// *Context method automatically carries the context's correlation_id.
func NewLogger(w io.Writer, level string) *slog.Logger {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		lvl = slog.LevelInfo
	}
	return slog.New(correlationHandler{slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})})
}

type correlationHandler struct{ slog.Handler }

func (h correlationHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	return h.Handler.Handle(ctx, r)
}

func (h correlationHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return correlationHandler{h.Handler.WithAttrs(attrs)}
}

func (h correlationHandler) WithGroup(name string) slog.Handler {
	return correlationHandler{h.Handler.WithGroup(name)}
}
