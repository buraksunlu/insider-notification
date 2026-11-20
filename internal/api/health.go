package api

import (
	"context"
	"net/http"
	"time"
)

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if s.ready != nil {
		if err := s.ready(ctx); err != nil {
			s.log.WarnContext(r.Context(), "readiness check failed", "error", err)
			writeError(w, http.StatusServiceUnavailable, "unavailable", "dependencies are not ready", nil)
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}
