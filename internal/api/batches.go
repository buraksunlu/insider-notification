package api

import (
	"net/http"

	"github.com/google/uuid"
)

func (s *server) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	details := map[string]any{}
	page, ok := parsePage(r.URL.Query().Get("limit"), r.URL.Query().Get("cursor"), details)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid_query", "invalid query parameters", details)
		return
	}

	sum, items, next, err := s.store.GetBatch(r.Context(), id, page)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	counts := make(map[string]int, len(sum.Counts))
	for st, c := range sum.Counts {
		counts[string(st)] = c
	}
	writeJSON(w, http.StatusOK, batchResponse{
		BatchID: sum.ID, Total: sum.Total, Counts: counts, CreatedAt: sum.CreatedAt,
		Data: toDTOs(items), NextCursor: next,
	})
}

type batchCancelResponse struct {
	BatchID   uuid.UUID `json:"batch_id"`
	Cancelled int64     `json:"cancelled"`
}

func (s *server) cancelBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	n, err := s.store.CancelBatch(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, batchCancelResponse{BatchID: id, Cancelled: n})
}
