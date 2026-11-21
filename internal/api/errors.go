package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/buraksunlu/insider-notification/internal/store"
)

type errorBody struct {
	Error errorPayload `json:"error"`
}

type errorPayload struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string, details map[string]any) {
	writeJSON(w, status, errorBody{errorPayload{Code: code, Message: msg, Details: details}})
}

// storeError maps repository errors to HTTP responses. Unknown errors are
// logged and answered with a generic 500 so internals never leak.
func (s *server) storeError(w http.ResponseWriter, r *http.Request, err error) {
	var itemErr *store.ItemError
	var details map[string]any
	if errors.As(err, &itemErr) {
		details = map[string]any{strconv.Itoa(itemErr.Index): itemErr.Err.Error()}
	}
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found", nil)
	case errors.Is(err, store.ErrNotCancellable):
		writeError(w, http.StatusConflict, "not_cancellable", err.Error(), nil)
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", store.ErrIdempotencyConflict.Error(), details)
	case errors.Is(err, store.ErrDuplicateKeyInBatch):
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", store.ErrDuplicateKeyInBatch.Error(), details)
	case errors.Is(err, store.ErrTemplateNameTaken):
		writeError(w, http.StatusConflict, "template_name_taken", store.ErrTemplateNameTaken.Error(), nil)
	case errors.Is(err, store.ErrBadCursor):
		writeError(w, http.StatusBadRequest, "invalid_query", "invalid query parameters",
			map[string]any{"cursor": "is not a valid cursor"})
	default:
		s.log.ErrorContext(r.Context(), "store error", "error", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
	}
}

// decodeJSON strictly decodes one JSON value from the request body.
// It answers the request itself and returns false on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	dec.UseNumber() // numbers in untyped fields (template variables) keep their exact text

	err := dec.Decode(dst)
	if err == nil {
		if dec.Decode(&struct{}{}) != io.EOF {
			err = errors.New("unexpected data after the JSON body")
		}
	}

	var tooLarge *http.MaxBytesError
	switch {
	case err == nil:
		return true
	case errors.As(err, &tooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "payload_too_large",
			"request body exceeds "+strconv.FormatInt(tooLarge.Limit, 10)+" bytes", nil)
	case errors.Is(err, io.EOF):
		writeError(w, http.StatusBadRequest, "invalid_json", "request body is required", nil)
	case strings.HasPrefix(err.Error(), "json: unknown field "):
		field := strings.Trim(strings.TrimPrefix(err.Error(), "json: unknown field "), `"`)
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", "request has an unknown field",
			map[string]any{field: "unknown field"})
	default:
		writeError(w, http.StatusBadRequest, "invalid_json", "malformed JSON: "+err.Error(), nil)
	}
	return false
}
