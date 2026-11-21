package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/buraksunlu/insider-notification/internal/domain"
	"github.com/buraksunlu/insider-notification/internal/store"
)

func (s *server) createTemplate(w http.ResponseWriter, r *http.Request) {
	var req createTemplateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	in := domain.CreateTemplateInput{Name: req.Name, Channel: req.Channel, Body: req.Body}
	if err := domain.ValidateTemplate(in); err != nil {
		writeValidationAs(w, err, "invalid template")
		return
	}
	t := domain.NewTemplate(in, s.now())
	if err := s.store.CreateTemplate(r.Context(), t); err != nil {
		s.storeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/api/v1/templates/"+t.ID.String())
	writeJSON(w, http.StatusCreated, toTemplateDTO(t))
}

func (s *server) getTemplate(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	t, err := s.store.GetTemplate(r.Context(), id)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTemplateDTO(t))
}

func (s *server) listTemplates(w http.ResponseWriter, r *http.Request) {
	ts, err := s.store.ListTemplates(r.Context())
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	out := templateListResponse{Data: make([]templateDTO, len(ts))}
	for i, t := range ts {
		out.Data[i] = toTemplateDTO(t)
	}
	writeJSON(w, http.StatusOK, out)
}

// templateCache keeps the templates looked up while handling one request,
// so a batch of 1000 items using one template reads it once.
type templateCache map[uuid.UUID]*domain.Template

// lookupError wraps a store failure met during validation; it is answered
// as a store error (500), not as a validation error.
type lookupError struct{ err error }

func (e *lookupError) Error() string { return e.err.Error() }

// validate resolves the request's template into in.Content and runs the
// domain validation. It returns nil, a *domain.ValidationError, or a
// *lookupError when the template could not be read.
func (s *server) validate(r *http.Request, req createRequest, in *domain.CreateInput, now time.Time, cache templateCache) error {
	tplFields, err := s.resolveTemplate(r, req, in, cache)
	if err != nil {
		return &lookupError{err}
	}
	verr := domain.Validate(*in, now)
	if req.TemplateID == nil && len(tplFields) == 0 {
		return verr
	}
	fields := map[string]string{}
	var v *domain.ValidationError
	if errors.As(verr, &v) {
		for k, msg := range v.Fields {
			fields[k] = msg
		}
	}
	if msg, ok := fields["content"]; ok && req.TemplateID != nil {
		delete(fields, "content")
		// The client sent no content: either the template failed (reported
		// below) or its rendered text broke a content rule, which the
		// variables caused.
		if len(tplFields) == 0 {
			fields["variables"] = "rendered content " + msg
		}
	}
	for k, msg := range tplFields {
		fields[k] = msg
	}
	if len(fields) == 0 {
		return nil
	}
	return &domain.ValidationError{Fields: fields}
}

// resolveTemplate renders req's template into in.Content and sets
// in.TemplateID. It returns field errors for invalid template use; the
// error is a store failure.
func (s *server) resolveTemplate(r *http.Request, req createRequest, in *domain.CreateInput, cache templateCache) (map[string]string, error) {
	if req.TemplateID == nil {
		if len(req.Variables) > 0 {
			return map[string]string{"variables": "requires template_id"}, nil
		}
		return nil, nil
	}
	if req.Content != "" {
		return map[string]string{"content": "must be empty when template_id is set"}, nil
	}
	vars := make(map[string]string, len(req.Variables))
	bad := map[string]string{}
	for k, v := range req.Variables {
		text, ok := variableText(v)
		if !ok {
			bad["variables."+k] = "must be a string, number or boolean"
			continue
		}
		vars[k] = text
	}
	if len(bad) > 0 {
		return bad, nil
	}
	id, err := uuid.Parse(*req.TemplateID)
	if err != nil {
		return map[string]string{"template_id": "must be a UUID"}, nil
	}

	t, cached := cache[id]
	if !cached {
		got, err := s.store.GetTemplate(r.Context(), id)
		switch {
		case errors.Is(err, store.ErrNotFound):
			cache[id] = nil
		case err != nil:
			return nil, err
		default:
			cache[id] = &got
		}
		t = cache[id]
	}
	if t == nil {
		return map[string]string{"template_id": "no such template"}, nil
	}
	if ch := domain.Channel(req.Channel); ch.Valid() && ch != t.Channel {
		return map[string]string{"template_id": fmt.Sprintf("is a %s template, the notification channel is %s", t.Channel, ch)}, nil
	}

	content, err := domain.Render(t.Body, vars)
	if err != nil {
		return map[string]string{"variables": err.Error()}, nil
	}
	in.Content = content
	in.TemplateID = &t.ID
	return nil, nil
}

// variableText turns a JSON scalar into the text inserted into a template.
// Numbers keep their exact JSON spelling (4821, 12.50); objects, arrays and
// null are rejected.
func variableText(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case json.Number:
		return x.String(), true
	case bool:
		return strconv.FormatBool(x), true
	default:
		return "", false
	}
}
