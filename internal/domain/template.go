package domain

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// Template is a reusable message body with {{variable}} placeholders.
// Templates are immutable: a notification's template_id always points at
// the body it was rendered from.
type Template struct {
	ID        uuid.UUID
	Name      string
	Channel   Channel
	Body      string
	CreatedAt time.Time
}

// CreateTemplateInput is a client request to create a template.
type CreateTemplateInput struct {
	Name    string
	Channel string
	Body    string
}

const (
	maxTemplateNameLen = 100
	// maxTemplateRunes bounds a body on any channel; the rendered content
	// is then checked against the channel's own limit.
	maxTemplateRunes = 10000
)

var (
	placeholder  = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)
	templateName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)
)

// ValidateTemplate checks a create-template request and returns a
// *ValidationError describing every invalid field, or nil.
func ValidateTemplate(in CreateTemplateInput) error {
	fields := map[string]string{}
	name := strings.TrimSpace(in.Name)
	switch {
	case name == "" || len(name) > maxTemplateNameLen:
		fields["name"] = fmt.Sprintf("must be 1-%d characters", maxTemplateNameLen)
	case !templateName.MatchString(name):
		fields["name"] = "may contain only letters, digits, '.', '_' and '-'"
	}
	if !Channel(in.Channel).Valid() {
		fields["channel"] = "must be one of sms, email, push"
	}
	switch {
	case strings.TrimSpace(in.Body) == "":
		fields["body"] = "is required"
	case utf8.RuneCountInString(in.Body) > maxTemplateRunes:
		fields["body"] = fmt.Sprintf("must be at most %d characters", maxTemplateRunes)
	default:
		// Whatever is left after removing valid placeholders must not
		// contain braces: those are typos such as {{name or {{first name}}.
		rest := placeholder.ReplaceAllString(in.Body, "")
		if strings.Contains(rest, "{{") || strings.Contains(rest, "}}") {
			fields["body"] = "has an invalid placeholder; use {{name}} with letters, digits and _"
		}
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

// NewTemplate builds a template from an already validated input.
func NewTemplate(in CreateTemplateInput, now time.Time) Template {
	return Template{
		ID:        uuid.New(),
		Name:      strings.TrimSpace(in.Name),
		Channel:   Channel(in.Channel),
		Body:      in.Body,
		CreatedAt: now,
	}
}

// Variables returns the distinct placeholder names in the body, sorted.
func (t Template) Variables() []string {
	seen := map[string]bool{}
	for _, m := range placeholder.FindAllStringSubmatch(t.Body, -1) {
		seen[m[1]] = true
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// MissingVariablesError lists placeholders that had no value.
type MissingVariablesError struct {
	Names []string
}

func (e *MissingVariablesError) Error() string {
	return "missing variables: " + strings.Join(e.Names, ", ")
}

// Render replaces every {{name}} in body with vars[name]. Values are
// inserted as-is and never expanded again. Variables the body does not use
// are ignored, so a template can drop a placeholder without breaking
// clients; a placeholder without a value is a *MissingVariablesError.
func Render(body string, vars map[string]string) (string, error) {
	missing := map[string]bool{}
	out := placeholder.ReplaceAllStringFunc(body, func(m string) string {
		name := placeholder.FindStringSubmatch(m)[1]
		v, ok := vars[name]
		if !ok {
			missing[name] = true
		}
		return v
	})
	if len(missing) > 0 {
		names := make([]string, 0, len(missing))
		for n := range missing {
			names = append(names, n)
		}
		sort.Strings(names)
		return "", &MissingVariablesError{Names: names}
	}
	return out, nil
}
