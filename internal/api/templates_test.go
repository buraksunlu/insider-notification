package api_test

import (
	"net/http"
	"strings"
	"testing"
)

// createTemplate posts a template and returns its id.
func (h *harness) createTemplate(t *testing.T, body string) string {
	t.Helper()
	r := h.do(t, "POST", "/api/v1/templates", body)
	if r.code != http.StatusCreated {
		t.Fatalf("create template: %d %s", r.code, r.raw)
	}
	id, _ := r.body["id"].(string)
	return id
}

func TestTemplates_CreateGetList(t *testing.T) {
	h := newHarness(t)
	r := h.do(t, "POST", "/api/v1/templates", `{"name":"order-shipped","channel":"sms","body":"Hi {{name}}, order {{ order }} shipped"}`)
	if r.code != http.StatusCreated {
		t.Fatalf("code = %d, body %s", r.code, r.raw)
	}
	id, _ := r.body["id"].(string)
	if r.header.Get("Location") != "/api/v1/templates/"+id {
		t.Fatalf("Location = %q", r.header.Get("Location"))
	}
	vars, _ := r.body["variables"].([]any)
	if len(vars) != 2 || vars[0] != "name" || vars[1] != "order" {
		t.Fatalf("variables = %v, want [name order]", r.body["variables"])
	}

	if g := h.do(t, "GET", "/api/v1/templates/"+id, ""); g.code != http.StatusOK || g.body["name"] != "order-shipped" {
		t.Fatalf("get: %d %s", g.code, g.raw)
	}
	if g := h.do(t, "GET", "/api/v1/templates/00000000-0000-0000-0000-000000000000", ""); g.code != http.StatusNotFound {
		t.Fatalf("get unknown: %d", g.code)
	}
	l := h.do(t, "GET", "/api/v1/templates", "")
	data, _ := l.body["data"].([]any)
	if l.code != http.StatusOK || len(data) != 1 {
		t.Fatalf("list: %d %s", l.code, l.raw)
	}
}

func TestTemplates_Errors(t *testing.T) {
	h := newHarness(t)
	h.createTemplate(t, `{"name":"dup","channel":"sms","body":"x"}`)

	tests := []struct {
		name, body string
		code       int
		field      string
	}{
		{"duplicate name", `{"name":"dup","channel":"sms","body":"y"}`, http.StatusConflict, ""},
		{"bad placeholder", `{"name":"bad","channel":"sms","body":"Hi {{first name}}"}`, http.StatusUnprocessableEntity, "body"},
		{"bad channel", `{"name":"bad","channel":"fax","body":"x"}`, http.StatusUnprocessableEntity, "channel"},
		{"unknown field", `{"name":"bad","channel":"sms","body":"x","bdy":"x"}`, http.StatusUnprocessableEntity, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := h.do(t, "POST", "/api/v1/templates", tc.body)
			if r.code != tc.code {
				t.Fatalf("code = %d, want %d (%s)", r.code, tc.code, r.raw)
			}
			if tc.field != "" && errDetails(t, r)[tc.field] == nil {
				t.Fatalf("details = %v, want %s", errDetails(t, r), tc.field)
			}
		})
	}
}

func TestCreate_FromTemplate(t *testing.T) {
	h := newHarness(t)
	id := h.createTemplate(t, `{"name":"otp","channel":"sms","body":"Kodunuz {{code}} 🔐"}`)

	r := h.do(t, "POST", "/api/v1/notifications",
		`{"channel":"sms","recipient":"+905551234567","template_id":"`+id+`","variables":{"code":"4821","unused":"x"}}`)
	if r.code != http.StatusAccepted {
		t.Fatalf("code = %d, body %s", r.code, r.raw)
	}
	if r.body["content"] != "Kodunuz 4821 🔐" || r.body["template_id"] != id {
		t.Fatalf("content %v template_id %v", r.body["content"], r.body["template_id"])
	}
}

func TestCreate_TemplateErrors(t *testing.T) {
	h := newHarness(t)
	sms := h.createTemplate(t, `{"name":"sms-hi","channel":"sms","body":"Hi {{name}}"}`)
	long := h.createTemplate(t, `{"name":"sms-long","channel":"sms","body":"{{text}}"}`)

	req := func(fields string) string {
		return `{"channel":"sms","recipient":"+905551234567",` + fields + `}`
	}
	tests := []struct {
		name, body, field, mention string
	}{
		{"content and template", req(`"content":"x","template_id":"` + sms + `","variables":{"name":"A"}`), "content", "template_id"},
		{"unknown template", req(`"template_id":"00000000-0000-0000-0000-000000000000"`), "template_id", "no such"},
		{"template id not a uuid", req(`"template_id":"otp"`), "template_id", "UUID"},
		{"missing variable", req(`"template_id":"` + sms + `","variables":{}`), "variables", "name"},
		{"variables without template", req(`"content":"x","variables":{"name":"A"}`), "variables", "template_id"},
		{"rendered content too long", req(`"template_id":"` + long + `","variables":{"text":"` + strings.Repeat("ş", 161) + `"}`), "content", "160"},
		{"channel mismatch", `{"channel":"email","recipient":"ada@example.com","template_id":"` + sms + `","variables":{"name":"A"}}`, "template_id", "sms"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := h.do(t, "POST", "/api/v1/notifications", tc.body)
			if r.code != http.StatusUnprocessableEntity {
				t.Fatalf("code = %d, want 422 (%s)", r.code, r.raw)
			}
			msg, _ := errDetails(t, r)[tc.field].(string)
			if !strings.Contains(msg, tc.mention) {
				t.Fatalf("details = %v, want %s mentioning %q", errDetails(t, r), tc.field, tc.mention)
			}
		})
	}
}

func TestBatch_FromTemplate(t *testing.T) {
	h := newHarness(t)
	id := h.createTemplate(t, `{"name":"sale","channel":"sms","body":"Hi {{name}}, sale starts now"}`)
	item := func(name string) string {
		return `{"channel":"sms","recipient":"+905551234567","template_id":"` + id + `","variables":{"name":"` + name + `"}}`
	}

	r := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(item("Ada"), item("Linus")))
	if r.code != http.StatusAccepted {
		t.Fatalf("code = %d, body %s", r.code, r.raw)
	}
	contents := map[string]bool{}
	for _, n := range h.store.byID {
		contents[n.Content] = true
	}
	if !contents["Hi Ada, sale starts now"] || !contents["Hi Linus, sale starts now"] {
		t.Fatalf("stored contents = %v", contents)
	}

	bad := h.do(t, "POST", "/api/v1/notifications/batch", batchBody(item("Ada"),
		`{"channel":"sms","recipient":"+905551234567","template_id":"`+id+`"}`))
	item1, _ := errDetails(t, bad)["1"].(map[string]any)
	if bad.code != http.StatusUnprocessableEntity || item1["variables"] == nil {
		t.Fatalf("code = %d body %s, want 422 on item 1 variables", bad.code, bad.raw)
	}
}
