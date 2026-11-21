package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRender(t *testing.T) {
	tests := []struct {
		name, body string
		vars       map[string]string
		want       string
	}{
		{"single", "Hi {{name}}", map[string]string{"name": "Ada"}, "Hi Ada"},
		{"spaces inside braces", "Hi {{ name }}!", map[string]string{"name": "Ada"}, "Hi Ada!"},
		{"repeated", "{{x}}-{{x}}", map[string]string{"x": "1"}, "1-1"},
		{"multibyte", "Merhaba {{ad}} 👋, siparişin {{no}} kargoda", map[string]string{"ad": "Şule", "no": "A-42"}, "Merhaba Şule 👋, siparişin A-42 kargoda"},
		{"extra variables ignored", "Hi {{name}}", map[string]string{"name": "Ada", "unused": "x"}, "Hi Ada"},
		{"no placeholders", "Static text", nil, "Static text"},
		{"value is not re-expanded", "{{a}}", map[string]string{"a": "{{b}}", "b": "no"}, "{{b}}"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Render(tc.body, tc.vars)
			if err != nil || got != tc.want {
				t.Fatalf("Render = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestRenderMissingVariables(t *testing.T) {
	_, err := Render("Hi {{name}}, order {{order}} ships {{ when }}", map[string]string{"order": "1"})
	var missing *MissingVariablesError
	if !errors.As(err, &missing) {
		t.Fatalf("err = %v, want *MissingVariablesError", err)
	}
	if strings.Join(missing.Names, ",") != "name,when" {
		t.Fatalf("missing = %v, want [name when]", missing.Names)
	}
	if !strings.Contains(err.Error(), "name") || !strings.Contains(err.Error(), "when") {
		t.Fatalf("error %q must name the missing variables", err)
	}
}

func TestValidateTemplate(t *testing.T) {
	ok := CreateTemplateInput{Name: "order-shipped", Channel: "sms", Body: "Hi {{name}}"}
	if err := ValidateTemplate(ok); err != nil {
		t.Fatalf("valid template: %v", err)
	}
	// Placeholders do not count toward the channel limit: only fixed text.
	fits := CreateTemplateInput{Name: "fits", Channel: "sms", Body: strings.Repeat("ş", 150) + "{{a_long_placeholder_name}}"}
	if err := ValidateTemplate(fits); err != nil {
		t.Fatalf("150 fixed characters plus a placeholder: %v", err)
	}
	tests := []struct {
		name  string
		edit  func(*CreateTemplateInput)
		field string
	}{
		{"empty name", func(in *CreateTemplateInput) { in.Name = " " }, "name"},
		{"name with spaces", func(in *CreateTemplateInput) { in.Name = "order shipped" }, "name"},
		{"name too long", func(in *CreateTemplateInput) { in.Name = strings.Repeat("a", 101) }, "name"},
		{"bad channel", func(in *CreateTemplateInput) { in.Channel = "fax" }, "channel"},
		{"empty body", func(in *CreateTemplateInput) { in.Body = "  " }, "body"},
		{"unclosed placeholder", func(in *CreateTemplateInput) { in.Body = "Hi {{name" }, "body"},
		{"invalid placeholder name", func(in *CreateTemplateInput) { in.Body = "Hi {{first name}}" }, "body"},
		{"stray closing braces", func(in *CreateTemplateInput) { in.Body = "Hi }} there" }, "body"},
		{"body over email limit", func(in *CreateTemplateInput) { in.Channel = "email"; in.Body = strings.Repeat("x", 10001) }, "body"},
		{"fixed text over sms limit", func(in *CreateTemplateInput) { in.Body = strings.Repeat("ş", 161) + "{{code}}" }, "body"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := ok
			tc.edit(&in)
			var verr *ValidationError
			if err := ValidateTemplate(in); !errors.As(err, &verr) || verr.Fields[tc.field] == "" {
				t.Fatalf("err = %v, want a %s error", err, tc.field)
			}
		})
	}
}

func TestNewTemplate(t *testing.T) {
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	tpl := NewTemplate(CreateTemplateInput{Name: " welcome ", Channel: "push", Body: "Hi {{name}}"}, now)
	if tpl.ID.String() == "" || tpl.Name != "welcome" || tpl.Channel != ChannelPush || !tpl.CreatedAt.Equal(now) {
		t.Fatalf("NewTemplate = %+v", tpl)
	}
	if got := tpl.Variables(); len(got) != 1 || got[0] != "name" {
		t.Fatalf("Variables = %v, want [name]", got)
	}
}
