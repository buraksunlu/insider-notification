package apispec_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"go.yaml.in/yaml/v3"

	apispec "github.com/buraksunlu/insider-notification/api"
	"github.com/buraksunlu/insider-notification/internal/api"
	"github.com/buraksunlu/insider-notification/internal/observability"
)

func load(t *testing.T) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal(apispec.Spec, &doc); err != nil {
		t.Fatalf("openapi.yaml is not valid YAML: %v", err)
	}
	return doc
}

// TestSpecRefsResolve checks every $ref points at an existing component.
func TestSpecRefsResolve(t *testing.T) {
	doc := load(t)
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if ref, ok := x["$ref"].(string); ok {
				if !resolves(doc, ref) {
					t.Errorf("unresolved $ref %s", ref)
				}
			}
			for _, c := range x {
				walk(c)
			}
		case []any:
			for _, c := range x {
				walk(c)
			}
		}
	}
	walk(doc)
}

func resolves(doc map[string]any, ref string) bool {
	path, ok := strings.CutPrefix(ref, "#/")
	if !ok {
		return false
	}
	var cur any = doc
	for _, part := range strings.Split(path, "/") {
		m, ok := cur.(map[string]any)
		if !ok {
			return false
		}
		if cur, ok = m[part]; !ok {
			return false
		}
	}
	return true
}

// TestSpecCoversEveryRoute keeps the spec in step with the router.
func TestSpecCoversEveryRoute(t *testing.T) {
	paths, _ := load(t)["paths"].(map[string]any)
	reg := observability.NewRegistry()
	router := api.NewRouter(api.Deps{
		Logger:         observability.NewLogger(io.Discard, "error"),
		MetricsHandler: observability.MetricsHandler(reg),
		OpenAPISpec:    apispec.Spec,
	})
	undocumented := map[string]bool{"/openapi.yaml": true, "/docs": true}

	walked := 0
	err := chi.Walk(router.(chi.Routes), func(method, route string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		walked++
		if undocumented[route] {
			return nil
		}
		ops, ok := paths[route].(map[string]any)
		if !ok {
			t.Errorf("route %s is not in openapi.yaml", route)
			return nil
		}
		if _, ok := ops[strings.ToLower(method)]; !ok {
			t.Errorf("%s %s is not in openapi.yaml", method, route)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if walked < 12 {
		t.Fatalf("walked %d routes, want at least 12: is the router still a chi router?", walked)
	}
}
