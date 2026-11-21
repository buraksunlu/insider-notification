// Package apispec embeds the OpenAPI specification so the api binary can
// serve it at /openapi.yaml with Swagger UI at /docs.
package apispec

import _ "embed"

// Spec is the OpenAPI 3 document in YAML.
//
//go:embed openapi.yaml
var Spec []byte
