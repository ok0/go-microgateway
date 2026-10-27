package lychgate

import (
	"net/http"

	"github.com/ok0/lychgate/config"
)

// Middleware wraps an http.Handler.
type Middleware = func(http.Handler) http.Handler

// MiddlewareFactory builds a Middleware for one configured middleware definition.
type MiddlewareFactory func(p MiddlewareParams) (Middleware, error)

// MiddlewareParams describes one configured middleware definition.
type MiddlewareParams struct {
	Name, Type string
	params     map[string]any
}

// Decode strictly decodes the definition's params into v; unknown keys and type mismatches are errors naming the field.
func (p MiddlewareParams) Decode(v any) error {
	return config.DecodeParams(p.params, v)
}

type registration struct {
	typeName string
	f        MiddlewareFactory
}

// WithMiddleware registers a middleware type that configuration can reference by middlewares[].type.
func WithMiddleware(typeName string, f MiddlewareFactory) Option {
	return func(o *options) {
		o.middlewares = append(o.middlewares, registration{typeName: typeName, f: f})
	}
}
