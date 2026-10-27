package middleware

import (
	"errors"
	"fmt"
	"slices"

	"github.com/ok0/lychgate/config"
)

// Factory builds a middleware from its configuration definition.
type Factory func(def config.Middleware) (Middleware, error)

// Registry maps middleware type names to factories.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]Factory)}
}

// Register associates typeName with f. It fails for an empty name, a nil factory, a built-in type name, or a name that is already registered.
func (r *Registry) Register(typeName string, f Factory) error {
	if typeName == "" {
		return errors.New("middleware type name is empty")
	}
	if f == nil {
		return fmt.Errorf("middleware type %q: nil factory", typeName)
	}
	if slices.Contains(config.BuiltinMiddlewareTypes, typeName) {
		return fmt.Errorf("middleware type %q is built in and cannot be registered", typeName)
	}
	if _, ok := r.factories[typeName]; ok {
		return fmt.Errorf("middleware type %q is already registered", typeName)
	}
	r.factories[typeName] = f
	return nil
}

// Known reports whether typeName is a built-in or registered middleware type.
func (r *Registry) Known(typeName string) bool {
	if slices.Contains(config.BuiltinMiddlewareTypes, typeName) {
		return true
	}
	_, ok := r.factories[typeName]
	return ok
}

// Build returns the middleware for def. Built-in types are constructed from their decoded params; other types are passed to their registered factory.
func (r *Registry) Build(def config.Middleware) (Middleware, error) {
	if slices.Contains(config.BuiltinMiddlewareTypes, def.Type) {
		p, err := def.BuiltinParams()
		if err != nil {
			return nil, err
		}
		switch p := p.(type) {
		case *config.CORSParams:
			return CORS(*p), nil
		case *config.HeadersParams:
			return Headers(*p), nil
		case *config.RequestSizeLimitParams:
			return RequestSizeLimit(*p), nil
		}
	}
	f, ok := r.factories[def.Type]
	if !ok {
		return nil, fmt.Errorf("unknown middleware type %q", def.Type)
	}
	mw, err := f(def)
	if err != nil {
		return nil, err
	}
	if mw == nil {
		return nil, errors.New("factory returned a nil middleware")
	}
	return mw, nil
}
