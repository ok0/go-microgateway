package microgateway

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/ok0/go-microgateway/config"
)

// Middleware wraps an http.Handler.
type Middleware = func(http.Handler) http.Handler

// MiddlewareFactory builds a Middleware for one configured middleware definition.
type MiddlewareFactory func(p MiddlewareParams) (Middleware, error)

// MiddlewareParams describes one configured middleware definition. Use Decode to read its params. Its String, Format, LogValue and JSON forms show only the name and type, so it is safe to log.
type MiddlewareParams struct {
	Name, Type string
	params     config.SecretParams
}

// Decode strictly decodes the definition's params into v. Errors are value-free: unknown keys are named, type mismatches name only the YAML tag and target type.
func (p MiddlewareParams) Decode(v any) error {
	return config.DecodeSecretParams(p.params, v)
}

// String describes the definition by name and type. Parameter values are never included.
func (p MiddlewareParams) String() string {
	return fmt.Sprintf("microgateway.MiddlewareParams{Name:%q, Type:%q}", p.Name, p.Type)
}

// GoString describes the definition by name and type, like String.
func (p MiddlewareParams) GoString() string {
	return p.String()
}

// Format writes the String form for every verb.
func (p MiddlewareParams) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, p.String())
}

// LogValue returns the name and type of the definition as a log group. Parameter values are never included.
func (p MiddlewareParams) LogValue() slog.Value {
	return slog.GroupValue(slog.String("name", p.Name), slog.String("type", p.Type))
}

// MarshalJSON encodes the name and type of the definition. Parameter values are never included.
func (p MiddlewareParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"name": p.Name, "type": p.Type})
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
