package microgateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/runtime"
	"github.com/ok0/go-microgateway/store"
)

// StoreFactory builds the backend of one configured store entry. It is called with the entry's parameters and
// must return a backend that the gateway owns from then on.
type StoreFactory func(p StoreParams) (store.Backend, error)

// StoreParams describes one configured store entry. Use Decode to read the entry's custom parameters. Its String,
// Format, LogValue and JSON forms show only the name and type, so it is safe to log.
type StoreParams struct {
	// Name is the name of the store entry, as referenced by rate_limit.store.
	Name string
	// Type is the store type, as registered with WithStoreType.
	Type string
	// Redis is a copy of the redis block of the entry, or nil when the entry has none.
	Redis *config.RedisStore
	// Logger carries the store name as the "store" attribute.
	Logger *slog.Logger

	params config.SecretParams
}

// Decode strictly decodes the entry's params into v. Unknown keys and type mismatches are errors that name the field
// and never contain a parameter value.
func (p StoreParams) Decode(v any) error {
	return config.DecodeSecretParams(p.params, v)
}

// String describes the entry by name and type. Parameter values are never included.
func (p StoreParams) String() string {
	return fmt.Sprintf("microgateway.StoreParams{Name:%q, Type:%q}", p.Name, p.Type)
}

// GoString describes the entry by name and type, like String.
func (p StoreParams) GoString() string {
	return p.String()
}

// Format writes the String form for every verb.
func (p StoreParams) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, p.String())
}

// LogValue returns the name and type of the entry as a log group. Parameter values are never included.
func (p StoreParams) LogValue() slog.Value {
	return slog.GroupValue(slog.String("name", p.Name), slog.String("type", p.Type))
}

// MarshalJSON encodes the name and type of the entry. Parameter values are never included.
func (p StoreParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"name": p.Name, "type": p.Type})
}

// WithStoreType registers a store type that configuration can reference by stores[].type. caps lists the optional
// features the backends of the type provide; CapRateLimit allows a rate-limit route to use the store.
//
// The factory runs only for store entries that a rate-limit route references, when the gateway is built, and again
// only when the entry's type, redis block or params change. The gateway closes exactly the backend returned by the
// factory, once, after requests that use it have drained. Embedders that share state between gateways return a
// distinct handle from each call. StoreParams.Redis is a clone, and secret fields are config.Secret values.
func WithStoreType(typeName string, caps store.Capabilities, f StoreFactory) Option {
	return func(o *options) {
		o.stores = append(o.stores, storeRegistration{typeName: typeName, caps: caps, f: f})
	}
}

type storeRegistration struct {
	typeName string
	caps     store.Capabilities
	f        StoreFactory
}

func checkStoreRegistrations(regs []storeRegistration) error {
	var errs []error
	seen := make(map[string]bool, len(regs))
	for _, r := range regs {
		if r.typeName == "" {
			errs = append(errs, errors.New("microgateway: WithStoreType: empty type name"))
		}
		if r.f == nil {
			errs = append(errs, fmt.Errorf("microgateway: WithStoreType(%q): nil factory", r.typeName))
		}
		if r.caps == 0 {
			errs = append(errs, fmt.Errorf("microgateway: WithStoreType(%q): capabilities must not be zero", r.typeName))
		}
		if unknown := r.caps &^ store.CapRateLimit; unknown != 0 {
			errs = append(errs, fmt.Errorf("microgateway: WithStoreType(%q): unknown capability bits %#x", r.typeName, unknown))
		}
		if seen[r.typeName] {
			errs = append(errs, fmt.Errorf("microgateway: WithStoreType(%q): type registered twice", r.typeName))
		}
		seen[r.typeName] = true
	}
	return errors.Join(errs...)
}

func storeSet(regs []storeRegistration) *runtime.StoreSet {
	types := make(map[string]runtime.StoreType, len(regs))
	for _, r := range regs {
		f := r.f
		types[r.typeName] = runtime.StoreType{
			Caps: r.caps,
			Build: func(e *config.Store, l *slog.Logger) (store.Backend, error) {
				return f(StoreParams{Name: e.Name, Type: e.Type, Redis: e.Redis.Clone(), Logger: l, params: e.Params})
			},
		}
	}
	return runtime.NewStoreSet(types)
}
