package microgateway

import (
	"errors"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/runtime"
)

// Validate reports whether cfg is a valid configuration for a gateway built with opts. Besides the schema rules of
// the configuration, it reports store and middleware types that are not registered, rate-limit routes that reference
// a store that is missing or whose type cannot rate limit, cache.store entries whose type cannot cache, and any
// problem found in the registered store options.
// Validate never calls a store factory, opens a connection or mutates cfg. A nil cfg is an error.
func Validate(cfg *config.Config, opts ...Option) error {
	if cfg == nil {
		return errors.New("microgateway: nil config")
	}
	o := newOptions(opts)
	if o.err != nil {
		return o.err
	}
	reg, err := o.middlewareRegistry()
	if err != nil {
		return err
	}
	return checkConfig(cfg.Clone(), storeSet(o.stores, o.clk), reg)
}

func checkConfig(c *config.Config, set *runtime.StoreSet, reg *middleware.Registry) error {
	return withResolution(c.Validate(), c, set, reg)
}

// withResolution adds the store and middleware resolution problems of cfg that are not about an already reported
// path (config.MergeCheck) to err when err is a validation error, or reports them alone when err is nil. Other errors
// are returned unchanged.
func withResolution(err error, cfg *config.Config, set *runtime.StoreSet, reg *middleware.Registry) error {
	var reported []config.FieldError
	if err != nil {
		var ve *config.ValidationError
		if !errors.As(err, &ve) {
			return err
		}
		reported = ve.Errors
	}
	all := config.MergeCheck(reported, resolve(cfg, set, reg))
	if len(all) == 0 {
		return nil
	}
	return config.NewValidationError(all)
}

// resolve reports the store and middleware types cfg uses that set and reg do not register, and the rate-limit
// routes whose store type cannot rate limit. It constructs nothing and does not modify cfg.
func resolve(cfg *config.Config, set *runtime.StoreSet, reg *middleware.Registry) []config.FieldError {
	return append(set.Resolve(cfg), runtime.ResolveMiddlewares(cfg, reg)...)
}

// loadOptions returns the parse options of a gateway whose registered types are set and reg: the problems resolve
// finds are reported together with the schema problems of the document, and environment references resolve
// through lookup when it is not nil.
func loadOptions(lookup func(string) (string, bool), set *runtime.StoreSet, reg *middleware.Registry) []config.ParseOption {
	opts := []config.ParseOption{config.WithCheck(func(c *config.Config) []config.FieldError {
		return resolve(c, set, reg)
	})}
	if lookup != nil {
		opts = append(opts, config.WithEnvLookup(lookup))
	}
	return opts
}
