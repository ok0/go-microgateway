package lychgate

import (
	"errors"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/middleware"
	"github.com/ok0/lychgate/internal/runtime"
)

// Validate reports whether cfg is a valid configuration for a gateway built with opts. Besides the schema rules of
// the configuration, it reports store and middleware types that are not registered, rate-limit routes that reference
// a store that is missing or whose type cannot rate limit, and any problem found in the registered store options.
// Validate never calls a store factory, opens a connection or mutates cfg. A nil cfg is an error.
func Validate(cfg *config.Config, opts ...Option) error {
	if cfg == nil {
		return errors.New("lychgate: nil config")
	}
	o := newOptions(opts)
	if o.err != nil {
		return o.err
	}
	reg, err := o.middlewareRegistry()
	if err != nil {
		return err
	}
	return checkConfig(cfg.Clone(), storeSet(o.stores), reg)
}

func checkConfig(c *config.Config, set *runtime.StoreSet, reg *middleware.Registry) error {
	return withResolution(c.Validate(), c, set, reg)
}

// withResolution adds the store and middleware resolution problems of cfg to err when err is a validation error,
// or reports them alone when err is nil. Other errors are returned unchanged.
func withResolution(err error, cfg *config.Config, set *runtime.StoreSet, reg *middleware.Registry) error {
	var all []config.FieldError
	if err != nil {
		var ve *config.ValidationError
		if !errors.As(err, &ve) {
			return err
		}
		all = append(all, ve.Errors...)
	}
	all = append(all, set.Resolve(cfg)...)
	all = append(all, runtime.ResolveMiddlewares(cfg, reg)...)
	if len(all) == 0 {
		return nil
	}
	return config.NewValidationError(all)
}
