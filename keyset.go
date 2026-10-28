package microgateway

import (
	"context"
	"errors"
	"net/http"
)

// KeySetFetcher returns the raw JWKS JSON for a JWT provider. The gateway parses, caches and validates the keys itself, separately in each instance.
//
// The fetcher is called with the configured, environment-substituted URL. Fetches are bounded by jwks.timeout through ctx.
// Panics are recovered and treated as failed fetches.
type KeySetFetcher interface {
	FetchKeySet(ctx context.Context, provider, url string) ([]byte, error)
}

// WithKeySetFetcher replaces all HTTP fetching of JWKS documents with f. New and NewFromYAML return an error when f is nil.
func WithKeySetFetcher(f KeySetFetcher) Option {
	return func(o *options) {
		if f == nil {
			if o.err == nil {
				o.err = errors.New("microgateway: WithKeySetFetcher: nil fetcher")
			}
			return
		}
		o.fetcher = f
	}
}

// WithHTTPClient sets the client used by the default JWKS fetcher. It has no effect when WithKeySetFetcher is also given.
// New and NewFromYAML return an error when c is nil.
func WithHTTPClient(c *http.Client) Option {
	return func(o *options) {
		if c == nil {
			if o.err == nil {
				o.err = errors.New("microgateway: WithHTTPClient: nil client")
			}
			return
		}
		o.httpClient = c
	}
}
