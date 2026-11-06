package middleware

import "slices"

// Names of the built-in pipeline stages, as used in DefaultStages and in a route pipeline.
const (
	StageTimeout        = "timeout"
	StageAuth           = "auth"
	StageRateLimit      = "rate_limit"
	StageCache          = "cache"
	StageCircuitBreaker = "circuit_breaker"
	StageRetry          = "retry"
)

// DefaultStages is the built-in stage order used when a route sets no explicit pipeline.
var DefaultStages = []string{StageTimeout, StageAuth, StageRateLimit, StageCache, StageCircuitBreaker, StageRetry}

// Order resolves the stage names of a route, outermost first. Without an explicit pipeline the route middlewares are followed by DefaultStages; with one, route middlewares not named in the pipeline precede it.
func Order(routeMiddlewares, pipeline []string) []string {
	if len(pipeline) == 0 {
		out := make([]string, 0, len(routeMiddlewares)+len(DefaultStages))
		out = append(out, routeMiddlewares...)
		return append(out, DefaultStages...)
	}
	var out []string
	for _, name := range routeMiddlewares {
		if !slices.Contains(pipeline, name) {
			out = append(out, name)
		}
	}
	return append(out, pipeline...)
}

// Pipeline returns the middleware for each name in order that has a non-nil entry in stages.
func Pipeline(order []string, stages map[string]Middleware) []Middleware {
	var out []Middleware
	for _, name := range order {
		if mw := stages[name]; mw != nil {
			out = append(out, mw)
		}
	}
	return out
}
