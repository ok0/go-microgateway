package runtime

import (
	"log/slog"
	"net/http"
	"slices"

	"github.com/ok0/go-microgateway/internal/auth"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/proxy"
	"github.com/ok0/go-microgateway/internal/reqctx"
)

// Step ids of the request flow. A snapshot built with Deps.FlowTrace records them on entry; the stage ids are the
// names in middleware.DefaultStages and the attempt ids are proxy.StepLoadBalancer and proxy.StepUpstream.
const (
	StepRequestID       = "request_id"
	StepClientIP        = "client_ip"
	StepObserve         = "observe"
	StepRecover         = "recover"
	StepStripHeaders    = "strip_headers"
	StepCanonicalPath   = "canonical_path"
	StepRouteMatch      = "route_match"
	StepRouteMiddleware = "route_middleware"
	StepProxy           = "proxy"
)

// entryFlow lists, in order, the steps a request passes before route dispatch. The entry table has one row per id.
var entryFlow = []string{StepRequestID, StepClientIP, StepObserve, StepRecover, StepStripHeaders, StepCanonicalPath, StepRouteMatch}

// Flow returns the ordered request-flow step ids: the steps before route dispatch, route_middleware, the default
// pipeline stages in default order, then proxy, load_balancer and upstream. Each call returns a new slice.
func Flow() []string {
	out := make([]string, 0, len(entryFlow)+1+len(middleware.DefaultStages)+3)
	out = append(out, entryFlow...)
	out = append(out, StepRouteMiddleware)
	out = append(out, middleware.DefaultStages...)
	return append(out, StepProxy, proxy.StepLoadBalancer, proxy.StepUpstream)
}

// flowStep is one row of the entry table: a step id and the middleware that runs the step. A nil middleware is skipped.
type flowStep struct {
	id string
	mw middleware.Middleware
}

// tracer installs the recording wrappers of a build. With a nil record it returns every input unchanged and installs nothing.
type tracer struct {
	record func(string)
	links  int // recording wrappers installed
}

// wrap returns mw preceded by a call to record(id) on entry. It returns mw unchanged when mw or record is nil.
func (t *tracer) wrap(id string, mw middleware.Middleware) middleware.Middleware {
	if t.record == nil || mw == nil {
		return mw
	}
	t.links++
	record := t.record
	return func(next http.Handler) http.Handler {
		inner := mw(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			record(id)
			inner.ServeHTTP(w, r)
		})
	}
}

// handler returns h preceded by a call to record(id) on entry. It returns h unchanged when record is nil.
func (t *tracer) handler(id string, h http.Handler) http.Handler {
	if t.record == nil {
		return h
	}
	t.links++
	record := t.record
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		record(id)
		h.ServeHTTP(w, r)
	})
}

// stageID returns the step id of the route-chain entry named name: the stage name of a built-in stage, otherwise route_middleware.
func stageID(name string) string {
	if slices.Contains(middleware.DefaultStages, name) {
		return name
	}
	return StepRouteMiddleware
}

// lastStep returns a middleware that serves h and ignores the next handler. It ends the entry table.
func lastStep(h http.Handler) middleware.Middleware {
	return func(http.Handler) http.Handler { return h }
}

// entryTable returns the entry table of s, first row outermost. The strip row is nil when strip is empty.
func (s *Snapshot) entryTable(requestIDHeader string, trusted *reqctx.TrustedProxies, observe middleware.Middleware, logger *slog.Logger, strip []string) []flowStep {
	var stripHeaders middleware.Middleware
	if len(strip) > 0 {
		stripHeaders = auth.Strip(strip)
	}
	return []flowStep{
		{StepRequestID, middleware.RequestID(requestIDHeader)},
		{StepClientIP, middleware.ClientIP(trusted)},
		{StepObserve, observe},
		{StepRecover, middleware.Recover(logger)},
		{StepStripHeaders, stripHeaders},
		{StepCanonicalPath, canonicalPath},
		{StepRouteMatch, lastStep(http.HandlerFunc(s.dispatch))},
	}
}

// chainEntry chains rows, first row outermost, passing each through t. The last row must be a lastStep.
func chainEntry(rows []flowStep, t *tracer) http.Handler {
	var h http.Handler
	for i := len(rows) - 1; i >= 0; i-- {
		if mw := t.wrap(rows[i].id, rows[i].mw); mw != nil {
			h = mw(h)
		}
	}
	return h
}
