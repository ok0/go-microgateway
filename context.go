package microgateway

import (
	"context"
	"net/http"

	"github.com/ok0/go-microgateway/internal/reqctx"
)

// Identity is the authenticated caller of a request.
type Identity = reqctx.Identity

// RequestIDFromContext returns the request ID attached to ctx, or the empty string when none is attached.
func RequestIDFromContext(ctx context.Context) string {
	return reqctx.RequestID(ctx)
}

// IdentityFromContext returns the identity attached to ctx. It reports false when no authentication ran.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	return reqctx.IdentityFrom(ctx)
}

// PathParam returns the value of the named path parameter of the matched route, or the empty string when the request has no such parameter.
func PathParam(r *http.Request, name string) string {
	return reqctx.ParamValue(r.Context(), name)
}
