package microgateway

import (
	"context"
	"net/http"

	"github.com/ok0/go-microgateway/internal/reqctx"
)

// Identity is the authenticated caller of a request.
type Identity = reqctx.Identity

func RequestIDFromContext(ctx context.Context) string {
	return reqctx.RequestID(ctx)
}

// IdentityFromContext returns the identity attached to ctx. It reports false when no authentication ran.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	return reqctx.IdentityFrom(ctx)
}

func PathParam(r *http.Request, name string) string {
	return reqctx.ParamValue(r.Context(), name)
}
