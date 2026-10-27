package lychgate

import (
	"context"
	"net/http"

	"github.com/ok0/lychgate/internal/reqctx"
)

func RequestIDFromContext(ctx context.Context) string {
	return reqctx.RequestID(ctx)
}

func PathParam(r *http.Request, name string) string {
	return reqctx.ParamValue(r.Context(), name)
}
