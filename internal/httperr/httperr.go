// Package httperr writes gateway-originated JSON errors and maps errors to statuses.
package httperr

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"

	"github.com/ok0/go-microgateway/internal/reqctx"
)

// Error codes carried in the code field of gateway-originated JSON error bodies.
const (
	CodeRouteNotFound        = "route_not_found"
	CodeUnauthorized         = "unauthorized"
	CodeForbidden            = "forbidden"
	CodePayloadTooLarge      = "payload_too_large"
	CodeRateLimited          = "rate_limited"
	CodeRateLimitUnavailable = "rate_limit_unavailable"
	CodeBadGateway           = "bad_gateway"
	CodeCircuitOpen          = "circuit_open"
	CodeNoHealthyUpstream    = "no_healthy_upstream"
	CodeGatewayTimeout       = "gateway_timeout"
	CodeInternal             = "internal_error"
	CodeClientClosed         = "client_closed_request"
	CodeNotFound             = "not_found"
	CodeMethodNotAllowed     = "method_not_allowed"
	CodeUnavailable          = "service_unavailable"
	CodeNotCached            = "not_cached"
	CodeBadRequest           = "bad_request"
	CodeStoreUnavailable     = "store_unavailable"
	CodeCacheNotConfigured   = "cache_not_configured"
)

// StatusClientClosed is the status for a request the client closed before the gateway responded. Nothing is written for it.
const StatusClientClosed = 499

// ErrNoHealthyUpstream reports that no healthy upstream host is available for a request.
var ErrNoHealthyUpstream = errors.New("no healthy upstream")

// ErrCircuitOpen reports that the upstream circuit breaker rejected a request.
var ErrCircuitOpen = errors.New("circuit open")

type body struct {
	Status    int    `json:"status"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// Write writes a JSON error body with the given status, code and message. Nothing is written for StatusClientClosed.
func Write(w http.ResponseWriter, r *http.Request, status int, code, msg string) {
	if status == StatusClientClosed {
		return
	}
	b, _ := json.Marshal(struct {
		Error body `json:"error"`
	}{Error: body{
		Status:    status,
		Code:      code,
		Message:   msg,
		RequestID: reqctx.RequestID(r.Context()),
	}})
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Cache-Control", "no-store")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Del("Content-Length")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}

// FromError maps err to an HTTP status and error code.
func FromError(err error) (status int, code string) {
	var ne net.Error
	var oe *net.OpError
	switch {
	case errors.Is(err, context.Canceled):
		return StatusClientClosed, CodeClientClosed
	case errors.As(err, &oe) && oe.Op == "dial":
		return http.StatusBadGateway, CodeBadGateway
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, CodeGatewayTimeout
	case errors.Is(err, ErrNoHealthyUpstream):
		return http.StatusServiceUnavailable, CodeNoHealthyUpstream
	case errors.Is(err, ErrCircuitOpen):
		return http.StatusServiceUnavailable, CodeCircuitOpen
	case errors.As(err, &ne) && ne.Timeout():
		return http.StatusGatewayTimeout, CodeGatewayTimeout
	default:
		return http.StatusBadGateway, CodeBadGateway
	}
}

// Message returns the fixed client-facing message for code.
func Message(code string) string {
	switch code {
	case CodeRouteNotFound:
		return "no route matches the request"
	case CodeBadGateway:
		return "upstream connection failed"
	case CodeGatewayTimeout:
		return "upstream timed out"
	case CodeNoHealthyUpstream:
		return "no healthy upstream host"
	case CodeCircuitOpen:
		return "upstream circuit is open"
	case CodeInternal:
		return "internal server error"
	case CodeUnauthorized:
		return "unauthorized"
	case CodeForbidden:
		return "forbidden"
	case CodePayloadTooLarge:
		return "request body too large"
	case CodeRateLimited:
		return "rate limit exceeded"
	case CodeRateLimitUnavailable:
		return "rate limiting is temporarily unavailable"
	case CodeClientClosed:
		return "client closed request"
	case CodeNotFound:
		return "not found"
	case CodeMethodNotAllowed:
		return "method not allowed"
	case CodeUnavailable:
		return "the gateway is not accepting requests"
	case CodeNotCached:
		return "response not available from cache"
	case CodeBadRequest:
		return "invalid request"
	case CodeStoreUnavailable:
		return "the cache store is unavailable"
	case CodeCacheNotConfigured:
		return "no response cache is configured"
	default:
		return "error"
	}
}
