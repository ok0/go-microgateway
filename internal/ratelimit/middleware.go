package ratelimit

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/reqctx"
)

type KeyFunc func(*http.Request) string

const (
	SourceIP       = "ip"
	SourceConsumer = "consumer"
	SourceHeader   = "header"
	SourceRoute    = "route"
)

func NewKeyFunc(source, header string) (KeyFunc, error) {
	switch source {
	case SourceIP:
		return ipKey, nil
	case SourceConsumer:
		return consumerKey, nil
	case SourceHeader:
		return func(r *http.Request) string {
			if v := r.Header.Get(header); v != "" {
				return "header:" + v
			}
			return ipKey(r)
		}, nil
	case SourceRoute:
		return routeKey, nil
	}
	return nil, fmt.Errorf("ratelimit: unknown key source %q", source)
}

func ipKey(r *http.Request) string {
	addr := reqctx.ClientIP(r.Context())
	if !addr.IsValid() {
		return "ip:unknown"
	}
	return "ip:" + addr.String()
}

func consumerKey(r *http.Request) string {
	if id, ok := reqctx.IdentityFrom(r.Context()); ok && id.Consumer != "" {
		return "consumer:" + id.Consumer
	}
	return ipKey(r)
}

func routeKey(*http.Request) string {
	return "route"
}

// Middleware enforces l on each request. The key function selects the rate
// limit key, and onLimited, when non-nil, is called for each 429 response.
// A Deny decision from the failure policy is answered with 503 and does not
// call next or onLimited.
func Middleware(l *Limiter, key KeyFunc, onLimited func()) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res := l.Take(r.Context(), key(r))
			if res.Policy == Deny {
				w.Header().Set("Retry-After", strconv.FormatInt(max(1, ceilSeconds(l.p.DenyRetryAfter)), 10))
				httperr.Write(w, r, http.StatusServiceUnavailable, httperr.CodeRateLimitUnavailable, httperr.Message(httperr.CodeRateLimitUnavailable))
				return
			}
			if res.Degraded {
				next.ServeHTTP(w, r)
				return
			}
			d := res.Decision
			h := w.Header()
			h.Set("RateLimit-Limit", strconv.FormatInt(d.Limit, 10))
			h.Set("RateLimit-Remaining", strconv.FormatInt(d.Remaining, 10))
			h.Set("RateLimit-Reset", strconv.FormatInt(ceilSeconds(d.ResetAfter), 10))
			if d.Allowed {
				next.ServeHTTP(w, r)
				return
			}
			h.Set("Retry-After", strconv.FormatInt(max(1, ceilSeconds(d.RetryAfter)), 10))
			if onLimited != nil {
				onLimited()
			}
			httperr.Write(w, r, http.StatusTooManyRequests, httperr.CodeRateLimited, httperr.Message(httperr.CodeRateLimited))
		})
	}
}

func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}
