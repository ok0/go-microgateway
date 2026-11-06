// Package middleware composes request stages: fixed framework stages, built-in configurable middleware, user middleware and the per-route pipeline.
package middleware

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/reqctx"
)

// Middleware wraps a handler with one stage of request processing.
type Middleware = func(http.Handler) http.Handler

// Chain wraps final with mws so that the first entry is the outermost. Nil entries are skipped.
func Chain(final http.Handler, mws ...Middleware) http.Handler {
	h := final
	for i := len(mws) - 1; i >= 0; i-- {
		if mws[i] == nil {
			continue
		}
		h = mws[i](h)
	}
	return h
}

// Recover returns a middleware that turns a panic in the next handler into a 500 internal error response and logs it with the request ID. http.ErrAbortHandler is re-panicked.
func Recover(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				v := recover()
				if v == nil {
					return
				}
				if v == http.ErrAbortHandler {
					panic(v)
				}
				logger.Error("panic recovered",
					"panic", fmt.Sprint(v),
					"request_id", reqctx.RequestID(r.Context()),
					"stack", string(debug.Stack()))
				httperr.Write(w, r, http.StatusInternalServerError, httperr.CodeInternal, httperr.Message(httperr.CodeInternal))
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// RequestID returns a middleware that keeps a valid request ID from header or generates one, sets it on the request and response header, and stores it in the request context.
func RequestID(header string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(header)
			if !ValidRequestID(id) {
				id = newID()
			}
			r.Header.Set(header, id)
			w.Header().Set(header, id)
			next.ServeHTTP(w, r.WithContext(reqctx.WithRequestID(r.Context(), id)))
		})
	}
}

// ValidRequestID reports whether s is 1 to 128 printable ASCII bytes without spaces.
func ValidRequestID(s string) bool {
	if len(s) < 1 || len(s) > 128 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// ClientIP returns a middleware that stores the client IP, resolved through t, in the request context.
func ClientIP(t *reqctx.TrustedProxies) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(reqctx.WithClientIP(r.Context(), t.ClientIP(r))))
		})
	}
}

// Timeout ends the request context d after the current time of clk (nil: real clock).
func Timeout(d time.Duration, clk clock.Clock) Middleware {
	if d <= 0 {
		return nil
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := clock.WithTimeout(r.Context(), clk, d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
