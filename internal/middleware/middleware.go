// Package middleware composes request stages: fixed framework stages, built-in configurable middleware, user middleware and the per-route pipeline.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/reqctx"
)

type Middleware = func(http.Handler) http.Handler

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

func ClientIP(t *reqctx.TrustedProxies) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(reqctx.WithClientIP(r.Context(), t.ClientIP(r))))
		})
	}
}

func Timeout(d time.Duration) Middleware {
	if d <= 0 {
		return nil
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
