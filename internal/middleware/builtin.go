package middleware

import (
	"bufio"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/httperr"
)

// Headers returns a middleware that edits request headers before the handler runs and response headers before the response is sent.
func Headers(p config.HeadersParams) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			applyHeaderOps(r.Header, p.Request)
			if hasHeaderOps(p.Response) {
				w = &hookWriter{ResponseWriter: w, before: func(h http.Header) {
					applyHeaderOps(h, p.Response)
				}}
			}
			next.ServeHTTP(w, r)
		})
	}
}

func hasHeaderOps(ops config.HeaderOps) bool {
	return len(ops.Add) > 0 || len(ops.Set) > 0 || len(ops.Remove) > 0
}

func applyHeaderOps(h http.Header, ops config.HeaderOps) {
	for _, name := range ops.Remove {
		h.Del(name)
	}
	for _, k := range slices.Sorted(maps.Keys(ops.Set)) {
		h.Set(k, ops.Set[k])
	}
	for _, k := range slices.Sorted(maps.Keys(ops.Add)) {
		h.Add(k, ops.Add[k])
	}
}

// hookWriter runs before once, just before the first final status is sent.
type hookWriter struct {
	http.ResponseWriter
	before func(http.Header)
	fired  bool
}

func (hw *hookWriter) fire(code int) {
	if hw.fired || (code >= 100 && code < 200 && code != http.StatusSwitchingProtocols) {
		return
	}
	hw.fired = true
	hw.before(hw.Header())
}

func (hw *hookWriter) WriteHeader(code int) {
	hw.fire(code)
	hw.ResponseWriter.WriteHeader(code)
}

func (hw *hookWriter) Write(b []byte) (int, error) {
	hw.fire(http.StatusOK)
	return hw.ResponseWriter.Write(b)
}

func (hw *hookWriter) Flush() {
	hw.fire(http.StatusOK)
	_ = http.NewResponseController(hw.ResponseWriter).Flush()
}

func (hw *hookWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(hw.ResponseWriter).Hijack()
}

func (hw *hookWriter) Unwrap() http.ResponseWriter {
	return hw.ResponseWriter
}

// RequestSizeLimit returns a middleware that rejects request bodies larger than p.MaxBytes with 413 and a JSON error. A body that exceeds the limit while the handler reads it is reported to the handler as *http.MaxBytesError, and the 413 replaces the handler's response if it has not started yet.
func RequestSizeLimit(p config.RequestSizeLimitParams) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > p.MaxBytes {
				httperr.Write(w, r, http.StatusRequestEntityTooLarge, httperr.CodePayloadTooLarge, httperr.Message(httperr.CodePayloadTooLarge))
				return
			}
			if r.Body == nil || r.Body == http.NoBody {
				next.ServeHTTP(w, r)
				return
			}
			exceeded := new(atomic.Bool)
			r2 := r.WithContext(r.Context())
			r2.Body = &limitBody{rc: r.Body, n: p.MaxBytes, limit: p.MaxBytes, exceeded: exceeded}
			lw := &limitWriter{ResponseWriter: w, r: r, exceeded: exceeded, saved: w.Header().Clone()}
			next.ServeHTTP(lw, r2)
		})
	}
}

type limitBody struct {
	rc       io.ReadCloser
	n        int64
	limit    int64
	exceeded *atomic.Bool
	err      error
}

func (b *limitBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if int64(len(p)) > b.n+1 {
		p = p[:b.n+1]
	}
	k, err := b.rc.Read(p)
	if int64(k) <= b.n {
		b.n -= int64(k)
		b.err = err
		return k, err
	}
	b.exceeded.Store(true)
	b.err = &http.MaxBytesError{Limit: b.limit}
	return int(b.n), b.err
}

func (b *limitBody) Close() error {
	return b.rc.Close()
}

// limitWriter decides once, at the first final status, header or body write, whether the response is replaced by a 413.
type limitWriter struct {
	http.ResponseWriter
	r        *http.Request
	exceeded *atomic.Bool
	saved    http.Header
	decided  bool
	replaced bool
}

func (lw *limitWriter) decide() {
	if lw.decided {
		return
	}
	lw.decided = true
	if !lw.exceeded.Load() {
		return
	}
	h := lw.Header()
	clear(h)
	maps.Copy(h, lw.saved)
	httperr.Write(lw.ResponseWriter, lw.r, http.StatusRequestEntityTooLarge, httperr.CodePayloadTooLarge, httperr.Message(httperr.CodePayloadTooLarge))
	lw.replaced = true
}

func (lw *limitWriter) WriteHeader(code int) {
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		lw.ResponseWriter.WriteHeader(code)
		return
	}
	lw.decide()
	if lw.replaced {
		return
	}
	lw.ResponseWriter.WriteHeader(code)
}

func (lw *limitWriter) Write(b []byte) (int, error) {
	lw.decide()
	if lw.replaced {
		return len(b), nil
	}
	return lw.ResponseWriter.Write(b)
}

func (lw *limitWriter) Flush() {
	lw.decide()
	if !lw.replaced {
		_ = http.NewResponseController(lw.ResponseWriter).Flush()
	}
}

func (lw *limitWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(lw.ResponseWriter).Hijack()
}

func (lw *limitWriter) Unwrap() http.ResponseWriter {
	return lw.ResponseWriter
}

// CORS returns a middleware implementing cross-origin resource sharing for the origins, methods and headers in p. Preflight requests are answered directly; other requests carrying an Origin header are passed to the handler with the CORS response headers applied.
func CORS(p config.CORSParams) Middleware {
	anyOrigin := slices.Contains(p.AllowedOrigins, "*")
	wildcard := anyOrigin && !p.AllowCredentials
	origins := make(map[string]bool, len(p.AllowedOrigins))
	for _, o := range p.AllowedOrigins {
		origins[o] = true
	}
	methods := strings.Join(p.AllowedMethods, ", ")
	allowedHeaders := strings.Join(p.AllowedHeaders, ", ")
	exposeHeaders := strings.Join(p.ExposeHeaders, ", ")
	maxAge := strconv.Itoa(p.MaxAge)
	allowed := func(origin string) bool {
		return anyOrigin || origins[origin]
	}
	setAllowOrigin := func(h http.Header, origin string) {
		if wildcard {
			h.Set("Access-Control-Allow-Origin", "*")
		} else {
			h.Set("Access-Control-Allow-Origin", origin)
		}
		if p.AllowCredentials {
			h.Set("Access-Control-Allow-Credentials", "true")
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				next.ServeHTTP(w, r)
				return
			}
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				h := w.Header()
				if !wildcard {
					addVary(h, "Origin")
				}
				if !allowed(origin) {
					httperr.Write(w, r, http.StatusForbidden, httperr.CodeForbidden, httperr.Message(httperr.CodeForbidden))
					return
				}
				setAllowOrigin(h, origin)
				h.Set("Access-Control-Allow-Methods", methods)
				if allowedHeaders != "" {
					h.Set("Access-Control-Allow-Headers", allowedHeaders)
				}
				if p.MaxAge > 0 {
					h.Set("Access-Control-Max-Age", maxAge)
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			hw := &hookWriter{ResponseWriter: w, before: func(h http.Header) {
				if !wildcard {
					addVary(h, "Origin")
				}
				if !allowed(origin) {
					return
				}
				setAllowOrigin(h, origin)
				if exposeHeaders != "" {
					h.Set("Access-Control-Expose-Headers", exposeHeaders)
				}
			}}
			next.ServeHTTP(hw, r)
		})
	}
}

// addVary appends v to the Vary header unless a value already lists it, comparing case-insensitively.
func addVary(h http.Header, v string) {
	for _, line := range h.Values("Vary") {
		for _, part := range strings.Split(line, ",") {
			if strings.EqualFold(strings.TrimSpace(part), v) {
				return
			}
		}
	}
	h.Add("Vary", v)
}
