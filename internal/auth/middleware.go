package auth

import (
	"errors"
	"log/slog"
	"net/http"
	"net/textproto"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/reqctx"
)

// Requirements are the scopes, roles and claim values a result must carry to be authorized.
type Requirements struct {
	Scopes []string
	Roles  []string
	Claims map[string]any
}

// Satisfied reports whether res carries every required scope and role and every required claim value.
func (q Requirements) Satisfied(res Result) bool {
	for _, s := range q.Scopes {
		if !slices.Contains(res.Scopes, s) {
			return false
		}
	}
	for _, role := range q.Roles {
		if !slices.Contains(res.Roles, role) {
			return false
		}
	}
	for name, want := range q.Claims {
		v, ok := res.Values[name]
		if !ok || !match(v, want) {
			return false
		}
	}
	return true
}

func match(v, want any) bool {
	switch x := v.(type) {
	case []any:
		for _, e := range x {
			if match(e, want) {
				return true
			}
		}
		return false
	case string:
		if w, ok := want.(string); ok {
			return x == w
		}
		c, ok := canonical(want)
		return ok && x == c
	case float64:
		f, ok := numericValue(want)
		return ok && x == f
	case bool:
		w, ok := want.(bool)
		return ok && x == w
	}
	return false
}

func canonical(v any) (string, bool) {
	switch x := v.(type) {
	case bool:
		return strconv.FormatBool(x), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	}
	return intText(v)
}

func numericValue(v any) (float64, bool) {
	if f, ok := v.(float64); ok {
		return f, true
	}
	s, ok := intText(v)
	if !ok {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

// RouteConfig configures the authentication middleware of one route: its providers, whether anonymous requests pass, the authorization requirements, and the failure callback.
type RouteConfig struct {
	Route          string
	Providers      []Authenticator
	AllowAnonymous bool
	Require        Requirements
	Logger         *slog.Logger
	OnFailure      func(Reason)
}

// Middleware returns the authentication middleware for c. It authenticates each request with the first provider that accepts it, answers 401 when no provider accepts and 403 when the requirements are not met, removes the credentials of the providers unless the winning one forwards them, and sets the forwarded headers of the result.
func Middleware(c RouteConfig) func(http.Handler) http.Handler {
	logger := c.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var winner Authenticator
			var winRes Result
			presented := false
			var first Reason
			for _, p := range c.Providers {
				res, err := p.Authenticate(r)
				if errors.Is(err, ErrNoCredentials) {
					continue
				}
				if err != nil {
					if !presented {
						first = ReasonOf(err)
					}
					presented = true
					continue
				}
				winner, winRes = p, res
				break
			}

			if winner == nil && !presented && c.AllowAnonymous {
				next.ServeHTTP(w, r)
				return
			}

			if winner == nil {
				reason := first
				if !presented {
					reason = ReasonMissing
				}
				w.Header().Set("WWW-Authenticate", c.Providers[0].Challenge())
				httperr.Write(w, r, http.StatusUnauthorized, httperr.CodeUnauthorized, httperr.Message(httperr.CodeUnauthorized))
				if c.OnFailure != nil {
					c.OnFailure(reason)
				}
				logger.DebugContext(r.Context(), "request not authenticated",
					"route", c.Route,
					"reason", string(reason),
					"request_id", reqctx.RequestID(r.Context()))
				return
			}

			if !c.Require.Satisfied(winRes) {
				httperr.Write(w, r, http.StatusForbidden, httperr.CodeForbidden, httperr.Message(httperr.CodeForbidden))
				if c.OnFailure != nil {
					c.OnFailure(ReasonForbidden)
				}
				logger.DebugContext(r.Context(), "request not authorized",
					"route", c.Route,
					"provider", winner.Name(),
					"request_id", reqctx.RequestID(r.Context()))
				return
			}

			r = r.WithContext(reqctx.WithIdentity(r.Context(), winRes.Identity))

			wh, wq := winner.Credentials()
			keep := winner.ForwardCredentials()
			for _, p := range c.Providers {
				h, q := p.Credentials()
				if h != "" && (!keep || !strings.EqualFold(h, wh)) {
					r.Header.Del(h)
				}
				if q != "" && (!keep || q != wq) {
					r.URL = withoutQueryParam(r.URL, q)
				}
			}

			for name, value := range winRes.Forward {
				r.Header.Set(name, value)
			}

			next.ServeHTTP(w, r)
		})
	}
}

func withoutQueryParam(u *url.URL, name string) *url.URL {
	parts := strings.Split(u.RawQuery, "&")
	kept := make([]string, 0, len(parts))
	found := false
	for _, part := range parts {
		k, _ := queryPart(part)
		if k == name {
			found = true
			continue
		}
		kept = append(kept, part)
	}
	if !found {
		return u
	}
	c := *u
	c.RawQuery = strings.Join(kept, "&")
	return &c
}

// Presented reports whether r carries a credential that one of providers reads, or whether an outer
// stage already authenticated it (the credential may have been removed since).
func Presented(r *http.Request, providers []Authenticator) bool {
	if len(r.Header.Values("Authorization")) > 0 {
		return true
	}
	for _, p := range providers {
		h, q := p.Credentials()
		if h != "" && len(r.Header.Values(h)) > 0 {
			return true
		}
		if q != "" && queryPresent(r.URL.RawQuery, q) {
			return true
		}
	}
	_, ok := reqctx.IdentityFrom(r.Context())
	return ok
}

func queryPresent(raw, name string) bool {
	for _, part := range strings.Split(raw, "&") {
		if k, _ := queryPart(part); k == name {
			return true
		}
	}
	return false
}

// Strip returns a middleware that deletes the named headers, canonicalized and without duplicates, from each request before the next handler.
func Strip(headers []string) func(http.Handler) http.Handler {
	names := make([]string, 0, len(headers))
	seen := make(map[string]bool, len(headers))
	for _, h := range headers {
		n := textproto.CanonicalMIMEHeaderKey(h)
		if !seen[n] {
			seen[n] = true
			names = append(names, n)
		}
	}
	if len(names) == 0 {
		return func(next http.Handler) http.Handler {
			return next
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for _, n := range names {
				r.Header.Del(n)
			}
			next.ServeHTTP(w, r)
		})
	}
}
