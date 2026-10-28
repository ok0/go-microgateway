package ratelimit

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/memstore"
	"github.com/ok0/lychgate/internal/reqctx"
	"github.com/ok0/lychgate/store"
)

func prepared(path, ip string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, path, nil)
	ctx := reqctx.WithRequestID(r.Context(), "req-1")
	if ip != "" {
		ctx = reqctx.WithClientIP(ctx, netip.MustParseAddr(ip))
	}
	return r.WithContext(ctx)
}

func TestHeadersAllowedAndDenied(t *testing.T) {
	clk := newClock()
	rl := memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 10, Clock: clk})
	t.Cleanup(func() { _ = rl.Close() })
	lim := New(Policy{ID: "api", Limit: tbLimit, Store: rl, Clock: clk})
	key, err := NewKeyFunc(SourceIP, "")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	})
	limited := 0
	h := Middleware(lim, key, func() { limited++ })(next)

	want := [][3]string{{"3", "2", "1"}, {"3", "1", "1"}, {"3", "0", "1"}}
	for i, w := range want {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, prepared("/x", "203.0.113.7"))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i+1, rec.Code)
		}
		got := [3]string{rec.Header().Get("RateLimit-Limit"), rec.Header().Get("RateLimit-Remaining"), rec.Header().Get("RateLimit-Reset")}
		if got != [3]string(w) {
			t.Fatalf("request %d: headers = %v, want %v", i+1, got, w)
		}
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, prepared("/x", "203.0.113.7"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("4th status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("RateLimit-Remaining"); got != "0" {
		t.Fatalf("4th RateLimit-Remaining = %q, want 0", got)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("4th Retry-After = %q, want 1", got)
	}
	var body struct {
		Error struct {
			Status    int    `json:"status"`
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	if body.Error.Code != "rate_limited" || body.Error.Status != http.StatusTooManyRequests || body.Error.RequestID != "req-1" {
		t.Fatalf("body = %+v", body)
	}
	if calls != 3 {
		t.Fatalf("next calls = %d, want 3", calls)
	}
	if limited != 1 {
		t.Fatalf("onLimited calls = %d, want 1", limited)
	}
}

func TestDeniedWithoutOnLimited(t *testing.T) {
	stub := &recStore{dec: store.Decision{Allowed: false, Limit: 3, RetryAfter: time.Second}}
	lim := New(Policy{ID: "api", Limit: tbLimit, Store: stub, Clock: newClock()})
	key, _ := NewKeyFunc(SourceRoute, "")
	rec := httptest.NewRecorder()
	Middleware(lim, key, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("next must not run when denied")
	})).ServeHTTP(rec, prepared("/x", "203.0.113.7"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
}

func TestRetryAfterRounding(t *testing.T) {
	cases := []struct {
		name       string
		dec        store.Decision
		wantRetry  string
		wantReset  string
		checkRetry bool
	}{
		{"denied retry 1ns", store.Decision{RetryAfter: time.Nanosecond}, "1", "0", true},
		{"denied retry 1s", store.Decision{RetryAfter: time.Second}, "1", "0", true},
		{"denied retry 1001ms", store.Decision{RetryAfter: 1001 * time.Millisecond}, "2", "0", true},
		{"denied retry 0", store.Decision{RetryAfter: 0}, "1", "0", true},
		{"allowed reset 0", store.Decision{Allowed: true, ResetAfter: 0}, "", "0", false},
		{"allowed reset 1500ms", store.Decision{Allowed: true, ResetAfter: 1500 * time.Millisecond}, "", "2", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stub := &recStore{dec: c.dec}
			lim := New(Policy{ID: "api", Limit: tbLimit, Store: stub, Clock: newClock()})
			key, _ := NewKeyFunc(SourceRoute, "")
			calls := 0
			rec := httptest.NewRecorder()
			Middleware(lim, key, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				calls++
			})).ServeHTTP(rec, prepared("/x", ""))
			if got := rec.Header().Get("RateLimit-Reset"); got != c.wantReset {
				t.Fatalf("RateLimit-Reset = %q, want %q", got, c.wantReset)
			}
			if c.checkRetry {
				if rec.Code != http.StatusTooManyRequests {
					t.Fatalf("status = %d, want 429", rec.Code)
				}
				if got := rec.Header().Get("Retry-After"); got != c.wantRetry {
					t.Fatalf("Retry-After = %q, want %q", got, c.wantRetry)
				}
			} else {
				if calls != 1 {
					t.Fatalf("next calls = %d, want 1", calls)
				}
				if got := rec.Header().Get("Retry-After"); got != "" {
					t.Fatalf("Retry-After on allowed = %q, want empty", got)
				}
			}
		})
	}
}

func TestDegradedPassesThrough(t *testing.T) {
	stub := &recStore{err: store.ErrUnavailable}
	lim := New(Policy{ID: "api", Limit: tbLimit, Store: stub, Clock: newClock()})
	key, _ := NewKeyFunc(SourceIP, "")
	calls := 0
	rec := httptest.NewRecorder()
	Middleware(lim, key, func() { t.Fatal("onLimited must not run when degraded") })(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusOK)
	})).ServeHTTP(rec, prepared("/x", "203.0.113.7"))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if calls != 1 {
		t.Fatalf("next calls = %d, want 1", calls)
	}
	for _, h := range []string{"RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset", "Retry-After"} {
		if got := rec.Header().Get(h); got != "" {
			t.Fatalf("%s = %q, want absent", h, got)
		}
	}
}

func TestKeyFuncs(t *testing.T) {
	withIdentity := func(id reqctx.Identity) func(*http.Request) *http.Request {
		return func(r *http.Request) *http.Request {
			return r.WithContext(reqctx.WithIdentity(r.Context(), id))
		}
	}
	withHeader := func(name, value string) func(*http.Request) *http.Request {
		return func(r *http.Request) *http.Request {
			r.Header.Set(name, value)
			return r
		}
	}
	withIP := func(ip string) func(*http.Request) *http.Request {
		return func(r *http.Request) *http.Request {
			return r.WithContext(reqctx.WithClientIP(r.Context(), netip.MustParseAddr(ip)))
		}
	}
	build := func(path string, mods ...func(*http.Request) *http.Request) *http.Request {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		for _, m := range mods {
			r = m(r)
		}
		return r
	}

	cases := []struct {
		name   string
		source string
		header string
		req    *http.Request
		want   string
	}{
		{"ip first client", SourceIP, "", build("/x", withIP("203.0.113.7")), "ip:203.0.113.7"},
		{"ip second client", SourceIP, "", build("/x", withIP("203.0.113.8")), "ip:203.0.113.8"},
		{"ip invalid", SourceIP, "", build("/x"), "ip:unknown"},
		{"consumer identified", SourceConsumer, "", build("/x", withIP("203.0.113.7"), withIdentity(reqctx.Identity{Consumer: "c1"})), "consumer:c1"},
		{"consumer no identity", SourceConsumer, "", build("/x", withIP("203.0.113.7")), "ip:203.0.113.7"},
		{"consumer empty name", SourceConsumer, "", build("/x", withIP("203.0.113.7"), withIdentity(reqctx.Identity{Consumer: ""})), "ip:203.0.113.7"},
		{"header present", SourceHeader, "X-Tenant", build("/x", withIP("203.0.113.7"), withHeader("X-Tenant", "t1")), "header:t1"},
		{"header absent", SourceHeader, "X-Tenant", build("/x", withIP("203.0.113.7")), "ip:203.0.113.7"},
		{"route", SourceRoute, "", build("/x", withIP("203.0.113.7")), "route"},
		{"route other client", SourceRoute, "", build("/x", withIP("203.0.113.9")), "route"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			kf, err := NewKeyFunc(c.source, c.header)
			if err != nil {
				t.Fatal(err)
			}
			if got := kf(c.req); got != c.want {
				t.Fatalf("key = %q, want %q", got, c.want)
			}
		})
	}

	for _, source := range []string{SourceIP, SourceConsumer, SourceHeader, SourceRoute} {
		kf, err := NewKeyFunc(source, "X-Tenant")
		if err != nil {
			t.Fatal(err)
		}
		a := kf(build("/one", withIP("203.0.113.7"), withHeader("X-Tenant", "t1"), withIdentity(reqctx.Identity{Consumer: "c1"})))
		b := kf(build("/two/deeper", withIP("203.0.113.7"), withHeader("X-Tenant", "t1"), withIdentity(reqctx.Identity{Consumer: "c1"})))
		if a != b {
			t.Fatalf("source %s: path changed key %q -> %q", source, a, b)
		}
	}
}

func TestNewKeyFuncUnknownSource(t *testing.T) {
	kf, err := NewKeyFunc("cookie", "")
	if err == nil {
		t.Fatalf("NewKeyFunc(cookie) error = nil, want error")
	}
	if kf != nil {
		t.Fatalf("NewKeyFunc(cookie) key func non-nil")
	}
}
