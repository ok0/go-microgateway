package ratelimit

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/store"
)

const (
	policyTimeout     = 500 * time.Millisecond
	policyOpenTimeout = 1500 * time.Millisecond
)

func policySettings() storeguard.Settings {
	return storeguard.Settings{ConsecutiveFailures: 1, OpenTimeout: policyOpenTimeout, MaxInflight: 4}
}

type countingLimiter struct {
	store.RateLimiter
	takes atomic.Int64
}

func (c *countingLimiter) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	c.takes.Add(1)
	return c.RateLimiter.Take(ctx, key, l)
}

type rig struct {
	f         *clock.Fake
	handle    *teststore.Handle
	guard     *storeguard.Guard
	local     *countingLimiter
	lim       *Limiter
	handler   http.Handler
	nextCalls int
	limited   int
	storeErrs int
	fallbacks []OnError
}

type rigOptions struct {
	mode      teststore.Mode
	fail      error
	onError   OnError
	settings  storeguard.Settings
	denyRetry time.Duration
	logger    *slog.Logger
}

func newRig(t *testing.T, f *clock.Fake, o rigOptions) *rig {
	t.Helper()
	fail := o.fail
	if fail == nil {
		fail = store.ErrUnavailable
	}
	shared := teststore.NewShared(f)
	t.Cleanup(func() { _ = shared.Close() })
	handle := shared.Handle()
	handle.SetMode(o.mode, fail)
	t.Cleanup(handle.Release)
	g, err := storeguard.New("db", o.settings, f, nil)
	if err != nil {
		t.Fatal(err)
	}
	localRL := memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 10, Clock: f})
	t.Cleanup(func() { _ = localRL.Close() })
	r := &rig{f: f, handle: handle, guard: g, local: &countingLimiter{RateLimiter: localRL}}
	r.lim = New(Policy{
		ID:             "api",
		Limit:          tbLimit,
		Store:          handle,
		Logger:         o.logger,
		Clock:          f,
		Guard:          g,
		Timeout:        policyTimeout,
		OnError:        o.onError,
		Local:          r.local,
		DenyRetryAfter: o.denyRetry,
		StoreName:      "db",
		OnStoreError:   func() { r.storeErrs++ },
		OnFallback:     func(p OnError) { r.fallbacks = append(r.fallbacks, p) },
	})
	key, err := NewKeyFunc(SourceRoute, "")
	if err != nil {
		t.Fatal(err)
	}
	r.handler = Middleware(r.lim, key, func() { r.limited++ })(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		r.nextCalls++
	}))
	return r
}

func (r *rig) serve() *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	r.handler.ServeHTTP(rec, prepared("/x", ""))
	return rec
}

// serveHanging sends one request whose store call parks, then advances fake
// time by the store timeout so that the guard times the call out.
func (r *rig) serveHanging() *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		r.handler.ServeHTTP(rec, prepared("/x", ""))
		close(done)
	}()
	r.handle.WaitBlocked(1)
	r.f.Advance(policyTimeout)
	<-done
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return body.Error.Code
}

func TestPolicyAllowDenyLocal(t *testing.T) {
	modes := []struct {
		name string
		mode teststore.Mode
		hang bool
	}{
		{"fail", teststore.Fail, false},
		{"hang", teststore.Hang, true},
		{"hang ignoring context", teststore.HangIgnoreContext, true},
	}
	for _, m := range modes {
		for _, p := range []OnError{Allow, Deny, Local} {
			t.Run(m.name+"/"+p.String(), func(t *testing.T) {
				f := newClock()
				r := newRig(t, f, rigOptions{mode: m.mode, onError: p, settings: policySettings(), denyRetry: policyOpenTimeout})
				start := f.Now()

				requests := 1
				if p == Local {
					requests = 4
				}
				recs := make([]*httptest.ResponseRecorder, 0, requests)
				for i := range requests {
					if i == 0 && m.hang {
						recs = append(recs, r.serveHanging())
					} else {
						recs = append(recs, r.serve())
					}
				}

				wantElapsed := time.Duration(0)
				if m.hang {
					wantElapsed = policyTimeout
				}
				if got := f.Now().Sub(start); got != wantElapsed {
					t.Fatalf("fake time elapsed = %v, want %v", got, wantElapsed)
				}
				if len(r.fallbacks) != requests {
					t.Fatalf("OnFallback calls = %d, want %d", len(r.fallbacks), requests)
				}
				for i, got := range r.fallbacks {
					if got != p {
						t.Fatalf("fallback %d = %v, want %v", i, got, p)
					}
				}
				if r.storeErrs != 1 {
					t.Fatalf("OnStoreError calls = %d, want 1", r.storeErrs)
				}

				switch p {
				case Allow:
					if rec := recs[0]; rec.Code != http.StatusOK {
						t.Fatalf("status = %d, want 200", rec.Code)
					}
					for _, h := range []string{"RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset", "Retry-After"} {
						if got := recs[0].Header().Get(h); got != "" {
							t.Fatalf("%s = %q, want absent", h, got)
						}
					}
					if r.nextCalls != 1 {
						t.Fatalf("next calls = %d, want 1", r.nextCalls)
					}
					if r.limited != 0 {
						t.Fatalf("onLimited calls = %d, want 0", r.limited)
					}
				case Deny:
					rec := recs[0]
					if rec.Code != http.StatusServiceUnavailable {
						t.Fatalf("status = %d, want 503", rec.Code)
					}
					if got := errorCode(t, rec); got != "rate_limit_unavailable" {
						t.Fatalf("code = %q, want rate_limit_unavailable", got)
					}
					if got := rec.Header().Get("Retry-After"); got != "2" {
						t.Fatalf("Retry-After = %q, want 2", got)
					}
					if r.nextCalls != 0 {
						t.Fatalf("next calls = %d, want 0", r.nextCalls)
					}
					if r.limited != 0 {
						t.Fatalf("onLimited calls = %d, want 0", r.limited)
					}
				case Local:
					for i := range 3 {
						rec := recs[i]
						if rec.Code != http.StatusOK {
							t.Fatalf("request %d status = %d, want 200", i+1, rec.Code)
						}
						if got := rec.Header().Get("RateLimit-Limit"); got != "3" {
							t.Fatalf("request %d RateLimit-Limit = %q, want 3", i+1, got)
						}
						if got, want := rec.Header().Get("RateLimit-Remaining"), strconv.Itoa(2-i); got != want {
							t.Fatalf("request %d RateLimit-Remaining = %q, want %q", i+1, got, want)
						}
					}
					if rec := recs[3]; rec.Code != http.StatusTooManyRequests {
						t.Fatalf("request 4 status = %d, want 429", rec.Code)
					}
					if got := errorCode(t, recs[3]); got != "rate_limited" {
						t.Fatalf("request 4 code = %q, want rate_limited", got)
					}
					if r.nextCalls != 3 {
						t.Fatalf("next calls = %d, want 3", r.nextCalls)
					}
					if r.limited != 1 {
						t.Fatalf("onLimited calls = %d, want 1", r.limited)
					}
				}
			})
		}
	}
}

func TestDenyRetryAfterMinimum(t *testing.T) {
	f := newClock()
	r := newRig(t, f, rigOptions{mode: teststore.Fail, onError: Deny, settings: policySettings(), denyRetry: 200 * time.Millisecond})
	rec := r.serve()
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
}

func TestCanceledContextNotCounted(t *testing.T) {
	f := newClock()
	r := newRig(t, f, rigOptions{mode: teststore.Hang, onError: Allow, settings: policySettings()})
	ctx, cancel := context.WithCancel(context.Background())
	res := make(chan Result, 1)
	go func() {
		res <- r.lim.Take(ctx, "ip:203.0.113.7")
	}()
	r.handle.WaitBlocked(1)
	cancel()
	got := <-res
	if got != (Result{Degraded: true}) {
		t.Fatalf("Take = %+v, want degraded with no policy", got)
	}
	if len(r.fallbacks) != 0 {
		t.Fatalf("OnFallback calls = %d, want 0", len(r.fallbacks))
	}
	if r.storeErrs != 0 {
		t.Fatalf("OnStoreError calls = %d, want 0", r.storeErrs)
	}
}

func TestInvalidLimitAdmitsAndLogs(t *testing.T) {
	f := newClock()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	r := newRig(t, f, rigOptions{mode: teststore.Fail, fail: store.ErrInvalidLimit, onError: Deny, settings: policySettings(), logger: logger})
	res := r.lim.Take(context.Background(), "ip:203.0.113.7")
	if !res.Degraded || res.Policy != NoFallback || res.Decision != (store.Decision{}) {
		t.Fatalf("Take = %+v, want degraded with NoFallback and zero decision", res)
	}
	if len(r.fallbacks) != 0 {
		t.Fatalf("OnFallback calls = %d, want 0", len(r.fallbacks))
	}
	recs := records(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("log records = %d, want 1", len(recs))
	}
	if recs[0]["level"] != "ERROR" {
		t.Fatalf("level = %v, want ERROR", recs[0]["level"])
	}
	if recs[0]["store"] != "db" {
		t.Fatalf("store attr = %v, want db", recs[0]["store"])
	}
}

func TestOpenGuardSkipsStore(t *testing.T) {
	f := newClock()
	r := newRig(t, f, rigOptions{mode: teststore.Fail, onError: Allow, settings: policySettings()})
	r.serve()
	takes := r.handle.Takes()
	for range 10 {
		if rec := r.serve(); rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
	}
	if got := r.handle.Takes(); got != takes {
		t.Fatalf("store calls = %d after trip, want %d", got, takes)
	}
	if len(r.fallbacks) != 11 {
		t.Fatalf("OnFallback calls = %d, want 11", len(r.fallbacks))
	}
	if r.storeErrs != 1 {
		t.Fatalf("OnStoreError calls = %d, want 1", r.storeErrs)
	}
}

func TestLocalRecovers(t *testing.T) {
	f := newClock()
	r := newRig(t, f, rigOptions{mode: teststore.Fail, onError: Local, settings: policySettings()})
	if rec := r.serve(); rec.Code != http.StatusOK {
		t.Fatalf("first status = %d, want 200", rec.Code)
	}
	if got := r.local.takes.Load(); got != 1 {
		t.Fatalf("local calls after trip = %d, want 1", got)
	}
	if r.guard.Available() {
		t.Fatal("guard available after trip")
	}

	f.Advance(policyOpenTimeout)
	r.handle.SetMode(teststore.Healthy, nil)
	res := r.lim.Take(context.Background(), "ip:203.0.113.7")
	if res.Degraded || res.Policy != NoFallback {
		t.Fatalf("Take = %+v, want store decision with NoFallback", res)
	}
	if got := r.handle.Takes(); got != 2 {
		t.Fatalf("store calls = %d, want 2", got)
	}
	if got := r.local.takes.Load(); got != 1 {
		t.Fatalf("local calls after recovery = %d, want 1", got)
	}
	if !r.guard.Available() {
		t.Fatal("guard not available after probe succeeded")
	}
}

func TestStoreFailureLogRateLimited(t *testing.T) {
	f := newClock()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	settings := storeguard.Settings{ConsecutiveFailures: 100, OpenTimeout: policyOpenTimeout, MaxInflight: 4}
	r := newRig(t, f, rigOptions{mode: teststore.Fail, onError: Allow, settings: settings, logger: logger})
	for range 5 {
		r.serve()
	}
	recs := records(t, &buf)
	if len(recs) != 1 {
		t.Fatalf("records after 5 failures in one second = %d, want 1", len(recs))
	}
	if recs[0]["level"] != "WARN" {
		t.Fatalf("level = %v, want WARN", recs[0]["level"])
	}
	if recs[0]["suppressed"] != float64(0) {
		t.Fatalf("first suppressed = %v, want 0", recs[0]["suppressed"])
	}

	f.Advance(time.Second)
	r.serve()
	recs = records(t, &buf)
	if len(recs) != 2 {
		t.Fatalf("records after one second = %d, want 2", len(recs))
	}
	if recs[1]["suppressed"] != float64(4) {
		t.Fatalf("second suppressed = %v, want 4", recs[1]["suppressed"])
	}
	if recs[1]["on_store_error"] != "allow" {
		t.Fatalf("on_store_error = %v, want allow", recs[1]["on_store_error"])
	}
}

func TestParseOnError(t *testing.T) {
	for _, c := range []struct {
		in   string
		want OnError
	}{
		{"local", Local},
		{"allow", Allow},
		{"deny", Deny},
	} {
		got, err := ParseOnError(c.in)
		if err != nil {
			t.Fatalf("ParseOnError(%q) error = %v", c.in, err)
		}
		if got != c.want {
			t.Fatalf("ParseOnError(%q) = %v, want %v", c.in, got, c.want)
		}
		if got.String() != c.in {
			t.Fatalf("String() of %q = %q", c.in, got.String())
		}
	}
	for _, bad := range []string{"", "Local", "none", "NoFallback"} {
		if _, err := ParseOnError(bad); err == nil {
			t.Fatalf("ParseOnError(%q) error = nil, want error", bad)
		}
	}
	if got := NoFallback.String(); got != "" {
		t.Fatalf("NoFallback.String() = %q, want empty", got)
	}
}

func mustPanic(t *testing.T, p Policy) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("New did not panic")
		}
	}()
	New(p)
}

func TestNewGuardedPolicyValidation(t *testing.T) {
	g, err := storeguard.New("db", policySettings(), newClock(), nil)
	if err != nil {
		t.Fatal(err)
	}
	base := Policy{ID: "api", Limit: tbLimit, Store: &recStore{}, Guard: g, Timeout: policyTimeout, OnError: Allow}
	New(base)

	noPolicy := base
	noPolicy.OnError = NoFallback
	mustPanic(t, noPolicy)

	noTimeout := base
	noTimeout.Timeout = 0
	mustPanic(t, noTimeout)

	localNoLimiter := base
	localNoLimiter.OnError = Local
	mustPanic(t, localNoLimiter)
}
