package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

// sieHeader is the origin response that fills the stale-if-error entries.
func sieHeader() http.Header {
	return http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}
}

// originHeader returns the origin response header with the Cache-Control value cc and a validator.
func originHeader(cc string) http.Header {
	return http.Header{"Cache-Control": {cc}, "Etag": {`"v1"`}}
}

// sieFill fills /sie through a rig with the route settings route and the origin response fill, with body "old".
// The clock is left at the fill time.
func sieFill(t *testing.T, route string, fill http.Header) *rig {
	t.Helper()
	rg := newRig(t, rigOpts{route: route})
	rg.orig.script(http.StatusOK, fill, "old")
	rg.get("/sie", nil)
	return rg
}

// expectOrigin fails t unless rec carries the origin response with status code and body "fail", and no stored entry
// was served in its place.
func expectOrigin(t *testing.T, rec *httptest.ResponseRecorder, code int) {
	t.Helper()
	if rec.Code != code || rec.Body.String() != "fail" {
		t.Fatalf("client response %d %q, want %d fail", rec.Code, rec.Body.String(), code)
	}
	if st := lastStatus(t, rec); st.Detail == detailSIE {
		t.Fatalf("status %+v, want no stale-if-error", st)
	}
}

func TestStaleIfErrorStatuses(t *testing.T) {
	for _, code := range []int{500, 502, 503, 504} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			rg := sieFill(t, "stale_if_error: 1h", sieHeader())
			rg.clk.Advance(61 * time.Second)
			rg.orig.script(code, http.Header{"Retry-After": {"5"}}, "fail")
			rec := rg.get("/sie", nil)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" {
				t.Fatalf("client response %d %q, want 200 old", rec.Code, rec.Body.String())
			}
			if v := rec.Header().Get("Retry-After"); v != "" {
				t.Errorf("Retry-After %q reached the client", v)
			}
			st := lastStatus(t, rec)
			if !st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != code || st.Detail != detailSIE || !st.HasTTL || st.TTL != -1 {
				t.Fatalf("status %+v, want hit fwd=stale fwd-status=%d detail=stale-if-error ttl -1", st, code)
			}
			if got := rec.Header().Get("Age"); got != "61" {
				t.Errorf("Age %q, want 61", got)
			}
			if n := rg.requests(resultStale); n != 1 {
				t.Errorf("stale count %v, want 1", n)
			}
			if n := len(rg.spy.callsOf(opPut)); n != 1 {
				t.Errorf("puts %d, want only the fill", n)
			}
			if n := rg.stores(storeNotStorable); n != 1 {
				t.Errorf("not_storable count %v, want 1", n)
			}
		})
	}
	t.Run("statuses outside the failure set reach the client", func(t *testing.T) {
		for _, code := range []int{501, 404, 302, 400} {
			t.Run(strconv.Itoa(code), func(t *testing.T) {
				rg := sieFill(t, "stale_if_error: 1h", sieHeader())
				rg.clk.Advance(61 * time.Second)
				rg.orig.script(code, http.Header{"Cache-Control": {"no-store"}}, "fail")
				expectOrigin(t, rg.get("/sie", nil), code)
			})
		}
	})
	t.Run("304 revalidates the entry", func(t *testing.T) {
		rg := sieFill(t, "stale_if_error: 1h", sieHeader())
		rg.clk.Advance(61 * time.Second)
		rg.orig.script(http.StatusNotModified, sieHeader(), "")
		rec := rg.get("/sie", nil)
		st := lastStatus(t, rec)
		if rec.Code != http.StatusOK || rec.Body.String() != "old" || !st.Hit || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
			t.Fatalf("response %d %q status %+v, want 200 old revalidated with fwd-status 304", rec.Code, rec.Body.String(), st)
		}
		if n := rg.requests(resultRevalidated); n != 1 {
			t.Errorf("revalidated count %v, want 1", n)
		}
	})
}

func TestStaleIfErrorBoundary(t *testing.T) {
	const window = 30 * time.Second
	cases := []struct {
		name  string
		route string
		cc    string
		at    time.Duration
		stale bool
		age   int64
		ttl   int64
		put   time.Duration
	}{
		{name: "directive one nanosecond inside the window", cc: "max-age=60, stale-if-error=30", at: ttlLifetime + window - time.Nanosecond, stale: true, age: 89, ttl: -30, put: ttlLifetime + window},
		{name: "directive at the window end", cc: "max-age=60, stale-if-error=30", at: ttlLifetime + window, put: ttlLifetime + window},
		{name: "directive one nanosecond after the window end", cc: "max-age=60, stale-if-error=30", at: ttlLifetime + window + time.Nanosecond, put: ttlLifetime + window},
		{name: "route window one nanosecond inside", route: "stale_if_error: 30s", cc: "max-age=60", at: ttlLifetime + window - time.Nanosecond, stale: true, age: 89, ttl: -30, put: ttlLifetime + window},
		{name: "route window at the window end", route: "stale_if_error: 30s", cc: "max-age=60", at: ttlLifetime + window, put: ttlLifetime + window},
		{name: "route window one nanosecond after the window end", route: "stale_if_error: 30s", cc: "max-age=60", at: ttlLifetime + window + time.Nanosecond, put: ttlLifetime + window},
		{name: "directive 0 overrides the route window", route: "stale_if_error: 1h", cc: "max-age=60, stale-if-error=0", at: ttlLifetime + time.Nanosecond, put: ttlLifetime},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rg := sieFill(t, c.route, originHeader(c.cc))
			rg.clk.Advance(c.at)
			rg.orig.script(http.StatusInternalServerError, http.Header{"Retry-After": {"5"}}, "fail")
			rec := rg.get("/sie", nil)
			puts := rg.spy.callsOf(opPut)
			if len(puts) != 1 {
				t.Fatalf("puts %d, want only the fill", len(puts))
			}
			if puts[0].TTL != c.put {
				t.Fatalf("storage ttl %v, want %v", puts[0].TTL, c.put)
			}
			if !c.stale {
				expectOrigin(t, rec, http.StatusInternalServerError)
				return
			}
			st := lastStatus(t, rec)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.Fwd != httpcache.FwdStale || st.FwdStatus != http.StatusInternalServerError || st.Detail != detailSIE || !st.HasTTL || st.TTL != c.ttl {
				t.Fatalf("response %d %q status %+v, want 200 old stale-if-error with ttl %d", rec.Code, rec.Body.String(), st, c.ttl)
			}
			if got := rec.Header().Get("Age"); got != strconv.FormatInt(c.age, 10) {
				t.Errorf("Age %q, want %d", got, c.age)
			}
		})
	}
}

func TestStaleIfErrorProhibitions(t *testing.T) {
	t.Run("must-revalidate", func(t *testing.T) {
		rg := sieFill(t, "stale_if_error: 1h", originHeader("max-age=60, must-revalidate"))
		rg.clk.Advance(61 * time.Second)
		rg.orig.script(http.StatusInternalServerError, nil, "fail")
		expectOrigin(t, rg.get("/sie", nil), http.StatusInternalServerError)
	})
	t.Run("no-cache with keep", func(t *testing.T) {
		rg := sieFill(t, "keep: 1h", originHeader("max-age=60, no-cache"))
		rg.clk.Advance(61 * time.Second)
		rg.orig.script(http.StatusInternalServerError, nil, "fail")
		expectOrigin(t, rg.get("/sie", nil), http.StatusInternalServerError)
	})
	t.Run("soft-purged entry is revalidated", func(t *testing.T) {
		rg := sieFill(t, "stale_if_error: 1h", sieHeader())
		rg.clk.Advance(61 * time.Second)
		primary := httpcache.PrimaryKey(httptest.NewRequest(http.MethodGet, "http://example.com/sie", nil))
		if _, err := rg.spy.Purge(context.Background(), store.Purge{Kind: store.PurgeKeys, Keys: []string{primary}, Soft: true, Retain: time.Hour}); err != nil {
			t.Fatal(err)
		}
		rg.orig.script(http.StatusInternalServerError, nil, "fail")
		expectOrigin(t, rg.get("/sie", nil), http.StatusInternalServerError)
		if got := rg.orig.last().Get("If-None-Match"); got != `"v1"` {
			t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
		}
	})
	t.Run("fresh entry revalidated by max-age=0", func(t *testing.T) {
		rg := sieFill(t, "stale_if_error: 1h", sieHeader())
		rg.clk.Advance(30 * time.Second)
		rg.orig.script(http.StatusInternalServerError, nil, "fail")
		expectOrigin(t, rg.get("/sie", http.Header{"Cache-Control": {"max-age=0"}}), http.StatusInternalServerError)
	})
	t.Run("no entry", func(t *testing.T) {
		rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
		rg.orig.script(http.StatusInternalServerError, nil, "fail")
		expectOrigin(t, rg.get("/sie", nil), http.StatusInternalServerError)
	})
}

func TestStaleIfErrorWaiters(t *testing.T) {
	t.Run("stale entry answers the leader and every waiter", func(t *testing.T) {
		rg := sieFill(t, "stale_if_error: 1h", sieHeader())
		rg.clk.Advance(61 * time.Second)
		rg.orig.script(http.StatusServiceUnavailable, http.Header{"Retry-After": {"5"}}, "fail")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		recs := make([]*httptest.ResponseRecorder, 5)
		done := parallel(5, func(i int) { recs[i] = rg.get("/sie", nil) })
		testwait.Recv(t, "leader at origin", arrived, nil)
		awaitWaiters(t, rg, 4)
		release()
		testwait.Done(t, "all requests returned", done, nil)
		for i, rec := range recs {
			st := lastStatus(t, rec)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.FwdStatus != http.StatusServiceUnavailable || st.Detail != detailSIE {
				t.Fatalf("request %d: %d %q status %+v, want 200 old under stale-if-error 503", i, rec.Code, rec.Body.String(), st)
			}
		}
		if n := rg.orig.count(); n != 2 {
			t.Fatalf("origin calls %d, want fill and one forward", n)
		}
		if n := rg.requests(resultStale); n != 5 {
			t.Errorf("stale count %v, want 5", n)
		}
		if n := rg.coalesced(); n != 0 {
			t.Errorf("coalesced count %v, want 0", n)
		}
	})
	t.Run("no entry forwards every waiter on its own", func(t *testing.T) {
		rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
		rg.orig.script(http.StatusInternalServerError, nil, "fail")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		recs := make([]*httptest.ResponseRecorder, 4)
		done := parallel(4, func(i int) { recs[i] = rg.get("/sie", nil) })
		testwait.Recv(t, "leader at origin", arrived, nil)
		awaitWaiters(t, rg, 3)
		release()
		testwait.Done(t, "all requests returned", done, nil)
		if n := rg.orig.count(); n != 4 {
			t.Fatalf("origin calls %d, want the leader and three waiters", n)
		}
		for _, rec := range recs {
			expectOrigin(t, rec, http.StatusInternalServerError)
		}
	})
	t.Run("fresh entry revalidated by max-age=0 forwards every request", func(t *testing.T) {
		rg := sieFill(t, "stale_if_error: 1h", sieHeader())
		rg.clk.Advance(30 * time.Second)
		rg.orig.script(http.StatusInternalServerError, nil, "fail")
		arrived, release := rg.orig.hold()
		t.Cleanup(release)
		hdr := http.Header{"Cache-Control": {"max-age=0"}}
		recs := make([]*httptest.ResponseRecorder, 2)
		done := parallel(2, func(i int) { recs[i] = rg.get("/sie", hdr) })
		testwait.Recv(t, "leader at origin", arrived, nil)
		awaitWaiters(t, rg, 1)
		release()
		testwait.Done(t, "all requests returned", done, nil)
		if n := rg.orig.count(); n != 3 {
			t.Fatalf("origin calls %d, want fill, leader and waiter", n)
		}
		for _, rec := range recs {
			expectOrigin(t, rec, http.StatusInternalServerError)
		}
	})
}

// codeRecorder records every status code written to the client.
type codeRecorder struct {
	*httptest.ResponseRecorder
	codes []int
}

func (c *codeRecorder) WriteHeader(code int) {
	c.codes = append(c.codes, code)
	c.ResponseRecorder.WriteHeader(code)
}

func TestStaleIfErrorNoBytes(t *testing.T) {
	rg := sieFill(t, "stale_if_error: 1h", sieHeader())
	rg.clk.Advance(61 * time.Second)
	rg.orig.script(http.StatusInternalServerError, nil, "fail")
	cr := &codeRecorder{ResponseRecorder: httptest.NewRecorder()}
	rg.h.ServeHTTP(cr, httptest.NewRequest(http.MethodGet, "http://example.com/sie", nil))
	if !slices.Equal(cr.codes, []int{http.StatusOK}) {
		t.Fatalf("status codes written %v, want only 200", cr.codes)
	}
	if cr.Body.String() != "old" {
		t.Fatalf("body %q, want old", cr.Body.String())
	}
}

func TestDegenerateWindows(t *testing.T) {
	long := "max-age=60, stale-while-revalidate=" + strings.Repeat("9", 40)
	cases := []struct {
		name   string
		route  string
		cc     string
		etag   bool
		probe  time.Duration
		served bool
		detail string
		ttl    int64
		put    time.Duration
	}{
		{name: "stale-while-revalidate=0 has no window", cc: "max-age=60, stale-while-revalidate=0", etag: true, probe: 61 * time.Second, put: ttlLifetime},
		{name: "stale-while-revalidate=-1 is read as 0", cc: "max-age=60, stale-while-revalidate=-1", etag: true, probe: 61 * time.Second, put: ttlLifetime},
		{name: "stale-while-revalidate=abc is read as 0", cc: "max-age=60, stale-while-revalidate=abc", etag: true, probe: 61 * time.Second, put: ttlLifetime},
		{name: "40-digit window is clamped to 365d", cc: long, etag: true, probe: 61 * time.Second, served: true, detail: detailSWR, ttl: -1, put: ttlLifetime + httpcache.MaxWindow},
		{name: "window above 365d is clamped to 365d", cc: "max-age=60, stale-while-revalidate=31622400", etag: true, probe: 61 * time.Second, served: true, detail: detailSWR, ttl: -1, put: ttlLifetime + httpcache.MaxWindow},
		{name: "route window without directive", route: "stale_while_revalidate: 10s", cc: "max-age=60", etag: true, probe: 61 * time.Second, served: true, detail: detailSWR, ttl: -1, put: ttlLifetime + 10*time.Second},
		{name: "directive 0 overrides route window", route: "stale_while_revalidate: 10s", cc: "max-age=60, stale-while-revalidate=0", etag: true, probe: 61 * time.Second, put: ttlLifetime},
		{name: "max-age clamped to max_ttl", route: "max_ttl: 1h", cc: "max-age=999999999", etag: true, probe: time.Hour + time.Nanosecond, put: time.Hour},
		{name: "max-age=0 is stale at once", cc: "max-age=0, stale-while-revalidate=10", etag: true, probe: 0, served: true, detail: detailSWR, ttl: 0, put: 10 * time.Second},
		{name: "keep with a validator", route: "keep: 1h", cc: "max-age=60", etag: true, probe: 61 * time.Second, put: ttlLifetime + time.Hour},
		{name: "keep without a validator adds nothing", route: "keep: 1h", cc: "max-age=60", probe: 61 * time.Second, put: ttlLifetime},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, rigOpts{route: c.route})
			fill := http.Header{"Cache-Control": {c.cc}}
			if c.etag {
				fill.Set("Etag", `"v1"`)
			}
			rg.orig.script(http.StatusOK, fill, "old")
			rg.get("/win", nil)
			puts := rg.spy.callsOf(opPut)
			if len(puts) != 1 {
				t.Fatalf("puts %d, want only the fill", len(puts))
			}
			if puts[0].TTL != c.put {
				t.Fatalf("storage ttl %v, want %v", puts[0].TTL, c.put)
			}
			if c.put > store.MaxTTL {
				t.Fatalf("storage ttl %v exceeds store.MaxTTL", c.put)
			}
			rg.clk.Advance(c.probe)
			rg.orig.script(http.StatusInternalServerError, nil, "fail")
			rec := rg.get("/win", nil)
			if !c.served {
				expectOrigin(t, rec, http.StatusInternalServerError)
				return
			}
			st := lastStatus(t, rec)
			age := strconv.FormatInt(int64(c.probe/time.Second), 10)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.Fwd != httpcache.FwdStale || st.Detail != c.detail || !st.HasTTL || st.TTL != c.ttl || rec.Header().Get("Age") != age {
				t.Fatalf("response %d %q status %+v Age %q, want 200 old stale with detail %s ttl %d Age %s", rec.Code, rec.Body.String(), st, rec.Header().Get("Age"), c.detail, c.ttl, age)
			}
			rg.settle()
		})
	}
	t.Run("stale-while-revalidate and stale-if-error together", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, originHeader("max-age=60, stale-while-revalidate=10, stale-if-error=20"), "old")
		rg.get("/win", nil)
		if puts := rg.spy.callsOf(opPut); len(puts) != 1 || puts[0].TTL != ttlLifetime+20*time.Second {
			t.Fatalf("puts %+v, want the fill with storage ttl 80s", puts)
		}
		rg.clk.Advance(65 * time.Second)
		rg.orig.script(http.StatusInternalServerError, nil, "fail")
		st := lastStatus(t, rg.get("/win", nil))
		if st.Detail != detailSWR || st.TTL != -5 {
			t.Fatalf("status %+v at 65s, want stale-while-revalidate with ttl -5", st)
		}
		rg.settle()
		rg.clk.Advance(10 * time.Second)
		rec := rg.get("/win", nil)
		st = lastStatus(t, rec)
		if rec.Code != http.StatusOK || rec.Body.String() != "old" || st.Detail != detailSIE || st.FwdStatus != http.StatusInternalServerError || st.TTL != -15 || rec.Header().Get("Age") != "75" {
			t.Fatalf("response %d %q status %+v at 75s, want stale-if-error 500 with ttl -15", rec.Code, rec.Body.String(), st)
		}
		rg.clk.Advance(5 * time.Second)
		expectOrigin(t, rg.get("/win", nil), http.StatusInternalServerError)
	})
}
