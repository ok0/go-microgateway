package cache

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

// bgResults lists the background results the stage counts.
var bgResults = []string{bgRevalidated, bgReplaced, bgNotStored, bgError, bgSkipped}

// swrHeader is the origin response that fills the stale-while-revalidate entries: a 30s window on a 60s lifetime.
func swrHeader() http.Header {
	return http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=30"}, "Etag": {`"v1"`}}
}

// swrRig returns a rig whose entry for /swr was filled by swrHeader with the body "old". The origin answers 304 from
// then on, so that a refresh revalidates the entry.
func swrRig(t *testing.T, o rigOpts) *rig {
	t.Helper()
	rg := newRig(t, o)
	rg.orig.script(http.StatusOK, swrHeader(), "old")
	rg.get("/swr", nil)
	rg.orig.script(http.StatusNotModified, swrHeader(), "")
	return rg
}

// expectBackground fails t unless the counter of result is 1 and every other background counter is 0. An empty result
// expects no background refresh at all.
func expectBackground(t *testing.T, rg *rig, result string) {
	t.Helper()
	for _, res := range bgResults {
		want := 0.0
		if res == result {
			want = 1
		}
		if got := rg.background(res); got != want {
			t.Errorf("background %s = %v, want %v", res, got, want)
		}
	}
}

func TestStaleWhileRevalidateBoundaries(t *testing.T) {
	const swr = 30 * time.Second
	t.Run("windows", func(t *testing.T) {
		cases := []struct {
			name  string
			at    time.Duration
			fresh bool
			sync  bool
			ttl   int64
		}{
			{name: "L-1ns is fresh", at: ttlLifetime - time.Nanosecond, fresh: true},
			{name: "L is served stale", at: ttlLifetime, ttl: 0},
			{name: "L+1ns is served stale", at: ttlLifetime + time.Nanosecond, ttl: -1},
			{name: "L+swr-1ns is served stale", at: ttlLifetime + swr - time.Nanosecond, ttl: -30},
			{name: "L+swr is revalidated synchronously", at: ttlLifetime + swr, sync: true},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				rg := swrRig(t, rigOpts{route: "enabled: true\nkeep: 1h"})
				rg.clk.Advance(c.at)
				rec := rg.get("/swr", nil)
				st := lastStatus(t, rec)
				if rec.Code != http.StatusOK || rec.Body.String() != "old" {
					t.Fatalf("client response %d %q, want 200 old", rec.Code, rec.Body.String())
				}
				switch {
				case c.fresh:
					if !st.Hit || st.Fwd != "" || rg.orig.count() != 1 {
						t.Fatalf("status %+v with %d origin calls, want a fresh hit without forward", st, rg.orig.count())
					}
					expectBackground(t, rg, "")
				case c.sync:
					if st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
						t.Fatalf("status %+v, want revalidated with fwd-status 304", st)
					}
					if got := rg.orig.last().Get("If-None-Match"); got != `"v1"` {
						t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
					}
					if rg.requests(resultRevalidated) != 1 {
						t.Fatalf("revalidated count %v, want 1", rg.requests(resultRevalidated))
					}
					expectBackground(t, rg, "")
				default:
					if !st.Hit || st.Fwd != httpcache.FwdStale || st.Detail != detailSWR || !st.HasTTL || st.TTL != c.ttl {
						t.Fatalf("status %+v, want stale hit under stale-while-revalidate with ttl %d", st, c.ttl)
					}
					if got, want := rec.Header().Get("Age"), strconv.FormatInt(int64(c.at/time.Second), 10); got != want {
						t.Fatalf("Age %q, want %s", got, want)
					}
					rg.settle()
					if rg.orig.count() != 2 {
						t.Fatalf("origin calls %d, want fill and one background refresh", rg.orig.count())
					}
					expectBackground(t, rg, bgRevalidated)
				}
			})
		}
	})
	t.Run("storage ends at lifetime plus window", func(t *testing.T) {
		const storage = ttlLifetime + swr
		t.Run("one nanosecond before revalidates synchronously", func(t *testing.T) {
			rg := swrRig(t, rigOpts{noBackground: true})
			rg.clk.Advance(storage - time.Nanosecond)
			rec := rg.get("/swr", nil)
			if st := lastStatus(t, rec); st.Fwd != httpcache.FwdStale {
				t.Fatalf("status %+v, want conditional forward", st)
			}
			if got := rg.orig.last().Get("If-None-Match"); got != `"v1"` {
				t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
			}
		})
		t.Run("at the boundary is a miss", func(t *testing.T) {
			rg := swrRig(t, rigOpts{})
			rg.orig.script(http.StatusOK, swrHeader(), "old")
			rg.clk.Advance(storage)
			rec := rg.get("/swr", nil)
			if st := lastStatus(t, rec); st.Fwd != httpcache.FwdURIMiss {
				t.Fatalf("fwd %q, want uri-miss", st.Fwd)
			}
			if got := rg.orig.last().Get("If-None-Match"); got != "" {
				t.Fatalf("origin request still conditional (%q)", got)
			}
		})
	})
}

func TestStaleWhileRevalidateOneRefresh(t *testing.T) {
	rg := swrRig(t, rigOpts{})
	rg.clk.Advance(61 * time.Second)
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	const n = 20
	recs := make([]*httptest.ResponseRecorder, n)
	done := parallel(n, func(i int) { recs[i] = rg.get("/swr", nil) })
	testwait.Done(t, "stale responses returned", done, nil)
	testwait.Recv(t, "refresh reached the origin", arrived, nil)
	for i, rec := range recs {
		if rec.Code != http.StatusOK || rec.Body.String() != "old" {
			t.Fatalf("response %d: %d %q, want 200 old", i, rec.Code, rec.Body.String())
		}
	}
	if got := rg.orig.count(); got != 2 {
		t.Fatalf("origin calls %d, want fill and one refresh", got)
	}
	if got := rg.requests(resultStale); got != n {
		t.Fatalf("stale count %v, want %d", got, n)
	}
	if got := rg.background(bgSkipped); got != n-1 {
		t.Fatalf("skipped refreshes %v, want %d", got, n-1)
	}
	release()
	rg.settle()
	if got := rg.background(bgRevalidated); got != 1 {
		t.Fatalf("revalidated refreshes %v, want 1", got)
	}
}

func TestBackgroundOutcomes(t *testing.T) {
	cases := []struct {
		name   string
		setup  func(rg *rig)
		result string
		logged bool
		check  func(t *testing.T, rg *rig)
	}{
		{
			name:   "304 revalidates the entry",
			setup:  func(rg *rig) { rg.orig.script(http.StatusNotModified, swrHeader(), "") },
			result: bgRevalidated,
			check: func(t *testing.T, rg *rig) {
				next := rg.get("/swr", nil)
				if st := lastStatus(t, next); !st.Hit || st.Fwd != "" || !st.HasTTL || st.TTL != 60 || next.Header().Get("Age") != "0" {
					t.Fatalf("next status %+v Age %q, want fresh hit with ttl 60 and Age 0", st, next.Header().Get("Age"))
				}
				puts := rg.spy.callsOf(opPut)
				if len(puts) != 2 || puts[1].TTL != 90*time.Second {
					t.Fatalf("puts %+v, want the refresh stored with ttl 90s", puts)
				}
			},
		},
		{
			name:   "200 replaces the entry",
			setup:  func(rg *rig) { rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "new") },
			result: bgReplaced,
			check: func(t *testing.T, rg *rig) {
				if next := rg.get("/swr", nil); !hit(t, next) || next.Body.String() != "new" {
					t.Fatalf("next body %q, want hit new", next.Body.String())
				}
			},
		},
		{
			name:   "500 leaves the entry",
			setup:  func(rg *rig) { rg.orig.script(http.StatusInternalServerError, nil, "err") },
			result: bgNotStored,
			check: func(t *testing.T, rg *rig) {
				next := rg.get("/swr", nil)
				rg.settle()
				if next.Body.String() != "old" || lastStatus(t, next).Fwd != httpcache.FwdStale {
					t.Fatalf("next %d %q, want the stale entry unchanged", next.Code, next.Body.String())
				}
			},
		},
		{
			name: "502 generated by the gateway is an error",
			setup: func(rg *rig) {
				rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadGateway) })
			},
			result: bgError,
		},
		{
			name: "origin panic is an error",
			setup: func(rg *rig) {
				rg.orig.setHandle(func(http.ResponseWriter, *http.Request) { panic("bg secret") })
			},
			result: bgError,
			logged: true,
		},
		{
			name:   "no-store is not stored",
			setup:  func(rg *rig) { rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"no-store"}}, "x") },
			result: bgNotStored,
		},
		{
			name:   "put error is not stored",
			setup:  func(rg *rig) { rg.spy.setErr(opPut, errors.New("put failed")) },
			result: bgNotStored,
			check: func(t *testing.T, rg *rig) {
				if got := rg.storeErrs(opPut); got != 1 {
					t.Fatalf("put errors %v, want 1", got)
				}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var logs bytes.Buffer
			rg := swrRig(t, rigOpts{logger: slog.New(slog.NewTextHandler(&logs, nil))})
			rg.clk.Advance(61 * time.Second)
			c.setup(rg)
			rec := rg.get("/swr", nil)
			if rec.Code != http.StatusOK || rec.Body.String() != "old" {
				t.Fatalf("triggering response %d %q, want 200 old", rec.Code, rec.Body.String())
			}
			rg.settle()
			expectBackground(t, rg, c.result)
			if c.logged && !strings.Contains(logs.String(), "panic recovered") {
				t.Fatalf("log %q has no panic record", logs.String())
			}
			if c.check != nil {
				c.check(t, rg)
			}
		})
	}
}

func TestBackgroundRequest(t *testing.T) {
	rg := swrRig(t, rigOpts{})
	rg.clk.Advance(61 * time.Second)
	mutated := make(chan struct{})
	seen := make(chan http.Header, 1)
	rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-mutated:
		case <-r.Context().Done():
			return
		}
		seen <- r.Header.Clone()
		ex := reqctx.ExchangeFrom(r.Context())
		ex.Begin()
		for k, v := range swrHeader() {
			w.Header()[k] = v
		}
		w.WriteHeader(http.StatusNotModified)
		ex.Finish()
	})
	req := httptest.NewRequest(http.MethodGet, "http://example.com/swr", nil)
	req.Header.Set("If-None-Match", `"zzz"`)
	req.Header.Set("If-Modified-Since", start.Format(http.TimeFormat))
	req.Header.Set("Cache-Control", "no-transform")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("X-Custom", "1")
	rec := do(rg.h, req)
	if rec.Code != http.StatusOK || rec.Body.String() != "old" {
		t.Fatalf("triggering response %d %q, want 200 old", rec.Code, rec.Body.String())
	}
	req.Header.Set("X-Custom", "2")
	req.Header.Set("If-None-Match", `"changed"`)
	close(mutated)
	got, _ := testwait.Recv(t, "refresh reached the origin", seen, nil)
	if v := got.Get("If-None-Match"); v != `"v1"` {
		t.Errorf("refresh If-None-Match %q, want \"v1\" from the entry", v)
	}
	if v := got.Get("If-Modified-Since"); v != "" {
		t.Errorf("refresh If-Modified-Since %q, want none", v)
	}
	if v := got.Get("X-Custom"); v != "1" {
		t.Errorf("refresh X-Custom %q, want the value at the time of the request", v)
	}
	if v := got.Get("Pragma"); v != "" {
		t.Errorf("refresh Pragma %q, want none", v)
	}
	if v := got.Get("Cache-Control"); v != "" {
		t.Errorf("refresh Cache-Control %q, want none", v)
	}
	rg.settle()
	expectBackground(t, rg, bgRevalidated)
}

func TestStaleWhileRevalidateRequestRules(t *testing.T) {
	t.Run("only-if-cached serves the stale entry", func(t *testing.T) {
		rg := swrRig(t, rigOpts{})
		rg.clk.Advance(61 * time.Second)
		rec := rg.get("/swr", http.Header{"Cache-Control": {"only-if-cached"}})
		if rec.Code != http.StatusOK || rec.Body.String() != "old" {
			t.Fatalf("response %d %q, want 200 old", rec.Code, rec.Body.String())
		}
		rg.settle()
		if rg.orig.count() != 1 {
			t.Fatalf("origin calls %d, want 1", rg.orig.count())
		}
		expectBackground(t, rg, "")
	})
	t.Run("max-age=0 revalidates synchronously", func(t *testing.T) {
		rg := swrRig(t, rigOpts{})
		rg.clk.Advance(61 * time.Second)
		rec := rg.get("/swr", http.Header{"Cache-Control": {"max-age=0"}})
		if rec.Body.String() != "old" || rg.orig.count() != 2 || rg.orig.last().Get("If-None-Match") != `"v1"` {
			t.Fatalf("response %q with %d origin calls, want a synchronous conditional revalidation", rec.Body.String(), rg.orig.count())
		}
		if rg.requests(resultRevalidated) != 1 {
			t.Fatalf("revalidated count %v, want 1", rg.requests(resultRevalidated))
		}
		expectBackground(t, rg, "")
	})
	t.Run("min-fresh revalidates a fresh entry synchronously", func(t *testing.T) {
		rg := swrRig(t, rigOpts{})
		rg.clk.Advance(55 * time.Second)
		rec := rg.get("/swr", http.Header{"Cache-Control": {"min-fresh=10"}})
		if rec.Body.String() != "old" || rg.orig.count() != 2 || rg.orig.last().Get("If-None-Match") != `"v1"` {
			t.Fatalf("response %q with %d origin calls, want a synchronous conditional revalidation", rec.Body.String(), rg.orig.count())
		}
		expectBackground(t, rg, "")
	})
	t.Run("max-stale serves without a refresh", func(t *testing.T) {
		rg := swrRig(t, rigOpts{})
		rg.clk.Advance(61 * time.Second)
		rec := rg.get("/swr", http.Header{"Cache-Control": {"max-stale=100"}})
		if st := lastStatus(t, rec); !st.Hit || st.Fwd != "" || st.Detail != "" || rec.Body.String() != "old" {
			t.Fatalf("status %+v body %q, want a stale hit with no forward", st, rec.Body.String())
		}
		rg.settle()
		if rg.orig.count() != 1 {
			t.Fatalf("origin calls %d, want 1", rg.orig.count())
		}
		expectBackground(t, rg, "")
	})
	t.Run("no-cache revalidates synchronously", func(t *testing.T) {
		rg := swrRig(t, rigOpts{})
		rg.clk.Advance(61 * time.Second)
		rec := rg.get("/swr", http.Header{"Cache-Control": {"no-cache"}})
		if rec.Body.String() != "old" || rg.orig.count() != 2 || rg.orig.last().Get("If-None-Match") != `"v1"` {
			t.Fatalf("response %q with %d origin calls, want a synchronous conditional revalidation", rec.Body.String(), rg.orig.count())
		}
		expectBackground(t, rg, "")
	})
}

func TestBackgroundInitialAge(t *testing.T) {
	rg := swrRig(t, rigOpts{})
	rg.clk.Advance(61 * time.Second)
	rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
		date := rg.clk.Now().Add(-2 * time.Second).Format(http.TimeFormat)
		rg.clk.Advance(time.Second)
		ex := reqctx.ExchangeFrom(r.Context())
		ex.Begin()
		w.Header().Set("Cache-Control", "max-age=60, stale-while-revalidate=30")
		w.Header().Set("Etag", `"v1"`)
		w.Header().Set("Date", date)
		w.Header().Set("Age", "3")
		w.WriteHeader(http.StatusNotModified)
		ex.Finish()
	})
	rg.get("/swr", nil)
	rg.settle()
	puts := rg.spy.callsOf(opPut)
	if len(puts) != 2 {
		t.Fatalf("puts %d, want the fill and the refresh", len(puts))
	}
	if got := puts[1].InitialAge; got != 4*time.Second {
		t.Fatalf("stored initial age %v, want 4s (apparent age 3s, corrected age 4s)", got)
	}
}

func TestBackgroundSoftPurge(t *testing.T) {
	rg := swrRig(t, rigOpts{})
	rg.clk.Advance(61 * time.Second)
	primary := httpcache.PrimaryKey(httptest.NewRequest(http.MethodGet, "http://example.com/swr", nil))
	if _, err := rg.spy.Purge(context.Background(), store.Purge{Kind: store.PurgeKeys, Keys: []string{primary}, Soft: true, Retain: time.Hour}); err != nil {
		t.Fatal(err)
	}
	rec := rg.get("/swr", nil)
	if st := lastStatus(t, rec); st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified {
		t.Fatalf("status %+v, want revalidated with fwd-status 304", st)
	}
	if rg.orig.count() != 2 {
		t.Fatalf("origin calls %d, want the soft-purged entry revalidated synchronously", rg.orig.count())
	}
	expectBackground(t, rg, "")
}
