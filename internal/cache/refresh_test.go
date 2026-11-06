package cache

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

// refreshFill is the origin response that fills the refresh entries: a 60 second lifetime with a validator.
func refreshFill() http.Header {
	return http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}}
}

// with304 returns the origin 304 header h with the validator of the refresh entries.
func with304(h http.Header) http.Header {
	out := h.Clone()
	out.Set("Etag", `"v1"`)
	return out
}

// fillRefresh stores the entry of path from the origin response fill, with the body "old", and returns the rig.
func fillRefresh(t *testing.T, o rigOpts, path string, fill http.Header) *rig {
	t.Helper()
	rg := newRig(t, o)
	rg.orig.script(http.StatusOK, fill, "old")
	if rec := rg.get(path, nil); rec.Body.String() != "old" {
		t.Fatalf("fill answered %q, want old", rec.Body.String())
	}
	return rg
}

// fillCall returns the put call that stored the entry of the fill.
func fillCall(t *testing.T, rg *rig) spyCall {
	t.Helper()
	puts := rg.spy.callsOf(opPut)
	if len(puts) == 0 {
		t.Fatal("no put call")
	}
	return puts[0]
}

// expectDeletes fails t unless exactly n delete calls were made and each names the variant of the fill.
func expectDeletes(t *testing.T, rg *rig, n int) {
	t.Helper()
	v := fillCall(t, rg)
	dels := rg.spy.callsOf(opDelete)
	if len(dels) != n {
		t.Fatalf("delete calls %d, want %d", len(dels), n)
	}
	for _, d := range dels {
		if d.Primary != v.Primary || d.Secondary != v.Secondary {
			t.Errorf("delete of %q/%q, want the variant %q/%q", d.Primary, d.Secondary, v.Primary, v.Secondary)
		}
	}
}

// primaryOf returns the primary key of a GET for path.
func primaryOf(path string) string {
	return httpcache.PrimaryKey(httptest.NewRequest(http.MethodGet, "http://example.com"+path, nil))
}

// softPurge soft-purges the entry of path, so that the next request revalidates it.
func softPurge(t *testing.T, rg *rig, path string) {
	t.Helper()
	p := store.Purge{Kind: store.PurgeKeys, Keys: []string{primaryOf(path)}, Soft: true, Retain: time.Hour}
	if _, err := rg.spy.Purge(context.Background(), p); err != nil {
		t.Fatal(err)
	}
}

// advance makes the entry of the fill 61 seconds old, past its 60 second lifetime.
func advance(_ *testing.T, rg *rig, _ string) {
	rg.clk.Advance(61 * time.Second)
}

// keepFresh leaves the entry of the fill as it was stored.
func keepFresh(_ *testing.T, _ *rig, _ string) {}

// expectNotStoredAnswer checks the client answer to a revalidation whose 304 may not be stored: the stored body under
// the updated headers of the 304, without hit, stored or ttl.
func expectNotStoredAnswer(t *testing.T, rec *httptest.ResponseRecorder, h http.Header) {
	t.Helper()
	st := lastStatus(t, rec)
	if rec.Code != http.StatusOK || rec.Body.String() != "old" {
		t.Fatalf("client response %d %q, want 200 old", rec.Code, rec.Body.String())
	}
	if st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified || st.Stored || st.HasTTL || st.Collapsed {
		t.Fatalf("status %+v, want fwd=stale fwd-status=304 without hit, stored or ttl", st)
	}
	if got := rec.Header().Get("Etag"); got != `"v1"` {
		t.Errorf("client Etag %q, want \"v1\"", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != h.Get("Cache-Control") {
		t.Errorf("client Cache-Control %q, want the 304's %q", got, h.Get("Cache-Control"))
	}
	if got, want := strings.Join(rec.Header().Values("Set-Cookie"), ","), strings.Join(h.Values("Set-Cookie"), ","); got != want {
		t.Errorf("client Set-Cookie %q, want %q", got, want)
	}
}

func TestRefreshNotStorable(t *testing.T) {
	paths := []struct {
		name string
		fill http.Header
		due  func(*testing.T, *rig, string)
	}{
		{"sync", refreshFill(), advance},
		{"swr", swrHeader(), advance},
		{"soft", refreshFill(), softPurge},
	}
	cases := []struct {
		name   string
		header http.Header
	}{
		{"private", http.Header{"Cache-Control": {"private"}}},
		{"no-store", http.Header{"Cache-Control": {"no-store"}}},
		{"private-max-age", http.Header{"Cache-Control": {"private, max-age=60"}}},
		{"no-store-max-age", http.Header{"Cache-Control": {"no-store, max-age=60"}}},
		{"set-cookie", http.Header{"Cache-Control": {"max-age=60"}, "Set-Cookie": {"s=1"}}},
		{"vary-star", http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"*"}}},
	}
	ttls := []struct {
		name  string
		route string
	}{
		{"none", "stale_if_error: 1h"},
		{"default", "stale_if_error: 1h\ndefault_ttl: 1h"},
	}
	for _, p := range paths {
		for _, c := range cases {
			for _, ttl := range ttls {
				t.Run(p.name+"/"+c.name+"/"+ttl.name, func(t *testing.T) {
					rg := fillRefresh(t, rigOpts{route: ttl.route}, "/nsb", p.fill)
					p.due(t, rg, "/nsb")
					rg.orig.script(http.StatusNotModified, with304(c.header), "")
					rec := rg.get("/nsb", nil)
					if p.name == "swr" {
						if st := lastStatus(t, rec); !st.Hit || st.Fwd != httpcache.FwdStale || st.Detail != detailSWR || rec.Body.String() != "old" {
							t.Fatalf("status %+v body %q, want the stale hit under stale-while-revalidate", st, rec.Body.String())
						}
						rg.settle()
						expectBackground(t, rg, bgNotStored)
					} else {
						expectNotStoredAnswer(t, rec, c.header)
						if rg.requests(resultRevalidated) != 1 {
							t.Errorf("revalidated count %v, want 1", rg.requests(resultRevalidated))
						}
					}
					if n := len(rg.spy.callsOf(opPut)); n != 1 {
						t.Fatalf("puts %d, want the fill only", n)
					}
					expectDeletes(t, rg, 1)
					if rg.stores(storeNotStorable) != 1 || rg.stores(storeStored) != 1 || rg.storeErrs(opDelete) != 0 {
						t.Fatalf("not_storable=%v stored=%v delete errors=%v, want 1 1 0", rg.stores(storeNotStorable), rg.stores(storeStored), rg.storeErrs(opDelete))
					}
					rg.orig.script(http.StatusInternalServerError, nil, "err")
					rec = rg.get("/nsb", nil)
					if rec.Code != http.StatusInternalServerError || rec.Body.String() != "err" {
						t.Fatalf("client response %d %q after the delete, want the origin 500", rec.Code, rec.Body.String())
					}
					if got := rg.orig.last().Get("If-None-Match"); got != "" {
						t.Errorf("origin If-None-Match %q after the delete, want none", got)
					}
					if st := lastStatus(t, rec); st.Fwd != httpcache.FwdURIMiss {
						t.Errorf("fwd %q after the delete, want uri-miss", st.Fwd)
					}
				})
			}
		}
	}
}

func TestRefreshRequestSide(t *testing.T) {
	requests := []struct {
		name  string
		hdr   http.Header
		fresh http.Header
	}{
		{
			name:  "no-store",
			hdr:   http.Header{"Cache-Control": {"no-store"}},
			fresh: http.Header{"Cache-Control": {"no-cache, no-store"}},
		},
		{
			name:  "authorization",
			hdr:   http.Header{"Authorization": {"Bearer t"}},
			fresh: http.Header{"Authorization": {"Bearer t"}, "Cache-Control": {"no-cache"}},
		},
	}
	paths := []struct {
		name  string
		fill  http.Header
		due   func(*testing.T, *rig, string)
		fresh bool
	}{
		{"sync-fresh", refreshFill(), keepFresh, true},
		{"sync-expired", refreshFill(), advance, false},
		{"swr", swrHeader(), advance, false},
		{"soft", refreshFill(), softPurge, false},
	}
	responses := []struct {
		name   string
		header http.Header
	}{
		{"shareable", http.Header{"Cache-Control": {"max-age=60"}}},
		{"private", http.Header{"Cache-Control": {"private, max-age=60"}}},
	}
	for _, p := range paths {
		for _, req := range requests {
			for _, resp := range responses {
				if p.name == "swr" && req.name == "no-store" && resp.name == "private" {
					continue
				}
				t.Run(p.name+"/"+req.name+"/"+resp.name, func(t *testing.T) {
					const path = "/rs"
					rg := fillRefresh(t, rigOpts{route: "stale_if_error: 1h"}, path, p.fill)
					p.due(t, rg, path)
					rg.orig.script(http.StatusNotModified, with304(resp.header), "")
					trigger := req.hdr
					if p.fresh {
						trigger = req.fresh
					}
					rec := rg.get(path, trigger)
					if p.name == "swr" {
						st := lastStatus(t, rec)
						if !st.Hit || st.Fwd != httpcache.FwdStale || st.Detail != detailSWR || rec.Body.String() != "old" {
							t.Fatalf("status %+v body %q, want the stale hit under stale-while-revalidate", st, rec.Body.String())
						}
						rg.settle()
						if req.name == "no-store" {
							if rg.orig.count() != 1 {
								t.Fatalf("origin calls %d, want the fill only", rg.orig.count())
							}
							expectBackground(t, rg, "")
						} else {
							expectBackground(t, rg, bgNotStored)
						}
					} else {
						expectNotStoredAnswer(t, rec, resp.header)
					}
					if n := len(rg.spy.callsOf(opPut)); n != 1 {
						t.Fatalf("puts %d, want the fill only", n)
					}
					if resp.name == "private" {
						expectDeletes(t, rg, 1)
						if rg.stores(storeNotStorable) != 1 {
							t.Fatalf("not_storable %v, want 1", rg.stores(storeNotStorable))
						}
						return
					}
					expectDeletes(t, rg, 0)
					if rg.storeErrs(opDelete) != 0 {
						t.Fatalf("delete errors %v, want 0", rg.storeErrs(opDelete))
					}
					if (p.name != "swr" || req.name != "no-store") && rg.stores(storeNotStorable) != 1 {
						t.Fatalf("not_storable %v, want 1", rg.stores(storeNotStorable))
					}
					before := rg.orig.count()
					switch p.name {
					case "sync-fresh":
						if st := lastStatus(t, rg.get(path, nil)); !st.Hit || rg.orig.count() != before {
							t.Fatalf("entry not intact: status %+v, origin calls %d want %d", st, rg.orig.count(), before)
						}
					case "sync-expired", "soft":
						next := rg.get(path, nil)
						if st := lastStatus(t, next); !st.Hit || st.FwdStatus != http.StatusNotModified {
							t.Fatalf("entry not intact: status %+v", st)
						}
						if got := rg.orig.last().Get("If-None-Match"); got != `"v1"` {
							t.Fatalf("origin If-None-Match %q, want \"v1\"", got)
						}
						if n := len(rg.spy.callsOf(opPut)); n != 2 {
							t.Fatalf("puts %d, want the fill and the stored revalidation", n)
						}
					case "swr":
						if st := lastStatus(t, rg.get(path, nil)); !st.Hit || st.Detail != detailSWR {
							t.Fatalf("entry not intact: status %+v, want the stale hit", st)
						}
						rg.settle()
					}
				})
			}
		}
	}
	t.Run("sync-expired/authorization/public", func(t *testing.T) {
		const path = "/rs"
		rg := fillRefresh(t, rigOpts{route: "stale_if_error: 1h"}, path, refreshFill())
		advance(t, rg, path)
		rg.orig.script(http.StatusNotModified, with304(http.Header{"Cache-Control": {"public, max-age=60"}}), "")
		rec := rg.get(path, http.Header{"Authorization": {"Bearer t"}})
		if st := lastStatus(t, rec); !st.Hit {
			t.Fatalf("status %+v, want hit", st)
		}
		expectDeletes(t, rg, 0)
		if n := len(rg.spy.callsOf(opPut)); n != 2 {
			t.Fatalf("puts %d, want 2", n)
		}
	})
}

func TestRefreshNotStorableWaiters(t *testing.T) {
	rg := newRig(t, rigOpts{route: "enabled: true\nkeep: 1h"})
	rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=1"}, "Etag": {`"v1"`}}, "old")
	rg.get("/w", nil)
	rg.clk.Advance(2 * time.Second)
	rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
		ex := reqctx.ExchangeFrom(r.Context())
		ex.Begin()
		w.Header().Set("Cache-Control", "private, max-age=60")
		w.Header().Set("Etag", `"v1"`)
		w.WriteHeader(http.StatusNotModified)
		ex.Finish()
	})
	arrived, release := rg.orig.hold()
	t.Cleanup(release)
	leader := serveAsync(rg, newGet(context.Background(), "/w", nil))
	testwait.Recv(t, "leader at origin", arrived, nil)
	first := serveAsync(rg, newGet(context.Background(), "/w", nil))
	second := serveAsync(rg, newGet(context.Background(), "/w", nil))
	awaitWaiters(t, rg, 2)
	release()
	answers := []<-chan served{leader, first, second}
	for i, ch := range answers {
		rec := awaitServed(t, "request returned", ch).rec
		st := lastStatus(t, rec)
		if rec.Code != http.StatusOK || rec.Body.String() != "old" {
			t.Errorf("request %d answered %d %q, want 200 old", i, rec.Code, rec.Body.String())
		}
		if st.Hit || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified || st.Collapsed {
			t.Errorf("request %d status %+v, want fwd-status 304 without hit or collapse", i, st)
		}
	}
	if n := rg.coalesced(); n != 0 {
		t.Errorf("coalesced %v, want 0", n)
	}
	if n := rg.orig.conditional(); n != 3 {
		t.Errorf("conditional origin requests %d, want 3", n)
	}
	expectDeletes(t, rg, 3)
	if n := len(rg.spy.callsOf(opPut)); n != 1 {
		t.Errorf("puts %d, want the fill only", n)
	}
}

func TestRefreshNotStorableVaryVariant(t *testing.T) {
	rg := newRig(t, rigOpts{route: "stale_if_error: 1h"})
	en := http.Header{"Accept-Language": {"en"}}
	fr := http.Header{"Accept-Language": {"fr"}}
	fill := http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}, "Vary": {"Accept-Language"}}
	rg.orig.script(http.StatusOK, fill, "en")
	rg.get("/vary", en)
	rg.orig.script(http.StatusOK, fill, "fr")
	rg.get("/vary", fr)
	if n := len(rg.spy.callsOf(opPut)); n != 2 {
		t.Fatalf("puts %d, want one per variant", n)
	}
	rg.orig.script(http.StatusNotModified, with304(http.Header{"Cache-Control": {"private, max-age=60"}}), "")
	noCache := http.Header{"Accept-Language": {"en"}, "Cache-Control": {"no-cache"}}
	if rec := rg.get("/vary", noCache); rec.Code != http.StatusOK || rec.Body.String() != "en" {
		t.Fatalf("client response %d %q, want 200 en", rec.Code, rec.Body.String())
	}
	expectDeletes(t, rg, 1)
	count := rg.orig.count()
	if rec := rg.get("/vary", fr); !hit(t, rec) || rg.orig.count() != count {
		t.Fatalf("fr variant not kept: origin calls %d want %d", rg.orig.count(), count)
	}
	rg.orig.script(http.StatusOK, fill, "en")
	if rec := rg.get("/vary", en); hit(t, rec) {
		t.Fatal("en variant still served from the cache after its delete")
	}
	if got := rg.orig.last().Get("If-None-Match"); got != "" {
		t.Errorf("origin If-None-Match %q after the delete, want none", got)
	}
}

func TestRefreshDeleteFailure(t *testing.T) {
	const path = "/df"
	private := with304(http.Header{"Cache-Control": {"private, max-age=60"}})
	trigger := http.Header{"Cache-Control": {"no-cache"}}
	ref := fillRefresh(t, rigOpts{route: "stale_if_error: 1h"}, path, refreshFill())
	ref.orig.script(http.StatusNotModified, private, "")
	base := ref.get(path, trigger)

	setup := func(t *testing.T) (*rig, *bytes.Buffer, func() []string) {
		t.Helper()
		clk := clock.NewFake(start)
		errs, logs := logBuffer(clk)
		var mu sync.Mutex
		var hooked []string
		rg := fillRefresh(t, rigOpts{route: "stale_if_error: 1h", guard: true, clk: clk, errs: errs, onStoreError: func(op string) {
			mu.Lock()
			hooked = append(hooked, op)
			mu.Unlock()
		}}, path, refreshFill())
		rg.orig.script(http.StatusNotModified, private, "")
		return rg, logs, func() []string {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(hooked)
		}
	}
	check := func(t *testing.T, rg *rig, rec *httptest.ResponseRecorder, logs *bytes.Buffer) {
		t.Helper()
		if rec.Code != base.Code || rec.Body.String() != base.Body.String() || !reflect.DeepEqual(rec.Header(), base.Header()) {
			t.Fatalf("response %d %q %v, want the baseline %d %q %v", rec.Code, rec.Body.String(), rec.Header(), base.Code, base.Body.String(), base.Header())
		}
		if out := logs.String(); !strings.Contains(out, "cache store call failed") || !strings.Contains(out, "op=delete") {
			t.Errorf("log %q lacks the delete failure", out)
		}
		if rg.stores(storeNotStorable) != 1 {
			t.Errorf("not_storable %v, want 1", rg.stores(storeNotStorable))
		}
		if n := len(rg.spy.callsOf(opPut)); n != 1 {
			t.Errorf("puts %d, want the fill only", n)
		}
	}

	t.Run("error", func(t *testing.T) {
		rg, logs, hooked := setup(t)
		rg.spy.setErr(opDelete, errStoreDown)
		rec := rg.get(path, trigger)
		check(t, rg, rec, logs)
		if rg.storeErrs(opDelete) != 1 {
			t.Fatalf("delete store errors %v, want 1", rg.storeErrs(opDelete))
		}
		if got := strings.Join(hooked(), ","); got != opDelete {
			t.Fatalf("OnStoreError calls %q, want [delete]", got)
		}
		v := fillCall(t, rg)
		sel := func([]string) (string, bool) { return v.Secondary, true }
		rg.clk.Advance(61*time.Minute - time.Nanosecond)
		if lk, err := rg.spy.inner.Get(context.Background(), v.Primary, sel); err != nil || lk.Entry == nil {
			t.Fatalf("entry gone before its storage deadline: %+v %v", lk, err)
		}
		rg.clk.Advance(time.Nanosecond)
		if lk, err := rg.spy.inner.Get(context.Background(), v.Primary, sel); err != nil || lk.Entry != nil {
			t.Fatalf("entry present at its storage deadline: %+v %v", lk, err)
		}
	})
	t.Run("panic", func(t *testing.T) {
		rg, logs, hooked := setup(t)
		rg.spy.panicOn(opDelete)
		rec := rg.get(path, trigger)
		check(t, rg, rec, logs)
		if rg.storeErrs(opDelete) != 1 {
			t.Fatalf("delete store errors %v, want 1", rg.storeErrs(opDelete))
		}
		if got := strings.Join(hooked(), ","); got != opDelete {
			t.Fatalf("OnStoreError calls %q, want [delete]", got)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		rg, logs, hooked := setup(t)
		release := rg.spy.hang(opDelete)
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() { done <- rg.get(path, trigger) }()
		testwait.Until(t, "delete call parked", func() bool { return len(rg.spy.callsOf(opDelete)) == 1 }, nil)
		rg.clk.Advance(100 * time.Millisecond)
		rec, ok := testwait.Recv(t, "revalidated response", done, nil)
		release()
		if !ok {
			t.Fatal("no response")
		}
		check(t, rg, rec, logs)
		if rg.storeErrs(opDelete) != 1 {
			t.Fatalf("delete store errors %v, want 1", rg.storeErrs(opDelete))
		}
		if got := strings.Join(hooked(), ","); got != opDelete {
			t.Fatalf("OnStoreError calls %q, want [delete]", got)
		}
	})
	t.Run("guard-open", func(t *testing.T) {
		rg, logs, hooked := setup(t)
		rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
			for range 2 {
				_ = rg.guard.Do(r.Context(), time.Second, func(context.Context) error { return errStoreDown })
			}
			w.Header().Set("Cache-Control", "private, max-age=60")
			w.Header().Set("Etag", `"v1"`)
			w.WriteHeader(http.StatusNotModified)
		})
		rec := rg.get(path, trigger)
		check(t, rg, rec, logs)
		if rg.storeErrs(opDelete) != 0 {
			t.Fatalf("delete store errors %v, want 0 for a short-circuited call", rg.storeErrs(opDelete))
		}
		if got := hooked(); len(got) != 0 {
			t.Fatalf("OnStoreError calls %v, want none", got)
		}
	})
}

func TestRefreshLifetimeFromMergedHeaders(t *testing.T) {
	stamp := func(d time.Duration) string { return start.Add(d).Format(http.TimeFormat) }
	cases := []struct {
		name     string
		route    string
		header   http.Header
		ttl      time.Duration
		seconds  int64
		boundary bool
	}{
		{
			name:     "no freshness on the 304",
			route:    "stale_if_error: 1h",
			header:   with304(http.Header{}),
			ttl:      60*time.Second + time.Hour,
			seconds:  60,
			boundary: true,
		},
		{
			name:    "s-maxage wins",
			route:   "stale_if_error: 1h",
			header:  with304(http.Header{"Cache-Control": {"s-maxage=30, max-age=600"}}),
			ttl:     30*time.Second + time.Hour,
			seconds: 30,
		},
		{
			name:    "expires cannot extend max-age",
			route:   "stale_if_error: 1h",
			header:  with304(http.Header{"Date": {stamp(61 * time.Second)}, "Expires": {stamp(61*time.Second + time.Hour)}}),
			ttl:     60*time.Second + time.Hour,
			seconds: 60,
		},
		{
			name:    "max_ttl caps",
			route:   "stale_if_error: 1h\nmax_ttl: 10m",
			header:  with304(http.Header{"Cache-Control": {"max-age=86400"}}),
			ttl:     10*time.Minute + time.Hour,
			seconds: 600,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rg := fillRefresh(t, rigOpts{route: c.route}, "/life", refreshFill())
			rg.clk.Advance(61 * time.Second)
			rg.orig.script(http.StatusNotModified, c.header, "")
			rec := rg.get("/life", nil)
			if st := lastStatus(t, rec); !st.Hit || !st.HasTTL || st.TTL != c.seconds {
				t.Fatalf("status %+v, want hit with ttl %d", st, c.seconds)
			}
			if puts := rg.spy.callsOf(opPut); len(puts) != 2 || puts[1].TTL != c.ttl {
				t.Fatalf("puts %+v, want a second put with ttl %v", puts, c.ttl)
			}
			expectDeletes(t, rg, 0)
			if !c.boundary {
				return
			}
			rg.clk.Advance(60*time.Second - time.Nanosecond)
			count := rg.orig.count()
			if rec := rg.get("/life", nil); !hit(t, rec) || rg.orig.count() != count {
				t.Fatalf("entry not fresh before its lifetime ends: origin calls %d want %d", rg.orig.count(), count)
			}
			rg.clk.Advance(time.Nanosecond)
			count = rg.orig.count()
			rg.get("/life", nil)
			if rg.orig.count() != count+1 {
				t.Fatalf("origin calls %d, want one conditional request at the lifetime end", rg.orig.count())
			}
		})
	}
}
