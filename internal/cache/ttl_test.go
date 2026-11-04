package cache

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
)

const ttlLifetime = 60 * time.Second

var etagOrigin = http.Header{"Etag": {`"v1"`}}

func withHeaders(h http.Header) http.Header {
	out := etagOrigin.Clone()
	out["Content-Length"] = []string{"1"}
	for k, v := range h {
		out[k] = v
	}
	return out
}

func TestTTLBoundaries(t *testing.T) {
	sources := []struct {
		name     string
		route    string
		origin   http.Header
		maxStale bool
	}{
		{name: "max-age", route: "stale_if_error: 1h",
			origin: withHeaders(http.Header{"Cache-Control": {"max-age=60"}}), maxStale: true},
		{name: "s-maxage over larger max-age", route: "stale_if_error: 1h",
			origin: withHeaders(http.Header{"Cache-Control": {"max-age=3600, s-maxage=60"}})},
		{name: "expires", route: "stale_if_error: 1h",
			origin: withHeaders(http.Header{"Date": {start.Format(http.TimeFormat)}, "Expires": {start.Add(ttlLifetime).Format(http.TimeFormat)}}), maxStale: true},
		{name: "default_ttl", route: "default_ttl: 60s\nstale_if_error: 1h",
			origin: withHeaders(nil), maxStale: true},
		{name: "max-age clamped to max_ttl", route: "max_ttl: 60s\nstale_if_error: 1h",
			origin: withHeaders(http.Header{"Cache-Control": {"max-age=3600"}}), maxStale: true},
	}
	for _, src := range sources {
		t.Run(src.name, func(t *testing.T) {
			rg := newRig(t, rigOpts{route: src.route})
			rg.orig.script(http.StatusOK, src.origin, "x")
			rg.get("/ttl", nil)
			rg.clk.Advance(ttlLifetime - time.Nanosecond)
			rec := rg.get("/ttl", nil)
			st := lastStatus(t, rec)
			if !st.Hit || !st.HasTTL || st.TTL != 1 {
				t.Fatalf("at L-1ns: %+v, want hit with ttl=1", st)
			}
			if got := rec.Header().Get("Age"); got != strconv.Itoa(int(ttlLifetime/time.Second)-1) {
				t.Fatalf("at L-1ns: Age %q, want %d", got, int(ttlLifetime/time.Second)-1)
			}
			rg.clk.Advance(time.Nanosecond)
			rec = rg.get("/ttl", nil)
			if st := lastStatus(t, rec); st.Hit || st.Fwd != httpcache.FwdStale {
				t.Fatalf("at L: %+v, want stale with a conditional forward", st)
			}
			if got := rg.orig.last().Get("If-None-Match"); got != `"v1"` {
				t.Fatalf("at L: origin If-None-Match %q, want \"v1\"", got)
			}
			if !src.maxStale {
				return
			}
			ms := newRig(t, rigOpts{route: src.route})
			ms.orig.script(http.StatusOK, src.origin, "x")
			ms.get("/ttl", nil)
			ms.clk.Advance(ttlLifetime)
			maxStale := http.Header{"Cache-Control": {"max-stale=3600"}}
			rec = ms.get("/ttl", maxStale)
			if st := lastStatus(t, rec); !st.Hit || !st.HasTTL || st.TTL != 0 {
				t.Fatalf("max-stale at L: %+v, want hit with ttl=0", st)
			}
			ms.clk.Advance(time.Nanosecond)
			rec = ms.get("/ttl", maxStale)
			if st := lastStatus(t, rec); !st.Hit || !st.HasTTL || st.TTL != -1 {
				t.Fatalf("max-stale at L+1ns: %+v, want hit with ttl=-1", st)
			}
			if ms.orig.count() != 1 {
				t.Fatalf("origin calls %d, want 1: serving stale must not refill", ms.orig.count())
			}
		})
	}
}

func TestStorageBoundary(t *testing.T) {
	cases := []struct {
		name         string
		route        string
		keep         time.Duration
		noBackground bool
	}{
		{name: "keep", route: "keep: 30s", keep: 30 * time.Second},
		{name: "stale_while_revalidate", route: "stale_while_revalidate: 40s", keep: 40 * time.Second, noBackground: true},
		{name: "stale_if_error", route: "stale_if_error: 50s", keep: 50 * time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := ttlLifetime + c.keep
			probe := func(offset time.Duration) *rig {
				rg := newRig(t, rigOpts{route: "enabled: true\n" + c.route, noBackground: c.noBackground})
				rg.orig.script(http.StatusOK, withHeaders(http.Header{"Cache-Control": {"max-age=60"}}), "x")
				rg.get("/s", nil)
				rg.clk.Advance(offset)
				return rg
			}
			rg := probe(s - time.Nanosecond)
			rec := rg.get("/s", nil)
			if st := lastStatus(t, rec); st.Fwd != httpcache.FwdStale {
				t.Fatalf("at S-1ns: %+v, want conditional forward", st)
			}
			if got := rg.orig.last().Get("If-None-Match"); got != `"v1"` {
				t.Fatalf("at S-1ns: origin If-None-Match %q, want \"v1\"", got)
			}
			for _, at := range []time.Duration{s, s + time.Nanosecond} {
				rg := probe(at)
				rec := rg.get("/s", nil)
				st := lastStatus(t, rec)
				if st.Fwd != httpcache.FwdURIMiss {
					t.Fatalf("at %v: fwd %q, want uri-miss", at, st.Fwd)
				}
				if got := rg.orig.last().Get("If-None-Match"); got != "" {
					t.Fatalf("at %v: origin request still conditional (%q)", at, got)
				}
			}
		})
	}
	t.Run("keep adds nothing without a validator", func(t *testing.T) {
		probe := func(offset time.Duration) *rig {
			rg := newRig(t, rigOpts{route: "keep: 30s"})
			rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
			rg.get("/nv", nil)
			rg.clk.Advance(offset)
			return rg
		}
		if !hit(t, probe(ttlLifetime-time.Nanosecond).get("/nv", nil)) {
			t.Fatal("at L-1ns: want hit")
		}
		if st := lastStatus(t, probe(ttlLifetime).get("/nv", nil)); st.Fwd != httpcache.FwdURIMiss {
			t.Fatalf("at L: fwd %q, want uri-miss", st.Fwd)
		}
	})
}

func TestTTLInputs(t *testing.T) {
	date := start.Format(http.TimeFormat)
	cases := []struct {
		name      string
		route     string
		origin    http.Header
		stored    bool
		ttl       *int64
		secondHit bool
		secondFwd string
	}{
		{name: "max-age=0 is not stored", origin: withHeaders(http.Header{"Cache-Control": {"max-age=0"}}), stored: false},
		{name: "max-age=-1 is zero lifetime and default_ttl is not used", route: "default_ttl: 30s",
			origin: withHeaders(http.Header{"Cache-Control": {"max-age=-1"}}), stored: false},
		{name: "40-digit max-age saturates and clamps to max_ttl", origin: withHeaders(http.Header{"Cache-Control": {"max-age=1234567890123456789012345678901234567890"}}),
			stored: true, ttl: ptr(int64(86400)), secondHit: true},
		{name: "Expires 0 gives zero lifetime", origin: withHeaders(http.Header{"Expires": {"0"}}), stored: false},
		{name: "Expires before Date gives zero lifetime", origin: withHeaders(http.Header{
			"Date":    {start.Add(10 * time.Second).Format(http.TimeFormat)},
			"Expires": {start.Add(5 * time.Second).Format(http.TimeFormat)},
		}), stored: false},
		{name: "invalid Date is replaced by the response time", origin: withHeaders(http.Header{
			"Date":    {"not a date"},
			"Expires": {start.Add(ttlLifetime).Format(http.TimeFormat)},
		}), stored: true, ttl: ptr(int64(60)), secondHit: true},
		{name: "negative Age is ignored", origin: withHeaders(http.Header{"Cache-Control": {"max-age=60"}, "Age": {"-1"}, "Date": {date}}),
			stored: true, ttl: ptr(int64(60)), secondHit: true},
		{name: "Age above 2^31 seconds is stored and stale on first read", origin: withHeaders(http.Header{
			"Cache-Control": {"max-age=60"}, "Age": {"4294967296"}, "Date": {date},
		}), stored: true, secondFwd: httpcache.FwdStale},
		{name: "no freshness and default_ttl 0 is not stored", origin: withHeaders(nil), stored: false},
		{name: "no freshness and default_ttl 30s is stored for 30s", route: "default_ttl: 30s",
			origin: withHeaders(nil), stored: true, ttl: ptr(int64(30)), secondHit: true},
		{name: "keep with validator and max-age=0 revalidates every request", route: "keep: 1h",
			origin: withHeaders(http.Header{"Cache-Control": {"max-age=0"}}), stored: true, secondFwd: httpcache.FwdStale},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rg := newRig(t, rigOpts{route: c.route})
			rg.orig.script(http.StatusOK, c.origin, "x")
			rec := rg.get("/in", nil)
			st := lastStatus(t, rec)
			if st.Stored != c.stored {
				t.Fatalf("stored=%v, want %v (%+v)", st.Stored, c.stored, st)
			}
			if c.ttl != nil && (!st.HasTTL || st.TTL != *c.ttl) {
				t.Fatalf("ttl=%d (has=%v), want %d", st.TTL, st.HasTTL, *c.ttl)
			}
			puts := len(rg.spy.callsOf(opPut))
			if c.stored && puts != 1 || !c.stored && puts != 0 {
				t.Fatalf("puts %d with stored=%v", puts, c.stored)
			}
			if c.secondFwd == "" && !c.stored {
				return
			}
			second := lastStatus(t, rg.get("/in", nil))
			if second.Hit != c.secondHit {
				t.Fatalf("second hit=%v, want %v", second.Hit, c.secondHit)
			}
			if c.secondFwd != "" && second.Fwd != c.secondFwd {
				t.Fatalf("second fwd %q, want %q", second.Fwd, c.secondFwd)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestApparentAge(t *testing.T) {
	rg := newRig(t, rigOpts{})
	rg.orig.script(http.StatusOK, http.Header{
		"Cache-Control":  {"max-age=60"},
		"Age":            {"30"},
		"Date":           {start.Add(-5 * time.Second).Format(http.TimeFormat)},
		"Content-Length": {"1"},
	}, "x")
	fill := lastStatus(t, rg.get("/age", nil))
	if !fill.Stored || !fill.HasTTL || fill.TTL != 30 {
		t.Fatalf("fill %+v, want stored with ttl=30", fill)
	}
	rec := rg.get("/age", nil)
	if got := rec.Header().Get("Age"); got != "30" {
		t.Fatalf("Age %q, want 30", got)
	}
	rg.clk.Advance(10 * time.Second)
	rec = rg.get("/age", nil)
	st := lastStatus(t, rec)
	if rec.Header().Get("Age") != "40" || !st.HasTTL || st.TTL != 20 {
		t.Fatalf("after 10s Age %q status %+v, want Age 40 ttl 20", rec.Header().Get("Age"), st)
	}
}

func TestClockSkewSharedStore(t *testing.T) {
	type decision struct {
		hit  bool
		fwd  string
		age  string
		ttl  int64
		hasT bool
	}
	decide := func(t *testing.T, fillerIsA bool, offset time.Duration) decision {
		storeClk := clock.NewFake(start)
		sp := newSpy(t, storeClk)
		gA := clock.NewFake(start.Add(time.Hour))
		gB := clock.NewFake(start.Add(-time.Hour))
		a := newRig(t, rigOpts{route: "stale_if_error: 1h", clk: gA, spy: sp})
		b := newRig(t, rigOpts{route: "stale_if_error: 1h", clk: gB, spy: sp})
		for _, r := range []*rig{a, b} {
			r.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Etag": {`"v1"`}, "Content-Length": {"1"}}, "x")
		}
		filler, other := a, b
		if !fillerIsA {
			filler, other = b, a
		}
		filler.get("/skew", nil)
		storeClk.Advance(offset)
		rec := other.get("/skew", nil)
		st := lastStatus(t, rec)
		return decision{hit: st.Hit, fwd: st.Fwd, age: rec.Header().Get("Age"), ttl: st.TTL, hasT: st.HasTTL}
	}
	for _, fillerIsA := range []bool{true, false} {
		fresh := decide(t, fillerIsA, ttlLifetime-time.Nanosecond)
		if !fresh.hit || fresh.age != "59" || !fresh.hasT || fresh.ttl != 1 {
			t.Fatalf("filler A=%v at L-1ns: %+v", fillerIsA, fresh)
		}
		for _, offset := range []time.Duration{ttlLifetime, ttlLifetime + time.Nanosecond} {
			d := decide(t, fillerIsA, offset)
			if d.hit || d.fwd != httpcache.FwdStale {
				t.Fatalf("filler A=%v at %v: %+v, want stale", fillerIsA, offset, d)
			}
		}
	}
}
