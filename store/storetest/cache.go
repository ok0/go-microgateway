package storetest

import (
	"bytes"
	"context"
	"errors"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/store"
)

const retain = store.MaxTTL

type cSelector struct {
	sec   string
	ok    bool
	calls int
	names []string
}

func (c *cSelector) sel(names []string) (string, bool) {
	c.calls++
	c.names = slices.Clone(names)
	return c.sec, c.ok
}

type cVariant struct {
	p, s, state string
}

// Cache runs the conformance subtests against the response-cache backend described by h.
func Cache(t *testing.T, h Harness[store.Cache]) {
	if h.New == nil || h.NewAt == nil || h.Advance == nil {
		t.Fatal("cache harness needs New, NewAt and Advance")
	}
	if h.Horizon.IsZero() {
		t.Fatal("cache harness needs a Horizon")
	}
	res := h.Resolution
	if res == 0 {
		res = time.Nanosecond
	}
	if res <= 0 || time.Second%res != 0 {
		t.Fatalf("cache harness Resolution %v must be positive and divide one second", res)
	}
	ctx := context.Background()

	t.Run("RoundTrip", func(t *testing.T) {
		a, b := h.New(t)
		miss := cget(t, a, "rt", "s")
		if miss.Entry != nil || len(miss.VaryNames) != 0 || miss.Token.IsZero() {
			t.Errorf("first Get: entry nil %v, %d vary names, zero token %v; want an empty miss with a token", miss.Entry == nil, len(miss.VaryNames), miss.Token.IsZero())
		}
		fill := &store.Entry{
			Status:     203,
			Header:     http.Header{"X-A": {"1", "2"}},
			Body:       []byte("b\x00dy"),
			Tags:       []string{"t1", "T1"},
			InitialAge: 42 * time.Second,
		}
		if !cput(t, a, miss.Token, "rt", "s", nil, fill, time.Hour) {
			t.Fatal("round trip fill was not stored")
		}
		exp := &store.Entry{
			Status:     203,
			Header:     http.Header{"X-A": {"1", "2"}},
			Body:       []byte("b\x00dy"),
			Tags:       []string{"t1", "T1"},
			InitialAge: 42 * time.Second,
		}
		for _, s := range []store.Cache{b, a} {
			l := cget(t, s, "rt", "s")
			if l.Entry == nil {
				t.Fatal("round trip: miss, want hit")
			}
			cSame(t, "round trip", l.Entry, exp)
			if l.Entry.Stale || !l.Now.Equal(l.Entry.StoredAt) {
				t.Errorf("round trip: stale %v, now %v, stored at %v; want fresh with now equal to stored at", l.Entry.Stale, l.Now, l.Entry.StoredAt)
			}
		}
	})

	t.Run("InputBuffersCopied", func(t *testing.T) {
		a, b := h.New(t)
		names := []string{"X-B", "Accept"}
		hdr := http.Header{"X-V": {"orig"}}
		body := []byte("orig-body")
		tg := []string{"tag-1"}
		e := &store.Entry{Status: 200, Header: hdr, Body: body, Tags: tg}
		if !cfill(t, a, "ib", "s", names, e, time.Hour) {
			t.Fatal("input fill was not stored")
		}
		hdr["X-V"][0] = "changed"
		body[0] = 'X'
		tg[0] = "changed"
		names[0] = "changed"
		l := cget(t, b, "ib", "s")
		if l.Entry == nil {
			t.Fatal("input copy: miss, want hit")
		}
		cSame(t, "input copy", l.Entry, &store.Entry{Status: 200, Header: http.Header{"X-V": {"orig"}}, Body: []byte("orig-body"), Tags: []string{"tag-1"}})
		if !slices.Equal(l.VaryNames, []string{"accept", "x-b"}) {
			t.Errorf("input copy: vary names %q, want [accept x-b]", l.VaryNames)
		}
	})

	t.Run("ClockAuthority", func(t *testing.T) {
		a, b := h.New(t)
		e := cent("clk")
		e.StoredAt = time.Unix(1, 0)
		e.Stale = true
		if !cfill(t, a, "clk", "s", nil, e, time.Hour) {
			t.Fatal("clock fill was not stored")
		}
		for _, s := range []store.Cache{a, b} {
			l := cget(t, s, "clk", "s")
			if l.Entry == nil {
				t.Fatal("clock: miss, want hit")
			}
			if l.Entry.Stale || l.Entry.StoredAt.Equal(time.Unix(1, 0)) || l.Now.Sub(l.Entry.StoredAt) != 0 {
				t.Errorf("clock: stale %v, stored at %v, age %v; want fresh, store-clock StoredAt and age 0", l.Entry.Stale, l.Entry.StoredAt, l.Now.Sub(l.Entry.StoredAt))
			}
		}
		h.Advance(t, 1500*time.Millisecond)
		la := cget(t, a, "clk", "s")
		lb := cget(t, b, "clk", "s")
		if la.Entry == nil || lb.Entry == nil {
			t.Fatal("clock after advance: miss, want hit")
		}
		if !la.Now.Equal(lb.Now) || !la.Entry.StoredAt.Equal(lb.Entry.StoredAt) {
			t.Errorf("clock after advance: handles disagree: now %v and %v, stored at %v and %v", la.Now, lb.Now, la.Entry.StoredAt, lb.Entry.StoredAt)
		}
		for _, l := range []store.Lookup{la, lb} {
			if l.Now.Sub(l.Entry.StoredAt) != 1500*time.Millisecond {
				t.Errorf("clock after advance: age %v, want 1.5s", l.Now.Sub(l.Entry.StoredAt))
			}
		}
	})

	t.Run("VarySelection", func(t *testing.T) {
		a, b := h.New(t)
		names := []string{"Accept-Encoding", "accept-encoding", "X-B"}
		wantNames := []string{"accept-encoding", "x-b"}
		if !cfill(t, a, "vs", "s1", names, cent("vs1"), time.Hour) {
			t.Fatal("vary fill was not stored")
		}
		sel := &cSelector{sec: "s1", ok: true}
		l := cgetSel(t, b, "vs", sel.sel)
		if sel.calls != 1 || !slices.Equal(sel.names, wantNames) {
			t.Errorf("sel: %d calls with %q, want 1 call with %q", sel.calls, sel.names, wantNames)
		}
		wantHit(t, "vary hit", l, "vs1", false)
		if !slices.Equal(l.VaryNames, wantNames) {
			t.Errorf("vary hit: vary names %q, want %q", l.VaryNames, wantNames)
		}

		miss := &cSelector{sec: "s1", ok: false}
		l = cgetSel(t, b, "vs", miss.sel)
		wantMiss(t, "vary declined", l)
		if !slices.Equal(l.VaryNames, wantNames) || miss.calls != 1 {
			t.Errorf("vary declined: vary names %q, sel calls %d; want %q and 1 call", l.VaryNames, miss.calls, wantNames)
		}

		absent := &cSelector{sec: "s1", ok: true}
		cgetSel(t, b, "vs-absent", absent.sel)
		if absent.calls != 0 {
			t.Errorf("absent primary: sel called %d times, want 0", absent.calls)
		}

		if !cfill(t, a, "vs", "s2", []string{"x-c"}, cent("vs2"), time.Hour) {
			t.Fatal("second vary fill was not stored")
		}
		other := &cSelector{sec: "s1", ok: true}
		l = cgetSel(t, b, "vs", other.sel)
		wantMiss(t, "discarded variant", l)
		if !slices.Equal(l.VaryNames, []string{"x-c"}) {
			t.Errorf("discarded variant: vary names %q, want [x-c]", l.VaryNames)
		}

		rn := []string{"x-a"}
		if !cfill(t, a, "vr", "v1", rn, cent("v1"), time.Hour) {
			t.Fatal("record fill v1 was not stored")
		}
		if !cfill(t, a, "vr", "v2", rn, cent("v2"), 10*time.Second) {
			t.Fatal("record fill v2 was not stored")
		}
		h.Advance(t, 10*time.Second)
		short := &cSelector{sec: "v2", ok: true}
		l = cgetSel(t, b, "vr", short.sel)
		wantMiss(t, "expired variant with live record", l)
		if !slices.Equal(l.VaryNames, rn) || short.calls != 1 {
			t.Errorf("expired variant with live record: vary names %q, sel calls %d; want %q and 1 call", l.VaryNames, short.calls, rn)
		}
		h.Advance(t, time.Hour-10*time.Second)
		gone := &cSelector{sec: "v1", ok: true}
		wantMiss(t, "record gone", cgetSel(t, b, "vr", gone.sel))
		if gone.calls != 0 {
			t.Errorf("record gone: sel called %d times, want 0", gone.calls)
		}
	})

	t.Run("VariantCapOldestFirst", func(t *testing.T) {
		a, b := h.New(t)
		for i := 0; i < store.MaxVariants; i++ {
			cstore(t, a, "vc", "v"+strconv.Itoa(i), nil, cent("b"+strconv.Itoa(i)), time.Hour)
			h.Advance(t, res)
		}
		cstore(t, a, "vc", "v32", nil, cent("b32"), time.Hour)
		wantMiss(t, "oldest variant at cap", cget(t, b, "vc", "v0"))
		for i := 1; i <= 32; i++ {
			wantHit(t, "variant after cap", cget(t, b, "vc", "v"+strconv.Itoa(i)), "b"+strconv.Itoa(i), false)
		}
		cstore(t, a, "vc", "v5", nil, cent("b5"), time.Hour)
		for i := 1; i <= 32; i++ {
			wantHit(t, "variant after re-put at cap", cget(t, b, "vc", "v"+strconv.Itoa(i)), "b"+strconv.Itoa(i), false)
		}
	})

	t.Run("TTLBoundaries", func(t *testing.T) {
		for _, c := range []struct {
			name string
			ttl  time.Duration
		}{{"1s", time.Second}, {"1h", time.Hour}, {"24h", 24 * time.Hour}, {"MaxTTL", store.MaxTTL}} {
			t.Run(c.name, func(t *testing.T) {
				a, b := h.New(t)
				cstore(t, a, "tb", "s", nil, cent("ttl"), c.ttl)
				h.Advance(t, c.ttl-res)
				for _, s := range []store.Cache{a, b} {
					l := cget(t, s, "tb", "s")
					wantHit(t, "before deadline", l, "ttl", false)
					if l.Entry != nil && l.Now.Sub(l.Entry.StoredAt) != c.ttl-res {
						t.Errorf("before deadline: age %v, want %v", l.Now.Sub(l.Entry.StoredAt), c.ttl-res)
					}
				}
				h.Advance(t, res)
				wantMiss(t, "at deadline on a", cget(t, a, "tb", "s"))
				wantMiss(t, "at deadline on b", cget(t, b, "tb", "s"))
				h.Advance(t, res)
				wantMiss(t, "after deadline on a", cget(t, a, "tb", "s"))
				wantMiss(t, "after deadline on b", cget(t, b, "tb", "s"))
			})
		}
	})

	t.Run("TTLNonPositive", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "k", "s", nil, cent("v1"), time.Hour)
		before := cget(t, b, "k", "s")
		if before.Entry == nil {
			t.Fatal("non-positive ttl: setup miss, want hit")
		}
		for _, ttl := range []time.Duration{0, -time.Nanosecond, math.MinInt64} {
			tok := cget(t, a, "k", "s").Token
			stored, err := a.Put(ctx, tok, "k", "s", nil, cent("v2"), ttl)
			if stored || err != nil {
				t.Errorf("Put with ttl %v: stored %v, error %v; want false and nil", ttl, stored, err)
			}
			l := cget(t, b, "k", "s")
			wantHit(t, "after non-positive ttl", l, "v1", false)
			if l.Entry != nil && !l.Entry.StoredAt.Equal(before.Entry.StoredAt) {
				t.Errorf("after non-positive ttl %v: stored at %v, want unchanged %v", ttl, l.Entry.StoredAt, before.Entry.StoredAt)
			}
			ztok := cget(t, a, "z", "s").Token
			stored, err = a.Put(ctx, ztok, "z", "s", nil, cent("v2"), ttl)
			if stored || err != nil {
				t.Errorf("absent key with ttl %v: stored %v, error %v; want false and nil", ttl, stored, err)
			}
			wantMiss(t, "absent key after non-positive ttl", cget(t, b, "z", "s"))
		}
		h.Advance(t, time.Hour-res)
		wantHit(t, "before deadline", cget(t, b, "k", "s"), "v1", false)
		h.Advance(t, res)
		wantMiss(t, "at deadline", cget(t, b, "k", "s"))
	})

	t.Run("TTLOutOfRange", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "k", "s", nil, cent("orig"), time.Hour)
		for _, ttl := range []time.Duration{store.MaxTTL + time.Nanosecond, math.MaxInt64} {
			tok := cget(t, a, "k", "s").Token
			stored, err := a.Put(ctx, tok, "k", "s", nil, cent("new"), ttl)
			if stored || !errors.Is(err, store.ErrInvalidEntry) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("Put with ttl %v: stored %v, error %v; want false and ErrInvalidEntry", ttl, stored, err)
			}
		}
		wantHit(t, "after out of range ttl", cget(t, b, "k", "s"), "orig", false)
	})

	t.Run("HitsDoNotExtendLife", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "hl", "s", nil, cent("hl"), 10*time.Second)
		for i := 1; i <= 9; i++ {
			h.Advance(t, time.Second)
			wantHit(t, "hit before deadline", cget(t, b, "hl", "s"), "hl", false)
		}
		h.Advance(t, time.Second-res)
		wantHit(t, "hit just before deadline", cget(t, b, "hl", "s"), "hl", false)
		h.Advance(t, res)
		wantMiss(t, "at deadline", cget(t, b, "hl", "s"))
	})

	t.Run("RePutReplacesDeadline", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "rp", "s", nil, cent("rp"), 10*time.Second)
		first := cget(t, b, "rp", "s")
		if first.Entry == nil {
			t.Fatal("re-put: setup miss, want hit")
		}
		h.Advance(t, 5*time.Second)
		cstore(t, a, "rp", "s", nil, cent("rp"), 10*time.Second)
		l := cget(t, b, "rp", "s")
		if l.Entry == nil {
			t.Fatal("re-put: miss after re-fill, want hit")
		}
		if l.Entry.StoredAt.Sub(first.Entry.StoredAt) != 5*time.Second {
			t.Errorf("re-put: stored at moved by %v, want 5s", l.Entry.StoredAt.Sub(first.Entry.StoredAt))
		}
		h.Advance(t, 10*time.Second-res)
		wantHit(t, "re-put before new deadline", cget(t, b, "rp", "s"), "rp", false)
		h.Advance(t, res)
		wantMiss(t, "re-put at new deadline", cget(t, b, "rp", "s"))

		cstore(t, a, "rp2", "s", nil, cent("rp2"), time.Hour)
		cstore(t, a, "rp2", "s", nil, cent("rp2"), time.Second)
		h.Advance(t, time.Second)
		wantMiss(t, "shorter re-put", cget(t, b, "rp2", "s"))
	})

	t.Run("PurgeKeepsDeadlines", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "k1", "s", nil, cent("k1", "t"), 10*time.Second)
		cstore(t, a, "k2", "s", nil, cent("k2"), 10*time.Second)
		cpurge(t, a, asSoft(keys("k1")))
		cpurge(t, a, keys("unrelated"))
		cpurge(t, a, asSoft(tags("t")))
		h.Advance(t, 10*time.Second-res)
		wantHit(t, "stale k1", cget(t, b, "k1", "s"), "k1", true)
		wantHit(t, "fresh k2", cget(t, b, "k2", "s"), "k2", false)
		h.Advance(t, res)
		wantMiss(t, "k1 at deadline", cget(t, b, "k1", "s"))
		wantMiss(t, "k2 at deadline", cget(t, b, "k2", "s"))
	})

	t.Run("Horizon", func(t *testing.T) {
		a, b := h.NewAt(t, h.Horizon.Add(-store.MaxTTL-24*time.Hour))
		cstore(t, a, "hz", "s", nil, cent("hz"), store.MaxTTL)
		h.Advance(t, store.MaxTTL-res)
		l := cget(t, b, "hz", "s")
		wantHit(t, "before deadline near horizon", l, "hz", false)
		if l.Entry != nil && l.Now.Sub(l.Entry.StoredAt) != store.MaxTTL-res {
			t.Errorf("near horizon: age %v, want %v", l.Now.Sub(l.Entry.StoredAt), store.MaxTTL-res)
		}
		h.Advance(t, res)
		wantMiss(t, "at deadline near horizon", cget(t, b, "hz", "s"))
	})

	t.Run("PurgeMatrix", func(t *testing.T) {
		for _, kind := range []string{"keys", "tags", "all"} {
			for _, mode := range []string{"hard", "soft"} {
				t.Run(kind+"-"+mode, func(t *testing.T) {
					soft := mode == "soft"
					covered := "miss"
					if soft {
						covered = "stale"
					}
					a, b := h.New(t)
					cPurgePopulate(t, a)
					h.Advance(t, time.Second)
					var p store.Purge
					var variants []cVariant
					var wantR store.PurgeResult
					switch kind {
					case "keys":
						p = keys("p1")
						wantR = store.PurgeResult{Accepted: 1, Affected: 2, AffectedKnown: true}
						if soft {
							wantR.Affected = 1
						}
						variants = []cVariant{{"p1", "s1", covered}, {"p1", "s2", covered}, {"p2", "s1", "fresh"}, {"p3", "s1", "fresh"}}
					case "tags":
						p = tags("t1", "t2")
						wantR = store.PurgeResult{Accepted: 2, Affected: 2, AffectedKnown: true}
						variants = []cVariant{{"p1", "s1", covered}, {"p1", "s2", "fresh"}, {"p2", "s1", covered}, {"p3", "s1", "fresh"}}
					default:
						p = all()
						wantR = store.PurgeResult{Accepted: 1}
						variants = []cVariant{{"p1", "s1", covered}, {"p1", "s2", covered}, {"p2", "s1", covered}, {"p3", "s1", covered}}
					}
					p.Soft = soft
					got := cpurge(t, a, p)
					if got.Accepted != wantR.Accepted {
						t.Errorf("purge result %+v, want accepted %d", got, wantR.Accepted)
					}
					if kind == "all" {
						if (got.AffectedKnown && got.Affected != 4) || (!got.AffectedKnown && got.Affected != 0) {
							t.Errorf("purge all result %+v, want affected 4 when known and 0 when unknown", got)
						}
					} else if got != wantR {
						t.Errorf("purge result %+v, want %+v", got, wantR)
					}
					for _, v := range variants {
						cExpect(t, b, v.p, v.s, v.state)
					}
				})
			}
		}
	})

	t.Run("PurgeTagsExactVariants", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "p", "s1", nil, cent("p/s1", "x"), time.Hour)
		cstore(t, a, "p", "s2", nil, cent("p/s2", "y"), time.Hour)
		cstore(t, a, "q", "s1", nil, cent("q/s1", "X"), time.Hour)
		got := cpurge(t, a, tags("x"))
		if got != (store.PurgeResult{Accepted: 1, Affected: 1, AffectedKnown: true}) {
			t.Errorf("purge tags x: result %+v, want accepted 1, affected 1, known", got)
		}
		cExpect(t, b, "p", "s1", "miss")
		cExpect(t, b, "p", "s2", "fresh")
		cExpect(t, b, "q", "s1", "fresh")
	})

	t.Run("PurgeSoftThenHard", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "k", "s", nil, cent("k"), time.Hour)
		cpurge(t, a, asSoft(keys("k")))
		wantHit(t, "after soft purge", cget(t, b, "k", "s"), "k", true)
		cpurge(t, a, keys("k"))
		wantMiss(t, "after hard purge", cget(t, b, "k", "s"))
	})

	t.Run("PurgeHardThenSoft", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "k", "s", nil, cent("k"), time.Hour)
		cpurge(t, a, keys("k"))
		wantMiss(t, "after hard purge", cget(t, b, "k", "s"))
		got := cpurge(t, a, asSoft(keys("k")))
		if got != (store.PurgeResult{Accepted: 1, Affected: 0, AffectedKnown: true}) {
			t.Errorf("soft purge after hard: result %+v, want accepted 1, affected 0, known", got)
		}
		wantMiss(t, "after soft purge", cget(t, b, "k", "s"))
	})

	t.Run("PurgeRepeated", func(t *testing.T) {
		a, b := h.New(t)
		one := store.PurgeResult{Accepted: 1, Affected: 1, AffectedKnown: true}
		none := store.PurgeResult{Accepted: 1, Affected: 0, AffectedKnown: true}
		cstore(t, a, "p", "s", nil, cent("p"), time.Hour)
		if got := cpurge(t, a, keys("p")); got != one {
			t.Errorf("first hard keys purge: result %+v, want %+v", got, one)
		}
		if got := cpurge(t, a, keys("p")); got != none {
			t.Errorf("second hard keys purge: result %+v, want %+v", got, none)
		}
		cstore(t, a, "q", "s", nil, cent("q"), time.Hour)
		if got := cpurge(t, a, asSoft(keys("q"))); got != one {
			t.Errorf("first soft keys purge: result %+v, want %+v", got, one)
		}
		if got := cpurge(t, a, asSoft(keys("q"))); got != one {
			t.Errorf("second soft keys purge: result %+v, want %+v", got, one)
		}
		wantHit(t, "after repeated soft keys purge", cget(t, b, "q", "s"), "q", true)
		cstore(t, a, "r", "s", nil, cent("r", "tr"), time.Hour)
		if got := cpurge(t, a, asSoft(tags("tr"))); got != one {
			t.Errorf("first soft tags purge: result %+v, want %+v", got, one)
		}
		if got := cpurge(t, a, asSoft(tags("tr"))); got != one {
			t.Errorf("second soft tags purge: result %+v, want %+v", got, one)
		}
	})

	t.Run("PurgeAbsentSelectors", func(t *testing.T) {
		a, _ := h.New(t)
		cstore(t, a, "p1", "s", nil, cent("p1"), time.Hour)
		cstore(t, a, "p2", "s", nil, cent("p2"), time.Hour)
		if got := cpurge(t, a, keys("nope")); got != (store.PurgeResult{Accepted: 1, Affected: 0, AffectedKnown: true}) {
			t.Errorf("absent keys purge: result %+v, want accepted 1, affected 0, known", got)
		}
		if got := cpurge(t, a, tags("nope", "nope")); got != (store.PurgeResult{Accepted: 1, Affected: 0, AffectedKnown: true}) {
			t.Errorf("absent duplicate tags purge: result %+v, want accepted 1, affected 0, known", got)
		}
		if got := cpurge(t, a, keys("p1", "p1", "p2")); got != (store.PurgeResult{Accepted: 2, Affected: 2, AffectedKnown: true}) {
			t.Errorf("duplicate keys purge: result %+v, want accepted 2, affected 2, known", got)
		}
	})

	t.Run("PurgeInvalid", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "k", "s", nil, cent("k"), time.Hour)
		tok := cget(t, a, "k", "s").Token
		big := make([]string, store.MaxPurgeSelectors+1)
		for i := range big {
			big[i] = "k"
		}
		invalid := []store.Purge{
			{Kind: 0, Keys: []string{"k"}, Retain: time.Hour},
			{Kind: 4, Keys: []string{"k"}, Retain: time.Hour},
			{Kind: store.PurgeKeys, Keys: []string{"k"}},
			{Kind: store.PurgeKeys, Keys: []string{"k"}, Retain: -time.Nanosecond},
			{Kind: store.PurgeKeys, Keys: []string{"k"}, Retain: store.MaxTTL + time.Nanosecond},
			{Kind: store.PurgeKeys, Retain: time.Hour},
			{Kind: store.PurgeKeys, Keys: []string{"k"}, Tags: []string{"t"}, Retain: time.Hour},
			{Kind: store.PurgeTags, Retain: time.Hour},
			{Kind: store.PurgeTags, Keys: []string{"k"}, Tags: []string{"t"}, Retain: time.Hour},
			{Kind: store.PurgeAll, Keys: []string{"k"}, Retain: time.Hour},
			{Kind: store.PurgeAll, Tags: []string{"t"}, Retain: time.Hour},
			{Kind: store.PurgeKeys, Keys: big, Retain: time.Hour},
			{Kind: store.PurgeTags, Tags: big, Retain: time.Hour},
			{Kind: store.PurgeKeys, Keys: []string{""}, Retain: time.Hour},
			{Kind: store.PurgeKeys, Keys: []string{strings.Repeat("k", store.MaxKeyBytes+1)}, Retain: time.Hour},
			{Kind: store.PurgeTags, Tags: []string{""}, Retain: time.Hour},
			{Kind: store.PurgeTags, Tags: []string{strings.Repeat("t", store.MaxTagBytes+1)}, Retain: time.Hour},
			{Kind: store.PurgeTags, Tags: []string{"a\n"}, Retain: time.Hour},
			{Kind: store.PurgeTags, Tags: []string{"\x7f"}, Retain: time.Hour},
		}
		for i, p := range invalid {
			if _, err := a.Purge(ctx, p); !errors.Is(err, store.ErrInvalidPurge) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("invalid purge %d: error %v, want ErrInvalidPurge and not ErrUnavailable", i, err)
			}
		}
		wantHit(t, "after invalid purges", cget(t, b, "k", "s"), "k", false)
		if !cput(t, b, tok, "k", "s", nil, cent("k2"), time.Hour) {
			t.Error("Put with a token taken before invalid purges was refused")
		}
	})

	t.Run("PutAfterPurgeFresh", func(t *testing.T) {
		for _, kind := range []string{"keys", "tags", "all"} {
			for _, mode := range []string{"hard", "soft"} {
				t.Run(kind+"-"+mode, func(t *testing.T) {
					a, b := h.New(t)
					cstore(t, a, "pa", "s", nil, cent("old", "tg"), time.Hour)
					cpurge(t, a, cKindPurge(kind, mode == "soft", "pa", "tg"))
					h.Advance(t, store.PurgeTolerance)
					if !cfill(t, a, "pa", "s", nil, cent("new", "tg"), time.Hour) {
						t.Fatal("fill after purge was refused")
					}
					wantHit(t, "fresh after purge", cget(t, b, "pa", "s"), "new", false)
				})
			}
		}
	})

	t.Run("FillTokenRace", func(t *testing.T) {
		for _, kind := range []string{"keys", "tags", "all"} {
			for _, mode := range []string{"hard", "soft"} {
				t.Run(kind+"-"+mode, func(t *testing.T) {
					soft := mode == "soft"
					a, b := h.New(t)
					cstore(t, a, "k", "s", nil, cent("old", "t"), time.Hour)
					tok := cget(t, a, "k", "s").Token
					cpurge(t, b, cKindPurge(kind, soft, "k", "t"))
					stored, err := a.Put(ctx, tok, "k", "s", nil, cent("new", "t"), time.Hour)
					if stored || err != nil {
						t.Errorf("Put with token issued before purge: stored %v, error %v; want false and nil", stored, err)
					}
					l := cget(t, b, "k", "s")
					if soft {
						wantHit(t, "soft purge after racing fill", l, "old", true)
					} else {
						wantMiss(t, "hard purge after racing fill", l)
					}

					mtok := cget(t, a, "m", "s").Token
					cpurge(t, b, cKindPurge(kind, soft, "m", "tm"))
					stored, err = a.Put(ctx, mtok, "m", "s", nil, cent("new", "tm"), time.Hour)
					if stored || err != nil {
						t.Errorf("miss-path Put with token issued before purge: stored %v, error %v; want false and nil", stored, err)
					}
					wantMiss(t, "miss path after racing fill", cget(t, b, "m", "s"))
				})
			}
		}
	})

	t.Run("FillTokenUnrelatedPurge", func(t *testing.T) {
		a, b := h.New(t)
		tok := cget(t, a, "k", "s").Token
		cpurge(t, b, keys("other"))
		cpurge(t, b, asSoft(keys("other")))
		cpurge(t, b, tags("other"))
		cpurge(t, b, asSoft(tags("other")))
		if !cput(t, a, tok, "k", "s", nil, cent("v", "t"), time.Hour) {
			t.Error("Put after unrelated purges was refused")
		}
	})

	t.Run("FillTokenAcrossHandles", func(t *testing.T) {
		a, b := h.New(t)
		tok := cget(t, a, "x", "s").Token
		if !cput(t, b, tok, "x", "s", nil, cent("x"), time.Hour) {
			t.Error("Put on handle b with token from handle a was refused")
		}
		wantHit(t, "across handles", cget(t, a, "x", "s"), "x", false)
	})

	t.Run("FillTokenZeroInvalid", func(t *testing.T) {
		a, b := h.New(t)
		stored, err := a.Put(ctx, store.FillToken{}, "z", "s", nil, cent("z"), time.Hour)
		if stored || !errors.Is(err, store.ErrInvalidEntry) || errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Put with zero token: stored %v, error %v; want false and ErrInvalidEntry", stored, err)
		}
		wantMiss(t, "after zero token", cget(t, b, "z", "s"))
	})

	t.Run("MarkerLifetime", func(t *testing.T) {
		for _, mode := range []string{"hard", "soft"} {
			t.Run(mode, func(t *testing.T) {
				soft := mode == "soft"
				const R = time.Hour
				a, b := h.New(t)
				cstore(t, a, "k", "s", nil, cent("old"), R)
				tok := cget(t, a, "k", "s").Token
				p := keys("k")
				p.Retain = R
				p.Soft = soft
				cpurge(t, b, p)
				h.Advance(t, R-res)
				stored, err := a.Put(ctx, tok, "k", "s", nil, cent("new"), time.Hour)
				if stored || err != nil {
					t.Errorf("Put with token issued before purge: stored %v, error %v; want false and nil", stored, err)
				}
				l := cget(t, b, "k", "s")
				if soft {
					wantHit(t, "soft marker before retain end", l, "old", true)
				} else {
					wantMiss(t, "hard marker before retain end", l)
				}
				h.Advance(t, res)
				wantMiss(t, "at entry deadline", cget(t, b, "k", "s"))
				h.Advance(t, res)
				wantMiss(t, "after entry deadline", cget(t, b, "k", "s"))
			})
		}
	})

	t.Run("InvalidEntries", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "k", "s", nil, cent("orig"), time.Hour)
		bigBody := make([]byte, store.MaxBodyBytes+1)
		names17 := make([]string, store.MaxVaryNames+1)
		for i := range names17 {
			names17[i] = "n" + strconv.Itoa(i)
		}
		many := make([]string, store.MaxTags+1)
		for i := range many {
			many[i] = "t"
		}
		type row struct {
			name    string
			zero    bool
			primary string
			sec     string
			names   []string
			e       *store.Entry
			ttl     time.Duration
		}
		rows := []row{
			{name: "zero token", zero: true, primary: "k", sec: "s", e: &store.Entry{Status: 200}, ttl: time.Hour},
			{name: "empty primary", primary: "", sec: "s", e: &store.Entry{Status: 200}, ttl: time.Hour},
			{name: "primary too long", primary: strings.Repeat("p", store.MaxKeyBytes+1), sec: "s", e: &store.Entry{Status: 200}, ttl: time.Hour},
			{name: "secondary too long", primary: "k", sec: strings.Repeat("s", store.MaxSecondaryBytes+1), e: &store.Entry{Status: 200}, ttl: time.Hour},
			{name: "nil entry", primary: "k", sec: "s", ttl: time.Hour},
			{name: "status 99", primary: "k", sec: "s", e: &store.Entry{Status: 99}, ttl: time.Hour},
			{name: "status 600", primary: "k", sec: "s", e: &store.Entry{Status: 600}, ttl: time.Hour},
			{name: "body too long", primary: "k", sec: "s", e: &store.Entry{Status: 200, Body: bigBody}, ttl: time.Hour},
			{name: "negative initial age", primary: "k", sec: "s", e: &store.Entry{Status: 200, InitialAge: -time.Nanosecond}, ttl: time.Hour},
			{name: "initial age above maximum", primary: "k", sec: "s", e: &store.Entry{Status: 200, InitialAge: store.MaxInitialAge + time.Nanosecond}, ttl: time.Hour},
			{name: "too many tags", primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: many}, ttl: time.Hour},
			{name: "empty tag", primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: []string{""}}, ttl: time.Hour},
			{name: "tag too long", primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: []string{strings.Repeat("t", store.MaxTagBytes+1)}}, ttl: time.Hour},
			{name: "tag with control byte", primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: []string{"a\x01"}}, ttl: time.Hour},
			{name: "tag with delete byte", primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: []string{"\x7f"}}, ttl: time.Hour},
			{name: "too many vary names", primary: "k", sec: "s", names: names17, e: &store.Entry{Status: 200}, ttl: time.Hour},
			{name: "empty vary name", primary: "k", sec: "s", names: []string{""}, e: &store.Entry{Status: 200}, ttl: time.Hour},
			{name: "ttl above maximum", primary: "k", sec: "s", e: &store.Entry{Status: 200}, ttl: store.MaxTTL + time.Nanosecond},
			{name: "ttl maximum int64", primary: "k", sec: "s", e: &store.Entry{Status: 200}, ttl: math.MaxInt64},
		}
		for _, r := range rows {
			tok := store.FillToken{}
			if !r.zero {
				tok = cget(t, a, "k", "s").Token
			}
			stored, err := a.Put(ctx, tok, r.primary, r.sec, r.names, r.e, r.ttl)
			if stored || !errors.Is(err, store.ErrInvalidEntry) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("%s: stored %v, error %v; want false and ErrInvalidEntry", r.name, stored, err)
			}
			wantHit(t, r.name, cget(t, b, "k", "s"), "orig", false)
		}
		if _, err := a.Get(ctx, "k", nil); !errors.Is(err, store.ErrInvalidEntry) || errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Get with nil sel: error %v, want ErrInvalidEntry and not ErrUnavailable", err)
		}
	})

	t.Run("GetOverlongPrimary", func(t *testing.T) {
		a, _ := h.New(t)
		for _, primary := range []string{strings.Repeat("p", store.MaxKeyBytes+1), ""} {
			sel := &cSelector{sec: "s", ok: true}
			l, err := a.Get(ctx, primary, sel.sel)
			if err != nil || l.Entry != nil || l.Token.IsZero() || sel.calls != 0 {
				t.Errorf("Get with primary of %d bytes: error %v, entry nil %v, zero token %v, sel calls %d; want nil error, miss with token, and no sel call", len(primary), err, l.Entry == nil, l.Token.IsZero(), sel.calls)
			}
		}
	})

	t.Run("CanceledContext", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "k", "s", nil, cent("k"), time.Hour)
		tokCn := cget(t, a, "cn", "s").Token
		tokCp := cget(t, a, "cp", "s").Token
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, s := range []store.Cache{a, b} {
			if _, err := s.Get(cctx, "k", fixed("s")); err != context.Canceled {
				t.Errorf("canceled Get: error %v, want context.Canceled", err)
			}
			if _, err := s.Put(cctx, tokCn, "cn", "s", nil, cent("cn"), time.Hour); err != context.Canceled {
				t.Errorf("canceled Put: error %v, want context.Canceled", err)
			}
			if _, err := s.Purge(cctx, keys("cp")); err != context.Canceled {
				t.Errorf("canceled Purge: error %v, want context.Canceled", err)
			}
		}
		wantHit(t, "stored before cancel", cget(t, b, "k", "s"), "k", false)
		wantMiss(t, "canceled Put", cget(t, b, "cn", "s"))
		if !cput(t, b, tokCp, "cp", "s", nil, cent("cp"), time.Hour) {
			t.Error("Put with a token taken before the canceled Purge was refused")
		}
	})

	t.Run("CloseOneHandle", func(t *testing.T) {
		a, b := h.New(t)
		tok := cget(t, a, "co", "s").Token
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, err := a.Get(ctx, "co", fixed("s")); !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Get on closed handle: error %v, want ErrUnavailable", err)
		}
		if _, err := a.Put(ctx, tok, "co", "s", nil, cent("co"), time.Hour); !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Put on closed handle: error %v, want ErrUnavailable", err)
		}
		if _, err := a.Purge(ctx, keys("co")); !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Purge on closed handle: error %v, want ErrUnavailable", err)
		}
		btok := cget(t, b, "co", "s").Token
		if !cput(t, b, btok, "co", "s", nil, cent("co"), time.Hour) {
			t.Error("Put on open handle was refused")
		}
		cpurge(t, b, keys("co"))
	})

	t.Run("DeleteOneVariant", func(t *testing.T) {
		a, b := h.New(t)
		names := []string{"x-v"}
		cstore(t, a, "dv", "s1", names, cent("dv1", "tg"), time.Hour)
		cstore(t, a, "dv", "s2", names, cent("dv2", "tg"), time.Hour)
		cstore(t, a, "do", "s1", nil, cent("do"), time.Hour)
		if !cdel(t, a, "dv", "s1") {
			t.Fatal("Delete of a stored variant reported false")
		}
		sel := &cSelector{sec: "s1", ok: true}
		l := cgetSel(t, b, "dv", sel.sel)
		wantMiss(t, "deleted variant", l)
		if sel.calls != 1 || !slices.Equal(l.VaryNames, names) {
			t.Errorf("deleted variant: sel calls %d, vary names %q; want 1 call and %q", sel.calls, l.VaryNames, names)
		}
		wantHit(t, "other variant of same primary", cget(t, b, "dv", "s2"), "dv2", false)
		wantHit(t, "variant of other primary", cget(t, b, "do", "s1"), "do", false)
		wantMiss(t, "deleted variant on a", cget(t, a, "dv", "s1"))
		if cdel(t, b, "dv", "s1") {
			t.Error("second Delete of a deleted variant reported true")
		}
	})

	t.Run("DeleteAbsent", func(t *testing.T) {
		a, b := h.New(t)
		if cdel(t, a, "da", "s") {
			t.Error("Delete of a never stored variant reported true")
		}
		cstore(t, a, "dx", "s1", nil, cent("dx"), time.Hour)
		if cdel(t, b, "dx", "s2") {
			t.Error("Delete of an absent secondary of a live primary reported true")
		}
		wantHit(t, "sibling after absent delete", cget(t, a, "dx", "s1"), "dx", false)
		cstore(t, a, "de", "s", nil, cent("de"), time.Second)
		h.Advance(t, time.Second)
		if cdel(t, b, "de", "s") {
			t.Error("Delete of an expired variant reported true")
		}
		cstore(t, a, "dp", "s", nil, cent("dp"), time.Hour)
		cpurge(t, b, keys("dp"))
		if cdel(t, a, "dp", "s") {
			t.Error("Delete of a hard purged variant reported true")
		}
		cstore(t, a, "dn", "", nil, cent("dn"), time.Hour)
		if !cdel(t, b, "dn", "") {
			t.Error("Delete of a stored variant with empty secondary reported false")
		}
		wantMiss(t, "empty secondary after delete", cget(t, a, "dn", ""))
	})

	t.Run("DeleteLastVariant", func(t *testing.T) {
		a, b := h.New(t)
		names := []string{"x-v"}
		cstore(t, a, "dl", "s", names, cent("dl"), time.Hour)
		if !cdel(t, a, "dl", "s") {
			t.Fatal("Delete of the last stored variant reported false")
		}
		sel := &cSelector{sec: "s", ok: true}
		l := cgetSel(t, b, "dl", sel.sel)
		wantMiss(t, "last variant deleted", l)
		if sel.calls != 0 || len(l.VaryNames) != 0 {
			t.Errorf("record gone after deleting the last variant: sel calls %d, vary names %q; want 0 calls and none", sel.calls, l.VaryNames)
		}
		if !cfill(t, b, "dl", "s2", []string{"x-w"}, cent("dl2"), time.Hour) {
			t.Fatal("fill after deleting the last variant was not stored")
		}
		l = cget(t, a, "dl", "s2")
		wantHit(t, "fill after deleting the last variant", l, "dl2", false)
		if !slices.Equal(l.VaryNames, []string{"x-w"}) {
			t.Errorf("fill after deleting the last variant: vary names %q, want [x-w]", l.VaryNames)
		}
	})

	t.Run("DeletePutAgain", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "dr", "s", nil, cent("old"), time.Hour)
		tok := cget(t, b, "dr", "s").Token
		if !cdel(t, a, "dr", "s") {
			t.Fatal("Delete of a stored variant reported false")
		}
		if !cput(t, b, tok, "dr", "s", nil, cent("new"), time.Hour) {
			t.Fatal("Put with a token taken before Delete was refused")
		}
		wantHit(t, "put after delete", cget(t, a, "dr", "s"), "new", false)
		if !cdel(t, b, "dr", "s") {
			t.Fatal("Delete of the re-stored variant reported false")
		}
		if !cfill(t, a, "dr", "s", nil, cent("again"), time.Hour) {
			t.Fatal("fill after second delete was not stored")
		}
		wantHit(t, "fill after second delete", cget(t, b, "dr", "s"), "again", false)
	})

	t.Run("DeleteLeavesMarkers", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "dm", "s1", nil, cent("dm1"), time.Hour)
		tok := cget(t, a, "dm", "s1").Token
		cpurge(t, b, keys("dm"))
		if cdel(t, a, "dm", "s1") {
			t.Error("Delete after hard key purge reported true")
		}
		if cput(t, a, tok, "dm", "s1", nil, cent("late"), time.Hour) {
			t.Error("Put with a token taken before a hard key purge was accepted after Delete")
		}
		h.Advance(t, store.PurgeTolerance+res)
		if !cfill(t, a, "dm", "s1", nil, cent("dm-new"), time.Hour) {
			t.Fatal("fill after hard key purge and Delete was refused")
		}
		wantHit(t, "fresh after hard key purge and Delete", cget(t, b, "dm", "s1"), "dm-new", false)

		cstore(t, a, "ds", "s1", nil, cent("ds1"), time.Hour)
		cstore(t, a, "ds", "s2", nil, cent("ds2"), time.Hour)
		tok = cget(t, a, "ds", "s1").Token
		cpurge(t, b, asSoft(keys("ds")))
		if !cdel(t, a, "ds", "s1") {
			t.Error("Delete of a variant marked stale by a soft key purge reported false")
		}
		wantMiss(t, "deleted stale variant", cget(t, b, "ds", "s1"))
		wantHit(t, "stale sibling after Delete", cget(t, b, "ds", "s2"), "ds2", true)
		if cput(t, a, tok, "ds", "s1", nil, cent("late"), time.Hour) {
			t.Error("Put with a token taken before a soft key purge was accepted after Delete")
		}
		h.Advance(t, store.PurgeTolerance+res)
		if !cfill(t, a, "ds", "s1", nil, cent("ds-new"), time.Hour) {
			t.Fatal("fill after soft key purge and Delete was refused")
		}
		wantHit(t, "fresh after soft key purge and Delete", cget(t, b, "ds", "s1"), "ds-new", false)

		cstore(t, a, "dt", "s", nil, cent("dt", "tm"), time.Hour)
		tok = cget(t, a, "dt", "s").Token
		cpurge(t, b, tags("tm"))
		if cdel(t, a, "dt", "s") {
			t.Error("Delete after tag purge reported true")
		}
		if cput(t, a, tok, "dt", "s", nil, cent("late", "tm"), time.Hour) {
			t.Error("Put with a token taken before a tag purge was accepted after Delete")
		}
		h.Advance(t, store.PurgeTolerance+res)
		if !cfill(t, a, "dt", "s", nil, cent("dt-new", "tm"), time.Hour) {
			t.Fatal("fill after tag purge and Delete was refused")
		}

		cstore(t, a, "dz", "s", nil, cent("dz"), time.Hour)
		tok = cget(t, a, "dz", "s").Token
		cpurge(t, b, all())
		if cdel(t, a, "dz", "s") {
			t.Error("Delete after purge all reported true")
		}
		if cput(t, a, tok, "dz", "s", nil, cent("late"), time.Hour) {
			t.Error("Put with a token taken before purge all was accepted after Delete")
		}
	})

	t.Run("DeleteKeepsOtherTags", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "dg", "s1", nil, cent("dg1", "tg"), time.Hour)
		cstore(t, a, "dg", "s2", nil, cent("dg2", "tg"), time.Hour)
		cstore(t, a, "dh", "s1", nil, cent("dh1", "tg"), time.Hour)
		if !cdel(t, a, "dg", "s1") {
			t.Fatal("Delete of a stored variant reported false")
		}
		wantHit(t, "sibling after Delete", cget(t, b, "dg", "s2"), "dg2", false)
		wantHit(t, "other primary after Delete", cget(t, b, "dh", "s1"), "dh1", false)
		r := cpurge(t, b, tags("tg"))
		if r.AffectedKnown && r.Affected != 2 {
			t.Errorf("tag purge after Delete: affected %d, want 2", r.Affected)
		}
		wantMiss(t, "deleted variant after tag purge", cget(t, b, "dg", "s1"))
		wantMiss(t, "sibling after tag purge", cget(t, b, "dg", "s2"))
		wantMiss(t, "other primary after tag purge", cget(t, b, "dh", "s1"))
	})

	t.Run("DeleteCallOrder", func(t *testing.T) {
		a, b := h.New(t)
		cstore(t, a, "dc", "s", nil, cent("dc"), time.Hour)
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		for _, s := range []store.Cache{a, b} {
			if deleted, err := s.Delete(cctx, "dc", "s"); deleted || err != context.Canceled {
				t.Errorf("canceled Delete: deleted %v, error %v; want false and context.Canceled", deleted, err)
			}
			if _, err := s.Delete(cctx, "", "s"); err != context.Canceled {
				t.Errorf("canceled Delete with empty primary: error %v, want context.Canceled", err)
			}
		}
		invalid := []struct {
			name      string
			primary   string
			secondary string
		}{
			{name: "empty primary", primary: "", secondary: "s"},
			{name: "primary too long", primary: strings.Repeat("p", store.MaxKeyBytes+1), secondary: "s"},
			{name: "secondary too long", primary: "dc", secondary: strings.Repeat("s", store.MaxSecondaryBytes+1)},
		}
		for _, r := range invalid {
			deleted, err := a.Delete(ctx, r.primary, r.secondary)
			if deleted || !errors.Is(err, store.ErrInvalidEntry) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("%s: deleted %v, error %v; want false and ErrInvalidEntry", r.name, deleted, err)
			}
		}
		if deleted, err := a.Delete(ctx, strings.Repeat("p", store.MaxKeyBytes), strings.Repeat("s", store.MaxSecondaryBytes)); deleted || err != nil {
			t.Errorf("boundary lengths: deleted %v, error %v; want false and nil", deleted, err)
		}
		wantHit(t, "stored before invalid deletes", cget(t, b, "dc", "s"), "dc", false)
		if err := a.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if deleted, err := a.Delete(ctx, "dc", "s"); deleted || !errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Delete on closed handle: deleted %v, error %v; want false and ErrUnavailable", deleted, err)
		}
		if _, err := a.Delete(cctx, "dc", "s"); err != context.Canceled {
			t.Errorf("canceled Delete on closed handle: error %v, want context.Canceled", err)
		}
		if _, err := a.Delete(ctx, "", "s"); !errors.Is(err, store.ErrInvalidEntry) || errors.Is(err, store.ErrUnavailable) {
			t.Errorf("invalid Delete on closed handle: error %v, want ErrInvalidEntry and not ErrUnavailable", err)
		}
		wantHit(t, "stored after close", cget(t, b, "dc", "s"), "dc", false)
		if !cdel(t, b, "dc", "s") {
			t.Error("Delete on open handle reported false")
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		a, b := h.New(t)
		handles := []store.Cache{a, b}
		start := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func(s store.Cache, g int) {
				defer wg.Done()
				<-start
				for i := 0; i < 100; i++ {
					key := "c" + strconv.Itoa((i+g)%8)
					c := byte('a' + i%26)
					l, err := s.Get(ctx, key, fixed("s"))
					if err != nil {
						t.Errorf("Get(%q): %v", key, err)
						return
					}
					hdr := http.Header{"X-Body": {string([]byte{c})}}
					body := bytes.Repeat([]byte{c}, 64)
					if _, err := s.Put(ctx, l.Token, key, "s", nil, &store.Entry{Status: 200, Header: hdr, Body: body}, time.Hour); err != nil {
						t.Errorf("Put(%q): %v", key, err)
						return
					}
					l, err = s.Get(ctx, key, fixed("s"))
					if err != nil {
						t.Errorf("Get(%q) after Put: %v", key, err)
						return
					}
					if e := l.Entry; e != nil {
						xb := e.Header.Get("X-Body")
						if len(e.Body) != 64 || len(xb) != 1 {
							t.Errorf("torn entry for %q: body length %d, header %q", key, len(e.Body), xb)
						} else {
							for _, x := range e.Body {
								if x != xb[0] {
									t.Errorf("torn entry for %q: body byte %q, header %q", key, x, xb)
									break
								}
							}
						}
					}
					if i%10 == 9 {
						p := keys(key)
						p.Soft = (i/10)%2 == 1
						if _, err := s.Purge(ctx, p); err != nil {
							t.Errorf("Purge(%q): %v", key, err)
							return
						}
					}
				}
			}(handles[g%2], g)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 50; i++ {
				key := "pv" + strconv.Itoa(i)
				l, err := a.Get(ctx, key, fixed("s"))
				if err != nil {
					t.Errorf("Get(%q): %v", key, err)
					return
				}
				if _, err := a.Put(ctx, l.Token, key, "s", nil, cent(key), time.Hour); err != nil {
					t.Errorf("Put(%q): %v", key, err)
					return
				}
				if _, err := a.Purge(ctx, keys(key)); err != nil {
					t.Errorf("Purge(%q): %v", key, err)
					return
				}
				lb, err := b.Get(ctx, key, fixed("s"))
				if err != nil {
					t.Errorf("Get(%q) on b: %v", key, err)
					return
				}
				if lb.Entry != nil {
					t.Errorf("hit on %q after hard purge", key)
				}
			}
		}()
		close(start)
		wg.Wait()
	})
}

func cSame(t *testing.T, step string, got, exp *store.Entry) {
	t.Helper()
	if got.Status != exp.Status {
		t.Errorf("%s: status %d, want %d", step, got.Status, exp.Status)
	}
	if len(got.Header) != len(exp.Header) {
		t.Errorf("%s: header has %d keys, want %d", step, len(got.Header), len(exp.Header))
	}
	for k, v := range exp.Header {
		if !slices.Equal(got.Header[k], v) {
			t.Errorf("%s: header %s is %q, want %q", step, k, got.Header[k], v)
		}
	}
	if !bytes.Equal(got.Body, exp.Body) {
		t.Errorf("%s: body %q, want %q", step, got.Body, exp.Body)
	}
	if !slices.Equal(got.Tags, exp.Tags) {
		t.Errorf("%s: tags %q, want %q", step, got.Tags, exp.Tags)
	}
	if got.InitialAge != exp.InitialAge {
		t.Errorf("%s: initial age %v, want %v", step, got.InitialAge, exp.InitialAge)
	}
}

func cent(body string, tags ...string) *store.Entry {
	return &store.Entry{
		Status: 200,
		Header: http.Header{"Content-Type": {"text/plain"}, "X-Body": {body}},
		Body:   []byte(body),
		Tags:   tags,
	}
}

func fixed(sec string) func([]string) (string, bool) {
	return func([]string) (string, bool) { return sec, true }
}

func cgetSel(t *testing.T, s store.Cache, primary string, sel func([]string) (string, bool)) store.Lookup {
	t.Helper()
	l, err := s.Get(context.Background(), primary, sel)
	if err != nil {
		t.Fatalf("Get(%q): %v", primary, err)
	}
	return l
}

func cget(t *testing.T, s store.Cache, primary, sec string) store.Lookup {
	t.Helper()
	return cgetSel(t, s, primary, fixed(sec))
}

func cput(t *testing.T, s store.Cache, tok store.FillToken, primary, sec string, names []string, e *store.Entry, ttl time.Duration) bool {
	t.Helper()
	stored, err := s.Put(context.Background(), tok, primary, sec, names, e, ttl)
	if err != nil {
		t.Fatalf("Put(%q, %q): %v", primary, sec, err)
	}
	return stored
}

func cfill(t *testing.T, s store.Cache, primary, sec string, names []string, e *store.Entry, ttl time.Duration) bool {
	t.Helper()
	l := cget(t, s, primary, sec)
	return cput(t, s, l.Token, primary, sec, names, e, ttl)
}

func cstore(t *testing.T, s store.Cache, primary, sec string, names []string, e *store.Entry, ttl time.Duration) {
	t.Helper()
	if !cfill(t, s, primary, sec, names, e, ttl) {
		t.Fatalf("Put(%q, %q) was not stored", primary, sec)
	}
}

func cpurge(t *testing.T, s store.Cache, p store.Purge) store.PurgeResult {
	t.Helper()
	r, err := s.Purge(context.Background(), p)
	if err != nil {
		t.Fatalf("Purge: %v", err)
	}
	return r
}

func cdel(t *testing.T, s store.Cache, primary, sec string) bool {
	t.Helper()
	deleted, err := s.Delete(context.Background(), primary, sec)
	if err != nil {
		t.Fatalf("Delete(%q, %q): %v", primary, sec, err)
	}
	return deleted
}

func wantHit(t *testing.T, step string, l store.Lookup, body string, stale bool) {
	t.Helper()
	if l.Entry == nil {
		t.Errorf("%s: miss, want hit with body %q", step, body)
		return
	}
	if string(l.Entry.Body) != body || l.Entry.Header.Get("X-Body") != body {
		t.Errorf("%s: body %q, header X-Body %q; want %q", step, l.Entry.Body, l.Entry.Header.Get("X-Body"), body)
	}
	if l.Entry.Stale != stale {
		t.Errorf("%s: stale %v, want %v", step, l.Entry.Stale, stale)
	}
}

func wantMiss(t *testing.T, step string, l store.Lookup) {
	t.Helper()
	if l.Entry != nil {
		t.Errorf("%s: hit with body %q, want miss", step, l.Entry.Body)
	}
	if l.Token.IsZero() {
		t.Errorf("%s: zero token on miss", step)
	}
}

func cExpect(t *testing.T, s store.Cache, primary, sec, state string) {
	t.Helper()
	l := cget(t, s, primary, sec)
	body := primary + "/" + sec
	switch state {
	case "miss":
		wantMiss(t, body, l)
	case "fresh":
		wantHit(t, body, l, body, false)
	case "stale":
		wantHit(t, body, l, body, true)
	}
}

func cPurgePopulate(t *testing.T, s store.Cache) {
	t.Helper()
	cstore(t, s, "p1", "s1", nil, cent("p1/s1", "t1", "t2"), time.Hour)
	cstore(t, s, "p1", "s2", nil, cent("p1/s2", "t3"), time.Hour)
	cstore(t, s, "p2", "s1", nil, cent("p2/s1", "t1"), time.Hour)
	cstore(t, s, "p3", "s1", nil, cent("p3/s1"), time.Hour)
	cstore(t, s, "p4", "s1", nil, cent("p4/s1", "t1"), time.Second)
}

func keys(k ...string) store.Purge {
	return store.Purge{Kind: store.PurgeKeys, Keys: k, Retain: retain}
}

func tags(t ...string) store.Purge {
	return store.Purge{Kind: store.PurgeTags, Tags: t, Retain: retain}
}

func all() store.Purge {
	return store.Purge{Kind: store.PurgeAll, Retain: retain}
}

func asSoft(p store.Purge) store.Purge {
	p.Soft = true
	return p
}

func cKindPurge(kind string, soft bool, key, tag string) store.Purge {
	var p store.Purge
	switch kind {
	case "keys":
		p = keys(key)
	case "tags":
		p = tags(tag)
	default:
		p = all()
	}
	p.Soft = soft
	return p
}
