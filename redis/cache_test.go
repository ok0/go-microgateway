package redis

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/leakcheck"
	"github.com/ok0/go-microgateway/store"
)

func ctHash(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])[:32]
}

func ctCeilMs(us int64) int64 {
	return (us + 999) / 1000
}

func ctPurge(kind string, soft bool, retain time.Duration) store.Purge {
	p := store.Purge{Soft: soft, Retain: retain}
	switch kind {
	case "keys":
		p.Kind, p.Keys = store.PurgeKeys, []string{"p"}
	case "tags":
		p.Kind, p.Tags = store.PurgeTags, []string{"t1"}
	default:
		p.Kind = store.PurgeAll
	}
	return p
}

func ctFresh(l store.Lookup) bool {
	return l.Entry != nil && !l.Entry.Stale
}

func ctMiss(t *testing.T, step string, l store.Lookup) {
	t.Helper()
	if l.Entry != nil {
		t.Errorf("%s: hit with body %q, want miss", step, l.Entry.Body)
	}
	if l.Token.IsZero() {
		t.Errorf("%s: zero token on miss", step)
	}
}

func ctHit(t *testing.T, step string, l store.Lookup, body string) {
	t.Helper()
	if l.Entry == nil {
		t.Fatalf("%s: miss, want hit with body %q", step, body)
	}
	if string(l.Entry.Body) != body {
		t.Errorf("%s: body %q, want %q", step, l.Entry.Body, body)
	}
	if l.Entry.Stale {
		t.Errorf("%s: stale, want fresh", step)
	}
}

func ctMust(t *testing.T, s store.Cache, primary, sec string, names []string, e *store.Entry, ttl time.Duration) {
	t.Helper()
	if !ckFill(t, s, primary, sec, names, e, ttl) {
		t.Fatalf("Put(%q, %q) was not stored", primary, sec)
	}
}

func ctCount(p *respProxy, name string) int {
	n := 0
	for _, c := range p.Commands() {
		if c == name {
			n++
		}
	}
	return n
}

func ctNoCommands(t *testing.T, p *respProxy, step string) {
	t.Helper()
	if c := p.Commands(); len(c) != 0 {
		t.Errorf("%s: %d requests %q, want none", step, len(c), c)
	}
}

func ctRange(t *testing.T, step string, err error) {
	t.Helper()
	if !errors.Is(err, store.ErrUnavailable) {
		t.Errorf("%s: error %v, want ErrUnavailable", step, err)
		return
	}
	if strings.Contains(err.Error(), "range") {
		t.Errorf("%s: error text %q carries server text", step, err)
	}
}

func ctExpiries(t *testing.T, c goredis.UniversalClient, prefix string) map[string]int64 {
	t.Helper()
	out := make(map[string]int64)
	for _, k := range ckKeys(t, c, prefix) {
		out[k] = ckExpiryMs(t, c, k)
	}
	return out
}

func ctSkewStore(t *testing.T, addr, prefix string, at time.Time) *Store {
	t.Helper()
	p := prefix
	s, err := newFromConfig(&config.RedisStore{Mode: "standalone", Addresses: []string{addr}, KeyPrefix: &p}, nil, clock.NewFake(at))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.sc = newScriptSet(luaKeyGuard + luaClockServer)
	return s
}

func ctSameLookup(t *testing.T, step string, x, y store.Lookup) {
	t.Helper()
	if (x.Entry == nil) != (y.Entry == nil) {
		t.Fatalf("%s: hit %v and %v", step, x.Entry != nil, y.Entry != nil)
	}
	if x.Entry != nil {
		if !bytes.Equal(x.Entry.Body, y.Entry.Body) || x.Entry.Stale != y.Entry.Stale || !x.Entry.StoredAt.Equal(y.Entry.StoredAt) {
			t.Errorf("%s: entries differ: %+v and %+v", step, x.Entry, y.Entry)
		}
	}
	if !slices.Equal(x.VaryNames, y.VaryNames) || !x.Now.Equal(y.Now) {
		t.Errorf("%s: names %q at %v and %q at %v", step, x.VaryNames, x.Now, y.VaryNames, y.Now)
	}
}

func TestCacheCallOrder(t *testing.T) {
	ctx := context.Background()
	m := newMini(t)
	s, p := ckProxied(t, m, "mgtest")
	closed := cacheStore(t, p.Addr(), 0, "mgtest")
	fixed := func([]string) (string, bool) { return "s", true }
	ok := &store.Entry{Status: 200}

	t.Run("canceled context", func(t *testing.T) {
		tok := ckGet(t, s, "k", "s").Token
		p.Reset()
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := s.Get(cctx, "k", fixed); err != context.Canceled {
			t.Errorf("Get: error %v, want context.Canceled", err)
		}
		if _, err := s.Put(cctx, tok, "k", "s", nil, ok, time.Hour); err != context.Canceled {
			t.Errorf("Put: error %v, want context.Canceled", err)
		}
		if _, err := s.Purge(cctx, ctPurge("keys", false, time.Hour)); err != context.Canceled {
			t.Errorf("Purge: error %v, want context.Canceled", err)
		}
		ctNoCommands(t, p, "canceled")
	})

	t.Run("nil selector", func(t *testing.T) {
		p.Reset()
		_, err := s.Get(ctx, "k", nil)
		if !errors.Is(err, store.ErrInvalidEntry) || errors.Is(err, store.ErrUnavailable) {
			t.Errorf("Get with nil sel: error %v, want ErrInvalidEntry", err)
		}
		ctNoCommands(t, p, "nil selector")
	})

	t.Run("invalid entries", func(t *testing.T) {
		tok := ckGet(t, s, "k", "s").Token
		other := ckGet(t, s, "other", "s").Token
		names17 := make([]string, store.MaxVaryNames+1)
		for i := range names17 {
			names17[i] = "n" + strconv.Itoa(i)
		}
		many := make([]string, store.MaxTags+1)
		for i := range many {
			many[i] = "t"
		}
		bigBody := make([]byte, store.MaxBodyBytes+1)
		type row struct {
			name    string
			tok     store.FillToken
			primary string
			sec     string
			names   []string
			e       *store.Entry
			ttl     time.Duration
		}
		rows := []row{
			{name: "zero token", tok: store.FillToken{}, primary: "k", sec: "s", e: ok, ttl: time.Hour},
			{name: "foreign token", tok: store.NewFillToken("r1|foreign"), primary: "k", sec: "s", e: ok, ttl: time.Hour},
			{name: "token of another primary", tok: other, primary: "k", sec: "s", e: ok, ttl: time.Hour},
			{name: "empty primary", tok: tok, primary: "", sec: "s", e: ok, ttl: time.Hour},
			{name: "primary too long", tok: tok, primary: strings.Repeat("p", store.MaxKeyBytes+1), sec: "s", e: ok, ttl: time.Hour},
			{name: "secondary too long", tok: tok, primary: "k", sec: strings.Repeat("s", store.MaxSecondaryBytes+1), e: ok, ttl: time.Hour},
			{name: "nil entry", tok: tok, primary: "k", sec: "s", ttl: time.Hour},
			{name: "status 99", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 99}, ttl: time.Hour},
			{name: "status 600", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 600}, ttl: time.Hour},
			{name: "body too long", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 200, Body: bigBody}, ttl: time.Hour},
			{name: "negative initial age", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 200, InitialAge: -time.Nanosecond}, ttl: time.Hour},
			{name: "initial age above maximum", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 200, InitialAge: store.MaxInitialAge + time.Nanosecond}, ttl: time.Hour},
			{name: "too many tags", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: many}, ttl: time.Hour},
			{name: "empty tag", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: []string{""}}, ttl: time.Hour},
			{name: "tag too long", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: []string{strings.Repeat("t", store.MaxTagBytes+1)}}, ttl: time.Hour},
			{name: "tag with control byte", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: []string{"a\x01"}}, ttl: time.Hour},
			{name: "tag with delete byte", tok: tok, primary: "k", sec: "s", e: &store.Entry{Status: 200, Tags: []string{"\x7f"}}, ttl: time.Hour},
			{name: "too many vary names", tok: tok, primary: "k", sec: "s", names: names17, e: ok, ttl: time.Hour},
			{name: "empty vary name", tok: tok, primary: "k", sec: "s", names: []string{""}, e: ok, ttl: time.Hour},
			{name: "ttl above maximum", tok: tok, primary: "k", sec: "s", e: ok, ttl: store.MaxTTL + time.Nanosecond},
			{name: "ttl maximum int64", tok: tok, primary: "k", sec: "s", e: ok, ttl: time.Duration(math.MaxInt64)},
		}
		for _, r := range rows {
			p.Reset()
			stored, err := s.Put(ctx, r.tok, r.primary, r.sec, r.names, r.e, r.ttl)
			if stored || !errors.Is(err, store.ErrInvalidEntry) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("%s: stored %v, error %v; want false and ErrInvalidEntry", r.name, stored, err)
			}
			ctNoCommands(t, p, r.name)
		}
		p.Reset()
		if _, err := s.Put(ctx, store.NewFillToken("r1|foreign"), "k", "s", nil, ok, time.Hour); err == nil || !strings.Contains(err.Error(), "foreign fill token") {
			t.Errorf("foreign token: error %v, want foreign fill token", err)
		}
		ctNoCommands(t, p, "foreign text")
	})

	t.Run("ttl at or below zero", func(t *testing.T) {
		tok := ckGet(t, s, "k", "s").Token
		for _, ttl := range []time.Duration{0, -time.Nanosecond, time.Duration(math.MinInt64)} {
			p.Reset()
			stored, err := s.Put(ctx, tok, "k", "s", nil, ok, ttl)
			if stored || err != nil {
				t.Errorf("ttl %v: stored %v, error %v; want false and nil", ttl, stored, err)
			}
			ctNoCommands(t, p, fmt.Sprintf("ttl %v", ttl))
		}
	})

	t.Run("empty and oversized primary", func(t *testing.T) {
		long := strings.Repeat("p", store.MaxKeyBytes+1)
		for _, primary := range []string{"", long} {
			p.Reset()
			calls := 0
			l, err := s.Get(ctx, primary, func([]string) (string, bool) { calls++; return "s", true })
			if err != nil || l.Entry != nil || l.Token.IsZero() || calls != 0 {
				t.Errorf("Get(%d bytes): error %v, hit %v, token zero %v, sel calls %d; want a miss with token and no sel", len(primary), err, l.Entry != nil, l.Token.IsZero(), calls)
			}
			ctNoCommands(t, p, "get of invalid primary")
			p.Reset()
			_, err = s.Put(ctx, l.Token, primary, "s", nil, ok, time.Hour)
			if !errors.Is(err, store.ErrInvalidEntry) {
				t.Errorf("Put(%d bytes): error %v, want ErrInvalidEntry", len(primary), err)
			}
			ctNoCommands(t, p, "put of invalid primary")
		}
		p.Reset()
		l, err := s.Get(ctx, "", fixed)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Put(ctx, l.Token, "k", "s", nil, ok, time.Hour); err == nil || !strings.Contains(err.Error(), "foreign fill token") {
			t.Errorf("token of empty primary used for k: error %v, want foreign fill token", err)
		}
		ctNoCommands(t, p, "token of empty primary")
	})

	t.Run("invalid purges", func(t *testing.T) {
		many := make([]string, store.MaxPurgeSelectors+1)
		for i := range many {
			many[i] = "k"
		}
		h := time.Hour
		bad := []store.Purge{
			{Kind: store.PurgeKeys, Keys: []string{"p"}, Retain: 0},
			{Kind: store.PurgeKeys, Keys: []string{"p"}, Retain: -time.Nanosecond},
			{Kind: store.PurgeKeys, Keys: []string{"p"}, Retain: store.MaxTTL + time.Nanosecond},
			{Kind: store.PurgeAll, Retain: store.MaxTTL + time.Nanosecond},
			{Kind: store.PurgeKeys, Retain: h},
			{Kind: store.PurgeTags, Retain: h},
			{Kind: store.PurgeTags, Tags: []string{"a"}, Keys: []string{"k"}, Retain: h},
			{Kind: store.PurgeAll, Keys: []string{"k"}, Retain: h},
			{Kind: 0, Retain: h},
			{Kind: 9, Retain: h},
			{Kind: store.PurgeKeys, Keys: []string{""}, Retain: h},
			{Kind: store.PurgeKeys, Keys: []string{strings.Repeat("k", store.MaxKeyBytes+1)}, Retain: h},
			{Kind: store.PurgeKeys, Keys: many, Retain: h},
			{Kind: store.PurgeTags, Tags: []string{""}, Retain: h},
			{Kind: store.PurgeTags, Tags: []string{"a\x01"}, Retain: h},
		}
		for i, pu := range bad {
			p.Reset()
			_, err := s.Purge(ctx, pu)
			if !errors.Is(err, store.ErrInvalidPurge) || errors.Is(err, store.ErrUnavailable) {
				t.Errorf("purge %d: error %v, want ErrInvalidPurge", i, err)
			}
			ctNoCommands(t, p, fmt.Sprintf("purge %d", i))
		}
	})

	t.Run("edge values are valid", func(t *testing.T) {
		tok := ckGet(t, s, "edge", "s").Token
		stored, err := s.Put(ctx, tok, "edge", "s", nil, ok, store.MaxTTL)
		if err != nil || !stored {
			t.Errorf("Put with ttl MaxTTL: stored %v, error %v", stored, err)
		}
		if _, err := s.Purge(ctx, ctPurge("keys", false, store.MaxTTL)); err != nil {
			t.Errorf("Purge with Retain MaxTTL: %v", err)
		}
	})

	t.Run("closed handle", func(t *testing.T) {
		tok := ckGet(t, s, "k", "s").Token
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		p.Reset()
		_, err := closed.Get(ctx, "k", fixed)
		ctRange(t, "closed get", err)
		ctNoCommands(t, p, "closed get")
		p.Reset()
		_, err = closed.Put(ctx, tok, "k", "s", nil, ok, time.Hour)
		ctRange(t, "closed put", err)
		ctNoCommands(t, p, "closed put")
		p.Reset()
		_, err = closed.Purge(ctx, ctPurge("all", false, time.Hour))
		ctRange(t, "closed purge", err)
		ctNoCommands(t, p, "closed purge")
	})
}

func TestCacheTTLTable(t *testing.T) {
	ttls := []time.Duration{time.Second, time.Hour, 24 * time.Hour, store.MaxTTL, time.Microsecond + time.Nanosecond, 1500 * time.Microsecond, 999 * time.Microsecond}
	for _, ttl := range ttls {
		for _, physical := range []bool{true, false} {
			t.Run(fmt.Sprintf("%v/physical=%v", ttl, physical), func(t *testing.T) {
				ctTTLBoundary(t, ttl, physical)
			})
		}
	}
	t.Run("replace", ctTTLReplace)
}

func ctTTLBoundary(t *testing.T, ttl time.Duration, physical bool) {
	t.Helper()
	ctx := context.Background()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	advance := mc.Advance
	if !physical {
		advance = func(d time.Duration) {
			mc.now = mc.now.Add(d)
			m.SetTime(mc.now)
		}
	}
	a, p := ckProxied(t, m, "mgtest")
	b := cacheStore(t, m.Addr(), 0, "mgtest")
	raw := newTestClient(t, m.Addr(), 0)
	ctMust(t, a, "p", "s", nil, ckEntry("body", "t"), ttl)
	tok := ckGet(t, a, "p", "s").Token
	before := ctExpiries(t, raw, "mgtest")
	for _, bad := range []time.Duration{0, -time.Nanosecond, time.Duration(math.MinInt64)} {
		p.Reset()
		stored, err := a.Put(ctx, tok, "p", "s", nil, ckEntry("other", "t"), bad)
		if stored || err != nil {
			t.Errorf("ttl %v: stored %v, error %v; want false and nil", bad, stored, err)
		}
		ctNoCommands(t, p, fmt.Sprintf("ttl %v", bad))
	}
	if got := ctExpiries(t, raw, "mgtest"); !maps.Equal(got, before) {
		t.Errorf("ttl <= 0 changed keys or expiries: %v, want %v", got, before)
	}

	dlu := int64(ttl / time.Microsecond)
	last := dlu - 1
	if ttl%time.Microsecond != 0 {
		last = dlu
	}
	advance(time.Duration(last) * time.Microsecond)
	beforeGet := ctExpiries(t, raw, "mgtest")
	l := ckGet(t, b, "p", "s")
	ctHit(t, "last instant alive", l, "body")
	if age := l.Now.Sub(l.Entry.StoredAt); age != time.Duration(last)*time.Microsecond {
		t.Errorf("age at last instant %v, want %v", age, time.Duration(last)*time.Microsecond)
	}
	if got := ctExpiries(t, raw, "mgtest"); !maps.Equal(got, beforeGet) {
		t.Errorf("Get changed keys or expiries: %v, want %v", got, beforeGet)
	}
	advance(time.Microsecond)
	ctMiss(t, "first instant dead", ckGet(t, a, "p", "s"))
	advance(time.Microsecond)
	ctMiss(t, "second instant dead", ckGet(t, b, "p", "s"))
}

func ctTTLReplace(t *testing.T) {
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	a := cacheStore(t, m.Addr(), 0, "mgtest")
	b := cacheStore(t, m.Addr(), 0, "mgtest")
	ctMust(t, a, "shorter", "s", nil, ckEntry("long"), time.Hour)
	ctMust(t, a, "shorter", "s", nil, ckEntry("short"), time.Second)
	ctMust(t, a, "longer", "s", nil, ckEntry("short"), time.Second)
	ctMust(t, a, "longer", "s", nil, ckEntry("long"), time.Hour)
	mc.Advance(time.Second)
	ctMiss(t, "shorter replaced", ckGet(t, b, "shorter", "s"))
	ctHit(t, "longer replaced", ckGet(t, b, "longer", "s"), "long")
	mc.Advance(time.Hour - time.Second - time.Microsecond)
	ctHit(t, "longer one microsecond before its deadline", ckGet(t, b, "longer", "s"), "long")
	mc.Advance(time.Microsecond)
	ctMiss(t, "longer at its deadline", ckGet(t, a, "longer", "s"))
}

func TestCacheClockStepBack(t *testing.T) {
	for _, kind := range []string{"keys", "tags", "all"} {
		for _, soft := range []bool{false, true} {
			for _, mini := range []bool{true, false} {
				variant := "redisSemantics"
				if mini {
					variant = "mini"
				}
				t.Run(fmt.Sprintf("%s/soft=%v/%s", kind, soft, variant), func(t *testing.T) {
					ctStepBack(t, kind, soft, mini)
				})
			}
		}
	}
}

func ctStepBack(t *testing.T, kind string, soft, mini bool) {
	ctx := context.Background()
	var a, b store.Cache
	var start time.Time
	var now func() time.Time
	var adv, back func(time.Duration)
	if mini {
		start = miniStart
		m := newMiniAt(t, start)
		mc := &miniClock{m: m, now: start}
		a = cacheStore(t, m.Addr(), 0, "mgtest")
		b = cacheStore(t, m.Addr(), 0, "mgtest")
		adv, back = mc.Advance, mc.Back
		now = func() time.Time { return mc.now }
	} else {
		start = injectedStart()
		m := newMini(t)
		fake := clock.NewFake(start)
		prefix := uniquePrefix(t)
		a = withInjectedClock(newTestStore(t, m.Addr(), 0, prefix), fake.Now)
		b = withInjectedClock(newTestStore(t, m.Addr(), 0, prefix), fake.Now)
		adv = fake.Advance
		back = func(d time.Duration) { fake.Set(fake.Now().Add(-d)) }
		now = fake.Now
	}
	ttl := time.Hour
	ctMust(t, a, "p", "s", nil, ckEntry("p", "t1"), ttl)
	ctMust(t, a, "keep", "s", nil, ckEntry("keep", "other"), ttl)
	adv(10 * time.Minute)
	if _, err := a.Purge(ctx, ctPurge(kind, soft, store.MaxTTL)); err != nil {
		t.Fatalf("purge: %v", err)
	}
	keepWant := func() (hit, checked bool) {
		if mini {
			switch {
			case now().Before(start.Add(-time.Second)):
				return true, true
			case !now().Before(start.Add(time.Second)):
				return false, true
			}
			return false, false
		}
		return now().Before(start.Add(time.Hour)), true
	}
	check := func(step string) {
		t.Helper()
		if ctFresh(ckGet(t, b, "p", "s")) {
			t.Errorf("%s: purged entry is fresh at %v", step, now())
		}
		l := ckGet(t, b, "keep", "s")
		if kind == "all" {
			if ctFresh(l) {
				t.Errorf("%s: purge all left keep fresh at %v", step, now())
			}
			return
		}
		hit, checked := keepWant()
		if !checked {
			return
		}
		if got := ctFresh(l); got != hit {
			t.Errorf("%s: keep fresh %v at %v, want %v", step, got, now(), hit)
		}
	}
	back(time.Hour)
	check("after step back")
	adv(time.Hour + 50*time.Minute - time.Microsecond)
	check("one microsecond before the deadline")
	adv(time.Microsecond)
	check("at the deadline")
	for i := 0; i < 3; i++ {
		adv(store.MaxTTL / 3)
		check(fmt.Sprintf("advance %d", i))
	}
}

func TestCacheHorizonAndRange(t *testing.T) {
	ctx := context.Background()
	fixed := func([]string) (string, bool) { return "s", true }

	t.Run("MaxTTL entry near the horizon", func(t *testing.T) {
		start := cacheHorizon.Add(-store.MaxTTL - 24*time.Hour)
		m := newMiniAt(t, start)
		mc := &miniClock{m: m, now: start}
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		b := cacheStore(t, m.Addr(), 0, "mgtest")
		ctMust(t, a, "p", "s", nil, ckEntry("h"), store.MaxTTL)
		mc.Advance(store.MaxTTL - time.Microsecond)
		ctHit(t, "one microsecond before the deadline", ckGet(t, b, "p", "s"), "h")
		mc.Advance(time.Microsecond)
		ctMiss(t, "at the deadline", ckGet(t, a, "p", "s"))
	})

	t.Run("server clock past the range", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		tok := ckGet(t, a, "p", "s").Token
		m.SetTime(time.Date(2253, 1, 1, 0, 0, 0, 0, time.UTC))
		_, err := a.Get(ctx, "p", fixed)
		ctRange(t, "get", err)
		_, err = a.Put(ctx, tok, "p", "s", nil, ckEntry("v"), time.Hour)
		ctRange(t, "put", err)
		_, err = a.Purge(ctx, ctPurge("keys", false, time.Hour))
		ctRange(t, "purge", err)
	})

	t.Run("exact bound single server", func(t *testing.T) {
		ctBoundCase(t, 0)
	})
	t.Run("exact bound cluster allowance", func(t *testing.T) {
		ctBoundCase(t, clusterSkewUs)
	})

	t.Run("bound constants", func(t *testing.T) {
		const wantMax = int64(1)<<53 - 2 - int64(store.MaxTTL/time.Microsecond) - clusterSkewUs
		if maxClockUs != wantMax {
			t.Errorf("maxClockUs = %d, want %d", maxClockUs, wantMax)
		}
		if !strings.Contains(luaCacheLib, "if U > "+strconv.FormatInt(maxClockUs, 10)+" then") {
			t.Error("luaCacheLib lacks the clock range check at maxClockUs")
		}
	})
}

func ctBoundCase(t *testing.T, allow int64) {
	t.Helper()
	ctx := context.Background()
	m := newMini(t)
	fake := clock.NewFake(time.UnixMicro(maxClockUs).UTC())
	s := withInjectedClock(newTestStore(t, m.Addr(), 0, uniquePrefix(t)), fake.Now)
	s.allowUs = allow
	raw := newTestClient(t, m.Addr(), 0)
	pk := s.primaryKeys("p")
	ttlUs := int64(store.MaxTTL / time.Microsecond)
	fixed := func([]string) (string, bool) { return "s", true }

	tok := ckGet(t, s, "p", "s").Token
	stored, err := s.Put(ctx, tok, "p", "s", nil, ckEntry("b", "t"), store.MaxTTL)
	if err != nil || !stored {
		t.Fatalf("Put at maxClockUs with ttl MaxTTL: stored %v, error %v", stored, err)
	}
	entry := ctCeilMs(maxClockUs + ttlUs)
	tagSet, _, tagMarker, _ := s.tagKeys("t")
	for _, c := range []struct {
		name string
		key  string
		want int64
	}{
		{"index", pk.index, entry},
		{"slot", pk.slots[0], entry},
		{"hard epoch", pk.eh, entry},
		{"soft epoch", pk.es, entry},
		{"tag set", tagSet, ctCeilMs(maxClockUs + ttlUs + allow)},
	} {
		if got := ckExpiryMs(t, raw, c.key); got != c.want {
			t.Errorf("after Put, %s expiry %d, want %d", c.name, got, c.want)
		}
	}

	if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"p"}, Retain: store.MaxTTL}); err != nil {
		t.Fatalf("keys purge at maxClockUs: %v", err)
	}
	if got := ckExpiryMs(t, raw, pk.marker); got != entry {
		t.Errorf("key purge marker expiry %d, want %d", got, entry)
	}
	if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeTags, Tags: []string{"t"}, Soft: true, Retain: store.MaxTTL}); err != nil {
		t.Fatalf("tags purge at maxClockUs: %v", err)
	}
	tq, _ := s.counterKeys()
	if got := ckExpiryMs(t, raw, tq); got != ctCeilMs(maxClockUs+ttlUs+allow) {
		t.Errorf("tag counter expiry %d, want %d", got, ctCeilMs(maxClockUs+ttlUs+allow))
	}
	if got := ckExpiryMs(t, raw, tagMarker); got != entry {
		t.Errorf("tag marker expiry %d, want %d", got, entry)
	}
	if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeAll, Retain: store.MaxTTL}); err != nil {
		t.Fatalf("purge all at maxClockUs: %v", err)
	}
	if got := ckExpiryMs(t, raw, pk.eh); got != ctCeilMs(maxClockUs+ttlUs+allow) {
		t.Errorf("hard epoch expiry after purge all %d, want %d", got, ctCeilMs(maxClockUs+ttlUs+allow))
	}

	fake.Advance(time.Microsecond)
	_, err = s.Get(ctx, "p", fixed)
	ctRange(t, "get at maxClockUs+1", err)
	_, err = s.Put(ctx, tok, "p", "s", nil, ckEntry("b"), time.Hour)
	ctRange(t, "put at maxClockUs+1", err)
	_, err = s.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"p"}, Retain: time.Hour})
	ctRange(t, "purge at maxClockUs+1", err)
}

func TestCacheVaryAndRoundTrips(t *testing.T) {
	ctx := context.Background()
	fixed := func(sec string) func([]string) (string, bool) {
		return func([]string) (string, bool) { return sec, true }
	}

	t.Run("names change discards", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		b := cacheStore(t, m.Addr(), 0, "mgtest")
		ctMust(t, a, "vn", "s1", []string{"Accept"}, ckEntry("one"), time.Hour)
		ctMust(t, a, "vn", "s2", []string{"Accept-Language"}, ckEntry("two"), time.Hour)
		ctMiss(t, "old variant", ckGet(t, b, "vn", "s1"))
		l := ckGet(t, b, "vn", "s2")
		ctHit(t, "new variant", l, "two")
		if !slices.Equal(l.VaryNames, []string{"accept-language"}) {
			t.Errorf("VaryNames %q, want [accept-language]", l.VaryNames)
		}
	})

	t.Run("cap drops the oldest, ties by insertion", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		b := cacheStore(t, m.Addr(), 0, "mgtest")
		for i := 0; i <= store.MaxVariants; i++ {
			ctMust(t, a, "cap", fmt.Sprintf("v%02d", i), nil, ckEntry(fmt.Sprintf("v%02d", i)), time.Hour)
		}
		ctMiss(t, "oldest variant", ckGet(t, b, "cap", "v00"))
		for i := 1; i <= store.MaxVariants; i++ {
			sec := fmt.Sprintf("v%02d", i)
			ctHit(t, sec, ckGet(t, b, "cap", sec), sec)
		}
	})

	t.Run("selector calls", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		calls := 0
		sel := func([]string) (string, bool) { calls++; return "s", true }
		if _, err := a.Get(ctx, "sc", sel); err != nil || calls != 0 {
			t.Fatalf("without a record: error %v, selector calls %d, want 0", err, calls)
		}
		ctMust(t, a, "sc", "s", nil, ckEntry("v"), time.Hour)
		calls = 0
		if _, err := a.Get(ctx, "sc", sel); err != nil || calls != 1 {
			t.Errorf("with a record: error %v, selector calls %d, want 1", err, calls)
		}
	})

	t.Run("selector miss keeps names and token", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		ctMust(t, a, "sm", "s", []string{"Accept"}, ckEntry("v"), time.Hour)
		l, err := a.Get(ctx, "sm", func([]string) (string, bool) { return "", false })
		if err != nil {
			t.Fatal(err)
		}
		if l.Entry != nil || !slices.Equal(l.VaryNames, []string{"accept"}) || l.Token.IsZero() {
			t.Errorf("ok=false: hit %v, names %q, token zero %v", l.Entry != nil, l.VaryNames, l.Token.IsZero())
		}
	})

	t.Run("request counts", func(t *testing.T) {
		m := newMini(t)
		a, p := ckProxied(t, m, "mgtest")
		ctMust(t, a, "rq", "s", nil, ckEntry("v"), time.Hour)
		ctMust(t, a, "rv", "s", []string{"Accept"}, ckEntry("v"), time.Hour)
		if _, err := a.Get(ctx, "rq", fixed("s")); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Get(ctx, "rv", fixed("s")); err != nil {
			t.Fatal(err)
		}
		p.Reset()
		ctHit(t, "without Vary", ckGet(t, a, "rq", "s"), "v")
		if n := ctCount(p, "EVALSHA"); n != 1 {
			t.Errorf("Get without Vary sent %d EVALSHA, want 1", n)
		}
		p.Reset()
		ctHit(t, "with Vary", ckGet(t, a, "rv", "s"), "v")
		if n := ctCount(p, "EVALSHA"); n != 2 {
			t.Errorf("Get with Vary sent %d EVALSHA, want 2", n)
		}
	})

	t.Run("record changed between the two calls", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		b := cacheStore(t, m.Addr(), 0, "mgtest")
		ctMust(t, a, "hk", "s", []string{"Accept"}, ckEntry("v"), time.Hour)
		a.hook = func(p hookPoint) {
			if p == hookSelected {
				ctMust(t, b, "hk", "s2", []string{"X-Other"}, ckEntry("w"), time.Hour)
			}
		}
		l := ckGet(t, a, "hk", "s")
		a.hook = nil
		ctMiss(t, "record changed", l)
	})

	t.Run("1 MiB body with every byte value", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		b := cacheStore(t, m.Addr(), 0, "mgtest")
		body := make([]byte, 1<<20)
		for i := range body {
			body[i] = byte(i)
		}
		e := &store.Entry{Status: 200, Header: http.Header{"X-Bin": {"\x00\xff\n"}}, Body: body}
		ctMust(t, a, "big", "s", nil, e, time.Hour)
		l := ckGet(t, b, "big", "s")
		if l.Entry == nil || !bytes.Equal(l.Entry.Body, body) {
			t.Fatalf("1 MiB body did not round-trip: hit %v", l.Entry != nil)
		}
	})

	t.Run("corrupted meta is a miss until replaced", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		b := cacheStore(t, m.Addr(), 0, "mgtest")
		raw := newTestClient(t, m.Addr(), 0)
		ctMust(t, a, "mc", "s", nil, ckEntry("ok"), time.Hour)
		slot := a.primaryKeys("mc").slots[0]
		if err := raw.HSet(ctx, slot, "m", "\xff\xff").Err(); err != nil {
			t.Fatal(err)
		}
		ctMiss(t, "corrupted meta", ckGet(t, b, "mc", "s"))
		ctMust(t, a, "mc", "s", nil, ckEntry("fresh"), time.Hour)
		ctHit(t, "replaced", ckGet(t, b, "mc", "s"), "fresh")
	})

	t.Run("wrong type index is unavailable", func(t *testing.T) {
		m := newMini(t)
		a := cacheStore(t, m.Addr(), 0, "mgtest")
		raw := newTestClient(t, m.Addr(), 0)
		if err := raw.Set(ctx, a.primaryKeys("wt").index, "x", 0).Err(); err != nil {
			t.Fatal(err)
		}
		_, err := a.Get(ctx, "wt", fixed("s"))
		if !errors.Is(err, store.ErrUnavailable) || !strings.Contains(err.Error(), "WRONGTYPE") {
			t.Errorf("Get on wrong type: error %v, want ErrUnavailable with WRONGTYPE", err)
		}
	})
}

func TestCacheKeysAndSlots(t *testing.T) {
	ctx := context.Background()

	t.Run("exact names", func(t *testing.T) {
		for _, prefix := range []string{"mgtest", "a:b", "x"} {
			m := newMini(t)
			s, _ := ckProxied(t, m, prefix)
			raw := newTestClient(t, m.Addr(), 0)
			ctMust(t, s, "k", "s", []string{"Accept"}, ckEntry("v", "t1"), time.Hour)
			ctHit(t, prefix+" get", ckGet(t, s, "k", "s"), "v")
			h := ctHash("k")
			base := prefix + ":c:{" + h[:2] + "}:"
			tb := prefix + ":c:t:{" + ctHash("t1") + "}:"
			want := []string{base + "i:" + h, base + "e:" + h + ":0", base + "eh", base + "es", tb + "s", tb + "sr"}
			sort.Strings(want)
			if got := ckKeys(t, raw, prefix); !slices.Equal(got, want) {
				t.Errorf("prefix %q: keys %q, want %q", prefix, got, want)
			}
		}
	})

	t.Run("every operation through the guard and audit", func(t *testing.T) {
		m := newMini(t)
		tags := make([]string, 50)
		for j := range tags {
			tags[j] = fmt.Sprintf("tg-%02d-\x80\x20#%d", j, j)
		}
		primaries := make([]string, 200)
		for i := range primaries {
			primaries[i] = fmt.Sprintf("pr-%03d-%c\xff", i, 'a'+rune(i%26))
		}
		for _, prefix := range []string{"pa", "pb", "pc"} {
			s, p := ckProxied(t, m, prefix)
			for i, primary := range primaries {
				names := []string(nil)
				if i%2 == 1 {
					names = []string{"Accept"}
				}
				ctMust(t, s, primary, "s", names, ckEntry("b", tags[i%50], tags[(i*7+3)%50]), time.Hour)
				if i%2 == 1 {
					ctHit(t, "vary get", ckGet(t, s, primary, "s"), "b")
				}
			}
			for i := 0; i < 5; i++ {
				ctx2, cancel := context.WithCancel(ctx)
				tok := ckGet(t, s, primaries[i], "s").Token
				s.hook = func(h hookPoint) {
					if h == hookJoined {
						cancel()
					}
				}
				_, _ = s.Put(ctx2, tok, primaries[i], "ab", nil, ckEntry("x", tags[i]), time.Hour)
				s.hook = nil
				cancel()
			}
			for _, soft := range []bool{false, true} {
				for _, pu := range []store.Purge{
					{Kind: store.PurgeKeys, Keys: primaries, Soft: soft, Retain: store.MaxTTL},
					{Kind: store.PurgeTags, Tags: tags, Soft: soft, Retain: store.MaxTTL},
					{Kind: store.PurgeAll, Soft: soft, Retain: store.MaxTTL},
				} {
					if _, err := s.Purge(ctx, pu); err != nil {
						t.Errorf("%s purge kind %d soft %v: %v", prefix, pu.Kind, soft, err)
					}
				}
			}
			ckGet(t, s, primaries[0], "s")
			for _, r := range p.Requests() {
				if strings.ToUpper(r[0]) == "EVALSHA" && r[2] == "0" {
					t.Errorf("%s: EVALSHA without keys: %q", prefix, r)
				}
			}
			if v := p.Violations(); len(v) != 0 {
				t.Errorf("%s: slot violations %q", prefix, v)
			}
		}
	})

	t.Run("negative controls", func(t *testing.T) {
		m := newMini(t)
		p := newRespProxy(t, m.Addr())
		c := newTestClient(t, p.Addr(), 0)
		if err := c.Eval(ctx, "return 1", []string{"{a}x", "{b}y"}).Err(); err != nil {
			t.Fatal(err)
		}
		if v := p.Violations(); len(v) != 1 {
			t.Errorf("cross-slot script: %d violations %q, want 1", len(v), v)
		}
		script := goredis.NewScript(luaKeyGuard + "\nredis.call('SET', 'undeclared', 'x')\nreturn 1")
		_, err := script.Run(ctx, c, []string{"declared"}).Result()
		if err == nil || !strings.Contains(err.Error(), "undeclared key") {
			t.Errorf("undeclared key: error %v, want the guard error", err)
		}
	})

	t.Run("rate limit and cache keys do not overlap", func(t *testing.T) {
		m := newMini(t)
		prefix := "mgtest-rl"
		s, _ := ckProxied(t, m, prefix)
		raw := newTestClient(t, m.Addr(), 0)
		if _, err := s.Take(ctx, "rk", store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}); err != nil {
			t.Fatal(err)
		}
		ctMust(t, s, "k", "s", nil, ckEntry("v"), time.Hour)
		all := ckKeys(t, raw, prefix)
		var rl, cache int
		for _, k := range all {
			switch {
			case strings.HasPrefix(k, prefix+":rl:") && !strings.HasPrefix(k, prefix+":c:"):
				rl++
			case strings.HasPrefix(k, prefix+":c:") && !strings.HasPrefix(k, prefix+":rl:"):
				cache++
			default:
				t.Errorf("key %q belongs to neither namespace", k)
			}
		}
		if rl == 0 || cache == 0 {
			t.Errorf("namespaces hold %d rl and %d cache keys, want both", rl, cache)
		}
	})

	t.Run("two prefixes on one server are isolated", func(t *testing.T) {
		kinds := []struct {
			kind string
			soft bool
		}{{"keys", false}, {"keys", true}, {"tags", false}, {"tags", true}, {"all", false}, {"all", true}}
		for _, k := range kinds {
			m := newMini(t)
			pa, pb := uniquePrefix(t), uniquePrefix(t)
			sa, _ := ckProxied(t, m, pa)
			sb, _ := ckProxied(t, m, pb)
			ctMust(t, sa, "p", "s", nil, ckEntry("A", "t1"), time.Hour)
			ctMust(t, sb, "p", "s", nil, ckEntry("B", "t1"), time.Hour)
			if _, err := sa.Purge(ctx, ctPurge(k.kind, k.soft, store.MaxTTL)); err != nil {
				t.Fatal(err)
			}
			if ctFresh(ckGet(t, sa, "p", "s")) {
				t.Errorf("%s soft=%v: purged prefix is fresh", k.kind, k.soft)
			}
			ctHit(t, k.kind+" other prefix", ckGet(t, sb, "p", "s"), "B")
		}
	})
}

func TestCacheScriptFlush(t *testing.T) {
	ctx := context.Background()
	m := newMini(t)
	s, p := ckProxied(t, m, "mgtest")
	fixed := func([]string) (string, bool) { return "s", true }
	ctMust(t, s, "pf", "s", nil, ckEntry("v", "t1"), time.Hour)
	ops := []struct {
		name string
		run  func() error
	}{
		{"get", func() error { _, err := s.Get(ctx, "pf", fixed); return err }},
		{"put with tags", func() error {
			l, err := s.Get(ctx, "pf", fixed)
			if err != nil {
				return err
			}
			_, err = s.Put(ctx, l.Token, "pf", "s", nil, ckEntry("v", "t1", "t2"), time.Hour)
			return err
		}},
		{"purge keys", func() error { _, err := s.Purge(ctx, ctPurge("keys", false, store.MaxTTL)); return err }},
		{"purge tags", func() error { _, err := s.Purge(ctx, ctPurge("tags", true, store.MaxTTL)); return err }},
		{"purge all", func() error { _, err := s.Purge(ctx, ctPurge("all", false, store.MaxTTL)); return err }},
	}
	for _, op := range ops {
		if err := op.run(); err != nil {
			t.Fatalf("%s warm-up: %v", op.name, err)
		}
		if err := s.c.Do(ctx, "SCRIPT", "FLUSH").Err(); err != nil {
			t.Fatalf("SCRIPT FLUSH: %v", err)
		}
		p.Reset()
		if err := op.run(); err != nil {
			t.Errorf("%s after flush: %v", op.name, err)
		}
		if n := ctCount(p, "SCRIPT"); n == 0 {
			t.Errorf("%s after flush: no script load was sent", op.name)
		}
		if n := ctCount(p, "EVAL"); n != 0 {
			t.Errorf("%s: %d plain EVAL requests, want 0", op.name, n)
		}
		p.Reset()
		if err := op.run(); err != nil {
			t.Errorf("%s with loaded scripts: %v", op.name, err)
		}
		if n := ctCount(p, "SCRIPT"); n != 0 {
			t.Errorf("%s with loaded scripts: %d script loads, want 0", op.name, n)
		}
	}
}

func TestCacheClockSkewIrrelevant(t *testing.T) {
	ctx := context.Background()
	m := newMini(t)
	prefix := uniquePrefix(t)
	ahead := ctSkewStore(t, m.Addr(), prefix, miniStart.Add(time.Hour))
	behind := ctSkewStore(t, m.Addr(), prefix, miniStart.Add(-time.Hour))
	fixed := func([]string) (string, bool) { return "s", true }

	ctMust(t, behind, "sk", "s", []string{"Accept"}, ckEntry("v", "t1"), time.Hour)
	ctSameLookup(t, "hit", ckGet(t, ahead, "sk", "s"), ckGet(t, behind, "sk", "s"))
	if _, err := ahead.Purge(ctx, ctPurge("tags", false, store.MaxTTL)); err != nil {
		t.Fatal(err)
	}
	ctSameLookup(t, "after tag purge", ckGet(t, behind, "sk", "s"), ckGet(t, ahead, "sk", "s"))
	m.SetTime(miniStart.Add(2 * time.Second))
	ctMust(t, ahead, "sk", "s", []string{"Accept"}, ckEntry("w", "t1"), time.Hour)
	x, err := behind.Get(ctx, "sk", fixed)
	if err != nil {
		t.Fatal(err)
	}
	y, err := ahead.Get(ctx, "sk", fixed)
	if err != nil {
		t.Fatal(err)
	}
	ctSameLookup(t, "after re-fill", x, y)
	if x.Entry == nil || string(x.Entry.Body) != "w" {
		t.Errorf("re-filled entry: %+v, want body w", x.Entry)
	}
}

func TestCacheCloseNoLeak(t *testing.T) {
	ctx := context.Background()
	pw := uniqueSecret(t, "pw-")
	m := newMini(t)
	m.RequireAuth(pw)
	prefix := uniquePrefix(t)
	s, err := NewFromConfig(&config.RedisStore{
		Mode:      "standalone",
		Addresses: []string{m.Addr()},
		Password:  config.NewSecret(pw),
		KeyPrefix: &prefix,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.sc = newScriptSet(luaKeyGuard + luaClockServer)
	ctMust(t, s, "lk", "s", []string{"Accept"}, ckEntry("v", "t1"), time.Hour)
	ctHit(t, "before close", ckGet(t, s, "lk", "s"), "v")
	if _, err := s.Purge(ctx, ctPurge("tags", false, store.MaxTTL)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, "lk", func([]string) (string, bool) { return "s", true }); !errors.Is(err, store.ErrUnavailable) {
		t.Errorf("Get after Close: error %v, want ErrUnavailable", err)
	}
	leakcheck.Check(t, "Store", s, pw)
	for _, f := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		if containsSecretForm(fmt.Sprintf(f, s), pw) {
			t.Errorf("%s of the closed store reveals the password", f)
		}
	}
}
