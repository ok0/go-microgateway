package redis

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/store"
)

const ttPrefix = "mgtest"

func ttScale() (flood, seeds, ops int) {
	switch {
	case os.Getenv("MICROGATEWAY_LOAD") != "":
		return 10000, 200, 1000
	case os.Getenv("MICROGATEWAY_CACHE_FULL") != "":
		return 10000, 20, 300
	default:
		return 1000, 4, 100
	}
}

func ttAt(d time.Duration) time.Time {
	return miniStart.Add(d)
}

// ttPending checks that exactly one pending fill key exists and that it expires at ceilMs(deadline).
func ttPending(t *testing.T, c goredis.UniversalClient, deadline time.Time) {
	t.Helper()
	var pend []string
	for _, k := range ckKeys(t, c, ttPrefix) {
		if strings.Contains(k, ":p:") {
			pend = append(pend, k)
		}
	}
	if len(pend) != 1 {
		t.Errorf("pending keys = %q, want exactly one", pend)
		return
	}
	if ms := ckExpiryMs(t, c, pend[0]); ms != ceilMs(deadline) {
		t.Errorf("pending key %q expiry %d, want %d", pend[0], ms, ceilMs(deadline))
	}
}

// ttTagSets checks that both the tag set and its replica of tag expire at ceilMs(at).
func ttTagSets(t *testing.T, c goredis.UniversalClient, s *Store, tag string, at time.Time) {
	t.Helper()
	set, repl, _, _ := s.tagKeys(tag)
	for _, k := range []string{set, repl} {
		if ms := ckExpiryMs(t, c, k); ms != ceilMs(at) {
			t.Errorf("tag %q key %q expiry %d, want %d", tag, k, ms, ceilMs(at))
		}
	}
}

// ttCheck compares the keys under ttPrefix with want at clock reading now. A key whose expiry ceilMs(want) is after
// now must exist with that PEXPIRETIME; a key whose expiry is before now must be absent; a key expiring exactly at
// now is not checked. No other key may exist.
func ttCheck(t *testing.T, c goredis.UniversalClient, now time.Time, want map[string]time.Time) {
	t.Helper()
	nowMs := now.UnixMilli()
	got := make(map[string]bool)
	for _, k := range ckKeys(t, c, ttPrefix) {
		got[k] = true
	}
	for k, at := range want {
		e := ceilMs(at)
		switch {
		case e == nowMs:
		case e < nowMs:
			if got[k] {
				t.Errorf("at %v: key %q outlived its lifetime (expiry %d)", now, k, e)
			}
		case !got[k]:
			t.Errorf("at %v: key %q missing, want expiry %d", now, k, e)
		default:
			if ms := ckExpiryMs(t, c, k); ms != e {
				t.Errorf("at %v: key %q expiry %d, want %d", now, k, ms, e)
			}
		}
		delete(got, k)
	}
	for k := range got {
		t.Errorf("at %v: unexpected key %q", now, k)
	}
}

func TestCacheKeyLifetimes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	s, _ := ckProxied(t, m, ttPrefix)
	c := newTestClient(t, m.Addr(), 0)

	want := make(map[string]time.Time)
	epochs := make(map[string]time.Time)
	raise := func(k string, at time.Time) {
		if at.After(want[k]) {
			want[k] = at
		}
	}
	raiseEpoch := func(bb string, at time.Time) {
		if at.After(epochs[bb]) {
			epochs[bb] = at
		}
	}
	raiseAllEpochs := func(at time.Time) {
		for _, bb := range cacheBucketNames {
			raiseEpoch(bb, at)
		}
	}
	raiseSet := func(tag string, at time.Time) {
		set, repl, _, _ := s.tagKeys(tag)
		raise(set, at)
		raise(repl, at)
	}
	raiseMark := func(tag string, at time.Time) {
		_, _, mk, r := s.tagKeys(tag)
		raise(mk, at)
		raise(r, at)
	}
	raiseCounter := func(at time.Time) {
		tq, tr := s.counterKeys()
		raise(tq, at)
		raise(tr, at)
	}
	check := func() {
		w := maps.Clone(want)
		for bb, at := range epochs {
			eh, es := s.bucketEpochKeys(bb)
			w[eh], w[es] = at, at
		}
		ttCheck(t, c, mc.now, w)
	}
	goTo := func(at time.Time) {
		if at.After(mc.now) {
			mc.Advance(at.Sub(mc.now))
		}
	}
	pa := s.primaryKeys("A")
	pb := s.primaryKeys("B")
	pd := s.primaryKeys("D")

	// Entry, pending fill, index, tag sets and replicas, and epochs after a first fill of A.
	h1 := ttAt(time.Hour)
	s.hook = func(p hookPoint) {
		switch p {
		case hookPending:
			ttPending(t, c, h1)
		case hookJoined:
			ttPending(t, c, h1)
			ttTagSets(t, c, s, "t1", h1)
			ttTagSets(t, c, s, "t2", h1)
		}
	}
	if !ckFill(t, s, "A", "x", []string{"Accept"}, ckEntry("x", "t1", "t2"), time.Hour) {
		t.Fatal("fill A/x refused")
	}
	s.hook = nil
	want[pa.index] = h1
	want[pa.slots[0]] = h1
	raiseSet("t1", h1)
	raiseSet("t2", h1)
	raiseEpoch(pa.h[:2], h1)
	check()

	// A second variant with a shorter lifetime does not shorten the index or the tag set.
	goTo(ttAt(time.Millisecond))
	dy := mc.now.Add(10 * time.Minute)
	if !ckFill(t, s, "A", "y", []string{"Accept"}, ckEntry("y", "t2"), 10*time.Minute) {
		t.Fatal("fill A/y refused")
	}
	want[pa.slots[1]] = dy
	raiseEpoch(pa.h[:2], dy)
	check()

	// A longer registration raises the tag set.
	goTo(ttAt(2 * time.Millisecond))
	dz1 := mc.now.Add(time.Hour)
	if !ckFill(t, s, "B", "z", nil, ckEntry("z", "t2"), time.Hour) {
		t.Fatal("fill B/z refused")
	}
	want[pb.index] = dz1
	want[pb.slots[0]] = dz1
	raiseSet("t2", dz1)
	raiseEpoch(pb.h[:2], dz1)
	check()

	// A shorter re-Put of the same variant shrinks the index but not the tag set.
	goTo(ttAt(3 * time.Millisecond))
	dz2 := mc.now.Add(5 * time.Minute)
	if !ckFill(t, s, "B", "z", nil, ckEntry("z", "t2"), 5*time.Minute) {
		t.Fatal("re-put B/z refused")
	}
	want[pb.index] = dz2
	want[pb.slots[0]] = dz2
	raiseSet("t2", dz2)
	raiseEpoch(pb.h[:2], dz2)
	check()

	// A soft key purge sets the purge marker and the index to the purge-until time.
	goTo(ttAt(4 * time.Millisecond))
	if ckGet(t, s, "A", "x").Entry == nil {
		t.Fatal("A/x missed before the soft purge")
	}
	p4 := mc.now.Add(time.Hour)
	r, err := s.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"A"}, Soft: true, Retain: time.Hour})
	if err != nil || r.Affected != 1 {
		t.Fatalf("soft key purge: %+v, %v; want one affected", r, err)
	}
	want[pa.index] = p4
	want[pa.marker] = p4
	check()

	// A hard key purge drops the variant; the index keeps the purge-until time alone.
	goTo(ttAt(5 * time.Millisecond))
	p5 := mc.now.Add(2 * time.Hour)
	r, err = s.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"B"}, Retain: 2 * time.Hour})
	if err != nil || r.Affected != 1 {
		t.Fatalf("hard key purge: %+v, %v; want one affected", r, err)
	}
	want[pb.index] = p5
	want[pb.marker] = p5
	delete(want, pb.slots[0])
	check()

	// A fill abandoned at hookJoined: its tag registration outlives it, its pending key and index record do not.
	goTo(ttAt(6 * time.Millisecond))
	pend6 := mc.now.Add(30 * time.Minute)
	p6 := mc.now.Add(time.Hour)
	lc := ckGet(t, s, "C", "q")
	s.hook = func(p hookPoint) {
		switch p {
		case hookPending:
			ttPending(t, c, pend6)
		case hookJoined:
			ttPending(t, c, pend6)
			ttTagSets(t, c, s, "t3", pend6)
			if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeTags, Tags: []string{"t3"}, Retain: time.Hour}); err != nil {
				t.Errorf("purge inside the fill: %v", err)
			}
		}
	}
	stored, err := s.Put(ctx, lc.Token, "C", "q", nil, ckEntry("q", "t3"), 30*time.Minute)
	s.hook = nil
	if err != nil || stored {
		t.Fatalf("abandoned fill: stored=%t err=%v, want refused", stored, err)
	}
	raiseSet("t3", pend6)
	raiseMark("t3", p6)
	raiseCounter(p6)
	raiseAllEpochs(p6)
	check()

	// A tag purge 1 µs before the deadline of a live entry still counts and covers it.
	goTo(ttAt(7 * time.Millisecond))
	dd := mc.now.Add(10 * time.Minute)
	if !ckFill(t, s, "D", "d", nil, ckEntry("d", "t4"), 10*time.Minute) {
		t.Fatal("fill D/d refused")
	}
	want[pd.index] = dd
	want[pd.slots[0]] = dd
	raiseSet("t4", dd)
	raiseEpoch(pd.h[:2], dd)
	check()

	goTo(dd.Add(-time.Microsecond))
	if ckGet(t, s, "D", "d").Entry == nil {
		t.Fatal("D/d missed 1 µs before its deadline")
	}
	r, err = s.Purge(ctx, store.Purge{Kind: store.PurgeTags, Tags: []string{"t4"}, Retain: time.Hour})
	if err != nil || r.Affected != 1 || !r.AffectedKnown {
		t.Fatalf("tag purge 1 µs before the deadline: %+v, %v; want one affected", r, err)
	}
	if ckGet(t, s, "D", "d").Entry != nil {
		t.Fatal("D/d survived the tag purge")
	}
	p7 := dd.Add(-time.Microsecond).Add(time.Hour)
	delete(want, pd.index)
	delete(want, pd.slots[0])
	raiseMark("t4", p7)
	raiseCounter(p7)
	raiseAllEpochs(p7)
	check()

	// Expiry: the tag set of t4 and the entry of A/y are gone, the index and marker of A and B remain.
	goTo(dd.Add(time.Millisecond))
	if ckGet(t, s, "A", "x").Entry == nil {
		t.Fatal("A/x missed after the purges")
	}
	check()

	goTo(ttAt(2*time.Hour + 4*time.Millisecond))
	check()

	goTo(ttAt(2*time.Hour + 6*time.Millisecond))
	check()
}

func TestCacheFloodExpires(t *testing.T) {
	t.Parallel()
	flood, _, _ := ttScale()
	ctx := context.Background()
	m := newMini(t)
	mc := &miniClock{m: m, now: miniStart}
	s, _ := ckProxied(t, m, ttPrefix)
	c := newTestClient(t, m.Addr(), 0)

	keys := make([]string, flood)
	tags := make([]string, flood)
	for i := range keys {
		keys[i] = fmt.Sprintf("flood:%d", i)
		tags[i] = fmt.Sprintf("flood-tag:%d", i)
	}
	for start := 0; start < flood; start += store.MaxPurgeSelectors {
		end := min(start+store.MaxPurgeSelectors, flood)
		if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: keys[start:end], Retain: time.Hour}); err != nil {
			t.Fatalf("purge keys: %v", err)
		}
		if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeTags, Tags: tags[start:end], Retain: time.Hour}); err != nil {
			t.Fatalf("purge tags: %v", err)
		}
	}

	last := ttAt(time.Hour)
	mc.Advance(last.Add(-time.Millisecond).Sub(mc.now))
	if n, err := c.DBSize(ctx).Result(); err != nil || n != int64(4*flood+514) {
		t.Fatalf("DBSIZE 1 ms before the last lifetime = %d, %v; want %d", n, err, 4*flood+514)
	}
	mc.Advance(2 * time.Millisecond)
	if n, err := c.DBSize(ctx).Result(); err != nil || n != 0 {
		t.Fatalf("DBSIZE after the last lifetime = %d, %v; want 0", n, err)
	}
}

func ttTrace(t *testing.T, physical bool) []string {
	t.Helper()
	ctx := context.Background()
	m := newMini(t)
	s, _ := ckProxied(t, m, ttPrefix)
	now := miniStart
	advance := func(d time.Duration) {
		now = now.Add(d)
		m.SetTime(now)
		if physical {
			m.FastForward(d)
		}
	}
	var out []string
	get := func(p, sec string) {
		l := ckGet(t, s, p, sec)
		line := fmt.Sprintf("get %s/%s now=%d names=%q nil=%t", p, sec, l.Now.UnixMicro(), l.VaryNames, l.VaryNames == nil)
		if e := l.Entry; e != nil {
			line += fmt.Sprintf(" hit body=%q stale=%t stored=%d tags=%q", e.Body, e.Stale, e.StoredAt.UnixMicro(), e.Tags)
		}
		out = append(out, line)
	}
	fill := func(p, sec string, names []string, ttl time.Duration, tags ...string) {
		stored := ckFill(t, s, p, sec, names, ckEntry(p+sec, tags...), ttl)
		out = append(out, fmt.Sprintf("put %s/%s stored=%t", p, sec, stored))
	}
	purge := func(p store.Purge) {
		r, err := s.Purge(ctx, p)
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		out = append(out, fmt.Sprintf("purge accepted=%d affected=%d known=%t", r.Accepted, r.Affected, r.AffectedKnown))
	}
	all := func() {
		get("P1", "a")
		get("P1", "b")
		get("P2", "a")
		get("P2", "b")
		get("P3", "c")
	}

	fill("P1", "a", nil, time.Microsecond+time.Nanosecond, "t1")
	fill("P1", "b", []string{"Accept"}, 999*time.Microsecond, "t1")
	fill("P2", "a", nil, 1500*time.Microsecond, "t2")
	fill("P2", "b", nil, time.Second, "t2")
	all()
	advance(999 * time.Microsecond)
	all()
	advance(time.Microsecond)
	all()
	advance(time.Millisecond)
	all()
	purge(store.Purge{Kind: store.PurgeTags, Tags: []string{"t2"}, Retain: 2 * time.Hour})
	all()
	advance(store.PurgeTolerance + time.Microsecond)
	purge(store.Purge{Kind: store.PurgeKeys, Keys: []string{"P1"}, Soft: true, Retain: time.Hour})
	all()
	advance(time.Hour)
	all()
	purge(store.Purge{Kind: store.PurgeAll, Soft: true, Retain: 3 * time.Hour})
	all()
	fill("P3", "c", nil, time.Hour, "t3")
	all()
	advance(2 * time.Hour)
	all()
	advance(4 * time.Hour)
	all()
	return out
}

func TestCacheExpiryLossless(t *testing.T) {
	t.Parallel()
	physical := ttTrace(t, true)
	logical := ttTrace(t, false)
	if len(physical) == 0 {
		t.Fatal("empty trace")
	}
	if len(physical) != len(logical) {
		t.Fatalf("trace lengths differ: %d with physical expiry, %d without", len(physical), len(logical))
	}
	for i := range physical {
		if physical[i] != logical[i] {
			t.Errorf("step %d: with physical expiry %q, without %q", i, physical[i], logical[i])
		}
	}
	if !slices.ContainsFunc(physical, func(s string) bool { return strings.Contains(s, " hit ") }) {
		t.Error("trace has no hit; the comparison is vacuous")
	}
}
