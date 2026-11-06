package redis

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/store"
)

var dfNamedDifferences = []string{
	"tolerance refusals: Redis refuses a fill whose token was issued within PurgeTolerance after a tag purge of one of its tags; every tag purge is followed by an advance of PurgeTolerance",
	"tokens past Retain: unspecified outcome; every token is used only in the operation that issued it, so none is used past a covering purge's Retain",
	"purge-all counts: Redis reports AffectedKnown false for PurgeAll, so Affected is compared only when Redis reports it",
}

var (
	dfPrimaries   = []string{"p0", "p1", "p2", "p3"}
	dfSecondaries = []string{"s0", "s1", "s2"}
	dfTags        = []string{"ta", "tb", "tc"}
	dfVary        = [][]string{nil, {"Accept"}, {"Accept-Encoding"}, {"Accept", "X-Lang"}}
	dfTTLs        = []time.Duration{time.Microsecond + time.Nanosecond, 999 * time.Microsecond, 1500 * time.Microsecond, time.Second, time.Hour}
)

const dfRetain = 2 * time.Hour

type dfWorld struct {
	t        *testing.T
	rng      clock.Rand
	physical bool
	now      time.Time
	m        *miniredis.Miniredis
	fake     *clock.Fake
	ref      *memstore.Cache
	h        [2]*Store
	fills    []time.Time
	markers  []time.Time
	n        int
}

func newDfWorld(t *testing.T, seed int, physical bool) *dfWorld {
	t.Helper()
	w := &dfWorld{t: t, rng: clock.NewRand(uint64(seed)), physical: physical, now: miniStart}
	w.m = newMiniAt(t, miniStart)
	w.fake = clock.NewFake(miniStart)
	w.ref = memstore.NewCache(memstore.CacheOptions{MaxBytes: 1 << 30, Shards: 4, Clock: w.fake})
	t.Cleanup(func() { _ = w.ref.Close() })
	for i := range w.h {
		w.h[i], _ = ckProxied(t, w.m, ttPrefix)
	}
	return w
}

func (w *dfWorld) pick(n int) int {
	return int(w.rng.Int64N(int64(n)))
}

func (w *dfWorld) advance(d time.Duration) {
	if d <= 0 {
		return
	}
	w.now = w.now.Add(d)
	w.fake.Advance(d)
	w.m.SetTime(w.now)
	if w.physical {
		w.m.FastForward(d)
	}
}

func (w *dfWorld) advanceTo(at time.Time) {
	w.advance(at.Sub(w.now))
}

// nearest moves the clock to one microsecond before, at, or one microsecond after the first deadline in list that
// is after now. Deadlines are truncated to whole microseconds, the clock resolution of Redis TIME.
func (w *dfWorld) nearest(list []time.Time) {
	var best time.Time
	found := false
	for _, at := range list {
		if at.After(w.now) && (!found || at.Before(best)) {
			best, found = at, true
		}
	}
	if !found {
		w.advance(time.Second)
		return
	}
	w.advanceTo(best.Truncate(time.Microsecond).Add(time.Duration(w.pick(3)-1) * time.Microsecond))
}

func (w *dfWorld) move() {
	switch r := w.pick(100); {
	case r < 25:
		w.advance(time.Microsecond)
	case r < 40:
		w.advance(999 * time.Microsecond)
	case r < 55:
		w.advance(1500 * time.Microsecond)
	case r < 65:
		w.advance(time.Second)
	case r < 75:
		w.nearest(w.fills)
	case r < 85:
		w.nearest(w.markers)
	case r < 95:
		w.advance(3 * time.Hour)
	default:
		w.advance(25*time.Hour + time.Duration(w.pick(1_000_000))*time.Microsecond)
	}
}

// tags returns n distinct tags from dfTags, starting at a random one.
func (w *dfWorld) tags(n int) []string {
	if n == 0 {
		return nil
	}
	start := w.pick(len(dfTags))
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, dfTags[(start+i)%len(dfTags)])
	}
	return out
}

func (w *dfWorld) purgeSpec() store.Purge {
	soft := w.pick(2) == 1
	switch w.pick(3) {
	case 0:
		keys := make([]string, 1+w.pick(3))
		for i := range keys {
			keys[i] = dfPrimaries[w.pick(len(dfPrimaries))]
		}
		return store.Purge{Kind: store.PurgeKeys, Keys: keys, Soft: soft, Retain: dfRetain}
	case 1:
		return store.Purge{Kind: store.PurgeTags, Tags: w.tags(1 + w.pick(2)), Soft: soft, Retain: dfRetain}
	default:
		return store.Purge{Kind: store.PurgeAll, Soft: soft, Retain: dfRetain}
	}
}

func (w *dfWorld) fatalf(format string, args ...any) {
	w.t.Helper()
	w.t.Fatalf("at %v: %s", w.now, fmt.Sprintf(format, args...))
}

func (w *dfWorld) getBoth(h int, p, sec string, ok bool) (store.Lookup, store.Lookup) {
	ctx := context.Background()
	sel := func([]string) (string, bool) { return sec, ok }
	rl, err := w.h[h].Get(ctx, p, sel)
	if err != nil {
		w.fatalf("redis get %s: %v", p, err)
	}
	ml, err := w.ref.Get(ctx, p, sel)
	if err != nil {
		w.fatalf("memstore get %s: %v", p, err)
	}
	w.compareLookup(rl, ml)
	return rl, ml
}

func (w *dfWorld) compareLookup(rl, ml store.Lookup) {
	if !rl.Now.Equal(ml.Now) {
		w.fatalf("Now: redis %v, memstore %v", rl.Now, ml.Now)
	}
	if !reflect.DeepEqual(rl.VaryNames, ml.VaryNames) {
		w.fatalf("VaryNames: redis %#v, memstore %#v", rl.VaryNames, ml.VaryNames)
	}
	if (rl.Entry == nil) != (ml.Entry == nil) {
		w.fatalf("hit: redis %t, memstore %t", rl.Entry != nil, ml.Entry != nil)
	}
	if rl.Entry == nil {
		return
	}
	re, me := rl.Entry, ml.Entry
	switch {
	case re.Status != me.Status:
		w.fatalf("status: redis %d, memstore %d", re.Status, me.Status)
	case !bytes.Equal(re.Body, me.Body):
		w.fatalf("body: redis %q, memstore %q", re.Body, me.Body)
	case !reflect.DeepEqual(re.Header, me.Header):
		w.fatalf("header: redis %v, memstore %v", re.Header, me.Header)
	case !slices.Equal(re.Tags, me.Tags):
		w.fatalf("tags: redis %q, memstore %q", re.Tags, me.Tags)
	case re.Stale != me.Stale:
		w.fatalf("stale: redis %t, memstore %t", re.Stale, me.Stale)
	case !re.StoredAt.Equal(me.StoredAt):
		w.fatalf("stored at: redis %v, memstore %v", re.StoredAt, me.StoredAt)
	}
}

type dfFill struct {
	p, sec string
	names  []string
	e      *store.Entry
	ttl    time.Duration
}

func (w *dfWorld) putBoth(h int, rl, ml store.Lookup, f dfFill) {
	ctx := context.Background()
	rs, err := w.h[h].Put(ctx, rl.Token, f.p, f.sec, f.names, f.e, f.ttl)
	if err != nil {
		w.fatalf("redis put %s/%s: %v", f.p, f.sec, err)
	}
	ms, err := w.ref.Put(ctx, ml.Token, f.p, f.sec, f.names, f.e, f.ttl)
	if err != nil {
		w.fatalf("memstore put %s/%s: %v", f.p, f.sec, err)
	}
	if rs != ms {
		w.fatalf("put %s/%s stored: redis %t, memstore %t", f.p, f.sec, rs, ms)
	}
	if rs {
		w.fills = append(w.fills, w.now.Add(f.ttl))
	}
}

func (w *dfWorld) newFill() dfFill {
	p := dfPrimaries[w.pick(len(dfPrimaries))]
	sec := dfSecondaries[w.pick(len(dfSecondaries))]
	names := dfVary[w.pick(len(dfVary))]
	var tags []string
	if n := w.pick(3); n > 0 {
		tags = w.tags(n)
	}
	w.n++
	body := fmt.Sprintf("body-%d", w.n)
	return dfFill{
		p:     p,
		sec:   sec,
		names: names,
		e:     &store.Entry{Status: 200, Header: http.Header{"X-Body": {body}}, Body: []byte(body), Tags: tags},
		ttl:   dfTTLs[w.pick(len(dfTTLs))],
	}
}

func (w *dfWorld) purgeBoth(h int, p store.Purge) {
	ctx := context.Background()
	rr, err := w.h[h].Purge(ctx, p)
	if err != nil {
		w.fatalf("redis purge: %v", err)
	}
	mr, err := w.ref.Purge(ctx, p)
	if err != nil {
		w.fatalf("memstore purge: %v", err)
	}
	if rr.Accepted != mr.Accepted {
		w.fatalf("purge accepted: redis %d, memstore %d", rr.Accepted, mr.Accepted)
	}
	if rr.AffectedKnown && (!mr.AffectedKnown || rr.Affected != mr.Affected) {
		w.fatalf("purge affected: redis %d (known), memstore %d (known %t)", rr.Affected, mr.Affected, mr.AffectedKnown)
	}
	w.markers = append(w.markers, w.now.Add(p.Retain))
}

// afterPurge advances past PurgeTolerance after a tag purge, so that no token issued later is refused by the tolerance.
func (w *dfWorld) afterPurge(p store.Purge) {
	if p.Kind == store.PurgeTags {
		w.advance(store.PurgeTolerance + time.Microsecond)
	}
}

func (w *dfWorld) fill() {
	f := w.newFill()
	h := w.pick(2)
	rl, ml := w.getBoth(h, f.p, f.sec, true)
	w.putBoth(h, rl, ml, f)
}

func (w *dfWorld) get() {
	p := dfPrimaries[w.pick(len(dfPrimaries))]
	sec := dfSecondaries[w.pick(len(dfSecondaries))]
	w.getBoth(w.pick(2), p, sec, w.pick(100) < 85)
}

func (w *dfWorld) purge() {
	p := w.purgeSpec()
	w.purgeBoth(w.pick(2), p)
	w.afterPurge(p)
}

// racing takes a token, purges between the read and the write, and then writes with that token.
func (w *dfWorld) racing() {
	f := w.newFill()
	h := w.pick(2)
	rl, ml := w.getBoth(h, f.p, f.sec, true)
	p := w.purgeSpec()
	w.purgeBoth(w.pick(2), p)
	w.putBoth(h, rl, ml, f)
	w.afterPurge(p)
}

func (w *dfWorld) del() {
	ctx := context.Background()
	p := dfPrimaries[w.pick(len(dfPrimaries))]
	sec := dfSecondaries[w.pick(len(dfSecondaries))]
	h := w.pick(2)
	rd, err := w.h[h].Delete(ctx, p, sec)
	if err != nil {
		w.fatalf("redis delete %s/%s: %v", p, sec, err)
	}
	md, err := w.ref.Delete(ctx, p, sec)
	if err != nil {
		w.fatalf("memstore delete %s/%s: %v", p, sec, err)
	}
	if rd != md {
		w.fatalf("delete %s/%s: redis %t, memstore %t", p, sec, rd, md)
	}
}

func (w *dfWorld) step() {
	switch r := w.pick(100); {
	case r < 35:
		w.fill()
	case r < 60:
		w.get()
	case r < 72:
		w.purge()
	case r < 82:
		w.racing()
	case r < 88:
		w.del()
	default:
		w.move()
	}
}

// dfRun runs ops seeded operations against two Redis handles on one miniredis and against memstore, and fails on
// the first observable difference. physical selects whether miniredis removes keys when their TTL passes.
func dfRun(t *testing.T, seed, ops int, physical bool) {
	t.Helper()
	w := newDfWorld(t, seed, physical)
	for i := 0; i < ops; i++ {
		w.step()
	}
}

func TestCacheDifferential(t *testing.T) {
	t.Parallel()
	t.Log(dfNamedDifferences)
	_, seeds, ops := ttScale()
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Run("physical", func(t *testing.T) { dfRun(t, seed, ops, true) })
			t.Run("logical", func(t *testing.T) { dfRun(t, seed, ops, false) })
		})
	}
}
