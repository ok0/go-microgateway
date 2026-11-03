package memstore

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

func cpopulate(t *testing.T, c *Cache, clk *clock.Fake) {
	t.Helper()
	mfill(t, c, "p1", "s1", ment("x", "a", "b"), time.Hour)
	mfill(t, c, "p1", "s2", ment("x", "c"), time.Hour)
	mfill(t, c, "p2", "s1", ment("x", "a"), time.Hour)
	mfill(t, c, "p3", "s1", ment("x"), time.Hour)
	mfill(t, c, "p4", "s1", ment("x", "a"), time.Second)
	clk.Advance(time.Second)
}

func TestCachePurgeCounts(t *testing.T) {
	t.Parallel()
	type step struct {
		p    store.Purge
		want store.PurgeResult
	}
	known := func(accepted, affected int) store.PurgeResult {
		return store.PurgeResult{Accepted: accepted, Affected: affected, AffectedKnown: true}
	}
	rows := []struct {
		name  string
		steps []step
	}{
		{"keys-p1-hard", []step{{pkeys(false, "p1"), known(1, 2)}}},
		{"keys-p1-soft", []step{{pkeys(true, "p1"), known(1, 1)}}},
		{"keys-p1-p1-p2-absent-hard", []step{{pkeys(false, "p1", "p1", "p2", "absent"), known(3, 3)}}},
		{"tags-ab-hard", []step{{ptags(false, "a", "b"), known(2, 2)}}},
		{"tags-ab-soft", []step{{ptags(true, "a", "b"), known(2, 2)}}},
		{"tags-a-a-zz", []step{{ptags(false, "a", "a", "zz"), known(2, 2)}}},
		{"all-hard", []step{{pall(false), known(1, 4)}}},
		{"all-soft", []step{{pall(true), known(1, 4)}}},
		{"all-soft-then-keys-p1-soft", []step{{pall(true), known(1, 4)}, {pkeys(true, "p1"), known(1, 1)}}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			t.Parallel()
			clk := clock.NewFake(epoch)
			c := newCache(t, CacheOptions{MaxBytes: 1 << 20, Shards: 4, Clock: quiet(clk)})
			cpopulate(t, c, clk)
			for i, s := range row.steps {
				if got := mpurge(t, c, s.p); got != s.want {
					t.Fatalf("step %d: got %+v, want %+v", i, got, s.want)
				}
			}
		})
	}
}

func TestCacheFrozenOrdering(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"keys", "tags", "all"} {
		for _, mode := range []string{"hard", "soft"} {
			t.Run(kind+"-"+mode, func(t *testing.T) {
				t.Parallel()
				clk := clock.NewFake(epoch)
				c := newCache(t, CacheOptions{MaxBytes: 1 << 20, Shards: 4, Clock: quiet(clk)})
				soft := mode == "soft"
				var covering store.Purge
				switch kind {
				case "keys":
					covering = pkeys(soft, "k")
				case "tags":
					covering = ptags(soft, "t")
				case "all":
					covering = pall(soft)
				}
				e := ment("body", "t")

				tok1 := mget(t, c, "k", "s").Token
				mpurge(t, c, covering)
				if ok, err := c.Put(bg, tok1, "k", "s", nil, e, time.Hour); ok || err != nil {
					t.Fatalf("Put with pre-purge token = (%v, %v), want (false, nil)", ok, err)
				}

				tok2 := mget(t, c, "k", "s").Token
				if ok, err := c.Put(bg, tok2, "k", "s", nil, e, time.Hour); !ok || err != nil {
					t.Fatalf("Put with post-purge token = (%v, %v), want (true, nil)", ok, err)
				}
				l := mget(t, c, "k", "s")
				mwantHit(t, "fresh", l, "body")
				if l.Entry.Stale {
					t.Fatal("entry stored after the purge is stale")
				}

				tok3 := mget(t, c, "k3", "s").Token
				mpurge(t, c, pkeys(soft, "other"))
				if ok, err := c.Put(bg, tok3, "k3", "s", nil, ment("y", "t"), time.Hour); !ok || err != nil {
					t.Fatalf("Put after unrelated purge = (%v, %v), want (true, nil)", ok, err)
				}
			})
		}
	}
}

func TestCacheMarkerLifetime(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"keys", "tags", "all"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			clk := clock.NewFake(epoch)
			c := newCache(t, CacheOptions{MaxBytes: 1 << 20, Shards: 4, Clock: quiet(clk)})
			tok := mget(t, c, "k", "s").Token
			var p store.Purge
			switch kind {
			case "keys":
				p = pkeys(false, "k")
			case "tags":
				p = ptags(false, "t")
			case "all":
				p = pall(false)
			}
			mpurge(t, c, p)
			clk.Advance(time.Hour - time.Nanosecond)
			c.Sweep(clk.Now())
			if m := c.Stats().Markers; m != 1 {
				t.Fatalf("Markers at P+1h-1ns = %d, want 1", m)
			}
			if ok, err := c.Put(bg, tok, "k", "s", nil, ment("x", "t"), time.Hour); ok || err != nil {
				t.Fatalf("Put with pre-purge token at P+1h-1ns = (%v, %v), want (false, nil)", ok, err)
			}
			clk.Advance(time.Nanosecond)
			c.Sweep(clk.Now())
			if m := c.Stats().Markers; m != 0 {
				t.Fatalf("Markers at P+1h = %d, want 0", m)
			}
			clk.Advance(time.Nanosecond)
			c.Sweep(clk.Now())
			if m := c.Stats().Markers; m != 0 {
				t.Fatalf("Markers at P+1h+1ns = %d, want 0", m)
			}
		})
	}
}

func TestCacheMarkerCap(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	c := newCache(t, CacheOptions{MaxBytes: 1 << 20, Shards: 4, MaxMarkers: 16, Clock: quiet(clk)})
	mfill(t, c, "keep", "s", ment("keep"), time.Hour)
	mfill(t, c, "k0", "s", ment("x"), time.Hour)
	tokK := mget(t, c, "k0", "s").Token
	tokT := mget(t, c, "tp", "s").Token
	for i := range 160 {
		mpurge(t, c, pkeys(false, "k"+strconv.Itoa(i)))
		if m := c.Stats().Markers; m > 16 {
			t.Fatalf("Markers = %d after key purge %d, want <= 16", m, i)
		}
	}
	for i := range 160 {
		mpurge(t, c, ptags(false, "t"+strconv.Itoa(i)))
		if m := c.Stats().Markers; m > 16 {
			t.Fatalf("Markers = %d after tag purge %d, want <= 16", m, i)
		}
	}
	mwantHit(t, "keep", mget(t, c, "keep", "s"), "keep")
	if ok, err := c.Put(bg, tokK, "k0", "s", nil, ment("x"), time.Hour); ok || err != nil {
		t.Fatalf("Put(tokK, k0) = (%v, %v), want (false, nil)", ok, err)
	}
	if ok, err := c.Put(bg, tokT, "tp", "s", nil, ment("x", "t0"), time.Hour); ok || err != nil {
		t.Fatalf("Put(tokT, tp) = (%v, %v), want (false, nil)", ok, err)
	}
	fresh := mget(t, c, "k0", "s").Token
	if ok, err := c.Put(bg, fresh, "k0", "s", nil, ment("x"), time.Hour); !ok || err != nil {
		t.Fatalf("fresh Put(k0) = (%v, %v), want (true, nil)", ok, err)
	}
	freshT := mget(t, c, "tp", "s").Token
	if ok, err := c.Put(bg, freshT, "tp", "s", nil, ment("x", "t0"), time.Hour); !ok || err != nil {
		t.Fatalf("fresh Put(tp) = (%v, %v), want (true, nil)", ok, err)
	}
}

type sweepOp struct {
	kind    int
	primary string
	sec     string
	ttl     time.Duration
	tags    []string
	purge   store.Purge
	advance time.Duration
}

func genSweepOps(rnd *rand.Rand, n int) []sweepOp {
	primaries := []string{"p0", "p1", "p2", "p3", "p4", "p5"}
	secs := []string{"s0", "s1", "s2"}
	tagNames := []string{"t0", "t1", "t2", "t3"}
	ops := make([]sweepOp, 0, n)
	for range n {
		switch r := rnd.IntN(100); {
		case r < 25:
			ops = append(ops, sweepOp{kind: 0, primary: primaries[rnd.IntN(len(primaries))], sec: secs[rnd.IntN(len(secs))]})
		case r < 60:
			var tags []string
			for _, tg := range tagNames {
				if rnd.IntN(2) == 0 {
					tags = append(tags, tg)
				}
			}
			ops = append(ops, sweepOp{
				kind:    1,
				primary: primaries[rnd.IntN(len(primaries))],
				sec:     secs[rnd.IntN(len(secs))],
				ttl:     time.Duration(1+rnd.IntN(60)) * time.Second,
				tags:    tags,
			})
		case r < 75:
			var p store.Purge
			switch rnd.IntN(3) {
			case 0:
				p = pkeys(rnd.IntN(2) == 0, primaries[rnd.IntN(len(primaries))], primaries[rnd.IntN(len(primaries))])
			case 1:
				p = ptags(rnd.IntN(2) == 0, tagNames[rnd.IntN(len(tagNames))], tagNames[rnd.IntN(len(tagNames))])
			default:
				p = pall(rnd.IntN(2) == 0)
			}
			ops = append(ops, sweepOp{kind: 2, purge: p})
		default:
			ops = append(ops, sweepOp{kind: 3, advance: time.Duration(1+rnd.IntN(10)) * time.Second})
		}
	}
	return ops
}

func runSweepOps(t *testing.T, c *Cache, clk *clock.Fake, ops []sweepOp, sweepEach bool, janitor <-chan int) []string {
	t.Helper()
	var out []string
	ticks := 0
	for i, op := range ops {
		switch op.kind {
		case 0:
			l := mget(t, c, op.primary, op.sec)
			out = append(out, fmt.Sprintf("get %d %s/%s %s names=%v now=%v", i, op.primary, op.sec, describe(l), l.VaryNames, l.Now))
		case 1:
			l := mget(t, c, op.primary, op.sec)
			e := &store.Entry{Status: 200, Body: []byte(op.primary + op.sec), Tags: op.tags}
			ok, err := c.Put(bg, l.Token, op.primary, op.sec, []string{"x-v"}, e, op.ttl)
			out = append(out, fmt.Sprintf("put %d %v %v", i, ok, err))
		case 2:
			r, err := c.Purge(bg, op.purge)
			out = append(out, fmt.Sprintf("purge %d %+v %v", i, r, err))
		case 3:
			clk.Advance(op.advance)
			if janitor != nil {
				crossed := int(clk.Now().Sub(epoch) / (10 * time.Second))
				for ticks < crossed {
					ticks++
					testwait.Recv(t, "janitor sweep", janitor, nil)
				}
			}
			out = append(out, fmt.Sprintf("advance %d %v", i, clk.Now()))
		}
		if sweepEach {
			c.Sweep(clk.Now())
		}
	}
	return out
}

func describe(l store.Lookup) string {
	if l.Entry == nil {
		return "miss"
	}
	return fmt.Sprintf("hit body=%q stale=%v stored=%v", l.Entry.Body, l.Entry.Stale, l.Entry.StoredAt)
}

func TestCacheSweepLossless(t *testing.T) {
	t.Parallel()
	for _, seed := range []uint64{1, 2, 3} {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			ops := genSweepOps(rand.New(rand.NewPCG(seed, seed)), 300)

			clkA := clock.NewFake(epoch)
			a := newCache(t, CacheOptions{MaxBytes: 1 << 30, Shards: 4, Clock: quiet(clkA)})
			traceA := runSweepOps(t, a, clkA, ops, true, nil)

			clkB := clock.NewFake(epoch)
			b := newCache(t, CacheOptions{MaxBytes: 1 << 30, Shards: 4, Clock: quiet(clkB)})
			traceB := runSweepOps(t, b, clkB, ops, false, nil)

			clkC := clock.NewFake(epoch)
			ch := make(chan int)
			cc := newCache(t, CacheOptions{
				MaxBytes:        1 << 30,
				Shards:          4,
				Clock:           clkC,
				JanitorInterval: 10 * time.Second,
				OnSweep:         func(n int) { ch <- n },
			})
			testwait.Until(t, "janitor ticker", func() bool { return clkC.HousekeepingWaiters() >= 1 }, nil)
			traceC := runSweepOps(t, cc, clkC, ops, false, ch)

			compare := func(name string, got []string) {
				t.Helper()
				if len(got) != len(traceB) {
					t.Fatalf("%s: %d observations, want %d", name, len(got), len(traceB))
				}
				for i := range got {
					if got[i] != traceB[i] {
						t.Fatalf("%s: observation %d = %q, want %q", name, i, got[i], traceB[i])
					}
				}
			}
			compare("swept after every op", traceA)
			compare("janitor swept", traceC)
		})
	}
}

func TestCacheClockStepBack(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	c := newCache(t, CacheOptions{MaxBytes: 1 << 20, Clock: quiet(clk)})
	t0 := clk.Now()
	mfill(t, c, "k", "s", ment("k", "t"), 2*time.Hour)
	mfill(t, c, "s", "s", ment("s", "t"), 2*time.Hour)
	mfill(t, c, "l", "s", ment("l", "t"), 2*time.Hour)
	tok := mget(t, c, "k", "s").Token
	mpurge(t, c, pkeys(false, "k"))
	mpurge(t, c, pkeys(true, "s"))

	clk.Set(t0.Add(-time.Hour))
	mwantMiss(t, "k after hard purge", mget(t, c, "k", "s"))
	sl := mget(t, c, "s", "s")
	mwantHit(t, "s after soft purge", sl, "s")
	if !sl.Entry.Stale {
		t.Fatal("soft-purged entry is not stale")
	}
	ll := mget(t, c, "l", "s")
	mwantHit(t, "l", ll, "l")
	if !ll.Now.Equal(t0) {
		t.Fatalf("l Now = %v, want %v", ll.Now, t0)
	}
	if ok, err := c.Put(bg, tok, "k", "s", nil, ment("k"), time.Hour); ok || err != nil {
		t.Fatalf("Put(tok, k) = (%v, %v), want (false, nil)", ok, err)
	}

	clk.Set(t0.Add(2*time.Hour - time.Nanosecond))
	mwantHit(t, "l before deadline", mget(t, c, "l", "s"), "l")
	clk.Advance(time.Nanosecond)
	mwantMiss(t, "l at deadline", mget(t, c, "l", "s"))
}

func TestCacheConcurrentReaders(t *testing.T) {
	t.Parallel()
	c := newCache(t, CacheOptions{MaxBytes: 1 << 20, Shards: 1, Clock: quiet(clock.NewFake(epoch))})
	const iterations = 200
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := range iterations {
				b := byte('a' + (i+w)%2)
				e := &store.Entry{
					Status: 200,
					Header: http.Header{"X-Body": {string([]byte{b})}},
					Body:   bytes.Repeat([]byte{b}, 4096),
				}
				l, err := c.Get(bg, "v", func([]string) (string, bool) { return "s", true })
				if err != nil {
					t.Errorf("writer Get: %v", err)
					return
				}
				if _, err := c.Put(bg, l.Token, "v", "s", nil, e, time.Hour); err != nil {
					t.Errorf("writer Put: %v", err)
					return
				}
			}
		}()
	}
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				l, err := c.Get(bg, "v", func([]string) (string, bool) { return "s", true })
				if err != nil {
					t.Errorf("reader Get: %v", err)
					return
				}
				if l.Entry == nil {
					continue
				}
				want := l.Entry.Header.Get("X-Body")
				if len(want) != 1 || len(l.Entry.Body) != 4096 {
					t.Errorf("reader saw header %q and %d body bytes", want, len(l.Entry.Body))
					return
				}
				for _, x := range l.Entry.Body {
					if x != want[0] {
						t.Errorf("torn entry: body byte %q under X-Body %q", x, want)
						return
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()
}
