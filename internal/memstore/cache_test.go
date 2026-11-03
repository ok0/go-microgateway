package memstore

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

func newCache(t *testing.T, o CacheOptions) *Cache {
	t.Helper()
	c := NewCache(o)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ment(body string, tags ...string) *store.Entry {
	return &store.Entry{Status: 200, Body: []byte(body), Tags: tags}
}

func mget(t *testing.T, c *Cache, primary, sec string) store.Lookup {
	t.Helper()
	l, err := c.Get(bg, primary, func([]string) (string, bool) { return sec, true })
	if err != nil {
		t.Fatalf("Get(%q): %v", primary, err)
	}
	return l
}

func mfill(t *testing.T, c *Cache, primary, sec string, e *store.Entry, ttl time.Duration) bool {
	t.Helper()
	return mfillVary(t, c, primary, sec, nil, e, ttl)
}

func mfillVary(t *testing.T, c *Cache, primary, sec string, names []string, e *store.Entry, ttl time.Duration) bool {
	t.Helper()
	l := mget(t, c, primary, sec)
	ok, err := c.Put(bg, l.Token, primary, sec, names, e, ttl)
	if err != nil {
		t.Fatalf("Put(%q/%q): %v", primary, sec, err)
	}
	return ok
}

func mpurge(t *testing.T, c *Cache, p store.Purge) store.PurgeResult {
	t.Helper()
	r, err := c.Purge(bg, p)
	if err != nil {
		t.Fatalf("Purge(kind %d): %v", p.Kind, err)
	}
	return r
}

func pkeys(soft bool, keys ...string) store.Purge {
	return store.Purge{Kind: store.PurgeKeys, Keys: keys, Soft: soft, Retain: time.Hour}
}

func ptags(soft bool, tags ...string) store.Purge {
	return store.Purge{Kind: store.PurgeTags, Tags: tags, Soft: soft, Retain: time.Hour}
}

func pall(soft bool) store.Purge {
	return store.Purge{Kind: store.PurgeAll, Soft: soft, Retain: time.Hour}
}

func mwantHit(t *testing.T, step string, l store.Lookup, body string) {
	t.Helper()
	if l.Entry == nil {
		t.Fatalf("%s: miss, want hit", step)
	}
	if string(l.Entry.Body) != body {
		t.Fatalf("%s: body %q, want %q", step, l.Entry.Body, body)
	}
}

func mwantMiss(t *testing.T, step string, l store.Lookup) {
	t.Helper()
	if l.Entry != nil {
		t.Fatalf("%s: hit, want miss", step)
	}
	if l.Token.IsZero() {
		t.Fatalf("%s: zero token on miss", step)
	}
}

func TestCacheOptions(t *testing.T) {
	t.Parallel()
	mustPanic := func(name string, o CacheOptions) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: no panic", name)
			}
		}()
		NewCache(o)
	}
	fake := func() clock.Clock { return quiet(clock.NewFake(epoch)) }
	mustPanic("MaxBytes 0", CacheOptions{MaxBytes: 0, Clock: fake()})
	mustPanic("MaxMarkers -1", CacheOptions{MaxBytes: 1 << 10, MaxMarkers: -1, Clock: fake()})
	mustPanic("JanitorInterval -1ns", CacheOptions{MaxBytes: 1 << 10, JanitorInterval: -time.Nanosecond, Clock: fake()})
	mustPanic("JanitorInterval time.Minute+1ns", CacheOptions{MaxBytes: 1 << 10, JanitorInterval: time.Minute + time.Nanosecond, Clock: fake()})

	def := newCache(t, CacheOptions{MaxBytes: 1 << 20, Clock: fake()})
	if n := len(def.shards); n != 16 {
		t.Fatalf("default shards = %d, want 16", n)
	}
	small := newCache(t, CacheOptions{MaxBytes: 3, Shards: 8, Clock: fake()})
	if n := len(small.shards); n != 3 {
		t.Fatalf("shards capped at MaxBytes = %d, want 3", n)
	}
	seven := newCache(t, CacheOptions{MaxBytes: 1000, Shards: 7, Clock: fake()})
	var sum int64
	for i := range seven.shards {
		sum += seven.shards[i].budget
	}
	if sum != 1000 {
		t.Fatalf("budgets sum to %d, want 1000", sum)
	}
}

func TestCacheByteBound(t *testing.T) {
	t.Parallel()
	for _, shards := range []int{1, 4, 16} {
		t.Run(strconv.Itoa(shards), func(t *testing.T) {
			t.Parallel()
			const maxBytes = 64 << 10
			var evN int
			var evB int64
			c := newCache(t, CacheOptions{
				MaxBytes: maxBytes,
				Shards:   shards,
				Clock:    quiet(clock.NewFake(epoch)),
				OnEvict:  func(n int, b int64) { evN += n; evB += b },
			})
			rnd := rand.New(rand.NewPCG(1, uint64(shards)))
			var attempted, storedW int64
			var storedCount int
			var storedKeys []string
			var storedEntries []*store.Entry
			for i := 0; attempted < 10*maxBytes; i++ {
				key := "key" + strconv.Itoa(i)
				body := make([]byte, rnd.IntN(4097))
				for j := range body {
					body[j] = byte(rnd.Uint32())
				}
				e := &store.Entry{Status: 200, Header: http.Header{"X-H": {"v"}}, Body: body}
				attempted += weight(key, "s", nil, e)
				if mfill(t, c, key, "s", e, time.Hour) {
					storedCount++
					storedW += weight(key, "s", nil, e)
					storedKeys = append(storedKeys, key)
					storedEntries = append(storedEntries, e)
				}
				if b := c.Bytes(); b > maxBytes {
					t.Fatalf("after put %d: Bytes() = %d > MaxBytes %d", i, b, maxBytes)
				}
			}
			st := c.Stats()
			if st.Evictions != int64(storedCount-c.Len()) {
				t.Fatalf("Evictions = %d, want stored %d - Len %d", st.Evictions, storedCount, c.Len())
			}
			if st.EvictedBytes != storedW-c.Bytes() {
				t.Fatalf("EvictedBytes = %d, want %d", st.EvictedBytes, storedW-c.Bytes())
			}
			if int64(evN) != st.Evictions || evB != st.EvictedBytes {
				t.Fatalf("OnEvict sums (%d, %d) disagree with Stats (%d, %d)", evN, evB, st.Evictions, st.EvictedBytes)
			}
			var live int64
			for i, k := range storedKeys {
				l := mget(t, c, k, "s")
				if l.Entry == nil {
					continue
				}
				live += int64(len(l.Entry.Body)) + int64(len(k)) + int64(len("s"))
				for name, vs := range l.Entry.Header {
					live += int64(len(name))
					for _, v := range vs {
						live += int64(len(v))
					}
				}
				if len(l.Entry.Body) != len(storedEntries[i].Body) {
					t.Fatalf("hit %q: body length changed", k)
				}
			}
			if live > c.Bytes() {
				t.Fatalf("remaining payload %d exceeds Bytes() %d", live, c.Bytes())
			}
		})
	}
}

func TestCacheByteBoundParallel(t *testing.T) {
	t.Parallel()
	const maxBytes = 64 << 10
	var evN, evB atomic.Int64
	c := newCache(t, CacheOptions{
		MaxBytes: maxBytes,
		Shards:   4,
		Clock:    quiet(clock.NewFake(epoch)),
		OnEvict: func(n int, b int64) {
			evN.Add(int64(n))
			evB.Add(b)
		},
	})
	const goroutines, perG = 4, 300
	var stored atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rnd := rand.New(rand.NewPCG(uint64(g), 7))
			for i := range perG {
				key := "g" + strconv.Itoa(g) + "-" + strconv.Itoa(i)
				body := make([]byte, rnd.IntN(2049))
				l, err := c.Get(bg, key, func([]string) (string, bool) { return "s", true })
				if err != nil {
					t.Errorf("Get(%q): %v", key, err)
					return
				}
				ok, err := c.Put(bg, l.Token, key, "s", nil, &store.Entry{Status: 200, Body: body}, time.Hour)
				if err != nil {
					t.Errorf("Put(%q): %v", key, err)
					return
				}
				if ok {
					stored.Add(1)
				}
				if b := c.Bytes(); b > maxBytes {
					t.Errorf("Bytes() = %d > MaxBytes %d", b, maxBytes)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	st := c.Stats()
	if st.Evictions != evN.Load() || st.EvictedBytes != evB.Load() {
		t.Fatalf("Stats evictions (%d, %d) disagree with OnEvict (%d, %d)", st.Evictions, st.EvictedBytes, evN.Load(), evB.Load())
	}
	if st.Entries != c.Len() || st.Bytes != c.Bytes() {
		t.Fatalf("Stats (%d entries, %d bytes) disagree with Len %d and Bytes %d", st.Entries, st.Bytes, c.Len(), c.Bytes())
	}
	if got := stored.Load() - st.Evictions - st.Expired; got != int64(st.Entries) {
		t.Fatalf("stored - evictions - expired = %d, want Entries %d", got, st.Entries)
	}
}

func TestCacheReclaimsExpiredFirst(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	body := make([]byte, 100)
	w := weight("e1", "s", nil, &store.Entry{Body: body})
	c := newCache(t, CacheOptions{MaxBytes: 4 * w, Shards: 1, Clock: quiet(clk)})
	if !mfill(t, c, "e1", "s", &store.Entry{Status: 200, Body: body}, time.Second) {
		t.Fatal("e1 not stored")
	}
	for _, k := range []string{"e2", "e3", "e4"} {
		if !mfill(t, c, k, "s", &store.Entry{Status: 200, Body: body}, time.Hour) {
			t.Fatalf("%s not stored", k)
		}
	}
	clk.Advance(time.Second)
	if !mfill(t, c, "e5", "s", &store.Entry{Status: 200, Body: body}, time.Hour) {
		t.Fatal("e5 not stored")
	}
	st := c.Stats()
	if st.Evictions != 0 || st.Expired != 1 {
		t.Fatalf("Evictions %d, Expired %d; want 0 and 1", st.Evictions, st.Expired)
	}
	for _, k := range []string{"e2", "e3", "e4", "e5"} {
		mwantHit(t, k, mget(t, c, k, "s"), string(body))
	}
}

func TestCacheRejectsOversized(t *testing.T) {
	t.Parallel()
	c := newCache(t, CacheOptions{MaxBytes: 4096, Shards: 1, Clock: quiet(clock.NewFake(epoch))})
	if !mfillVary(t, c, "k", "s1", []string{"X-A"}, ment("small"), time.Hour) {
		t.Fatal("small entry not stored")
	}
	before := c.Bytes()
	tok := mget(t, c, "k", "s2").Token
	ok, err := c.Put(bg, tok, "k", "s2", []string{"X-B"}, ment(string(make([]byte, 5000))), time.Hour)
	if err != nil || ok {
		t.Fatalf("oversized Put = (%v, %v), want (false, nil)", ok, err)
	}
	if st := c.Stats(); st.Rejected != 1 {
		t.Fatalf("Rejected = %d, want 1", st.Rejected)
	}
	if c.Bytes() != before {
		t.Fatalf("Bytes() changed from %d to %d", before, c.Bytes())
	}
	l := mget(t, c, "k", "s1")
	mwantHit(t, "k/s1", l, "small")
	if len(l.VaryNames) != 1 || l.VaryNames[0] != "x-a" {
		t.Fatalf("VaryNames = %v, want [x-a]", l.VaryNames)
	}
}

func TestCacheHitsRefreshRecency(t *testing.T) {
	t.Parallel()
	e := ment("v")
	c := newCache(t, CacheOptions{MaxBytes: 8 * weight("hot", "s", nil, e), Shards: 1, Clock: quiet(clock.NewFake(epoch))})
	mfill(t, c, "hot", "s", e, time.Hour)
	for i := range 40 {
		mfill(t, c, "c"+strconv.Itoa(10+i), "s", e, time.Hour)
		mwantHit(t, "hot after put "+strconv.Itoa(i), mget(t, c, "hot", "s"), "v")
	}
	mwantHit(t, "hot", mget(t, c, "hot", "s"), "v")
	mwantMiss(t, "c10", mget(t, c, "c10", "s"))
}

func TestCacheEmptyPrimaryRemoved(t *testing.T) {
	t.Parallel()
	e := ment("v")
	c := newCache(t, CacheOptions{MaxBytes: 2 * weight("a", "s1", nil, e), Shards: 1, Clock: quiet(clock.NewFake(epoch))})
	mfill(t, c, "a", "s1", e, time.Hour)
	mfill(t, c, "a", "s2", e, time.Hour)
	if st := c.Stats(); st.Primaries != 1 || st.Entries != 2 {
		t.Fatalf("before eviction: %+v", st)
	}
	mfill(t, c, "b", "s1", e, time.Hour)
	if st := c.Stats(); st.Primaries != 2 || st.Entries != 2 {
		t.Fatalf("after first eviction: %+v", st)
	}
	mfill(t, c, "b", "s2", e, time.Hour)
	st := c.Stats()
	if st.Primaries != 1 || st.Entries != 2 {
		t.Fatalf("after second eviction: Primaries %d, Entries %d; want 1 and 2", st.Primaries, st.Entries)
	}
	mwantMiss(t, "a", mget(t, c, "a", "s1"))
}

func TestCacheTagIndexClean(t *testing.T) {
	t.Parallel()
	for _, how := range []string{"expiry", "eviction", "purge-keys", "purge-tags", "purge-all", "replacement", "soft-keys"} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			clk := clock.NewFake(epoch)
			tagged := ment("body", "t")
			ttl := time.Hour
			if how == "expiry" {
				ttl = time.Second
			}
			c := newCache(t, CacheOptions{MaxBytes: weight("p", "s", nil, tagged), Shards: 1, Clock: quiet(clk)})
			if !mfill(t, c, "p", "s", tagged, ttl) {
				t.Fatal("tagged entry not stored")
			}
			if st := c.Stats(); st.Tags == 0 {
				t.Fatal("tag index empty before the action")
			}
			switch how {
			case "expiry":
				clk.Advance(time.Second)
				c.Sweep(clk.Now())
			case "eviction":
				for _, k := range []string{"u0", "u1", "u2"} {
					mfill(t, c, k, "s", ment("x"), time.Hour)
				}
			case "purge-keys":
				mpurge(t, c, pkeys(false, "p"))
			case "purge-tags":
				mpurge(t, c, ptags(false, "t"))
			case "purge-all":
				mpurge(t, c, pall(false))
			case "soft-keys":
				mpurge(t, c, pkeys(true, "p"))
			case "replacement":
				mfill(t, c, "p", "s", ment("body"), ttl)
			}
			st := c.Stats().Tags
			if how == "soft-keys" {
				if st == 0 {
					t.Fatal("soft purge emptied the tag index")
				}
				return
			}
			if st != 0 {
				t.Fatalf("Tags = %d after %s, want 0", st, how)
			}
		})
	}
}

func TestCacheJanitor(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	ch := make(chan int)
	c := newCache(t, CacheOptions{
		MaxBytes:        1 << 20,
		Clock:           clk,
		JanitorInterval: 10 * time.Second,
		OnSweep:         func(n int) { ch <- n },
	})
	for _, k := range []string{"a", "b", "c"} {
		mfill(t, c, k, "s", ment("x"), 5*time.Second)
	}
	testwait.Until(t, "janitor ticker", func() bool { return clk.HousekeepingWaiters() >= 1 }, nil)
	clk.Advance(10 * time.Second)
	if n, _ := testwait.Recv(t, "janitor sweep", ch, nil); n != 3 {
		t.Fatalf("janitor removed %d, want 3", n)
	}
	if c.Len() != 0 {
		t.Fatalf("Len() = %d after janitor sweep, want 0", c.Len())
	}
	tok := mget(t, c, "z", "s").Token
	if err := c.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	select {
	case <-c.janitorDone:
	default:
		t.Fatal("janitorDone not closed after Close returned")
	}
	if w := clk.HousekeepingWaiters(); w != 0 {
		t.Fatalf("HousekeepingWaiters() = %d after Close, want 0", w)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if _, err := c.Get(bg, "a", func([]string) (string, bool) { return "s", true }); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("Get after Close: %v, want ErrUnavailable", err)
	}
	if _, err := c.Put(bg, tok, "a", "s", nil, ment("x"), time.Hour); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("Put after Close: %v, want ErrUnavailable", err)
	}
	if _, err := c.Purge(bg, pall(false)); !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("Purge after Close: %v, want ErrUnavailable", err)
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	if _, err := c.Get(ctx, "a", func([]string) (string, bool) { return "s", true }); err != context.Canceled {
		t.Fatalf("canceled Get after Close: %v, want context.Canceled", err)
	}
}

func TestCacheCloseRepeated(t *testing.T) {
	t.Parallel()
	for range 20 {
		c := NewCache(CacheOptions{MaxBytes: 1 << 10, Clock: clock.NewFake(epoch)})
		if err := c.Close(); err != nil {
			t.Fatalf("Close() = %v", err)
		}
		select {
		case <-c.janitorDone:
		default:
			t.Fatal("janitorDone not closed after Close returned")
		}
	}
}

func TestCacheTwoInstancesIndependent(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(epoch)
	a := newCache(t, CacheOptions{MaxBytes: 1 << 20, Clock: quiet(clk)})
	b := newCache(t, CacheOptions{MaxBytes: 1 << 20, Clock: quiet(clk)})
	mfill(t, a, "k", "s", ment("x"), time.Hour)
	mfill(t, b, "k", "s", ment("x"), time.Hour)
	before := b.Stats()
	mpurge(t, a, pall(false))
	mwantMiss(t, "a", mget(t, a, "k", "s"))
	mwantHit(t, "b", mget(t, b, "k", "s"), "x")
	if after := b.Stats(); after != before {
		t.Fatalf("b stats changed: %+v -> %+v", before, after)
	}
}

func TestCacheCrossesUnixNanoLimit(t *testing.T) {
	t.Parallel()
	clk := clock.NewFake(time.Unix(0, math.MaxInt64).UTC().Add(-time.Hour))
	c := newCache(t, CacheOptions{MaxBytes: 1 << 20, Clock: quiet(clk)})
	if !mfill(t, c, "k", "s", ment("x"), store.MaxTTL) {
		t.Fatal("entry not stored")
	}
	clk.Advance(store.MaxTTL - time.Nanosecond)
	l := mget(t, c, "k", "s")
	mwantHit(t, "before deadline", l, "x")
	if d := l.Now.Sub(l.Entry.StoredAt); d != store.MaxTTL-time.Nanosecond {
		t.Fatalf("Now - StoredAt = %v, want %v", d, store.MaxTTL-time.Nanosecond)
	}
	clk.Advance(time.Nanosecond)
	mwantMiss(t, "at deadline", mget(t, c, "k", "s"))
}
