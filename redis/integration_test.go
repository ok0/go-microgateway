//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
	"github.com/ok0/go-microgateway/store/storetest"
)

type storeFactory func(t *testing.T, prefix string) *Store

func newConfigStore(t *testing.T, cfg *config.RedisStore) *Store {
	t.Helper()
	s, err := newFromConfig(cfg, nil, clock.NewFake(miniStart))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func cleanPrefix(t *testing.T, s *Store) {
	t.Helper()
	t.Cleanup(func() { deleteAllPrefix(t, s.c, s.prefix) })
}

func freshStore(t *testing.T, open storeFactory) *Store {
	t.Helper()
	s := open(t, uniquePrefix(t))
	cleanPrefix(t, s)
	return s
}

func deleteAllPrefix(t *testing.T, c goredis.UniversalClient, prefix string) {
	t.Helper()
	cc, ok := c.(*goredis.ClusterClient)
	if !ok {
		deletePrefix(t, c, prefix)
		return
	}
	err := cc.ForEachMaster(context.Background(), func(ctx context.Context, n *goredis.Client) error {
		var cursor uint64
		for {
			keys, next, err := n.Scan(ctx, cursor, prefix+":*", 1000).Result()
			if err != nil {
				return err
			}
			for _, k := range keys {
				if err := n.Del(ctx, k).Err(); err != nil {
					return err
				}
			}
			cursor = next
			if cursor == 0 {
				return nil
			}
		}
	})
	if err != nil {
		t.Errorf("deleting keys for cleanup: %v", err)
	}
}

func scanPrefix(t *testing.T, c goredis.UniversalClient, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	var out []string
	var cursor uint64
	for {
		keys, next, err := c.Scan(ctx, cursor, prefix+":*", 1000).Result()
		if err != nil {
			t.Fatalf("scanning keys: %v", err)
		}
		out = append(out, keys...)
		cursor = next
		if cursor == 0 {
			return out
		}
	}
}

func expiryMsIfLive(t *testing.T, c goredis.UniversalClient, key string) (int64, time.Duration, bool) {
	t.Helper()
	ctx := context.Background()
	for attempt := 0; attempt < 100; attempt++ {
		pipe := c.Pipeline()
		t0 := pipe.Time(ctx)
		pttl := pipe.PTTL(ctx, key)
		t1 := pipe.Time(ctx)
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("PTTL %q: %v", key, err)
		}
		if t0.Val().UnixMilli() == t1.Val().UnixMilli() {
			if pttl.Val() < 0 {
				return 0, pttl.Val(), false
			}
			return t0.Val().UnixMilli() + pttl.Val().Milliseconds(), pttl.Val(), true
		}
	}
	t.Fatal("no PTTL bracketed by TIME readings within one millisecond in 100 attempts")
	return 0, 0, false
}

func realExpiryMs(t *testing.T, c goredis.UniversalClient, key string) int64 {
	t.Helper()
	e, pttl, live := expiryMsIfLive(t, c, key)
	if !live {
		t.Fatalf("PTTL %q = %s, want a live expiry", key, pttl)
	}
	return e
}

func durCeilMs(d time.Duration) int64 {
	return (int64(d) + int64(time.Millisecond) - 1) / int64(time.Millisecond)
}

func checkExactWithoutRefill(t *testing.T, s *Store, key string) {
	t.Helper()
	ctx := context.Background()
	l := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 3}
	t0 := serverMs(t, s.c)
	for i := 1; i <= 3; i++ {
		d, err := s.Take(ctx, key, l)
		if err != nil {
			t.Fatalf("take %d of %q: %v", i, key, err)
		}
		if !d.Allowed || d.Remaining != int64(3-i) {
			t.Fatalf("take %d of %q: %+v, want allowed with Remaining %d", i, key, d, 3-i)
		}
		want := time.Duration(i) * time.Hour
		delta := time.Duration(serverMs(t, s.c)-t0+1) * time.Millisecond
		if d.ResetAfter > want || d.ResetAfter < want-delta {
			t.Errorf("take %d of %q: ResetAfter %s, want within [%s, %s]", i, key, d.ResetAfter, want-delta, want)
		}
	}
	d, err := s.Take(ctx, key, l)
	if err != nil {
		t.Fatalf("take 4 of %q: %v", key, err)
	}
	if d.Allowed {
		t.Fatalf("take 4 of %q: %+v, want denied", key, d)
	}
	if d.RetryAfter <= 0 || d.RetryAfter > time.Hour {
		t.Errorf("take 4 of %q: RetryAfter %s, want in (0, 1h]", key, d.RetryAfter)
	}
}

// hammer runs takes from perStore callers per store until the limit has denied minDenied takes. It returns the admitted takes, the total takes and the server time that passed, plus one millisecond.
func hammer(t *testing.T, stores []*Store, key string, l store.Limit, minDenied int64) (int64, int64, time.Duration) {
	t.Helper()
	ctx := context.Background()
	const perStore = 8
	var admitted, total, denied atomic.Int64
	errs := make([]error, len(stores)*perStore)
	var stop atomic.Bool
	defer stop.Store(true)
	start := serverMs(t, stores[0].c)
	var wg sync.WaitGroup
	for g := range errs {
		s := stores[g%len(stores)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() && denied.Load() < minDenied {
				d, err := s.Take(ctx, key, l)
				if err != nil {
					errs[g] = err
					stop.Store(true)
					return
				}
				total.Add(1)
				if d.Allowed {
					admitted.Add(1)
				} else {
					denied.Add(1)
				}
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	testwait.Done(t, fmt.Sprintf("%d denied takes of %q", minDenied, key), done, func() string {
		return fmt.Sprintf("admitted %d, denied %d, total %d", admitted.Load(), denied.Load(), total.Load())
	})
	elapsed := time.Duration(serverMs(t, stores[0].c)-start+1) * time.Millisecond
	for g := range errs {
		if errs[g] != nil {
			t.Fatalf("take %q: %v", key, errs[g])
		}
	}
	return admitted.Load(), total.Load(), elapsed
}

func checkBoundUnderConcurrency(t *testing.T, open storeFactory) {
	t.Helper()
	prefix := uniquePrefix(t)
	stores := []*Store{open(t, prefix), open(t, prefix)}
	cleanPrefix(t, stores[0])

	tb := store.Limit{Algorithm: store.TokenBucket, Count: 20, Period: time.Second, Burst: 5}
	// Run until the limit has denied as many takes as there are callers.
	admitted, total, elapsed := hammer(t, stores, "bound-tb", tb, 16)
	if bound := 5 + int64(elapsed/(50*time.Millisecond)); admitted > bound {
		t.Errorf("token bucket admitted %d in %s, want at most %d", admitted, elapsed, bound)
	}
	if admitted < 5 {
		t.Errorf("token bucket admitted %d, want at least the burst 5", admitted)
	}
	if admitted >= total {
		t.Errorf("token bucket denied none of %d takes: limit never reached", total)
	}

	sw := store.Limit{Algorithm: store.SlidingWindow, Count: 10, Period: 500 * time.Millisecond}
	// Run until the limit has denied as many takes as there are callers.
	admitted, total, elapsed = hammer(t, stores, "bound-sw", sw, 16)
	windows := int64((elapsed + 500*time.Millisecond - 1) / (500 * time.Millisecond))
	if bound := 10 * (1 + windows); admitted > bound {
		t.Errorf("sliding window admitted %d in %s, want at most %d", admitted, elapsed, bound)
	}
	if admitted < 10 {
		t.Errorf("sliding window admitted %d, want at least the count 10", admitted)
	}
	if admitted >= total {
		t.Errorf("sliding window denied none of %d takes: limit never reached", total)
	}
}

func checkTTL(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	cases := []struct {
		key     string
		l       store.Limit
		allowed []bool
	}{
		{"ttl-tb", store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 2}, []bool{true, true, false}},
		{"ttl-sw", store.Limit{Algorithm: store.SlidingWindow, Count: 2, Period: time.Hour}, []bool{true, true, false}},
	}
	bounds := map[string]time.Duration{}
	for _, tc := range cases {
		for i, want := range tc.allowed {
			d, err := s.Take(ctx, tc.key, tc.l)
			if err != nil {
				t.Fatalf("take %d of %q: %v", i, tc.key, err)
			}
			if d.Allowed != want {
				t.Fatalf("take %d of %q: %+v, want allowed %t", i, tc.key, d, want)
			}
			if d.Allowed {
				bounds[s.keyFor(tc.key, tc.l.Algorithm)] = d.ResetAfter
			}
		}
	}
	keys := scanPrefix(t, s.c, s.prefix)
	if len(keys) != len(cases) {
		t.Errorf("%d keys under prefix, want %d", len(keys), len(cases))
	}
	for _, k := range keys {
		bound, ok := bounds[k]
		if !ok {
			t.Errorf("unexpected key %q", k)
			continue
		}
		pttl := s.c.PTTL(ctx, k).Val()
		if limit := durCeilMs(bound) + 1; pttl <= 0 || pttl.Milliseconds() > limit {
			t.Errorf("key %q: PTTL %s, want in (0, %d ms]", k, pttl, limit)
		}
	}
}

func serverMs(t *testing.T, c goredis.UniversalClient) int64 {
	t.Helper()
	now, err := c.Time(context.Background()).Result()
	if err != nil {
		t.Fatalf("TIME: %v", err)
	}
	return now.UnixMilli()
}

// checkFloodExpires runs floodExpires with the key period. An attempt that does not finish within the key period is inconclusive: its keys are deleted and it is repeated with twice the period.
func checkFloodExpires(t *testing.T, s *Store) {
	t.Helper()
	for period := 10 * time.Second; !floodExpires(t, s, period); period *= 2 {
		t.Logf("flood and checks outlasted the %s key period; repeating with %s", period, 2*period)
	}
}

func floodExpires(t *testing.T, s *Store, period time.Duration) bool {
	t.Helper()
	ctx := context.Background()
	const n = 10000
	periodMs := period.Milliseconds()
	l := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: period, Burst: 1}

	before, err := s.c.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	if before != 0 {
		t.Fatalf("DBSIZE = %d before the flood, want 0", before)
	}

	t0 := serverMs(t, s.c)
	const workers = 16
	errs := make([]error, workers)
	denied := make([]int64, workers)
	var wg sync.WaitGroup
	for g := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := g; i < n; i += workers {
				d, err := s.Take(ctx, fmt.Sprintf("flood:%d", i), l)
				if err != nil {
					errs[g] = err
					return
				}
				if !d.Allowed {
					denied[g]++
				}
			}
		}()
	}
	wg.Wait()
	var nDenied int64
	for g := range errs {
		if errs[g] != nil {
			t.Fatalf("take: %v", errs[g])
		}
		nDenied += denied[g]
	}
	if nDenied != 0 {
		t.Fatalf("%d flood takes denied, want all allowed", nDenied)
	}
	t1 := serverMs(t, s.c)

	size, err := s.c.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	samples := []int{0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 9999}
	for _, i := range samples {
		e, _, live := expiryMsIfLive(t, s.c, s.keyFor(fmt.Sprintf("flood:%d", i), store.TokenBucket))
		if !live {
			deleteAllPrefix(t, s.c, s.prefix)
			return false
		}
		if e < t0+periodMs || e > t1+periodMs+1 {
			t.Errorf("key flood:%d expires at %d ms, want within [%d, %d]", i, e, t0+periodMs, t1+periodMs+1)
		}
	}
	t2 := serverMs(t, s.c)
	if t2 >= t0+periodMs {
		deleteAllPrefix(t, s.c, s.prefix)
		return false
	}
	if size != n {
		t.Fatalf("DBSIZE = %d right after the flood, want %d", size, n)
	}

	maxExp := t1 + periodMs + 1
	testwait.Until(t, "server time to pass the flood expiries", func() bool {
		return serverMs(t, s.c) > maxExp
	}, func() string {
		return fmt.Sprintf("server time %d ms, last expiry %d ms", serverMs(t, s.c), maxExp)
	})
	for _, i := range samples {
		if s.c.Exists(ctx, s.keyFor(fmt.Sprintf("flood:%d", i), store.TokenBucket)).Val() != 0 {
			t.Errorf("key flood:%d still exists after server time passed its expiry", i)
		}
	}

	testwait.Until(t, "every flood key to be removed after its expiry", func() bool {
		left, err := s.c.DBSize(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		return left == 0
	}, func() string {
		return fmt.Sprintf("DBSIZE = %d", s.c.DBSize(ctx).Val())
	})
	return true
}

func checkScriptFlush(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	sha := s.sc.tb.Hash()
	exists := func() bool {
		v, err := s.c.ScriptExists(ctx, sha).Result()
		if err != nil {
			t.Fatal(err)
		}
		return v[0]
	}
	l := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 5}
	before := mustTake(t, s, "flush", l)
	if !exists() {
		t.Fatalf("token-bucket script not cached after a take")
	}
	if err := s.c.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if exists() {
		t.Fatalf("token-bucket script still cached after SCRIPT FLUSH")
	}
	after := mustTake(t, s, "flush", l)
	if after.Remaining != before.Remaining-1 {
		t.Errorf("Remaining %d after SCRIPT FLUSH, want %d", after.Remaining, before.Remaining-1)
	}
	if !exists() {
		t.Errorf("token-bucket script not reloaded after SCRIPT FLUSH")
	}
}

func checkPrefixes(t *testing.T, open storeFactory) {
	t.Helper()
	p1, p2 := uniquePrefix(t), uniquePrefix(t)
	a := open(t, p1)
	cleanPrefix(t, a)
	b := open(t, p1)
	other := open(t, p2)
	cleanPrefix(t, other)
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 1}
	if d := mustTake(t, a, "iso", tb); !d.Allowed {
		t.Fatalf("first take: %+v, want allowed", d)
	}
	if d := mustTake(t, b, "iso", tb); d.Allowed {
		t.Errorf("same prefix take: %+v, want denied", d)
	}
	if d := mustTake(t, other, "iso", tb); !d.Allowed {
		t.Errorf("other prefix take: %+v, want allowed", d)
	}
}

func checkWrongType(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	for _, l := range []store.Limit{
		{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3},
		{Algorithm: store.SlidingWindow, Count: 4, Period: 8 * time.Second},
	} {
		if err := s.c.HSet(ctx, s.keyFor("wrongtype", l.Algorithm), "f", "v").Err(); err != nil {
			t.Fatal(err)
		}
		_, err := s.Take(ctx, "wrongtype", l)
		if !errors.Is(err, store.ErrUnavailable) || !strings.Contains(err.Error(), "server error WRONGTYPE") {
			t.Errorf("%v: error %v, want ErrUnavailable containing server error WRONGTYPE", l.Algorithm, err)
		}
	}
}

func injectedHarnessOpen(open storeFactory) storetest.Harness[store.RateLimiter] {
	var fake *clock.Fake
	return storetest.Harness[store.RateLimiter]{
		New: func(t *testing.T) (store.RateLimiter, store.RateLimiter) {
			t.Helper()
			prefix := uniquePrefix(t)
			fake = clock.NewFake(injectedStart())
			sa := withInjectedClock(open(t, prefix), fake.Now)
			sb := withInjectedClock(open(t, prefix), fake.Now)
			t.Cleanup(func() { deleteAllPrefix(t, sa.c, prefix) })
			return sa, sb
		},
		Advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			fake.Advance(d)
		},
	}
}

func injectedCacheHarnessOpen(open storeFactory) storetest.Harness[store.Cache] {
	var fake *clock.Fake
	build := func(t *testing.T, start time.Time) (store.Cache, store.Cache) {
		t.Helper()
		prefix := uniquePrefix(t)
		fake = clock.NewFake(start)
		sa := withInjectedClock(open(t, prefix), fake.Now)
		sb := withInjectedClock(open(t, prefix), fake.Now)
		t.Cleanup(func() { deleteAllPrefix(t, sa.c, prefix) })
		return sa, sb
	}
	return storetest.Harness[store.Cache]{
		New: func(t *testing.T) (store.Cache, store.Cache) {
			t.Helper()
			return build(t, injectedStart())
		},
		NewAt: func(t *testing.T, start time.Time) (store.Cache, store.Cache) {
			t.Helper()
			if start.Before(injectedStart()) {
				t.Fatalf("injected cache clock starts at %v, before %v", start, injectedStart())
			}
			return build(t, start)
		},
		Advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			fake.Advance(d)
		},
		Resolution: time.Microsecond,
		Horizon:    cacheHorizon,
	}
}

func checkCacheServerTime(t *testing.T, addr string) {
	t.Helper()
	prefix := uniquePrefix(t)
	c := newTestClient(t, addr, 15)
	t.Cleanup(func() { deletePrefix(t, c, prefix) })
	s := cacheStore(t, addr, 15, prefix)
	const ttl = 1500 * time.Millisecond
	if !ckFill(t, s, "st", "s", nil, ckEntry("st"), ttl) {
		t.Fatal("entry not stored")
	}
	l := ckGet(t, s, "st", "s")
	if l.Entry == nil {
		t.Fatal("entry missing right after the fill")
	}
	deadline := l.Entry.StoredAt.Add(ttl)
	var entry, index string
	for _, k := range ckKeys(t, c, prefix) {
		switch {
		case strings.Contains(k, ":e:"):
			entry = k
		case strings.Contains(k, ":i:"):
			index = k
		}
	}
	if entry == "" || index == "" {
		t.Fatalf("keys after the fill: entry %q, index %q", entry, index)
	}
	if serverAtLeast(t, c, 7, 0) {
		for _, k := range []string{entry, index} {
			if ms := ckExpiryMs(t, c, k); ms != ceilMs(deadline) {
				t.Errorf("%s: PEXPIRETIME %d, want %d", k, ms, ceilMs(deadline))
			}
		}
	} else {
		t.Log("PEXPIRETIME needs Redis 7.0: the absolute expiry is checked only by the expiry wait below")
	}
	testwait.Until(t, "the entry to expire on the server clock", func() bool {
		return ckGet(t, s, "st", "s").Entry == nil
	}, func() string {
		return fmt.Sprintf("server time %d ms, deadline %d ms", serverMs(t, c), ceilMs(deadline))
	})
	testwait.Until(t, "every key of the prefix to be removed after its lifetime", func() bool {
		return len(ckKeys(t, c, prefix)) == 0
	}, func() string {
		return fmt.Sprintf("keys %q", ckKeys(t, c, prefix))
	})
}

func checkCacheScriptFlush(t *testing.T, addr string) {
	t.Helper()
	ctx := context.Background()
	s := cacheStore(t, addr, 15, uniquePrefix(t))
	cleanPrefix(t, s)
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
		{"purge keys", func() error {
			_, err := s.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"pf"}, Retain: time.Hour})
			return err
		}},
		{"purge tags", func() error {
			_, err := s.Purge(ctx, store.Purge{Kind: store.PurgeTags, Tags: []string{"t1"}, Soft: true, Retain: time.Hour})
			return err
		}},
		{"purge all", func() error {
			_, err := s.Purge(ctx, store.Purge{Kind: store.PurgeAll, Retain: time.Hour})
			return err
		}},
	}
	for _, op := range ops {
		if err := op.run(); err != nil {
			t.Fatalf("%s warm-up: %v", op.name, err)
		}
		if err := s.c.Do(ctx, "SCRIPT", "FLUSH").Err(); err != nil {
			t.Fatalf("SCRIPT FLUSH: %v", err)
		}
		if err := op.run(); err != nil {
			t.Errorf("%s after flush: %v", op.name, err)
		}
		if err := op.run(); err != nil {
			t.Errorf("%s with loaded scripts: %v", op.name, err)
		}
	}
}

func igLossRun(t *testing.T, addr, kind string, soft bool, lk ptLoss, when string) {
	t.Helper()
	ctx := context.Background()
	prefix := uniquePrefix(t)
	raw := newTestClient(t, addr, 15)
	t.Cleanup(func() { deletePrefix(t, raw, prefix) })
	fake := clock.NewFake(injectedStart())
	a := withInjectedClock(cacheStore(t, addr, 15, prefix), fake.Now)
	b := withInjectedClock(cacheStore(t, addr, 15, prefix), fake.Now)
	const ttl = time.Minute
	key := lk.key(a)
	if when == "joined" {
		joins := 0
		a.hook = func(hp hookPoint) {
			if hp == hookJoined && joins == 0 {
				joins++
				if err := a.c.Del(ctx, key).Err(); err != nil {
					t.Errorf("DEL %s: %v", key, err)
				}
			}
		}
		if !ckFill(t, a, "lm-cov", "a", nil, ckEntry("a", "t1", "t2"), ttl) {
			t.Fatal("lm-cov/a not stored")
		}
		a.hook = nil
		if joins != 1 {
			t.Fatalf("hookJoined fired %d times, want 1", joins)
		}
	} else if !ckFill(t, a, "lm-cov", "a", nil, ckEntry("a", "t1", "t2"), ttl) {
		t.Fatal("lm-cov/a not stored")
	}
	if !ckFill(t, a, "lm-cov", "b", nil, ckEntry("b", "t1"), ttl) {
		t.Fatal("lm-cov/b not stored")
	}
	if !ckFill(t, a, "lm-keep", "a", nil, ckEntry("keep", "keep"), ttl) {
		t.Fatal("lm-keep/a not stored")
	}
	tokOld := ckGet(t, a, "lm-cov", "c").Token
	if when == "before" {
		ptDel(t, a, key)
	}
	res := ptRun(t, a, ptPurgeFor(kind, soft, "lm-cov", "t1"))
	if when == "after" {
		ptDel(t, a, key)
	}

	wantAffected := 2
	if kind == "keys" && soft {
		wantAffected = 1
	}
	switch {
	case kind == "all":
		if res.Accepted != 1 || res.AffectedKnown || res.Affected != 0 {
			t.Fatalf("all purge: got %+v", res)
		}
	case lk.name == "index" && when != "after":
	default:
		if res.Accepted != 1 || !res.AffectedKnown || res.Affected != wantAffected {
			t.Fatalf("%s purge: got %+v, want affected %d", kind, res, wantAffected)
		}
	}

	ptNotFresh(t, a, "lm-cov", "a")
	ptNotFresh(t, b, "lm-cov", "a")
	ptNotFresh(t, a, "lm-cov", "b")
	ptNotFresh(t, b, "lm-cov", "b")

	stored, err := b.Put(ctx, tokOld, "lm-cov", "c", nil, ckEntry("c", "t1"), ttl)
	if err != nil || stored {
		t.Fatalf("fill with a token from before the purge: stored %t, err %v", stored, err)
	}

	if kind != "all" {
		ptHit(t, a, "lm-keep", "a")
		ptHit(t, b, "lm-keep", "a")
	}

	if kind == "all" {
		if !ckFill(t, b, "lm-late", "a", nil, ckEntry("late"), 2*time.Hour) {
			t.Fatal("entry stored after the purge-all not stored")
		}
		fake.Advance(time.Hour + time.Microsecond)
		ptHit(t, a, "lm-late", "a")
	}
}

func checkCacheLossMatrix(t *testing.T, addr string) {
	t.Helper()
	for _, kind := range []string{"keys", "tags", "all"} {
		for _, soft := range []bool{false, true} {
			for _, lk := range ptLossKinds {
				for _, when := range []string{"before", "after", "joined"} {
					if (when == "before" && !lk.before) || (when == "joined" && !lk.joined) {
						continue
					}
					t.Run(fmt.Sprintf("%s/soft=%t/%s/%s", kind, soft, lk.name, when), func(t *testing.T) {
						igLossRun(t, addr, kind, soft, lk, when)
					})
				}
			}
		}
	}
}

func igLargeRun(t *testing.T, addr string, soft bool) {
	t.Helper()
	prefix := uniquePrefix(t)
	raw := newTestClient(t, addr, 15)
	t.Cleanup(func() { deletePrefix(t, raw, prefix) })
	p := newRespProxy(t, addr)
	t.Cleanup(func() {
		if v := p.Violations(); len(v) > 0 {
			t.Errorf("slot co-location violations: %q", v)
		}
	})
	fake := clock.NewFake(injectedStart())
	s := withInjectedClock(cacheStore(t, p.Addr(), 15, prefix), fake.Now)
	set, repl, _, _ := s.tagKeys(ptBigTag)
	total, _ := ptBigFill(t, s, 1100, 0)
	res := ptRun(t, s, store.Purge{Kind: store.PurgeTags, Tags: []string{ptBigTag}, Soft: soft, Retain: time.Hour})
	if res.Accepted != 1 || !res.AffectedKnown || res.Affected != total {
		t.Fatalf("Purge: got %+v, want accepted 1, affected %d, known", res, total)
	}
	for _, key := range []string{set, repl} {
		if calls := ptHscanCalls(t, p, key); calls < 2 {
			t.Fatalf("%s: %d HSCAN calls, want at least 2", key, calls)
		}
	}
	ptNoBulkReads(t, p, set, repl)
	for i := 0; i < 1100; i++ {
		ptNotFresh(t, s, ptBigPrimary(i), "a")
		if i < 50 {
			ptNotFresh(t, s, ptBigPrimary(i), "b")
		}
	}
}

func igPass(t *testing.T, c goredis.UniversalClient, d time.Duration) {
	t.Helper()
	until := serverMs(t, c) + d.Milliseconds()
	testwait.Until(t, fmt.Sprintf("server time to pass %s", d), func() bool {
		return serverMs(t, c) >= until
	}, func() string {
		return fmt.Sprintf("server time %d ms, want %d ms", serverMs(t, c), until)
	})
}

func igFixtureOf(t *testing.T, addr string, n int) *gcFixture {
	t.Helper()
	prefix := uniquePrefix(t)
	f := &gcFixture{
		t:        t,
		client:   newTestClient(t, addr, 15),
		prefix:   prefix,
		origin:   newGCOrigin(t),
		cacheTop: "  store: shared\n",
		routes:   gcRoute("r", "stale_while_revalidate: 1h", "stale_if_error: 1h"),
	}
	t.Cleanup(func() { deletePrefix(t, f.client, f.prefix) })
	f.stores = redisStoreYAML("shared", []string{addr}, fmt.Sprintf("db: 15\nkey_prefix: %s\n", prefix), "")
	for i := 0; i < n; i++ {
		f.gws = append(f.gws, f.gateway(gcOffsets[i%len(gcOffsets)]))
	}
	return f
}

func igMatrix(t *testing.T, addr string, n int, sel string, library, soft bool, v gcValidator) {
	t.Helper()
	f := igFixtureOf(t, addr, n)
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v1")
	f.origin.tag("/p", "tg1 tg2")
	f.origin.tag("/o", "tg2")
	a := f.gws[0]
	for _, e := range gcEntries {
		rec, st := gcFetch(t, a, e)
		if rec.Code != http.StatusOK || st.Hit || !st.Stored {
			t.Fatalf("fill %s %s: status %d %+v, want 200 stored miss", e.method, e.path, rec.Code, st)
		}
	}
	if calls, _ := f.origin.counts(); calls != len(gcEntries) {
		t.Fatalf("origin requests after the fill = %d, want %d", calls, len(gcEntries))
	}
	for _, e := range gcEntries {
		ra, sa := gcFetch(t, a, e)
		if !sa.Hit {
			t.Fatalf("%s %s on the filling gateway: status %+v, want hit", e.method, e.path, sa)
		}
		for _, g := range f.gws[1:] {
			rb, sb := gcFetch(t, g, e)
			if !sb.Hit || sb.TTL != sa.TTL || sb.HasTTL != sa.HasTTL || rb.Header().Get("Age") != ra.Header().Get("Age") {
				t.Fatalf("%s %s on another gateway: status %+v Age %q, want %+v Age %q", e.method, e.path, sb, rb.Header().Get("Age"), sa, ra.Header().Get("Age"))
			}
			if e.method == http.MethodGet && rb.Body.String() != "v1|"+e.lang {
				t.Fatalf("%s %s on another gateway: body %q, want %q", e.method, e.path, rb.Body.String(), "v1|"+e.lang)
			}
		}
	}
	if calls, _ := f.origin.counts(); calls != len(gcEntries) {
		t.Fatalf("origin requests after the hits = %d, want %d", calls, len(gcEntries))
	}

	if !library {
		before := f.gwSnapshot()
		plain := mustGateway(t, f.doc(""), clock.NewFake(miniStart), nil)
		body := gcPurgeBody(sel, soft)
		rejected := []struct {
			rec    *httptest.ResponseRecorder
			status int
		}{
			{gcPurge(t, a, "", body), http.StatusUnauthorized},
			{gcPurge(t, a, gcToken+"x", body), http.StatusForbidden},
			{gcPurge(t, a, gcToken, `{"urls":`), http.StatusBadRequest},
			{gcPurge(t, a, gcToken, `{"urls":["ftp://example.com/p"]}`), http.StatusBadRequest},
			{gcPurge(t, plain, gcToken, body), http.StatusNotFound},
		}
		for i, r := range rejected {
			if r.rec.Code != r.status {
				t.Fatalf("rejected purge %d: status %d, want %d: %s", i, r.rec.Code, r.status, r.rec.Body.String())
			}
		}
		if after := f.gwSnapshot(); !reflect.DeepEqual(before, after) {
			t.Fatal("a rejected purge changed the store")
		}
	}

	var got gcReply
	if library {
		res, err := a.Purge(context.Background(), gcPurgeRequest(sel, soft))
		if err != nil {
			t.Fatalf("Purge(%s): %v", sel, err)
		}
		got = gcLibraryReply(res)
	} else {
		got = gcPurgeOK(t, gcPurge(t, a, gcToken, gcPurgeBody(sel, soft)))
	}
	if want := gcWant(sel, soft); !reflect.DeepEqual(got, want) {
		t.Fatalf("purge reply %+v, want %+v", got, want)
	}
	if sel == "tag" {
		igPass(t, f.client, 2*time.Second)
	}
	late := f.gateway(2 * time.Hour)

	f.origin.set(http.StatusOK, gcResponseHeader(v), "v2")
	b := f.gws[1]
	calls0, _ := f.origin.counts()
	covered := 0
	for _, e := range gcEntries {
		if !gcCovered(sel, e) {
			continue
		}
		covered++
		calls1, cond1 := f.origin.counts()
		hdr := e.header()
		if hdr == nil {
			hdr = http.Header{}
		}
		if soft {
			hdr.Set("Cache-Control", "max-stale")
		}
		rec := gcRequest(b, e.method, e.url(), hdr)
		st := gcStatus(t, rec)
		calls2, cond2 := f.origin.counts()
		if calls2-calls1 != 1 {
			t.Fatalf("%s %s on the second gateway: origin requests +%d, want +1", e.method, e.path, calls2-calls1)
		}
		wantCond := 0
		if soft && v.cond != "" {
			wantCond = 1
		}
		if cond2-cond1 != wantCond {
			t.Fatalf("%s %s on the second gateway: conditional origin requests +%d, want +%d", e.method, e.path, cond2-cond1, wantCond)
		}
		last := f.origin.lastHeader()
		for _, h := range []string{"If-None-Match", "If-Modified-Since"} {
			want := ""
			if soft && h == v.cond {
				want = v.value
			}
			if got := last.Get(h); got != want {
				t.Fatalf("%s %s on the second gateway: origin %s = %q, want %q", e.method, e.path, h, got, want)
			}
		}
		wantFwd := httpcache.FwdURIMiss
		switch {
		case soft:
			wantFwd = httpcache.FwdStale
		case e.method == http.MethodGet && e.path == "/p" && e.lang != "en":
			wantFwd = httpcache.FwdVaryMiss
		}
		if rec.Code != http.StatusOK || st.Hit || !st.Stored || st.Fwd != wantFwd {
			t.Fatalf("%s %s on the second gateway: status %d %+v, want 200 miss stored fwd=%s", e.method, e.path, rec.Code, st, wantFwd)
		}
		if st.Detail == "stale-while-revalidate" || st.Detail == "stale-if-error" {
			t.Fatalf("%s %s on the second gateway served a stale entry: %+v", e.method, e.path, st)
		}
		if e.method == http.MethodGet && rec.Body.String() != "v2|"+e.lang {
			t.Fatalf("%s %s on the second gateway: body %q, want %q", e.method, e.path, rec.Body.String(), "v2|"+e.lang)
		}
		if _, st2 := gcFetch(t, b, e); !st2.Hit {
			t.Fatalf("%s %s second request on the second gateway: status %+v, want hit", e.method, e.path, st2)
		}
	}
	if calls, _ := f.origin.counts(); calls-calls0 != covered {
		t.Fatalf("origin requests for the covered entries = %d, want %d", calls-calls0, covered)
	}

	for _, g := range append(append([]*microgateway.Gateway(nil), f.gws...), late) {
		for _, e := range gcEntries {
			rec, st := gcFetch(t, g, e)
			want := "v1|" + e.lang
			if gcCovered(sel, e) {
				want = "v2|" + e.lang
			}
			if !st.Hit {
				t.Fatalf("%s %s after the purge: status %+v, want hit", e.method, e.path, st)
			}
			if e.method == http.MethodGet && rec.Body.String() != want {
				t.Fatalf("%s %s after the purge: body %q, want %q", e.method, e.path, rec.Body.String(), want)
			}
		}
	}
	if calls, _ := f.origin.counts(); calls-calls0 != covered {
		t.Fatalf("origin requests after the hits = %d, want %d", calls-calls0, covered)
	}
}

func igSoftRevalidation(t *testing.T, addr string, n int) {
	t.Helper()
	f := igFixtureOf(t, addr, n)
	v := gcValidators[0]
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v1")
	a, b := f.gws[0], f.gws[1]
	e := gcEntries[0]
	if rec, st := gcFetch(t, a, e); rec.Code != http.StatusOK || !st.Stored {
		t.Fatalf("fill: status %d %+v, want 200 stored", rec.Code, st)
	}
	if rep := gcPurgeOK(t, gcPurge(t, a, gcToken, gcPurgeBody("url", true))); !reflect.DeepEqual(rep, gcReplyOf("url", true, 1)) {
		t.Fatalf("soft purge reply %+v, want %+v", rep, gcReplyOf("url", true, 1))
	}
	f.origin.set(http.StatusServiceUnavailable, gcResponseHeader(v), "down")
	for i := 1; i <= 2; i++ {
		calls0, cond0 := f.origin.counts()
		rec := gcRequest(b, e.method, e.url(), e.header())
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("request %d during origin failure: status %d, want 503", i, rec.Code)
		}
		calls1, cond1 := f.origin.counts()
		if calls1-calls0 != 1 || cond1-cond0 != 1 {
			t.Fatalf("request %d: origin requests +%d (conditional +%d), want +1 (+1): the entry is not soft-purged", i, calls1-calls0, cond1-cond0)
		}
	}
	f.origin.set(http.StatusNotModified, gcResponseHeader(v), "")
	rec, st := gcFetch(t, b, e)
	if rec.Code != http.StatusOK || rec.Body.String() != "v1|en" || !st.Hit || st.Fwd != httpcache.FwdStale || !st.HasFwdStatus || st.FwdStatus != http.StatusNotModified || st.Detail != "" {
		t.Fatalf("304 response %d %q status %+v, want 200 old hit fwd=stale fwd-status=304", rec.Code, rec.Body.String(), st)
	}
	calls0, _ := f.origin.counts()
	recA, stA := gcFetch(t, a, e)
	if recA.Code != http.StatusOK || recA.Body.String() != "v1|en" || !stA.Hit || stA.Fwd != "" {
		t.Fatalf("refreshed entry on the first gateway: %d %q status %+v, want fresh hit with old body", recA.Code, recA.Body.String(), stA)
	}
	if calls, _ := f.origin.counts(); calls != calls0 {
		t.Fatalf("refreshed entry reached the origin: %d requests, want %d", calls, calls0)
	}
}

func igSoftCoalesced(t *testing.T, addr string, n int) {
	t.Helper()
	f := igFixtureOf(t, addr, n)
	v := gcValidators[0]
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v1")
	a, b := f.gws[0], f.gws[1]
	e := gcEntries[0]
	if rec, st := gcFetch(t, a, e); rec.Code != http.StatusOK || !st.Stored {
		t.Fatalf("fill: status %d %+v, want 200 stored", rec.Code, st)
	}
	if rep := gcPurgeOK(t, gcPurge(t, a, gcToken, gcPurgeBody("url", true))); !reflect.DeepEqual(rep, gcReplyOf("url", true, 1)) {
		t.Fatalf("soft purge reply %+v, want %+v", rep, gcReplyOf("url", true, 1))
	}
	f.origin.set(http.StatusOK, gcResponseHeader(v), "v2")
	calls0, cond0 := f.origin.counts()
	f.origin.hold()
	const waiters = 100
	out := make(chan *httptest.ResponseRecorder, waiters)
	for range waiters {
		go func() {
			out <- gcRequest(b, e.method, e.url(), e.header())
		}()
	}
	testwait.Recv(t, "leader at origin", f.origin.arrived, nil)
	testwait.Until(t, "every other concurrent request parked on the leader's call", func() bool {
		st := hooks.Inspect(b).Coalesce
		return st != nil && st.Waiters == waiters-1
	}, nil)
	f.origin.release()
	for range waiters {
		rec, _ := testwait.Recv(t, "concurrent response", out, nil)
		if rec.Code != http.StatusOK || rec.Body.String() != "v2|en" {
			t.Errorf("concurrent response %d %q, want 200 v2|en", rec.Code, rec.Body.String())
		}
	}
	calls1, cond1 := f.origin.counts()
	if calls1-calls0 != 1 || cond1-cond0 != 1 {
		t.Fatalf("origin requests for %d concurrent requests = +%d (conditional +%d), want +1 (+1)", waiters, calls1-calls0, cond1-cond0)
	}
}

func igHeldFill(t *testing.T, addr string, n int, soft bool) {
	t.Helper()
	f := igFixtureOf(t, addr, n)
	f.origin.set(http.StatusOK, gcResponseHeader(gcValidators[2]), "held")
	f.origin.hold()
	a, b := f.gws[0], f.gws[1]
	e := gcEntry{method: http.MethodGet, path: "/h"}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- gcRequest(a, e.method, e.url(), e.header())
	}()
	testwait.Recv(t, "held fill at origin", f.origin.arrived, nil)
	rep := gcPurgeOK(t, gcPurge(t, b, gcToken, fmt.Sprintf(`{"urls":["http://example.com/h"],"soft":%t}`, soft)))
	if want := (gcReply{Type: "url", Soft: soft, Accepted: 1, Affected: gcIntp(0), Scope: "store"}); !reflect.DeepEqual(rep, want) {
		t.Fatalf("purge reply %+v, want %+v", rep, want)
	}
	f.origin.release()
	rec, _ := testwait.Recv(t, "held fill response", done, nil)
	if st := gcStatus(t, rec); rec.Code != http.StatusOK || rec.Body.String() != "held|" || st.Hit {
		t.Fatalf("held fill: status %d %q %+v, want 200 held delivered", rec.Code, rec.Body.String(), st)
	}
	refused, ok := seriesValue(metricsText(t, a), "microgateway_cache_stores_total", map[string]string{"route": "r", "result": "refused"})
	if !ok || refused != 1 {
		t.Fatalf("held fill: stores refused = %v (present %t), want 1", refused, ok)
	}
	for i, g := range []*microgateway.Gateway{b, a} {
		calls0, _ := f.origin.counts()
		rec, st := gcFetch(t, g, e)
		calls1, _ := f.origin.counts()
		if rec.Body.String() != "held|" {
			t.Fatalf("request %d: body %q, want held|", i+1, rec.Body.String())
		}
		if i == 0 && (st.Hit || st.Fwd != httpcache.FwdURIMiss || calls1-calls0 != 1) {
			t.Fatalf("first request after the purge: status %+v origin +%d, want uri-miss with one origin request", st, calls1-calls0)
		}
		if i == 1 && (!st.Hit || calls1 != calls0) {
			t.Fatalf("second request after the purge: status %+v origin +%d, want a hit", st, calls1-calls0)
		}
	}
}

func checkCacheTwoGateways(t *testing.T, addr string) {
	t.Helper()
	for _, n := range []int{2, 3} {
		t.Run(fmt.Sprintf("gateways=%d", n), func(t *testing.T) {
			for _, sel := range []string{"url", "tag", "all"} {
				for _, library := range []bool{false, true} {
					surface := "admin"
					if library {
						surface = "library"
					}
					t.Run(sel+"/"+surface+"/hard", func(t *testing.T) {
						igMatrix(t, addr, n, sel, library, false, gcValidators[2])
					})
					for _, v := range gcValidators {
						t.Run(sel+"/"+surface+"/soft/"+v.name, func(t *testing.T) {
							igMatrix(t, addr, n, sel, library, true, v)
						})
					}
				}
			}
			t.Run("soft/origin_failure_then_304", func(t *testing.T) {
				igSoftRevalidation(t, addr, n)
			})
			t.Run("soft/coalesced", func(t *testing.T) {
				igSoftCoalesced(t, addr, n)
			})
			t.Run("held_fill/hard", func(t *testing.T) {
				igHeldFill(t, addr, n, false)
			})
			t.Run("held_fill/soft", func(t *testing.T) {
				igHeldFill(t, addr, n, true)
			})
		})
	}
}

func checkCacheSlotCrossing(t *testing.T, open storeFactory) {
	t.Helper()
	ctx := context.Background()
	s := open(t, uniquePrefix(t))
	cleanPrefix(t, s)
	const n = 64
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("slot-%02d", i)
		if !ckFill(t, s, keys[i], "a", nil, ckEntry(keys[i], "slots"), time.Hour) {
			t.Fatalf("%s/a not stored", keys[i])
		}
	}
	res := ptRun(t, s, store.Purge{Kind: store.PurgeTags, Tags: []string{"slots"}, Retain: time.Hour})
	if res.Accepted != 1 || !res.AffectedKnown || res.Affected != n {
		t.Fatalf("tag purge across slots: got %+v, want accepted 1, affected %d, known", res, n)
	}
	for _, k := range keys {
		ptNotFresh(t, s, k, "a")
	}
	res = ptRun(t, s, store.Purge{Kind: store.PurgeKeys, Keys: keys, Retain: time.Hour})
	if res.Accepted != n || !res.AffectedKnown {
		t.Fatalf("key purge across slots: got %+v, want accepted %d, known", res, n)
	}
	if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeAll, Retain: time.Hour}); err != nil {
		t.Fatalf("purge all across slots: %v", err)
	}
}

func TestIntegrationSuite(t *testing.T) {
	addr := os.Getenv("MICROGATEWAY_REDIS_ADDR")
	if addr == "" {
		t.Fatal("MICROGATEWAY_REDIS_ADDR is not set: the integration build tag runs this suite against a Redis 6.2+ server at that host:port, e.g. docker run -d --rm -p 6379:6379 redis:7.4-alpine")
	}
	c := newTestClient(t, addr, 15)
	if !serverAtLeast(t, c, 6, 2) {
		t.Fatal("Redis version is below 6.2")
	}
	n, err := c.DBSize(context.Background()).Result()
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("database 15 must be empty")
	}

	open := func(t *testing.T, prefix string) *Store {
		return newTestStore(t, addr, 15, prefix)
	}
	injected := func(t *testing.T) (timedStore, time.Time) {
		return injectedBackend(t, addr, 15)
	}

	t.Run("Fidelity", func(t *testing.T) {
		runFidelityProbes(t, fidelityEnv{c: c, addr: addr})
	})

	t.Run("InjectedTTL", func(t *testing.T) {
		prefix := uniquePrefix(t)
		fake := clock.NewFake(injectedStart())
		s := withInjectedClock(newTestStore(t, addr, 15, prefix), fake.Now)
		cleanPrefix(t, s)
		runTTLContract(t, s, fake.Now, fake.Advance,
			func(t *testing.T, key string) int64 { return realExpiryMs(t, c, key) },
			func() []string { return scanPrefix(t, c, prefix) })
	})

	t.Run("Spike", func(t *testing.T) {
		runSpikeSequences(t, injected)
	})

	t.Run("Conformance", func(t *testing.T) {
		storetest.RateLimiter(t, injectedHarness(func(*testing.T) (string, int) { return addr, 15 }))
	})

	t.Run("Differential", func(t *testing.T) {
		runDifferential(t, injected)
	})

	t.Run("ServerTime", func(t *testing.T) {
		t.Run("ExactWithoutRefill", func(t *testing.T) {
			checkExactWithoutRefill(t, freshStore(t, open), "exact")
		})
		t.Run("BoundUnderConcurrency", func(t *testing.T) {
			checkBoundUnderConcurrency(t, open)
		})
		t.Run("TTL", func(t *testing.T) {
			checkTTL(t, freshStore(t, open))
		})
		t.Run("FloodExpires", func(t *testing.T) {
			checkFloodExpires(t, freshStore(t, open))
		})
		t.Run("ScriptFlush", func(t *testing.T) {
			checkScriptFlush(t, freshStore(t, open))
		})
		t.Run("Prefixes", func(t *testing.T) {
			checkPrefixes(t, open)
		})
		t.Run("WrongType", func(t *testing.T) {
			checkWrongType(t, freshStore(t, open))
		})
	})

	t.Run("CacheConformance", func(t *testing.T) {
		storetest.Cache(t, injectedCacheHarness(func(*testing.T) (string, int) { return addr, 15 }))
	})

	t.Run("CacheServerTime", func(t *testing.T) {
		checkCacheServerTime(t, addr)
	})

	t.Run("CacheScriptFlush", func(t *testing.T) {
		checkCacheScriptFlush(t, addr)
	})

	t.Run("CacheLossMatrix", func(t *testing.T) {
		checkCacheLossMatrix(t, addr)
	})

	t.Run("CacheLargeTag", func(t *testing.T) {
		t.Run("hard", func(t *testing.T) { igLargeRun(t, addr, false) })
		t.Run("soft", func(t *testing.T) { igLargeRun(t, addr, true) })
	})

	t.Run("CacheTwoGateways", func(t *testing.T) {
		checkCacheTwoGateways(t, addr)
	})

	serverAdvance := func(t *testing.T, d time.Duration) {
		target := serverMs(t, c) + int64((d+time.Millisecond-1)/time.Millisecond)
		testwait.Until(t, "server clock to advance "+d.String(), func() bool {
			return serverMs(t, c) >= target
		}, func() string {
			return fmt.Sprintf("server %d ms, target %d ms", serverMs(t, c), target)
		})
	}
	serverNow := func(t *testing.T) time.Time { return time.UnixMilli(serverMs(t, c)) }

	t.Run("MultiInstance", func(t *testing.T) {
		runMultiInstance(t, redisServer{addr: addr, db: 15, exact: false, advance: serverAdvance, now: serverNow})
	})

	t.Run("MixedMultiInstance", func(t *testing.T) {
		runMixed(t, redisServer{addr: addr, db: 15, exact: false, advance: serverAdvance, now: serverNow}, 3)
	})

	t.Run("MixedRestartLoad", func(t *testing.T) {
		if os.Getenv("MICROGATEWAY_LOAD") == "" {
			t.Skip("load check: set MICROGATEWAY_LOAD=1 to run")
		}
		srv := redisServer{addr: addr, db: 15, exact: false, advance: serverAdvance, now: serverNow}
		for range 100 {
			runMixed(t, srv, 3)
			if err := c.Do(context.Background(), "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Err(); err != nil {
				t.Fatalf("CLIENT KILL: %v", err)
			}
			if err := c.FlushDB(context.Background()).Err(); err != nil {
				t.Fatalf("FLUSHDB: %v", err)
			}
			runMixed(t, srv, 3)
		}
	})

	t.Run("Sentinel", func(t *testing.T) {
		v := os.Getenv("MICROGATEWAY_REDIS_SENTINEL")
		if v == "" {
			t.Skip("not run: set MICROGATEWAY_REDIS_SENTINEL=<master>@host:port[,host:port]")
		}
		master, rest, ok := strings.Cut(v, "@")
		if !ok || master == "" || rest == "" {
			t.Fatal("MICROGATEWAY_REDIS_SENTINEL must be <master>@host:port[,host:port]")
		}
		sentinelOpen := func(t *testing.T, prefix string) *Store {
			return newConfigStore(t, &config.RedisStore{
				Mode:       "sentinel",
				MasterName: master,
				Addresses:  strings.Split(rest, ","),
				DB:         15,
				KeyPrefix:  &prefix,
			})
		}
		t.Run("ExactWithoutRefill", func(t *testing.T) {
			checkExactWithoutRefill(t, freshStore(t, sentinelOpen), "exact")
		})
		t.Run("Prefixes", func(t *testing.T) {
			checkPrefixes(t, sentinelOpen)
		})
		t.Run("CacheConformance", func(t *testing.T) {
			storetest.Cache(t, injectedCacheHarnessOpen(sentinelOpen))
		})
		t.Run("CacheSlotCrossing", func(t *testing.T) {
			checkCacheSlotCrossing(t, sentinelOpen)
		})
	})

	t.Run("Cluster", func(t *testing.T) {
		v := os.Getenv("MICROGATEWAY_REDIS_CLUSTER")
		if v == "" {
			t.Skip("not run: set MICROGATEWAY_REDIS_CLUSTER=host:port[,...]")
		}
		addrs := strings.Split(v, ",")
		clusterOpen := func(t *testing.T, prefix string) *Store {
			return newConfigStore(t, &config.RedisStore{
				Mode:      "cluster",
				Addresses: addrs,
				KeyPrefix: &prefix,
			})
		}
		t.Run("ExactWithoutRefill", func(t *testing.T) {
			s := freshStore(t, clusterOpen)
			for i := 0; i < 50; i++ {
				checkExactWithoutRefill(t, s, fmt.Sprintf("exact-%d", i))
			}
		})
		t.Run("Conformance", func(t *testing.T) {
			storetest.RateLimiter(t, injectedHarnessOpen(clusterOpen))
		})
		t.Run("CacheConformance", func(t *testing.T) {
			storetest.Cache(t, injectedCacheHarnessOpen(clusterOpen))
		})
		t.Run("CacheSlotCrossing", func(t *testing.T) {
			checkCacheSlotCrossing(t, clusterOpen)
		})
	})
}
