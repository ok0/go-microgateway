//go:build integration

package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
	"github.com/ok0/go-microgateway/store/storetest"
)

type storeFactory func(t *testing.T, prefix string) *Store

func newConfigStore(t *testing.T, cfg *config.RedisStore) *Store {
	t.Helper()
	s, err := NewFromConfig(cfg, nil)
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

func realExpiryMs(t *testing.T, c goredis.UniversalClient, key string) int64 {
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
				t.Fatalf("PTTL %q = %s, want a live expiry", key, pttl.Val())
			}
			return t0.Val().UnixMilli() + pttl.Val().Milliseconds()
		}
	}
	t.Fatal("no PTTL bracketed by TIME readings within one millisecond in 100 attempts")
	return 0
}

func durCeilMs(d time.Duration) int64 {
	return (int64(d) + int64(time.Millisecond) - 1) / int64(time.Millisecond)
}

func checkExactWithoutRefill(t *testing.T, s *Store, key string) {
	t.Helper()
	ctx := context.Background()
	l := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 3}
	start := time.Now()
	for i := 1; i <= 3; i++ {
		d, err := s.Take(ctx, key, l)
		if err != nil {
			t.Fatalf("take %d of %q: %v", i, key, err)
		}
		if !d.Allowed || d.Remaining != int64(3-i) {
			t.Fatalf("take %d of %q: %+v, want allowed with Remaining %d", i, key, d, 3-i)
		}
		want := time.Duration(i) * time.Hour
		delta := time.Since(start)
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

func hammer(t *testing.T, stores []*Store, key string, l store.Limit, window time.Duration) (int64, int64, time.Duration) {
	t.Helper()
	ctx := context.Background()
	const perStore = 8
	counts := make([]int64, len(stores)*perStore)
	totals := make([]int64, len(stores)*perStore)
	errs := make([]error, len(stores)*perStore)
	start := time.Now()
	deadline := start.Add(window)
	var wg sync.WaitGroup
	for g := range counts {
		s := stores[g%len(stores)]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				d, err := s.Take(ctx, key, l)
				if err != nil {
					errs[g] = err
					return
				}
				totals[g]++
				if d.Allowed {
					counts[g]++
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	var admitted, total int64
	for g := range counts {
		if errs[g] != nil {
			t.Fatalf("take %q: %v", key, errs[g])
		}
		admitted += counts[g]
		total += totals[g]
	}
	return admitted, total, elapsed
}

func checkBoundUnderConcurrency(t *testing.T, open storeFactory) {
	t.Helper()
	prefix := uniquePrefix(t)
	stores := []*Store{open(t, prefix), open(t, prefix)}
	cleanPrefix(t, stores[0])

	tb := store.Limit{Algorithm: store.TokenBucket, Count: 20, Period: time.Second, Burst: 5}
	admitted, total, elapsed := hammer(t, stores, "bound-tb", tb, 600*time.Millisecond)
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
	admitted, total, elapsed = hammer(t, stores, "bound-sw", sw, 600*time.Millisecond)
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

func checkFloodExpires(t *testing.T, s *Store) {
	t.Helper()
	ctx := context.Background()
	const n = 10000
	const period = 10 * time.Second
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
		e := realExpiryMs(t, s.c, s.keyFor(fmt.Sprintf("flood:%d", i), store.TokenBucket))
		if e < t0+periodMs || e > t1+periodMs+1 {
			t.Errorf("key flood:%d expires at %d ms, want within [%d, %d]", i, e, t0+periodMs, t1+periodMs+1)
		}
	}
	t2 := serverMs(t, s.c)
	if t2 >= t0+periodMs {
		t.Fatalf("flood and checks took %d ms of server time, not within the %s period: expiry check inconclusive", t2-t0, period)
	}
	if size != n {
		t.Fatalf("DBSIZE = %d right after the flood, want %d", size, n)
	}

	maxExp := t1 + periodMs + 1
	giveUp := time.Now().Add(period + 5*time.Second)
	for {
		if serverMs(t, s.c) > maxExp {
			break
		}
		if time.Now().After(giveUp) {
			t.Fatalf("server time did not pass the flood expiries within %s", period+5*time.Second)
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, i := range samples {
		if s.c.Exists(ctx, s.keyFor(fmt.Sprintf("flood:%d", i), store.TokenBucket)).Val() != 0 {
			t.Errorf("key flood:%d still exists after server time passed its expiry", i)
		}
	}

	deadline := time.Now().Add(15 * time.Second)
	for {
		left, err := s.c.DBSize(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		if left == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("DBSIZE = %d 15 s after every flood key expired, want 0", left)
		}
		time.Sleep(100 * time.Millisecond)
	}
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
		runFidelityProbes(t, fidelityEnv{c: c})
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

	t.Run("MultiInstance", func(t *testing.T) {
		runMultiInstance(t, redisServer{addr: addr, db: 15, exact: false, advance: time.Sleep})
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
	})
}
