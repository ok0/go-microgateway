package redis

import (
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
)

var diffLimits = []store.Limit{
	{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3},
	{Algorithm: store.TokenBucket, Count: 7, Period: time.Second, Burst: 4},
	{Algorithm: store.TokenBucket, Count: 3, Period: time.Second, Burst: 1},
	{Algorithm: store.TokenBucket, Count: 1, Period: 250 * time.Millisecond, Burst: 4},
	{Algorithm: store.TokenBucket, Count: 1000000, Period: time.Second, Burst: 1000},
	{Algorithm: store.TokenBucket, Count: 1, Period: 24 * time.Hour, Burst: 1},
	{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 24},
	{Algorithm: store.TokenBucket, Count: 3, Period: 10 * time.Second, Burst: 7},
	{Algorithm: store.SlidingWindow, Count: 5, Period: 2 * time.Second},
	{Algorithm: store.SlidingWindow, Count: 3, Period: 333 * time.Millisecond},
	{Algorithm: store.SlidingWindow, Count: 2, Period: 333 * time.Millisecond},
	{Algorithm: store.SlidingWindow, Count: 10, Period: 7 * time.Second},
	{Algorithm: store.SlidingWindow, Count: 1, Period: time.Millisecond},
	{Algorithm: store.SlidingWindow, Count: 100, Period: 24 * time.Hour},
	{Algorithm: store.SlidingWindow, Count: 1000, Period: time.Second},
}

func diffAdvance(rng clock.Rand, i int, p time.Duration) time.Duration {
	switch r := rng.Int64N(100); {
	case r < 40:
		return time.Duration(rng.Int64N(1000)) * time.Microsecond
	case r < 65:
		return time.Duration(rng.Int64N(2_000_000)) * time.Microsecond
	case r < 75:
		return p
	case r < 85:
		if i%2 == 0 {
			return 2*p - time.Microsecond
		}
		return 2 * p
	case r < 90:
		return 0
	case r < 95:
		return time.Duration(rng.Int64N(25*3600))*time.Second + time.Duration(rng.Int64N(1_000_000))*time.Microsecond
	default:
		return p - time.Microsecond
	}
}

func diffSteps(seed uint64, ops int) []step {
	rng := clock.NewRand(seed)
	steps := make([]step, ops)
	for i := range steps {
		k := rng.Int64N(16)
		l := diffLimits[int(k)%len(diffLimits)]
		steps[i] = step{
			advance: diffAdvance(rng, i, l.Period),
			key:     fmt.Sprintf("diff:%d", k),
			limit:   l,
		}
	}
	return steps
}

func runDifferentialSeeds(t *testing.T, newBackend func(t *testing.T) (timedStore, time.Time), seeds []uint64, ops int) {
	t.Helper()
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			got, start := newBackend(t)
			ref := memBackend(t, start)
			compareSteps(t, fmt.Sprintf("seed %d", seed), diffSteps(seed, ops), got, ref)
		})
	}
}

func runDifferential(t *testing.T, newBackend func(t *testing.T) (timedStore, time.Time)) {
	t.Helper()
	runDifferentialSeeds(t, newBackend, []uint64{1, 2, 3, 4, 5, 6}, 2000)
}

func TestDifferential(t *testing.T) {
	runDifferential(t, miniBackend)
}

func TestDifferentialInjected(t *testing.T) {
	runDifferential(t, func(t *testing.T) (timedStore, time.Time) {
		m := newMini(t)
		return injectedBackend(t, m.Addr(), 0)
	})
}

func TestDifferentialFarFuture(t *testing.T) {
	runDifferentialSeeds(t, func(t *testing.T) (timedStore, time.Time) {
		start := time.Date(2200, 1, 1, 0, 0, 0, 123456000, time.UTC)
		m := miniredis.RunT(t)
		m.SetTime(start)
		mc := &miniClock{m: m, now: start}
		return timedStore{rl: newTestStore(t, m.Addr(), 0, "mgtest"), advance: mc.Advance}, start
	}, []uint64{1}, 500)
}
