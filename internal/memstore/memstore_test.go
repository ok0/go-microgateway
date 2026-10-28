package memstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
)

var epoch = time.Unix(1_700_000_000, 0)

var bg = context.Background()

type quietClock struct {
	*clock.Fake
	idle *clock.Fake
}

func (q quietClock) NewTicker(d time.Duration) clock.Ticker {
	return q.idle.NewTicker(d)
}

func quiet(f *clock.Fake) quietClock {
	return quietClock{Fake: f, idle: clock.NewFake(epoch)}
}

func newRL(t *testing.T, o RateLimiterOptions) *RateLimiter {
	t.Helper()
	r := NewRateLimiter(o)
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func take(t *testing.T, r *RateLimiter, key string, l store.Limit) store.Decision {
	t.Helper()
	d, err := r.Take(bg, key, l)
	if err != nil {
		t.Fatalf("Take(%q): %v", key, err)
	}
	return d
}

func want(t *testing.T, step string, got, exp store.Decision) {
	t.Helper()
	if got != exp {
		t.Fatalf("%s: got %+v, want %+v", step, got, exp)
	}
}

func dec(allowed bool, limit, remaining int64, retry, reset time.Duration) store.Decision {
	return store.Decision{Allowed: allowed, Limit: limit, Remaining: remaining, RetryAfter: retry, ResetAfter: reset}
}

func TestTokenBucketScript(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 16, Clock: clk})
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	const ms = time.Millisecond

	want(t, "t+0 #1", take(t, r, "tb", tb), dec(true, 3, 2, 0, 200*ms))
	want(t, "t+0 #2", take(t, r, "tb", tb), dec(true, 3, 1, 0, 400*ms))
	want(t, "t+0 #3", take(t, r, "tb", tb), dec(true, 3, 0, 0, 600*ms))
	want(t, "t+0 #4", take(t, r, "tb", tb), dec(false, 3, 0, 200*ms, 600*ms))
	clk.Advance(199 * ms)
	want(t, "t+199ms", take(t, r, "tb", tb), dec(false, 3, 0, 1*ms, 401*ms))
	clk.Advance(1 * ms)
	want(t, "t+200ms", take(t, r, "tb", tb), dec(true, 3, 0, 0, 600*ms))
	clk.Advance(600 * ms)
	want(t, "t+800ms", take(t, r, "tb", tb), dec(true, 3, 2, 0, 200*ms))
}

func TestSlidingWindowScript(t *testing.T) {
	if epoch.UnixNano()%int64(8*time.Second) != 0 {
		t.Fatal("epoch is not aligned to an 8s window start")
	}
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 16, Clock: clk})
	sw := store.Limit{Algorithm: store.SlidingWindow, Count: 4, Period: 8 * time.Second}
	const ms = time.Millisecond

	want(t, "elapsed0 #1", take(t, r, "sw", sw), dec(true, 4, 3, 0, 16*time.Second))
	want(t, "elapsed0 #2", take(t, r, "sw", sw), dec(true, 4, 2, 0, 16*time.Second))
	want(t, "elapsed0 #3", take(t, r, "sw", sw), dec(true, 4, 1, 0, 16*time.Second))
	want(t, "elapsed0 #4", take(t, r, "sw", sw), dec(true, 4, 0, 0, 16*time.Second))
	want(t, "exhausted", take(t, r, "sw", sw), dec(false, 4, 0, 10*time.Second, 16*time.Second))
	clk.Advance(9999 * ms)
	want(t, "t+9999ms", take(t, r, "sw", sw), dec(false, 4, 0, 1*ms, 6001*ms))
	clk.Advance(1 * ms)
	want(t, "t+10000ms", take(t, r, "sw", sw), dec(true, 4, 0, 0, 14*time.Second))
	clk.Advance(4 * time.Second)
	want(t, "t+14000ms", take(t, r, "sw", sw), dec(true, 4, 1, 0, 10*time.Second))
	clk.Advance(18 * time.Second)
	want(t, "t+32000ms", take(t, r, "sw", sw), dec(true, 4, 3, 0, 16*time.Second))
}

func TestInvalidLimit(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 4, Clock: clk})
	tb := store.TokenBucket
	sw := store.SlidingWindow
	bad := []store.Limit{
		{Algorithm: 0, Count: 1, Period: time.Second, Burst: 1},
		{Algorithm: 3, Count: 1, Period: time.Second, Burst: 1},
		{Algorithm: tb, Count: 0, Period: time.Second, Burst: 1},
		{Algorithm: tb, Count: 1, Period: 0, Burst: 1},
		{Algorithm: tb, Count: 1, Period: time.Second, Burst: 0},
		{Algorithm: tb, Count: 1001, Period: time.Millisecond, Burst: 1},
		{Algorithm: tb, Count: 1, Period: time.Hour, Burst: 25},
		{Algorithm: tb, Count: 1, Period: time.Duration(math.MaxInt64), Burst: math.MaxInt64},
		{Algorithm: sw, Count: 1001, Period: time.Millisecond},
		{Algorithm: sw, Count: 1, Period: 24*time.Hour + time.Nanosecond},
	}
	for i, l := range bad {
		d, err := r.Take(bg, "k", l)
		if !errors.Is(err, store.ErrInvalidLimit) || errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("case %d: err = %v, want ErrInvalidLimit only", i, err)
		}
		if d != (store.Decision{}) {
			t.Fatalf("case %d: decision = %+v, want zero", i, d)
		}
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d after invalid takes, want 0", r.Len())
	}
}

func TestCanceledContext(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 4, Clock: clk})
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	ctx, cancel := context.WithCancel(bg)
	cancel()
	_, err := r.Take(ctx, "k", tb)
	if !errors.Is(err, context.Canceled) || errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("err = %v, want context.Canceled only", err)
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d after canceled take, want 0", r.Len())
	}
	want(t, "fresh after cancel", take(t, r, "k", tb), dec(true, 3, 2, 0, 200*time.Millisecond))
}

func TestTakeAfterClose(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := NewRateLimiter(RateLimiterOptions{MaxKeys: 4, Clock: clk})
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	if err := r.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	_, err := r.Take(bg, "k", tb)
	if !errors.Is(err, store.ErrUnavailable) || errors.Is(err, store.ErrInvalidLimit) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	_, err = r.Take(bg, "k", store.Limit{})
	if !errors.Is(err, store.ErrInvalidLimit) {
		t.Fatalf("invalid limit after close: err = %v, want ErrInvalidLimit", err)
	}
}

func TestCloseIdempotent(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := NewRateLimiter(RateLimiterOptions{MaxKeys: 4, Clock: clk})
	if err := r.Close(); err != nil {
		t.Fatalf("first Close() = %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
}

func TestAlgorithmSwitchSameKey(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 4, Clock: clk})
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 1}
	sw := store.Limit{Algorithm: store.SlidingWindow, Count: 2, Period: time.Hour}
	if d := take(t, r, "k", tb); !d.Allowed {
		t.Fatalf("token bucket first take denied: %+v", d)
	}
	d := take(t, r, "k", sw)
	if !d.Allowed || d.Remaining != 1 {
		t.Fatalf("sliding window after algorithm switch: %+v, want allowed with Remaining 1", d)
	}
}

func TestShardCaps(t *testing.T) {
	clk := clock.NewFake(epoch)
	cases := []struct {
		maxKeys, shards, wantShards int
	}{
		{1, 0, 1},
		{5, 0, 5},
		{64, 0, 64},
		{100, 0, 64},
		{100000, 0, 64},
		{10, 3, 3},
		{3, 64, 3},
	}
	for _, c := range cases {
		r := newRL(t, RateLimiterOptions{MaxKeys: c.maxKeys, Shards: c.shards, Clock: clk})
		if len(r.shards) != c.wantShards {
			t.Fatalf("MaxKeys %d Shards %d: got %d shards, want %d", c.maxKeys, c.shards, len(r.shards), c.wantShards)
		}
		sum := 0
		for i := range r.shards {
			sum += r.shards[i].cap
		}
		if sum != c.maxKeys {
			t.Fatalf("MaxKeys %d Shards %d: caps sum to %d", c.maxKeys, c.shards, sum)
		}
	}
}

func TestNewRateLimiterPanicsWithoutKeys(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewRateLimiter(MaxKeys: 0) did not panic")
		}
	}()
	NewRateLimiter(RateLimiterOptions{MaxKeys: 0})
}

func TestDefaultClockAndShards(t *testing.T) {
	r := newRL(t, RateLimiterOptions{MaxKeys: 1000})
	if len(r.shards) != DefaultShards {
		t.Fatalf("got %d shards, want %d", len(r.shards), DefaultShards)
	}
	tb := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	if d := take(t, r, "real", tb); !d.Allowed {
		t.Fatalf("take with real clock denied: %+v", d)
	}
}

func TestCapMillionKeys(t *testing.T) {
	flood(t, 10000, DefaultShards, true)
	flood(t, 5, 0, false)
}

func flood(t *testing.T, maxKeys, shards int, checkHot bool) {
	t.Helper()
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: maxKeys, Shards: shards, Clock: clk})
	l := store.Limit{Algorithm: store.TokenBucket, Count: 1000, Period: 1000 * time.Second, Burst: 1000}
	take(t, r, "cold", l)
	const inserts = 1_000_000
	hotTakes := 0
	for i := 0; i < inserts; i++ {
		take(t, r, "k"+strconv.Itoa(i), l)
		if checkHot && i%100 == 99 {
			hotTakes++
			j := hotTakes
			var exp store.Decision
			if j <= 1000 {
				exp = store.Decision{Allowed: true, Limit: 1000, Remaining: int64(1000 - j), ResetAfter: time.Duration(j) * time.Second}
			} else {
				exp = store.Decision{Allowed: false, Limit: 1000, Remaining: 0, RetryAfter: time.Second, ResetAfter: 1000 * time.Second}
			}
			if got := take(t, r, "hot", l); got != exp {
				t.Fatalf("hot take %d at insert %d: got %+v, want %+v", j, i, got, exp)
			}
		}
		if i%1000 == 999 {
			if n := r.Len(); n > maxKeys {
				t.Fatalf("after %d inserts Len() = %d, cap %d", i+1, n, maxKeys)
			}
		}
	}
	if n := r.Len(); n != maxKeys {
		t.Fatalf("final Len() = %d, want %d", n, maxKeys)
	}
	if !checkHot {
		return
	}
	if hotTakes != 10000 {
		t.Fatalf("hot takes = %d, want 10000", hotTakes)
	}
	if d := take(t, r, "cold", l); d.Remaining != 999 {
		t.Fatalf("evicted cold key Remaining = %d, want 999 (fresh state)", d.Remaining)
	}
}

func TestCapEveryTake(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 3, Clock: clk})
	l := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 2}
	rnd := clock.NewRand(1)
	keys := make([]string, 50)
	for i := range keys {
		keys[i] = "key" + strconv.Itoa(i)
	}
	for i := 0; i < 10000; i++ {
		take(t, r, keys[rnd.Int64N(int64(len(keys)))], l)
		if n := r.Len(); n > 3 {
			t.Fatalf("take %d: Len() = %d, cap 3", i, n)
		}
		clk.Advance(time.Duration(rnd.Int64N(int64(300 * time.Millisecond))))
	}
}

func TestRecencyOnDeny(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 2, Shards: 1, Clock: clk})
	l := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Second, Burst: 1}
	if !take(t, r, "A", l).Allowed {
		t.Fatal("A first take denied")
	}
	if !take(t, r, "B", l).Allowed {
		t.Fatal("B first take denied")
	}
	if take(t, r, "A", l).Allowed {
		t.Fatal("A second take allowed")
	}
	if !take(t, r, "C", l).Allowed {
		t.Fatal("C take denied")
	}
	if take(t, r, "A", l).Allowed {
		t.Fatal("A evicted: denial did not refresh recency")
	}
	if d := take(t, r, "B", l); !d.Allowed || d.Remaining != 0 {
		t.Fatalf("B after eviction = %+v, want fresh allowed with Remaining 0", d)
	}
}

func TestSweepExact(t *testing.T) {
	const ms = time.Millisecond
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 100, Clock: clk})
	k200 := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 1}
	k400 := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: 2 * time.Second, Burst: 1}
	k600 := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: 3 * time.Second, Burst: 1}
	take(t, r, "k200", k200)
	take(t, r, "k400", k400)
	take(t, r, "k600", k600)
	if r.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", r.Len())
	}
	clk.Advance(400 * ms)
	if n := r.Sweep(clk.Now()); n != 2 {
		t.Fatalf("Sweep at +400ms removed %d, want 2", n)
	}
	if r.Len() != 1 {
		t.Fatalf("Len() = %d after sweep, want 1", r.Len())
	}
	want(t, "retained state", take(t, r, "k600", k600), dec(false, 1, 0, 200*ms, 200*ms))

	swClk := clock.NewFake(epoch)
	swr := newRL(t, RateLimiterOptions{MaxKeys: 100, Clock: swClk})
	sw := store.Limit{Algorithm: store.SlidingWindow, Count: 10, Period: 8 * time.Second}
	take(t, swr, "sw", sw)
	swClk.Advance(16*time.Second - time.Nanosecond)
	if n := swr.Sweep(swClk.Now()); n != 0 {
		t.Fatalf("Sweep at (idx+2)*P-1ns removed %d, want 0", n)
	}
	swClk.Advance(time.Nanosecond)
	if n := swr.Sweep(swClk.Now()); n != 1 {
		t.Fatalf("Sweep at (idx+2)*P removed %d, want 1", n)
	}
	if swr.Len() != 0 {
		t.Fatalf("Len() = %d, want 0", swr.Len())
	}
}

func TestSweepLossless(t *testing.T) {
	limits := []store.Limit{
		{Algorithm: store.TokenBucket, Count: 3, Period: time.Second, Burst: 2},
		{Algorithm: store.SlidingWindow, Count: 5, Period: 2 * time.Second},
		{Algorithm: store.TokenBucket, Count: 10, Period: 500 * time.Millisecond, Burst: 4},
		{Algorithm: store.SlidingWindow, Count: 3, Period: 1500 * time.Millisecond},
	}
	for seed := uint64(1); seed <= 5; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			clk := clock.NewFake(epoch)
			x := newRL(t, RateLimiterOptions{MaxKeys: 10000, Clock: quiet(clk)})
			y := newRL(t, RateLimiterOptions{MaxKeys: 10000, Clock: quiet(clk)})
			rnd := clock.NewRand(seed)
			for i := 0; i < 5000; i++ {
				ki := rnd.Int64N(20)
				key := "key" + strconv.FormatInt(ki, 10)
				l := limits[int(ki)%len(limits)]
				if rnd.Float64() < 0.2 {
					x.Sweep(clk.Now())
				}
				dx, errx := x.Take(bg, key, l)
				dy, erry := y.Take(bg, key, l)
				if errx != nil || erry != nil {
					t.Fatalf("op %d: errors %v, %v", i, errx, erry)
				}
				if dx != dy {
					t.Fatalf("op %d key %s: swept %+v, unswept %+v", i, key, dx, dy)
				}
				if lx, ly := x.Len(), y.Len(); lx > ly {
					t.Fatalf("op %d: Len swept %d > unswept %d", i, lx, ly)
				}
				clk.Advance(time.Duration(rnd.Int64N(int64(1500*time.Millisecond) + 1)))
			}
		})
	}
}

func TestJanitor(t *testing.T) {
	clk := clock.NewFake(epoch)
	ch := make(chan int)
	r := NewRateLimiter(RateLimiterOptions{MaxKeys: 100, Clock: clk, OnSweep: func(n int) { ch <- n }})
	l := store.Limit{Algorithm: store.TokenBucket, Count: 3, Period: time.Second, Burst: 3}
	for _, k := range []string{"a", "b", "c"} {
		take(t, r, k, l)
	}
	clk.BlockUntil(1)
	clk.Advance(JanitorInterval)
	if n := <-ch; n != 3 {
		t.Fatalf("janitor removed %d, want 3", n)
	}
	if r.Len() != 0 {
		t.Fatalf("Len() = %d after janitor sweep, want 0", r.Len())
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close() = %v", err)
	}
	if w := clk.Waiters(); w != 0 {
		t.Fatalf("Waiters() = %d after Close, want 0", w)
	}
	clk.Advance(JanitorInterval)
	select {
	case n := <-ch:
		t.Fatalf("sweep %d after Close", n)
	default:
	}
	for i := 0; i < 10000 && janitorRunning(); i++ {
		runtime.Gosched()
	}
	if janitorRunning() {
		t.Fatal("janitor goroutine still running after Close")
	}
}

func janitorRunning() bool {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Contains(string(buf[:n]), "memstore.(*RateLimiter).janitor")
}

func TestNoKeyRetention(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 4, Clock: clk})
	big := strings.Repeat("x", 1<<16)
	key := big[:8]
	take(t, r, key, store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3})
	found := 0
	for i := range r.shards {
		for k := range r.shards[i].m {
			found++
			if unsafe.StringData(k) == unsafe.StringData(key) {
				t.Fatal("stored key shares memory with the caller's key")
			}
		}
	}
	if found != 1 {
		t.Fatalf("found %d stored keys, want 1", found)
	}
}

func TestZeroAllocsExistingKey(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 16, Clock: clk})
	cases := []struct {
		name string
		l    store.Limit
	}{
		{"token_bucket_allowed", store.Limit{Algorithm: store.TokenBucket, Count: 1_000_000, Period: time.Second, Burst: 1_000_000}},
		{"token_bucket_denied", store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 1}},
		{"sliding_window_allowed", store.Limit{Algorithm: store.SlidingWindow, Count: 1_000_000, Period: 24 * time.Hour}},
		{"sliding_window_denied", store.Limit{Algorithm: store.SlidingWindow, Count: 1, Period: time.Hour}},
	}
	for _, c := range cases {
		take(t, r, c.name, c.l)
		allocs := testing.AllocsPerRun(1000, func() {
			_, _ = r.Take(bg, c.name, c.l)
		})
		if allocs != 0 {
			t.Fatalf("%s: %v allocs per take, want 0", c.name, allocs)
		}
	}
}

func TestTwoInstancesIndependent(t *testing.T) {
	clk := clock.NewFake(epoch)
	l := store.Limit{Algorithm: store.TokenBucket, Count: 3, Period: time.Hour, Burst: 3}
	for i := 0; i < 2; i++ {
		r := newRL(t, RateLimiterOptions{MaxKeys: 4, Clock: clk})
		allowed := 0
		for j := 0; j < 5; j++ {
			if take(t, r, "same", l).Allowed {
				allowed++
			}
		}
		if allowed != 3 {
			t.Fatalf("instance %d admitted %d, want 3", i, allowed)
		}
	}
}

func TestConcurrentSameKey(t *testing.T) {
	clk := clock.NewFake(epoch)
	r := newRL(t, RateLimiterOptions{MaxKeys: 16, Clock: clk})
	l := store.Limit{Algorithm: store.TokenBucket, Count: 50, Period: time.Hour, Burst: 50}
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				d, err := r.Take(bg, "same", l)
				if err != nil {
					t.Errorf("Take: %v", err)
					return
				}
				if d.Allowed {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if n := allowed.Load(); n != 50 {
		t.Fatalf("allowed %d takes, want 50", n)
	}
}

func TestFloorDiv(t *testing.T) {
	cases := []struct{ a, b, want int64 }{
		{7, 2, 3},
		{-7, 2, -4},
		{-8, 2, -4},
		{0, 5, 0},
		{-1, 8, -1},
	}
	for _, c := range cases {
		if got := floorDiv(c.a, c.b); got != c.want {
			t.Fatalf("floorDiv(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}
