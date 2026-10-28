package memstore

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/store"
	"github.com/ok0/lychgate/store/storetest"
)

const brokenEnv = "LYCHGATE_STORETEST_BROKEN"

type handle struct {
	rl     *RateLimiter
	closed atomic.Bool
}

func (h *handle) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	if h.closed.Load() {
		return store.Decision{}, fmt.Errorf("memstore handle closed: %w", store.ErrUnavailable)
	}
	return h.rl.Take(ctx, key, l)
}

func (h *handle) Close() error {
	h.closed.Store(true)
	return nil
}

type pairBuilder func(clk *clock.Fake, cleanup func(func())) (store.RateLimiter, store.RateLimiter)

func newHarness(build pairBuilder) storetest.Harness[store.RateLimiter] {
	var clk *clock.Fake
	return storetest.Harness[store.RateLimiter]{
		New: func(t *testing.T) (store.RateLimiter, store.RateLimiter) {
			clk = clock.NewFake(time.Unix(1_700_000_000, 0))
			return build(clk, t.Cleanup)
		},
		Advance: func(t *testing.T, d time.Duration) {
			clk.Advance(d)
		},
	}
}

func sharedPair(clk *clock.Fake, cleanup func(func())) (store.RateLimiter, store.RateLimiter) {
	rl := NewRateLimiter(RateLimiterOptions{MaxKeys: 100000, Clock: clk})
	cleanup(func() { _ = rl.Close() })
	return &handle{rl: rl}, &handle{rl: rl}
}

func perHandlePair(clk *clock.Fake, cleanup func(func())) (store.RateLimiter, store.RateLimiter) {
	x := NewRateLimiter(RateLimiterOptions{MaxKeys: 100000, Clock: clk})
	y := NewRateLimiter(RateLimiterOptions{MaxKeys: 100000, Clock: clk})
	cleanup(func() { _ = x.Close() })
	cleanup(func() { _ = y.Close() })
	return &handle{rl: x}, &handle{rl: y}
}

func sharedHarness(t *testing.T) storetest.Harness[store.RateLimiter] {
	t.Helper()
	return newHarness(sharedPair)
}

func wrapShared(wrap func(store.RateLimiter) store.RateLimiter) pairBuilder {
	return func(clk *clock.Fake, cleanup func(func())) (store.RateLimiter, store.RateLimiter) {
		a, b := sharedPair(clk, cleanup)
		return wrap(a), wrap(b)
	}
}

type noConsume struct {
	store.RateLimiter
	n *atomic.Int64
}

func (w noConsume) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	return w.RateLimiter.Take(ctx, key+"#"+strconv.FormatInt(w.n.Add(1), 10), l)
}

type ignoreCancel struct {
	store.RateLimiter
}

func (w ignoreCancel) Take(_ context.Context, key string, l store.Limit) (store.Decision, error) {
	return w.RateLimiter.Take(context.Background(), key, l)
}

type invalidAsUnavailable struct {
	store.RateLimiter
}

func (w invalidAsUnavailable) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	d, err := w.RateLimiter.Take(ctx, key, l)
	if err != nil {
		return d, fmt.Errorf("%w: %v", store.ErrUnavailable, err)
	}
	return d, nil
}

type retryTruncated struct {
	store.RateLimiter
}

func (w retryTruncated) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	d, err := w.RateLimiter.Take(ctx, key, l)
	d.RetryAfter = d.RetryAfter.Truncate(time.Second)
	return d, err
}

var brokenBackends = map[string]pairBuilder{
	"noConsume": func(clk *clock.Fake, cleanup func(func())) (store.RateLimiter, store.RateLimiter) {
		a, b := sharedPair(clk, cleanup)
		n := new(atomic.Int64)
		return noConsume{RateLimiter: a, n: n}, noConsume{RateLimiter: b, n: n}
	},
	"perHandle": perHandlePair,
	"ignoreCancel": wrapShared(func(inner store.RateLimiter) store.RateLimiter {
		return ignoreCancel{RateLimiter: inner}
	}),
	"invalidAsUnavailable": wrapShared(func(inner store.RateLimiter) store.RateLimiter {
		return invalidAsUnavailable{RateLimiter: inner}
	}),
	"retryTruncated": wrapShared(func(inner store.RateLimiter) store.RateLimiter {
		return retryTruncated{RateLimiter: inner}
	}),
}

func TestConformance(t *testing.T) {
	storetest.RateLimiter(t, sharedHarness(t))
}

func TestBrokenBackend(t *testing.T) {
	name := os.Getenv(brokenEnv)
	if name == "" {
		t.Skipf("set %s to run a broken backend", brokenEnv)
	}
	build, ok := brokenBackends[name]
	if !ok {
		t.Fatalf("unknown broken backend %q", name)
	}
	storetest.RateLimiter(t, newHarness(build))
}

func TestSuiteRejectsBrokenBackends(t *testing.T) {
	cases := []struct {
		name     string
		expected string
	}{
		{"noConsume", "TokenBucketScriptedDecisions"},
		{"perHandle", "ConsistentAcrossHandles"},
		{"ignoreCancel", "CanceledContext"},
		{"invalidAsUnavailable", "InvalidLimits"},
		{"retryTruncated", "TokenBucketScriptedDecisions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestBrokenBackend$", "-test.v")
			cmd.Env = append(os.Environ(), brokenEnv+"="+tc.name)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("broken backend %s: subprocess passed, want failure\n%s", tc.name, out)
			}
			if want := "--- FAIL: TestBrokenBackend/" + tc.expected; !strings.Contains(string(out), want) {
				t.Errorf("broken backend %s: output lacks %q\n%s", tc.name, want, out)
			}
		})
	}
}
