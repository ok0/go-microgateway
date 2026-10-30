package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/storeguard"
	"github.com/ok0/go-microgateway/store"
)

func BenchmarkRateLimitStage(b *testing.B) {
	clk := clock.NewFake(time.Unix(1_700_000_040, 0))
	rl := memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 1000, Clock: clk})
	b.Cleanup(func() { _ = rl.Close() })
	lim := New(Policy{
		ID:    "bench",
		Limit: store.Limit{Algorithm: store.TokenBucket, Count: 1000000, Period: time.Second, Burst: 1000000},
		Store: rl,
		Clock: clk,
	})
	key, err := NewKeyFunc(SourceIP, "")
	if err != nil {
		b.Fatal(err)
	}
	h := Middleware(lim, key, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req = req.WithContext(reqctx.WithClientIP(req.Context(), netip.MustParseAddr("203.0.113.7")))

	first := httptest.NewRecorder()
	h.ServeHTTP(first, req)
	if first.Code != http.StatusOK {
		b.Fatalf("first response status = %d, want 200", first.Code)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}

func BenchmarkStoredRouteStage(b *testing.B) {
	// Real clock: the benchmark measures the production clock path; no limit decides an outcome.
	clk := clock.Real()
	rl := memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 1000, Clock: clk})
	b.Cleanup(func() { _ = rl.Close() })
	local := memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 1000, Clock: clk})
	b.Cleanup(func() { _ = local.Close() })
	g, err := storeguard.New("bench", storeguard.Settings{ConsecutiveFailures: 5, OpenTimeout: time.Second, MaxInflight: 64}, clk, nil)
	if err != nil {
		b.Fatal(err)
	}
	lim := New(Policy{
		ID:        "bench",
		Limit:     store.Limit{Algorithm: store.TokenBucket, Count: 1000000, Period: time.Second, Burst: 1000000},
		Store:     rl,
		Clock:     clk,
		Guard:     g,
		Timeout:   time.Second,
		OnError:   Local,
		Local:     local,
		StoreName: "bench",
	})
	key, err := NewKeyFunc(SourceIP, "")
	if err != nil {
		b.Fatal(err)
	}
	h := Middleware(lim, key, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	req = req.WithContext(reqctx.WithClientIP(req.Context(), netip.MustParseAddr("203.0.113.7")))

	first := httptest.NewRecorder()
	h.ServeHTTP(first, req)
	if first.Code != http.StatusOK {
		b.Fatalf("first response status = %d, want 200", first.Code)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}
