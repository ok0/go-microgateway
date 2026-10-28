package memstore

import (
	"strconv"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/store"
)

func BenchmarkTake(b *testing.B) {
	limits := []struct {
		name string
		l    store.Limit
	}{
		{"token_bucket", store.Limit{Algorithm: store.TokenBucket, Count: 1000000, Period: time.Second, Burst: 1000000}},
		{"sliding_window", store.Limit{Algorithm: store.SlidingWindow, Count: 1000000, Period: 24 * time.Hour}},
	}
	keys := make([]string, 100000)
	for i := range keys {
		keys[i] = "key" + strconv.Itoa(i)
	}
	for _, lim := range limits {
		b.Run(lim.name, func(b *testing.B) {
			newBenchLimiter := func(b *testing.B) *RateLimiter {
				r := NewRateLimiter(RateLimiterOptions{MaxKeys: 200000, Clock: clock.NewFake(epoch)})
				b.Cleanup(func() { _ = r.Close() })
				return r
			}
			b.Run("one_key", func(b *testing.B) {
				r := newBenchLimiter(b)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := r.Take(bg, "one", lim.l); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("100k_keys", func(b *testing.B) {
				r := newBenchLimiter(b)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := r.Take(bg, keys[i%len(keys)], lim.l); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("parallel", func(b *testing.B) {
				r := newBenchLimiter(b)
				parallelKeys := keys[:1024]
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					i := 0
					for pb.Next() {
						if _, err := r.Take(bg, parallelKeys[i%len(parallelKeys)], lim.l); err != nil {
							b.Error(err)
							return
						}
						i++
					}
				})
			})
		})
	}
}
