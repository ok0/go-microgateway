package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
)

func BenchmarkTake(b *testing.B) {
	m := miniredis.RunT(b)
	m.SetTime(miniStart)
	prefix := "mgtest"
	s, err := newFromConfig(&config.RedisStore{
		Mode:      "standalone",
		Addresses: []string{m.Addr()},
		KeyPrefix: &prefix,
	}, nil, clock.NewFake(miniStart))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	ctx := context.Background()

	algorithms := []struct {
		name string
		l    store.Limit
	}{
		{"token_bucket", store.Limit{Algorithm: store.TokenBucket, Count: 1000000, Period: time.Second, Burst: 1000000}},
		{"sliding_window", store.Limit{Algorithm: store.SlidingWindow, Count: 1000000, Period: time.Second}},
	}
	for _, a := range algorithms {
		b.Run(a.name, func(b *testing.B) {
			b.Run("single_key", func(b *testing.B) {
				b.ReportAllocs()
				key := "bench:" + a.name + ":single"
				for i := 0; i < b.N; i++ {
					if _, err := s.Take(ctx, key, a.l); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("100k_keys", func(b *testing.B) {
				keys := make([]string, 100000)
				for i := range keys {
					keys[i] = "bench:" + a.name + ":k" + strconv.Itoa(i)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := s.Take(ctx, keys[i%len(keys)], a.l); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("parallel", func(b *testing.B) {
				var gid atomic.Int64
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					id := gid.Add(1)
					keys := make([]string, 1000)
					for j := range keys {
						keys[j] = fmt.Sprintf("bench:%s:p%d:%d", a.name, id, j)
					}
					i := 0
					for pb.Next() {
						if _, err := s.Take(ctx, keys[i%len(keys)], a.l); err != nil {
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
