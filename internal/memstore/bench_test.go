package memstore

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
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

func benchCache(b *testing.B, maxBytes int64) *Cache {
	b.Helper()
	c := NewCache(CacheOptions{MaxBytes: maxBytes, Clock: quiet(clock.NewFake(epoch))})
	b.Cleanup(func() { _ = c.Close() })
	return c
}

func benchPut(b *testing.B, c *Cache, primary string, e *store.Entry, tags ...string) {
	b.Helper()
	l, err := c.Get(bg, primary, func([]string) (string, bool) { return "s", true })
	if err != nil {
		b.Fatal(err)
	}
	if _, err := c.Put(bg, l.Token, primary, "s", nil, e, time.Hour); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkCacheGet(b *testing.B) {
	sel := func([]string) (string, bool) { return "s", true }
	keys := make([]string, 100000)
	for i := range keys {
		keys[i] = "key" + strconv.Itoa(i)
	}
	b.Run("hit-single", func(b *testing.B) {
		c := benchCache(b, 1<<20)
		benchPut(b, c, "k", ment("body"))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := c.Get(bg, "k", sel); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("hit-100k", func(b *testing.B) {
		c := benchCache(b, 1<<30)
		for _, k := range keys {
			benchPut(b, c, k, ment("body"))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := c.Get(bg, keys[i%len(keys)], sel); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("hit-parallel", func(b *testing.B) {
		c := benchCache(b, 1<<30)
		for _, k := range keys {
			benchPut(b, c, k, ment("body"))
		}
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if _, err := c.Get(bg, keys[i%len(keys)], sel); err != nil {
					b.Error(err)
					return
				}
				i++
			}
		})
	})
	b.Run("miss", func(b *testing.B) {
		c := benchCache(b, 1<<20)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := c.Get(bg, "absent", sel); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCachePut(b *testing.B) {
	body512 := make([]byte, 512)
	b.Run("no-eviction", func(b *testing.B) {
		c := benchCache(b, 1<<30)
		e := &store.Entry{Status: 200, Body: body512}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchPut(b, c, "k"+strconv.Itoa(i), e)
		}
	})
	b.Run("eviction-pressure", func(b *testing.B) {
		c := benchCache(b, 1<<20)
		e := &store.Entry{Status: 200, Body: body512}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			benchPut(b, c, "k"+strconv.Itoa(i), e)
		}
	})
	b.Run("parallel", func(b *testing.B) {
		c := benchCache(b, 1<<30)
		e := &store.Entry{Status: 200, Body: body512}
		var seq atomic.Uint64
		b.ReportAllocs()
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				benchPut(b, c, "p"+strconv.FormatUint(seq.Add(1), 10), e)
			}
		})
	})
}

func BenchmarkCachePurge(b *testing.B) {
	const population = 10000
	const tagCount = 100
	fillPopulation := func(b *testing.B, c *Cache) {
		b.Helper()
		for i := 0; i < population; i++ {
			benchPut(b, c, "k"+strconv.Itoa(i), ment("body", "t"+strconv.Itoa(i%tagCount)))
		}
	}
	b.Run("keys", func(b *testing.B) {
		c := benchCache(b, 1<<30)
		fillPopulation(b, c)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			j := i % population
			key := "k" + strconv.Itoa(j)
			if _, err := c.Purge(bg, store.Purge{Kind: store.PurgeKeys, Keys: []string{key}, Retain: time.Hour}); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			benchPut(b, c, key, ment("body", "t"+strconv.Itoa(j%tagCount)))
			b.StartTimer()
		}
	})
	b.Run("tags", func(b *testing.B) {
		c := benchCache(b, 1<<30)
		fillPopulation(b, c)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			t := i % tagCount
			tag := "t" + strconv.Itoa(t)
			if _, err := c.Purge(bg, store.Purge{Kind: store.PurgeTags, Tags: []string{tag}, Retain: time.Hour}); err != nil {
				b.Fatal(err)
			}
			b.StopTimer()
			for j := t; j < population; j += tagCount {
				benchPut(b, c, "k"+strconv.Itoa(j), ment("body", tag))
			}
			b.StartTimer()
		}
	})
	b.Run("all", func(b *testing.B) {
		c := benchCache(b, 1<<30)
		fillPopulation(b, c)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := c.Purge(bg, store.Purge{Kind: store.PurgeAll, Soft: true, Retain: time.Hour}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
