package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
)

func bcStore(b *testing.B) *Store {
	b.Helper()
	m := miniredis.RunT(b)
	m.SetTime(miniStart)
	prefix := "mgbench"
	s, err := newFromConfig(&config.RedisStore{
		Mode:      "standalone",
		Addresses: []string{m.Addr()},
		KeyPrefix: &prefix,
	}, nil, clock.NewFake(miniStart))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

func bcSel(sec string) func([]string) (string, bool) {
	return func([]string) (string, bool) { return sec, true }
}

func bcFill(b *testing.B, s *Store, primary, sec string, names []string, ttl time.Duration, tags ...string) {
	b.Helper()
	ctx := context.Background()
	l, err := s.Get(ctx, primary, bcSel(sec))
	if err != nil {
		b.Fatal(err)
	}
	stored, err := s.Put(ctx, l.Token, primary, sec, names, ckEntry(sec, tags...), ttl)
	if err != nil {
		b.Fatal(err)
	}
	if !stored {
		b.Fatalf("fill %s/%s not stored", primary, sec)
	}
}

func BenchmarkCacheGetHit(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	bcFill(b, s, "hit", "s", nil, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l, err := s.Get(ctx, "hit", bcSel("s"))
		if err != nil {
			b.Fatal(err)
		}
		if l.Entry == nil {
			b.Fatal("miss")
		}
	}
}

func BenchmarkCacheGetMiss(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l, err := s.Get(ctx, "miss", bcSel("s"))
		if err != nil {
			b.Fatal(err)
		}
		if l.Entry != nil {
			b.Fatal("hit")
		}
	}
}

func BenchmarkCacheGetVary(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	names := []string{"Accept"}
	bcFill(b, s, "vary", "gzip", names, time.Hour)
	bcFill(b, s, "vary", "br", names, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l, err := s.Get(ctx, "vary", bcSel("br"))
		if err != nil {
			b.Fatal(err)
		}
		if l.Entry == nil {
			b.Fatal("miss")
		}
	}
}

func BenchmarkCachePut(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	l, err := s.Get(ctx, "put", bcSel("s"))
	if err != nil {
		b.Fatal(err)
	}
	e := ckEntry("put")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stored, err := s.Put(ctx, l.Token, "put", "s", nil, e, time.Hour)
		if err != nil {
			b.Fatal(err)
		}
		if !stored {
			b.Fatal("not stored")
		}
	}
}

func BenchmarkCachePutTags(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	l, err := s.Get(ctx, "put-tags", bcSel("s"))
	if err != nil {
		b.Fatal(err)
	}
	e := ckEntry("put-tags", "bench-a", "bench-b")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stored, err := s.Put(ctx, l.Token, "put-tags", "s", nil, e, time.Hour)
		if err != nil {
			b.Fatal(err)
		}
		if !stored {
			b.Fatal("not stored")
		}
	}
}

func BenchmarkCachePurgeKey(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	bcFill(b, s, "purge-key", "s", nil, time.Hour)
	p := store.Purge{Kind: store.PurgeKeys, Keys: []string{"purge-key"}, Soft: true, Retain: 2 * time.Hour}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Purge(ctx, p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCachePurgeTag1(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	bcFill(b, s, "purge-tag1", "s", nil, time.Hour, "tag-one")
	p := store.Purge{Kind: store.PurgeTags, Tags: []string{"tag-one"}, Soft: true, Retain: 2 * time.Hour}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Purge(ctx, p); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCachePurgeTag1000(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		bcFill(b, s, fmt.Sprintf("purge-tag1000-%d", i), "s", nil, time.Hour, "tag-many")
	}
	p := store.Purge{Kind: store.PurgeTags, Tags: []string{"tag-many"}, Soft: true, Retain: 2 * time.Hour}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, err := s.Purge(ctx, p)
		if err != nil {
			b.Fatal(err)
		}
		if r.Affected != 1000 {
			b.Fatalf("affected %d, want 1000", r.Affected)
		}
	}
}

func BenchmarkCachePurgeAll(b *testing.B) {
	s := bcStore(b)
	ctx := context.Background()
	bcFill(b, s, "purge-all", "s", nil, time.Hour)
	p := store.Purge{Kind: store.PurgeAll, Soft: true, Retain: 2 * time.Hour}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Purge(ctx, p); err != nil {
			b.Fatal(err)
		}
	}
}
