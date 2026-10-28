package storeguard

import (
	"context"
	"testing"
	"time"

	"github.com/ok0/lychgate/store"
)

func BenchmarkDoClosed(b *testing.B) {
	g, err := New("bench", Settings{ConsecutiveFailures: 3, OpenTimeout: time.Hour, MaxInflight: 1024}, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	fn := func(context.Context) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := g.Do(context.Background(), time.Second, fn); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDoOpen(b *testing.B) {
	g, err := New("bench", Settings{ConsecutiveFailures: 3, OpenTimeout: time.Hour, MaxInflight: 1024}, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	fail := func(context.Context) error { return store.ErrUnavailable }
	for range 3 {
		_ = g.Do(context.Background(), time.Second, fail)
	}
	if g.Available() {
		b.Fatal("guard not tripped")
	}
	fn := func(context.Context) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := g.Do(context.Background(), time.Second, fn); !ShortCircuit(err) {
			b.Fatalf("Do() = %v, want short-circuit", err)
		}
	}
}
