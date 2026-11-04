package coalesce

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

func BenchmarkAcquireLeader(b *testing.B) {
	b.ReportAllocs()
	g := New(1, 1, clock.Real())
	for i := range b.N {
		c, leader, ok := g.Acquire(strconv.Itoa(i))
		if !leader || !ok {
			b.Fatal("Acquire: want leader")
		}
		c.Complete(nil, nil)
	}
}

func BenchmarkAcquireWaiter(b *testing.B) {
	b.ReportAllocs()
	g := New(1, 1, clock.Real())
	ctx := context.Background()
	for range b.N {
		c, leader, ok := g.Acquire("k")
		if !leader || !ok {
			b.Fatal("Acquire: want leader")
		}
		w, leader, ok := g.Acquire("k")
		if leader || !ok {
			b.Fatal("Acquire: want waiter")
		}
		c.Complete("v", nil)
		if _, err := w.Wait(ctx, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}
