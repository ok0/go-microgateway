package teststore

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

func TestSpyRecordsCalls(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Unix(1_700_000_000, 0))
	shared := NewShared(fake)
	t.Cleanup(func() { _ = shared.Close() })
	sc := NewSharedCache(fake, 1<<20)
	t.Cleanup(func() { _ = sc.Close() })

	spy := NewSpy()
	rl1, c1 := shared.Handle(), sc.Handle()
	innerCache2 := sc.Handle()
	h1 := spy.Wrap(rl1, c1)
	h2 := spy.Wrap(shared.Handle(), innerCache2)

	if h1.ID() != 1 || h2.ID() != 2 {
		t.Fatalf("handle IDs = %d, %d, want 1, 2", h1.ID(), h2.ID())
	}

	lim := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Second, Burst: 1}
	if _, err := h1.Take(ctx, "k1", lim); err != nil {
		t.Fatal(err)
	}

	lookup, err := h2.Get(ctx, "p", func([]string) (string, bool) { return "", false })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h2.Put(ctx, lookup.Token, "p", "", nil, &store.Entry{Status: 200, Body: []byte("x")}, time.Minute); err != nil {
		t.Fatal(err)
	}

	if _, err := h1.Purge(ctx, store.Purge{Kind: store.PurgeAll, Retain: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if err := h2.Close(); err != nil {
		t.Fatal(err)
	}

	want := []SpyCall{
		{Handle: 1, Op: "take", Key: "k1"},
		{Handle: 2, Op: "get", Key: "p"},
		{Handle: 2, Op: "put", Key: "p"},
		{Handle: 1, Op: "purge", Key: ""},
		{Handle: 2, Op: "close", Key: ""},
	}
	if got := spy.Calls(); !slices.Equal(got, want) {
		t.Fatalf("Calls() = %+v, want %+v", got, want)
	}
	if n := spy.Count("take"); n != 1 {
		t.Fatalf("Count(take) = %d, want 1", n)
	}
	if n := spy.CountHandle(2, ""); n != 3 {
		t.Fatalf("CountHandle(2, all) = %d, want 3", n)
	}
	if n := h2.Closes(); n != 1 {
		t.Fatalf("h2 Closes() = %d, want 1", n)
	}
	if n := innerCache2.Closes(); n != 1 {
		t.Fatalf("inner cache handle Closes() = %d, want 1", n)
	}
}

func TestSpyFailAndHang(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Unix(1_700_000_000, 0))
	shared := NewShared(fake)
	t.Cleanup(func() { _ = shared.Close() })

	spy := NewSpy()
	rl := shared.Handle()
	h := spy.Wrap(rl, nil)

	spy.SetMode("take", Fail, errBoom)
	if _, err := h.Take(ctx, "k", slidingLimit(5)); !errors.Is(err, errBoom) {
		t.Fatalf("Take error = %v, want %v", err, errBoom)
	}
	if n := rl.Takes(); n != 0 {
		t.Fatalf("inner Takes() = %d, want 0", n)
	}
	if n := spy.Count("take"); n != 1 {
		t.Fatalf("Count(take) after failed call = %d, want 1", n)
	}

	type result struct {
		d   store.Decision
		err error
	}
	spy.SetMode("take", Hang, nil)
	done := make(chan result, 1)
	go func() {
		d, err := h.Take(ctx, "k", slidingLimit(5))
		done <- result{d, err}
	}()
	spy.WaitBlocked(1)
	spy.Release()
	res, _ := testwait.Recv(t, "parked take", done, nil)
	if res.err != nil {
		t.Fatalf("released Take error = %v", res.err)
	}
	if !res.d.Allowed {
		t.Fatalf("released Take decision = %+v, want allowed", res.d)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := h.Take(cancelled, "k", slidingLimit(5)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled parked Take error = %v, want context.Canceled", err)
	}

	spy.SetMode("", Inherit, nil)
	d, err := h.Take(ctx, "k", slidingLimit(5))
	if err != nil {
		t.Fatalf("pass-through Take error = %v", err)
	}
	if !d.Allowed {
		t.Fatalf("pass-through Take decision = %+v, want allowed", d)
	}
}

func TestSpyMissingCapability(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_700_000_000, 0))
	shared := NewShared(fake)
	t.Cleanup(func() { _ = shared.Close() })

	h := NewSpy().Wrap(shared.Handle(), nil)
	_, err := h.Get(context.Background(), "p", nil)
	if !errors.Is(err, store.ErrUnavailable) {
		t.Fatalf("Get error = %v, want store.ErrUnavailable", err)
	}
}
