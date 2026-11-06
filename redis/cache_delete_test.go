package redis

import (
	"context"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ok0/go-microgateway/store"
)

// cdSlot returns the slot index that the variant record sec of the index key holds.
func cdSlot(t *testing.T, m *miniredis.Miniredis, index, sec string) int {
	t.Helper()
	first, _, _ := strings.Cut(m.HGet(index, "v:"+sec), " ")
	slot, err := strconv.Atoi(first)
	if err != nil {
		t.Fatalf("slot of %s: %v", sec, err)
	}
	return slot
}

// cdSameKeysExcept fails the test unless every key of before, other than the named ones, is in after with the
// same type, value and lifetime.
func cdSameKeysExcept(t *testing.T, before, after miniState, skip ...string) {
	t.Helper()
	for k, mk := range before.keys {
		if slices.Contains(skip, k) {
			continue
		}
		ak, ok := after.keys[k]
		if !ok {
			t.Errorf("key %s removed by Delete", k)
			continue
		}
		if !reflect.DeepEqual(ak, mk) {
			t.Errorf("key %s changed by Delete: before %+v, after %+v", k, mk, ak)
		}
	}
	for k := range after.keys {
		if _, ok := before.keys[k]; !ok {
			t.Errorf("key %s created by Delete", k)
		}
	}
}

func TestCacheDeleteLayout(t *testing.T) {
	t.Parallel()
	m := newMini(t)
	s, p := ckProxied(t, m, ttPrefix)
	pend := cacheStore(t, m.Addr(), 0, ttPrefix)
	ctx := context.Background()
	if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeAll, Soft: true, Retain: time.Minute}); err != nil {
		t.Fatalf("purge all: %v", err)
	}
	if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeKeys, Keys: []string{"dk"}, Soft: true, Retain: 30 * time.Minute}); err != nil {
		t.Fatalf("purge key: %v", err)
	}
	ctMust(t, s, "dk", "s1", []string{"x-v"}, ckEntry("one", "t1"), 2*time.Hour)
	ctMust(t, s, "dk", "s2", []string{"x-v"}, ckEntry("two", "t1"), time.Hour)
	pk := s.primaryKeys("dk")
	scripts := storeScripts(t, s.sc)
	ran := false
	pend.hook = func(hp hookPoint) {
		if hp != hookPending || ran {
			return
		}
		ran = true
		slot1 := cdSlot(t, m, pk.index, "s1")
		if ok, err := s.Delete(ctx, "dk", "none"); ok || err != nil {
			t.Fatalf("warm-up Delete(dk, none) = %t, %v; want false, nil", ok, err)
		}
		before := miniSnapshot(t, m, s.c, scripts)
		p.Reset()
		ok, err := s.Delete(ctx, "dk", "s1")
		if !ok || err != nil {
			t.Fatalf("Delete(dk, s1) = %t, %v; want true, nil", ok, err)
		}
		if n := ctCount(p, "EVALSHA"); n != 1 || len(p.Commands()) != 1 {
			t.Fatalf("Delete sent %q, want one EVALSHA", p.Commands())
		}
		after := miniSnapshot(t, m, s.c, scripts)
		if got := m.HGet(pk.index, "v:s1"); got != "" {
			t.Errorf("variant record v:s1 kept: %q", got)
		}
		if m.Exists(pk.slots[slot1]) {
			t.Errorf("slot %d kept after Delete", slot1)
		}
		cdSameKeysExcept(t, before, after, pk.index, pk.slots[slot1])
		wantHash := maps.Clone(before.keys[pk.index].hash)
		delete(wantHash, "v:s1")
		if got := after.keys[pk.index]; got.typ != "hash" || !reflect.DeepEqual(got.hash, wantHash) {
			t.Errorf("index after Delete %+v, want the hash %v", got, wantHash)
		}
		if got, want := ckExpiryMs(t, s.c, pk.index), miniStart.Add(time.Hour).UnixMilli(); got != want {
			t.Errorf("index expiry %d, want %d (the lifetime of dk/s2)", got, want)
		}
		for k, mk := range after.keys {
			if mk.ttl <= 0 {
				t.Errorf("key %s has no TTL after Delete", k)
			}
		}
	}
	if !ckFill(t, pend, "dk", "s3", []string{"x-v"}, ckEntry("three"), 10*time.Minute) {
		t.Fatal("pending fill dk/s3 was not stored after Delete")
	}
	if !ran {
		t.Fatal("the hook of the pending fill did not run")
	}
	ctHit(t, "dk/s2", ckGet(t, s, "dk", "s2"), "two")
	ctHit(t, "dk/s3", ckGet(t, s, "dk", "s3"), "three")
	ctMiss(t, "dk/s1", ckGet(t, s, "dk", "s1"))
}

func TestCacheDeleteLastDropsIndex(t *testing.T) {
	t.Parallel()
	m := newMini(t)
	s, p := ckProxied(t, m, ttPrefix)
	ctx := context.Background()
	ctMust(t, s, "dl", "s", nil, ckEntry("x"), time.Hour)
	pk := s.primaryKeys("dl")
	slot := cdSlot(t, m, pk.index, "s")
	before := miniSnapshot(t, m, s.c, storeScripts(t, s.sc))
	p.Reset()
	ok, err := s.Delete(ctx, "dl", "s")
	if !ok || err != nil {
		t.Fatalf("Delete(dl, s) = %t, %v; want true, nil", ok, err)
	}
	if m.Exists(pk.index) {
		t.Error("index kept after the last variant was deleted")
	}
	if m.Exists(pk.slots[slot]) {
		t.Errorf("slot %d kept after the last variant was deleted", slot)
	}
	after := miniSnapshot(t, m, s.c, storeScripts(t, s.sc))
	cdSameKeysExcept(t, before, after, pk.index, pk.slots[slot])
	for k, mk := range after.keys {
		if mk.ttl <= 0 {
			t.Errorf("key %s has no TTL after Delete", k)
		}
	}
	ctMiss(t, "dl/s", ckGet(t, s, "dl", "s"))
}

func TestCacheDeleteForeignSlot(t *testing.T) {
	t.Parallel()
	m := newMini(t)
	s, _ := ckProxied(t, m, ttPrefix)
	ctx := context.Background()
	ctMust(t, s, "df", "s", nil, ckEntry("x"), time.Hour)
	pk := s.primaryKeys("df")
	slot := cdSlot(t, m, pk.index, "s")
	m.HSet(pk.slots[slot], "f", strings.Repeat("0", 32))
	before := miniSnapshot(t, m, s.c, storeScripts(t, s.sc))
	ok, err := s.Delete(ctx, "df", "s")
	if ok || err != nil {
		t.Fatalf("Delete(df, s) = %t, %v; want false, nil", ok, err)
	}
	after := miniSnapshot(t, m, s.c, storeScripts(t, s.sc))
	if !reflect.DeepEqual(after.keys, before.keys) {
		t.Errorf("Delete over a foreign slot changed the keys: before %+v, after %+v", before.keys, after.keys)
	}
}

func TestCacheDeleteCallOrder(t *testing.T) {
	t.Parallel()
	m := newMini(t)
	s, p := ckProxied(t, m, ttPrefix)
	ctx := context.Background()
	ctMust(t, s, "co", "s", nil, ckEntry("x"), time.Hour)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	p.Reset()
	if _, err := s.Delete(cctx, "co", "s"); err != context.Canceled {
		t.Errorf("canceled Delete: error %v, want context.Canceled", err)
	}
	ctNoCommands(t, p, "canceled Delete")
	if _, err := s.Delete(cctx, "", "s"); err != context.Canceled {
		t.Errorf("canceled Delete with an empty primary: error %v, want context.Canceled", err)
	}
	ctNoCommands(t, p, "canceled Delete with an empty primary")
	if ok, err := s.Delete(ctx, "", "s"); ok || !errors.Is(err, store.ErrInvalidEntry) {
		t.Errorf("Delete with an empty primary = %t, %v; want false, ErrInvalidEntry", ok, err)
	}
	ctNoCommands(t, p, "Delete with an empty primary")
	c := cacheStore(t, p.Addr(), 0, ttPrefix)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	p.Reset()
	if ok, err := c.Delete(ctx, "co", "s"); ok || !errors.Is(err, store.ErrUnavailable) {
		t.Errorf("Delete on a closed store = %t, %v; want false, ErrUnavailable", ok, err)
	}
	ctNoCommands(t, p, "Delete on a closed store")
	ctMust(t, s, "cf", "s", nil, ckEntry("x"), time.Hour)
	if err := s.c.Do(ctx, "SCRIPT", "FLUSH").Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	p.Reset()
	if ok, err := s.Delete(ctx, "cf", "s"); !ok || err != nil {
		t.Fatalf("Delete after SCRIPT FLUSH = %t, %v; want true, nil", ok, err)
	}
	if ctCount(p, "SCRIPT") == 0 || ctCount(p, "EVAL") != 0 {
		t.Errorf("Delete after SCRIPT FLUSH sent %q, want a SCRIPT load and no EVAL", p.Commands())
	}
}
