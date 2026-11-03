package memstore

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/store"
)

const (
	// DefaultCacheShards is the number of shards used when CacheOptions.Shards is not positive.
	DefaultCacheShards = 16
	// DefaultMaxMarkers is the number of purge markers kept when CacheOptions.MaxMarkers is zero.
	DefaultMaxMarkers = 1 << 16
	// DefaultCacheJanitorInterval is the clock interval between sweeps when CacheOptions.JanitorInterval is zero.
	DefaultCacheJanitorInterval = 30 * time.Second
	// MaxCacheJanitorInterval is the largest accepted CacheOptions.JanitorInterval.
	MaxCacheJanitorInterval = time.Minute
	// EntryOverhead is the fixed number of bytes added to the weight of every entry. The weight of an entry is
	// EntryOverhead + len(primary) + len(secondary) + the sum of len(vary name) + the sum of len(tag) + the sum of
	// (len(header name) + the sum of len(value)) over the header + len(body).
	EntryOverhead = 256
)

// CacheOptions configures a Cache.
type CacheOptions struct {
	// MaxBytes is the byte budget of the cache. It must be at least 1; otherwise NewCache panics.
	MaxBytes int64
	// Shards is the number of shards. Values <= 0 select DefaultCacheShards. The count is capped at MaxBytes.
	Shards int
	// Clock supplies the time. Nil selects clock.Real().
	Clock clock.Clock
	// OnEvict, if set, is called synchronously after a Put that evicted live entries to make room, with the
	// number of entries and their total weight. It is called outside all locks.
	OnEvict func(n int, bytes int64)
	// OnSweep, if set, is called by the janitor after each sweep with the number of entries and markers removed.
	OnSweep func(removed int)
	// MaxMarkers caps the number of purge markers. Zero selects DefaultMaxMarkers; a negative value panics.
	MaxMarkers int
	// JanitorInterval is the clock interval between sweeps. Zero selects DefaultCacheJanitorInterval; a negative
	// value or a value above MaxCacheJanitorInterval panics.
	JanitorInterval time.Duration
}

// CacheStats reports the state of a Cache.
type CacheStats struct {
	Entries, Primaries int
	Bytes              int64
	// Tags is the number of distinct tags in the index across all shards.
	Tags, Markers int
	// Evictions and EvictedBytes count entries removed to make room for a Put.
	Evictions, EvictedBytes int64
	// Expired counts entries removed after their deadline, lazily, by Sweep, or to make room.
	Expired int64
	// Rejected counts Puts refused because the entry is heavier than its shard budget.
	Rejected int64
}

// Cache is an in-memory store.Cache. It is byte-bounded and per instance: a purge reaches only the cache that
// receives it. Time comes only from the injected clock.
type Cache struct {
	shards      []cshard
	clk         clock.Clock
	seq         atomic.Uint64
	mk          markerTable
	onEvict     func(int, int64)
	onSweep     func(int)
	closed      atomic.Bool
	done        chan struct{}
	janitorDone chan struct{}
	once        sync.Once
	wg          sync.WaitGroup
}

var _ store.Cache = (*Cache)(nil)

// NewCache returns a Cache configured by o and starts its janitor. Call Close to stop the janitor.
// It panics if the options are out of range.
func NewCache(o CacheOptions) *Cache {
	if o.MaxBytes < 1 {
		panic("memstore: MaxBytes must be >= 1")
	}
	if o.MaxMarkers < 0 {
		panic("memstore: MaxMarkers must be >= 0")
	}
	if o.JanitorInterval < 0 || o.JanitorInterval > MaxCacheJanitorInterval {
		panic("memstore: JanitorInterval must be in [0, MaxCacheJanitorInterval]")
	}
	n := o.Shards
	if n <= 0 {
		n = DefaultCacheShards
	}
	if int64(n) > o.MaxBytes {
		n = int(o.MaxBytes)
	}
	clk := o.Clock
	if clk == nil {
		clk = clock.Real()
	}
	maxMarkers := o.MaxMarkers
	if maxMarkers == 0 {
		maxMarkers = DefaultMaxMarkers
	}
	interval := o.JanitorInterval
	if interval == 0 {
		interval = DefaultCacheJanitorInterval
	}
	c := &Cache{
		shards:      make([]cshard, n),
		clk:         clk,
		onEvict:     o.OnEvict,
		onSweep:     o.OnSweep,
		done:        make(chan struct{}),
		janitorDone: make(chan struct{}),
	}
	base, extra := o.MaxBytes/int64(n), o.MaxBytes%int64(n)
	for i := range c.shards {
		s := &c.shards[i]
		s.budget = base
		if int64(i) < extra {
			s.budget++
		}
		s.recs = make(map[string]*primaryRec)
		s.tags = make(map[string]map[*variant]struct{})
		s.lru.lruPrev = &s.lru
		s.lru.lruNext = &s.lru
	}
	c.mk.init(maxMarkers, n)
	t := clock.Housekeeping(clk).NewTicker(interval)
	c.wg.Add(1)
	go c.janitor(t)
	return c
}

func (c *Cache) shardIndex(primary string) int {
	h := uint64(14695981039346656037)
	for i := 0; i < len(primary); i++ {
		h ^= uint64(primary[i])
		h *= 1099511628211
	}
	return int(h % uint64(len(c.shards)))
}

func (c *Cache) token() store.FillToken {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], c.seq.Add(1))
	return store.NewFillToken(string(b[:]))
}

func decodeToken(tok store.FillToken) (uint64, bool) {
	s := tok.Opaque()
	if len(s) != 8 {
		return 0, false
	}
	seq := binary.BigEndian.Uint64([]byte(s))
	return seq, seq != 0
}

// Get returns the entry for primary whose secondary key sel selects, or a miss.
// It follows the call order described in the store package.
func (c *Cache) Get(ctx context.Context, primary string, sel func(varyNames []string) (secondary string, ok bool)) (store.Lookup, error) {
	if err := ctx.Err(); err != nil {
		return store.Lookup{}, err
	}
	if sel == nil {
		return store.Lookup{}, fmt.Errorf("%w: nil selector", store.ErrInvalidEntry)
	}
	if c.closed.Load() {
		return store.Lookup{}, fmt.Errorf("memstore: cache closed: %w", store.ErrUnavailable)
	}
	tok := c.token()
	if primary == "" || len(primary) > store.MaxKeyBytes {
		return store.Lookup{Token: tok}, nil
	}
	s := &c.shards[c.shardIndex(primary)]
	s.mu.Lock()
	now := s.tick(c.clk.Now())
	e, names := s.get(primary, sel, now)
	s.mu.Unlock()
	return store.Lookup{Entry: e, VaryNames: names, Now: now, Token: tok}, nil
}

// Put stores e under primary and secondary if tok is valid for the state of the cache.
// It follows the call order described in the store package.
func (c *Cache) Put(ctx context.Context, tok store.FillToken, primary, secondary string, varyNames []string, e *store.Entry, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	names, err := store.ValidateFill(tok, primary, secondary, varyNames, e, ttl)
	if err != nil {
		return false, err
	}
	seq, ok := decodeToken(tok)
	if !ok {
		return false, fmt.Errorf("%w: foreign fill token", store.ErrInvalidEntry)
	}
	if c.closed.Load() {
		return false, fmt.Errorf("memstore: cache closed: %w", store.ErrUnavailable)
	}
	if ttl <= 0 {
		return false, nil
	}
	idx := c.shardIndex(primary)
	s := &c.shards[idx]
	s.mu.Lock()
	now := s.tick(c.clk.Now())
	if c.mk.refuses(idx, seq, primary, e.Tags, now) {
		s.mu.Unlock()
		return false, nil
	}
	stored, n, evicted := s.put(primary, secondary, names, e, ttl, now)
	s.mu.Unlock()
	if n > 0 && c.onEvict != nil {
		c.onEvict(n, evicted)
	}
	return stored, nil
}

// Purge removes or marks stale the entries covered by p and records a marker so that older fill tokens cannot
// store covered entries. It follows the call order described in the store package.
func (c *Cache) Purge(ctx context.Context, p store.Purge) (store.PurgeResult, error) {
	if err := ctx.Err(); err != nil {
		return store.PurgeResult{}, err
	}
	if err := p.Validate(); err != nil {
		return store.PurgeResult{}, err
	}
	if c.closed.Load() {
		return store.PurgeResult{}, fmt.Errorf("memstore: cache closed: %w", store.ErrUnavailable)
	}
	var sel []string
	switch p.Kind {
	case store.PurgeKeys:
		sel = distinct(p.Keys)
	case store.PurgeTags:
		sel = distinct(p.Tags)
	}
	seq, at := c.mk.record(p.Kind, sel, c.clk.Now(), p.Retain, c.seq.Add, c.shardIndex)
	affected := 0
	switch p.Kind {
	case store.PurgeKeys:
		groups := make([][]string, len(c.shards))
		for _, k := range sel {
			i := c.shardIndex(k)
			groups[i] = append(groups[i], k)
		}
		for i := range c.shards {
			if len(groups[i]) == 0 {
				continue
			}
			s := &c.shards[i]
			s.mu.Lock()
			affected += s.purgeKeys(groups[i], p.Soft, s.tick(at))
			s.mu.Unlock()
		}
	case store.PurgeTags:
		for i := range c.shards {
			s := &c.shards[i]
			s.mu.Lock()
			affected += s.purgeTags(sel, p.Soft, seq, s.tick(at))
			s.mu.Unlock()
		}
	case store.PurgeAll:
		for i := range c.shards {
			s := &c.shards[i]
			s.mu.Lock()
			affected += s.purgeAll(p.Soft, s.tick(at))
			s.mu.Unlock()
		}
	}
	accepted := 1
	if p.Kind != store.PurgeAll {
		accepted = len(sel)
	}
	return store.PurgeResult{Accepted: accepted, Affected: affected, AffectedKnown: true}, nil
}

func distinct(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	return slices.Compact(out)
}

// Sweep removes entries and markers whose deadline is at or before now, and returns the number removed.
// now must be a reading of the cache's clock. Sweep never changes the result of any later call.
func (c *Cache) Sweep(now time.Time) (removed int) {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		removed += s.sweep(now)
		s.mu.Unlock()
	}
	return removed + c.mk.sweep(now)
}

// Bytes returns the total weight of the entries held.
func (c *Cache) Bytes() int64 {
	var total int64
	for i := range c.shards {
		total += c.shards[i].pub.Load()
	}
	return total
}

// Len returns the number of entries held.
func (c *Cache) Len() int {
	total := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		total += s.entries
		s.mu.Unlock()
	}
	return total
}

// Stats returns a snapshot of the cache state.
func (c *Cache) Stats() CacheStats {
	var st CacheStats
	tags := make(map[string]struct{})
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		st.Entries += s.entries
		st.Primaries += len(s.recs)
		st.Bytes += s.bytes
		st.Evictions += s.evictions
		st.EvictedBytes += s.evictedBytes
		st.Expired += s.expired
		st.Rejected += s.rejected
		for t := range s.tags {
			tags[t] = struct{}{}
		}
		s.mu.Unlock()
	}
	st.Tags = len(tags)
	st.Markers = c.mk.count()
	return st
}

// Close stops the janitor and marks the cache closed. Subsequent calls to Get, Put and Purge fail with an error
// wrapping store.ErrUnavailable. Close is idempotent and always returns nil.
func (c *Cache) Close() error {
	c.once.Do(func() {
		c.closed.Store(true)
		close(c.done)
	})
	c.wg.Wait()
	return nil
}

func (c *Cache) janitor(t clock.Ticker) {
	defer c.wg.Done()
	defer close(c.janitorDone)
	defer t.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-t.C():
			n := c.Sweep(c.clk.Now())
			if c.onSweep != nil {
				c.onSweep(n)
			}
		}
	}
}
