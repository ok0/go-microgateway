package memstore

import (
	"sync"
	"time"

	"github.com/ok0/go-microgateway/store"
)

type marker struct {
	kind  store.PurgeKind
	sel   string
	shard int
	seq   uint64
	until time.Time
	prev  *marker
	next  *marker
}

type markerTable struct {
	mu         sync.RWMutex
	keys       map[string]*marker
	tags       map[string]*marker
	all        *marker
	list       marker
	n, cap     int
	keyBarrier []uint64
	tagBarrier uint64
	allBarrier uint64
	hw         time.Time
}

func (mt *markerTable) init(capacity, shards int) {
	mt.keys = make(map[string]*marker)
	mt.tags = make(map[string]*marker)
	mt.cap = capacity
	mt.keyBarrier = make([]uint64, shards)
	mt.list.prev = &mt.list
	mt.list.next = &mt.list
}

func (mt *markerTable) linkTail(m *marker) {
	m.prev = mt.list.prev
	m.next = &mt.list
	mt.list.prev.next = m
	mt.list.prev = m
}

func (mt *markerTable) unlink(m *marker) {
	m.prev.next = m.next
	m.next.prev = m.prev
	m.prev, m.next = nil, nil
}

func (mt *markerTable) mark(m *marker, at time.Time, seq uint64, retain time.Duration) {
	until := at.Add(retain)
	if m.next != nil {
		mt.unlink(m)
		if at.Before(m.until) && m.until.After(until) {
			until = m.until
		}
	} else {
		mt.n++
	}
	m.seq = seq
	m.until = until
	mt.linkTail(m)
}

func (mt *markerTable) drop(m *marker) {
	mt.unlink(m)
	switch m.kind {
	case store.PurgeKeys:
		delete(mt.keys, m.sel)
	case store.PurgeTags:
		delete(mt.tags, m.sel)
	case store.PurgeAll:
		mt.all = nil
	}
	mt.n--
}

func (mt *markerTable) record(kind store.PurgeKind, sel []string, now time.Time, retain time.Duration, next func(uint64) uint64, shardOf func(string) int) (uint64, time.Time) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	at := tickAt(&mt.hw, now)
	seq := next(1)
	switch kind {
	case store.PurgeKeys:
		for _, k := range sel {
			m := mt.keys[k]
			if m == nil {
				m = &marker{kind: kind, sel: k, shard: shardOf(k)}
				mt.keys[k] = m
			}
			mt.mark(m, at, seq, retain)
		}
	case store.PurgeTags:
		for _, t := range sel {
			m := mt.tags[t]
			if m == nil {
				m = &marker{kind: kind, sel: t}
				mt.tags[t] = m
			}
			mt.mark(m, at, seq, retain)
		}
	case store.PurgeAll:
		m := mt.all
		if m == nil {
			m = &marker{kind: kind}
			mt.all = m
		}
		mt.mark(m, at, seq, retain)
	}
	for mt.n > mt.cap {
		h := mt.list.next
		mt.drop(h)
		switch h.kind {
		case store.PurgeKeys:
			mt.keyBarrier[h.shard] = max(mt.keyBarrier[h.shard], h.seq)
		case store.PurgeTags:
			mt.tagBarrier = max(mt.tagBarrier, h.seq)
		case store.PurgeAll:
			mt.allBarrier = max(mt.allBarrier, h.seq)
		}
	}
	return seq, at
}

func (mt *markerTable) refuses(shard int, seq uint64, primary string, tags []string, now time.Time) bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	if seq < mt.keyBarrier[shard] || seq < mt.allBarrier || (len(tags) > 0 && seq < mt.tagBarrier) {
		return true
	}
	if m := mt.keys[primary]; m != nil && now.Before(m.until) && m.seq > seq {
		return true
	}
	if m := mt.all; m != nil && now.Before(m.until) && m.seq > seq {
		return true
	}
	for _, t := range tags {
		if m := mt.tags[t]; m != nil && now.Before(m.until) && m.seq > seq {
			return true
		}
	}
	return false
}

func (mt *markerTable) sweep(now time.Time) (removed int) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	at := tickAt(&mt.hw, now)
	for m := mt.list.next; m != &mt.list; {
		next := m.next
		if !at.Before(m.until) {
			mt.drop(m)
			removed++
		}
		m = next
	}
	return removed
}

func (mt *markerTable) count() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.n
}
