package memstore

import (
	"bytes"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/go-microgateway/store"
)

type variant struct {
	rec      *primaryRec
	sec      string
	entry    *store.Entry
	itags    []string
	weight   int64
	deadline time.Time
	ins      uint64
	mark     uint64
	lruPrev  *variant
	lruNext  *variant
	hidx     int
}

type primaryRec struct {
	key      string
	names    []string
	vars     map[string]*variant
	deadline time.Time
}

type cshard struct {
	mu            sync.Mutex
	recs          map[string]*primaryRec
	lru           variant
	heap          []*variant
	tags          map[string]map[*variant]struct{}
	bytes, budget int64
	pub           atomic.Int64
	hw            time.Time
	ins           uint64
	entries       int
	evictions     int64
	evictedBytes  int64
	expired       int64
	rejected      int64
}

type reason uint8

const (
	reasonEvicted reason = iota
	reasonExpired
	reasonDropped
)

func tickAt(hw *time.Time, t time.Time) time.Time {
	t = t.Round(0)
	if t.After(*hw) {
		*hw = t
	}
	return *hw
}

func (s *cshard) tick(t time.Time) time.Time {
	return tickAt(&s.hw, t)
}

func (s *cshard) pushFront(v *variant) {
	v.lruPrev = &s.lru
	v.lruNext = s.lru.lruNext
	s.lru.lruNext.lruPrev = v
	s.lru.lruNext = v
}

func (s *cshard) unlink(v *variant) {
	v.lruPrev.lruNext = v.lruNext
	v.lruNext.lruPrev = v.lruPrev
	v.lruPrev, v.lruNext = nil, nil
}

func (s *cshard) moveToFront(v *variant) {
	s.unlink(v)
	s.pushFront(v)
}

func (s *cshard) heapLess(i, j int) bool {
	a, b := s.heap[i], s.heap[j]
	if !a.deadline.Equal(b.deadline) {
		return a.deadline.Before(b.deadline)
	}
	return a.ins < b.ins
}

func (s *cshard) heapSwap(i, j int) {
	s.heap[i], s.heap[j] = s.heap[j], s.heap[i]
	s.heap[i].hidx = i
	s.heap[j].hidx = j
}

func (s *cshard) heapUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !s.heapLess(i, p) {
			break
		}
		s.heapSwap(i, p)
		i = p
	}
}

func (s *cshard) heapDown(i int) {
	n := len(s.heap)
	for {
		m := 2*i + 1
		if m >= n {
			break
		}
		if r := m + 1; r < n && s.heapLess(r, m) {
			m = r
		}
		if !s.heapLess(m, i) {
			break
		}
		s.heapSwap(i, m)
		i = m
	}
}

func (s *cshard) heapPush(v *variant) {
	v.hidx = len(s.heap)
	s.heap = append(s.heap, v)
	s.heapUp(v.hidx)
}

func (s *cshard) heapRemove(i int) {
	last := len(s.heap) - 1
	if i != last {
		s.heapSwap(i, last)
	}
	s.heap[last] = nil
	s.heap = s.heap[:last]
	if i != last {
		s.heapDown(i)
		s.heapUp(i)
	}
}

func (s *cshard) remove(v *variant, why reason) {
	s.unlink(v)
	s.heapRemove(v.hidx)
	for _, t := range v.itags {
		set := s.tags[t]
		delete(set, v)
		if len(set) == 0 {
			delete(s.tags, t)
		}
	}
	rec := v.rec
	delete(rec.vars, v.sec)
	s.bytes -= v.weight
	s.entries--
	switch why {
	case reasonEvicted:
		s.evictions++
		s.evictedBytes += v.weight
	case reasonExpired:
		s.expired++
	}
	if len(rec.vars) == 0 {
		delete(s.recs, rec.key)
		return
	}
	if v.deadline.Equal(rec.deadline) {
		first := true
		for _, o := range rec.vars {
			if first || o.deadline.After(rec.deadline) {
				rec.deadline = o.deadline
				first = false
			}
		}
	}
}

func (s *cshard) removeAll(rec *primaryRec, why reason) {
	for _, v := range rec.vars {
		s.remove(v, why)
	}
}

func (s *cshard) get(primary string, sel func([]string) (string, bool), now time.Time) (*store.Entry, []string) {
	rec := s.recs[primary]
	if rec == nil {
		return nil, nil
	}
	if !now.Before(rec.deadline) {
		s.removeAll(rec, reasonExpired)
		s.pub.Store(s.bytes)
		return nil, nil
	}
	names := slices.Clone(rec.names)
	sec, ok := sel(names)
	if !ok {
		return nil, names
	}
	v := rec.vars[sec]
	if v == nil {
		return nil, names
	}
	if !now.Before(v.deadline) {
		s.remove(v, reasonExpired)
		s.pub.Store(s.bytes)
		return nil, names
	}
	s.moveToFront(v)
	return v.entry, names
}

func oldest(rec *primaryRec) *variant {
	var best *variant
	for _, v := range rec.vars {
		if best == nil || v.entry.StoredAt.Before(best.entry.StoredAt) ||
			(v.entry.StoredAt.Equal(best.entry.StoredAt) && v.ins < best.ins) {
			best = v
		}
	}
	return best
}

func (s *cshard) put(primary, sec string, names []string, e *store.Entry, ttl time.Duration, now time.Time) (bool, int, int64) {
	w := weight(primary, sec, names, e)
	if w > s.budget {
		s.rejected++
		return false, 0, 0
	}
	deadline := now.Add(ttl)

	if rec := s.recs[primary]; rec != nil {
		switch {
		case !now.Before(rec.deadline):
			s.removeAll(rec, reasonExpired)
		case !slices.Equal(rec.names, names):
			s.removeAll(rec, reasonDropped)
		default:
			if v := rec.vars[sec]; v != nil {
				s.remove(v, reasonDropped)
			}
		}
	}

	if rec := s.recs[primary]; rec != nil && len(rec.vars) >= store.MaxVariants {
		for _, v := range rec.vars {
			if !now.Before(v.deadline) {
				s.remove(v, reasonExpired)
			}
		}
		for len(rec.vars) >= store.MaxVariants {
			s.remove(oldest(rec), reasonDropped)
		}
	}

	n := 0
	var evictedBytes int64
	for s.bytes+w > s.budget {
		if top := s.heap[0]; !now.Before(top.deadline) {
			s.remove(top, reasonExpired)
		} else {
			victim := s.lru.lruPrev
			evictedBytes += victim.weight
			s.remove(victim, reasonEvicted)
			n++
		}
	}

	rec := s.recs[primary]
	if rec == nil {
		rec = &primaryRec{key: strings.Clone(primary), names: names, vars: make(map[string]*variant)}
		s.recs[rec.key] = rec
	}

	itags := slices.Clone(e.Tags)
	slices.Sort(itags)
	itags = slices.Compact(itags)

	v := &variant{
		rec: rec,
		sec: strings.Clone(sec),
		entry: &store.Entry{
			Status:     e.Status,
			Header:     e.Header.Clone(),
			Body:       bytes.Clone(e.Body),
			Tags:       slices.Clone(e.Tags),
			InitialAge: e.InitialAge,
			StoredAt:   now,
		},
		itags:    itags,
		weight:   w,
		deadline: deadline,
		ins:      s.ins,
	}
	s.ins++
	s.pushFront(v)
	s.heapPush(v)
	for _, t := range itags {
		set := s.tags[t]
		if set == nil {
			set = make(map[*variant]struct{})
			s.tags[t] = set
		}
		set[v] = struct{}{}
	}
	rec.vars[v.sec] = v
	if len(rec.vars) == 1 || deadline.After(rec.deadline) {
		rec.deadline = deadline
	}
	s.bytes += w
	s.entries++
	s.pub.Store(s.bytes)
	return true, n, evictedBytes
}

func (s *cshard) staleCopy(v *variant) {
	if v.entry.Stale {
		return
	}
	c := *v.entry
	c.Stale = true
	v.entry = &c
}

func (s *cshard) purgeKeys(keys []string, soft bool, now time.Time) (affected int) {
	for _, k := range keys {
		rec := s.recs[k]
		if rec == nil {
			continue
		}
		live := 0
		for _, v := range rec.vars {
			if !now.Before(v.deadline) {
				s.remove(v, reasonExpired)
				continue
			}
			live++
			if soft {
				s.staleCopy(v)
			} else {
				s.remove(v, reasonDropped)
			}
		}
		if soft {
			if live > 0 {
				affected++
			}
		} else {
			affected += live
		}
	}
	s.pub.Store(s.bytes)
	return affected
}

func (s *cshard) purgeTags(tags []string, soft bool, stamp uint64, now time.Time) (affected int) {
	for _, t := range tags {
		var batch []*variant
		for v := range s.tags[t] {
			if v.mark != stamp {
				v.mark = stamp
				batch = append(batch, v)
			}
		}
		for _, v := range batch {
			if !now.Before(v.deadline) {
				s.remove(v, reasonExpired)
				continue
			}
			affected++
			if soft {
				s.staleCopy(v)
			} else {
				s.remove(v, reasonDropped)
			}
		}
	}
	s.pub.Store(s.bytes)
	return affected
}

func (s *cshard) purgeAll(soft bool, now time.Time) (affected int) {
	if soft {
		for v := s.lru.lruNext; v != &s.lru; {
			next := v.lruNext
			if !now.Before(v.deadline) {
				s.remove(v, reasonExpired)
			} else {
				affected++
				s.staleCopy(v)
			}
			v = next
		}
	} else {
		for v := s.lru.lruNext; v != &s.lru; v = v.lruNext {
			if !now.Before(v.deadline) {
				s.expired++
			} else {
				affected++
			}
		}
		s.recs = make(map[string]*primaryRec)
		s.tags = make(map[string]map[*variant]struct{})
		s.lru.lruPrev, s.lru.lruNext = &s.lru, &s.lru
		s.heap = nil
		s.bytes, s.entries = 0, 0
	}
	s.pub.Store(s.bytes)
	return affected
}

func (s *cshard) sweep(now time.Time) (removed int) {
	at := s.tick(now)
	for len(s.heap) > 0 && !at.Before(s.heap[0].deadline) {
		s.remove(s.heap[0], reasonExpired)
		removed++
	}
	s.pub.Store(s.bytes)
	return removed
}

func weight(primary, sec string, names []string, e *store.Entry) int64 {
	w := int64(EntryOverhead) + int64(len(primary)) + int64(len(sec))
	for _, n := range names {
		w += int64(len(n))
	}
	for _, t := range e.Tags {
		w += int64(len(t))
	}
	for k, vs := range e.Header {
		w += int64(len(k))
		for _, v := range vs {
			w += int64(len(v))
		}
	}
	return w + int64(len(e.Body))
}
