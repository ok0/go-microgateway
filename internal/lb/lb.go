// Package lb implements load-balancing algorithms over a list of endpoints.
// Selection is deterministic given the Rand; the package reads no clock and no global randomness.
package lb

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
)

type Endpoint struct {
	ID       string
	Weight   int
	InFlight func() int64
}

type Input struct {
	// Eligible reports whether endpoint i may be chosen; nil means every endpoint with a positive weight. Pick calls it at most once per endpoint and decides from those answers only, so a change made during a Pick takes effect on the next Pick.
	Eligible func(i int) bool
	Key      string
	HasKey   bool
}

type Balancer interface {
	Pick(in Input) (int, bool)
}

var ErrUnknownAlgorithm = errors.New("lb: unknown algorithm")

var errNilRand = errors.New("lb: random source is required")

const (
	algoP2C                  = "p2c"
	algoRoundRobin           = "round_robin"
	algoWeightedRoundRobin   = "weighted_round_robin"
	algoRandom               = "random"
	algoConsistentHash       = "consistent_hash"
	defaultVirtualNodes      = 160
	defaultBoundedLoadFactor = 1.25
)

func New(cfg config.LoadBalancer, eps []Endpoint, rnd clock.Rand) (Balancer, error) {
	cp := make([]Endpoint, len(eps))
	copy(cp, eps)
	algo := cfg.Algorithm
	if algo == "" {
		algo = algoP2C
	}
	switch algo {
	case algoRoundRobin:
		return &roundRobin{eps: cp}, nil
	case algoWeightedRoundRobin:
		return &smoothWRR{eps: cp, cur: make([]int64, len(cp))}, nil
	case algoP2C:
		if rnd == nil {
			return nil, errNilRand
		}
		return &p2c{eps: cp, rnd: rnd}, nil
	case algoRandom:
		if rnd == nil {
			return nil, errNilRand
		}
		return &weightedRandom{eps: cp, rnd: rnd}, nil
	case algoConsistentHash:
		if rnd == nil {
			return nil, errNilRand
		}
		vn, factor, err := consistentParams(cfg)
		if err != nil {
			return nil, err
		}
		return newConsistentHash(cp, vn, factor, rnd), nil
	}
	return nil, fmt.Errorf("%w %q", ErrUnknownAlgorithm, algo)
}

func consistentParams(cfg config.LoadBalancer) (int, float64, error) {
	vn := cfg.VirtualNodes
	if vn < 0 {
		return 0, 0, fmt.Errorf("lb: virtual_nodes must not be negative: %d", vn)
	}
	if vn == 0 {
		vn = defaultVirtualNodes
	}
	f := cfg.BoundedLoadFactor
	if f == 0 {
		f = defaultBoundedLoadFactor
	}
	if math.IsNaN(f) || math.IsInf(f, 0) || f < 1 {
		return 0, 0, fmt.Errorf("lb: bounded_load_factor must be a finite number of at least 1: %v", f)
	}
	return vn, f, nil
}

const inlineWords = 4

type candidates []uint64

// snapshot evaluates eligibility once per endpoint; Pick decides only from the returned set.
func snapshot(inline *[inlineWords]uint64, eps []Endpoint, in Input) candidates {
	n := (len(eps) + 63) >> 6
	var c candidates
	if n <= inlineWords {
		c = inline[:n]
	} else {
		c = make(candidates, n)
	}
	for i := range eps {
		if eps[i].Weight > 0 && (in.Eligible == nil || in.Eligible(i)) {
			c[i>>6] |= 1 << (uint(i) & 63)
		}
	}
	return c
}

func (c candidates) has(i int) bool {
	return c[i>>6]&(1<<(uint(i)&63)) != 0
}

func (c candidates) count() int {
	m := 0
	for _, w := range c {
		m += bits.OnesCount64(w)
	}
	return m
}

func inflight(e Endpoint) int64 {
	if e.InFlight == nil {
		return 0
	}
	return e.InFlight()
}

type roundRobin struct {
	eps []Endpoint
	n   atomic.Uint64
}

func (r *roundRobin) Pick(in Input) (int, bool) {
	var inline [inlineWords]uint64
	cand := snapshot(&inline, r.eps, in)
	m := cand.count()
	if m == 0 {
		return -1, false
	}
	k := (r.n.Add(1) - 1) % uint64(m)
	for i := range r.eps {
		if !cand.has(i) {
			continue
		}
		if k == 0 {
			return i, true
		}
		k--
	}
	return -1, false
}

type smoothWRR struct {
	mu  sync.Mutex
	eps []Endpoint
	cur []int64
}

func (s *smoothWRR) Pick(in Input) (int, bool) {
	var inline [inlineWords]uint64
	cand := snapshot(&inline, s.eps, in)
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	best := -1
	for i := range s.eps {
		if !cand.has(i) {
			continue
		}
		w := int64(s.eps[i].Weight)
		s.cur[i] += w
		total += w
		if best < 0 || s.cur[i] > s.cur[best] {
			best = i
		}
	}
	if best < 0 {
		return -1, false
	}
	s.cur[best] -= total
	return best, true
}

type p2c struct {
	eps []Endpoint
	rnd clock.Rand
}

func (p *p2c) Pick(in Input) (int, bool) {
	var inline [inlineWords]uint64
	cand := snapshot(&inline, p.eps, in)
	m := cand.count()
	if m == 0 {
		return -1, false
	}
	if m == 1 {
		for i := range p.eps {
			if cand.has(i) {
				return i, true
			}
		}
	}
	a := p.rnd.Int64N(int64(m))
	b := p.rnd.Int64N(int64(m - 1))
	if b >= a {
		b++
	}
	ia, ib := -1, -1
	var k int64
	for i := range p.eps {
		if !cand.has(i) {
			continue
		}
		if k == a {
			ia = i
		}
		if k == b {
			ib = i
		}
		k++
	}
	ea, eb := p.eps[ia], p.eps[ib]
	if inflight(ea)*int64(eb.Weight) <= inflight(eb)*int64(ea.Weight) {
		return ia, true
	}
	return ib, true
}

type weightedRandom struct {
	eps []Endpoint
	rnd clock.Rand
}

func (w *weightedRandom) Pick(in Input) (int, bool) {
	return weightedPick(w.eps, in, w.rnd)
}

func weightedPick(eps []Endpoint, in Input, rnd clock.Rand) (int, bool) {
	var inline [inlineWords]uint64
	cand := snapshot(&inline, eps, in)
	var total int64
	for i := range eps {
		if cand.has(i) {
			total += int64(eps[i].Weight)
		}
	}
	if total == 0 {
		return -1, false
	}
	r := rnd.Int64N(total)
	last := -1
	for i := range eps {
		if !cand.has(i) {
			continue
		}
		r -= int64(eps[i].Weight)
		if r < 0 {
			return i, true
		}
		last = i
	}
	return last, last >= 0
}
