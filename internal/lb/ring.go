package lb

import (
	"math"
	"sort"
	"strconv"

	"github.com/ok0/lychgate/internal/clock"
)

func hash64(s string) uint64 {
	const (
		offset = 0xcbf29ce484222325
		prime  = 0x100000001b3
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

type point struct {
	hash uint64
	ep   int
}

type consistentHash struct {
	eps    []Endpoint
	points []point
	factor float64
	rnd    clock.Rand
}

// newConsistentHash builds a ring of VirtualNodes*Weight points per endpoint with a positive weight. With V points per unit of weight a host's share of the key space deviates from its weight share by about 1/sqrt(V) relative (about 8% at the default 160 virtual nodes); removing a host moves only the keys it owned.
func newConsistentHash(eps []Endpoint, vn int, factor float64, rnd clock.Rand) *consistentHash {
	var pts []point
	for i := range eps {
		if eps[i].Weight <= 0 {
			continue
		}
		for j := 0; j < vn*eps[i].Weight; j++ {
			pts = append(pts, point{hash: hash64(eps[i].ID + "#" + strconv.Itoa(j)), ep: i})
		}
	}
	sort.Slice(pts, func(a, b int) bool {
		if pts[a].hash != pts[b].hash {
			return pts[a].hash < pts[b].hash
		}
		return pts[a].ep < pts[b].ep
	})
	return &consistentHash{eps: eps, points: pts, factor: factor, rnd: rnd}
}

// Pick walks the ring clockwise from the key's hash over the endpoints eligible at the start of the call. A host is skipped when this request would push its in-flight count above the bound; if every eligible host is over the bound, the first one on the walk is returned.
func (c *consistentHash) Pick(in Input) (int, bool) {
	if !in.HasKey {
		return weightedPick(c.eps, in, c.rnd)
	}
	var inline [inlineWords]uint64
	cand := snapshot(&inline, c.eps, in)
	m := 0
	var total int64
	for i := range c.eps {
		if cand.has(i) {
			m++
			total += inflight(c.eps[i])
		}
	}
	if m == 0 {
		return -1, false
	}
	bound := int64(math.Ceil(c.factor * float64(total+1) / float64(m)))
	h := hash64(in.Key)
	n := len(c.points)
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if c.points[mid].hash < h {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	first := -1
	for j := 0; j < n; j++ {
		i := c.points[(lo+j)%n].ep
		if !cand.has(i) {
			continue
		}
		if inflight(c.eps[i])+1 <= bound {
			return i, true
		}
		if first < 0 {
			first = i
		}
	}
	return first, first >= 0
}
