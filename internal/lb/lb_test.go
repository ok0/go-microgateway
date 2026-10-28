package lb

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
)

var algorithms = []string{"p2c", "round_robin", "weighted_round_robin", "random", "consistent_hash"}

func zeroInFlight() int64 { return 0 }

func mkEps(ws ...int) []Endpoint {
	eps := make([]Endpoint, len(ws))
	for i, w := range ws {
		eps[i] = Endpoint{ID: fmt.Sprintf("h%d", i), Weight: w}
	}
	return eps
}

func mkInFlightEps(ws []int, ins []int64) []Endpoint {
	eps := make([]Endpoint, len(ws))
	for i := range ws {
		v := ins[i]
		eps[i] = Endpoint{ID: fmt.Sprintf("h%d", i), Weight: ws[i], InFlight: func() int64 { return v }}
	}
	return eps
}

func mkURLEps(n int) []Endpoint {
	eps := make([]Endpoint, n)
	for i := range eps {
		eps[i] = Endpoint{ID: fmt.Sprintf("http://h%d:80", i), Weight: 1}
	}
	return eps
}

func mustNew(t *testing.T, algo string, eps []Endpoint, seed uint64) Balancer {
	t.Helper()
	b, err := New(config.LoadBalancer{Algorithm: algo}, eps, clock.NewRand(seed))
	if err != nil {
		t.Fatalf("New(%s): %v", algo, err)
	}
	return b
}

func mustPick(t *testing.T, b Balancer, in Input) int {
	t.Helper()
	i, ok := b.Pick(in)
	if !ok {
		t.Fatalf("Pick found no candidate")
	}
	return i
}

func tally(t *testing.T, b Balancer, in Input, n, size int) []int {
	t.Helper()
	counts := make([]int, size)
	for k := 0; k < n; k++ {
		counts[mustPick(t, b, in)]++
	}
	return counts
}

func TestNewErrors(t *testing.T) {
	rnd := clock.NewRand(1)
	eps := mkEps(1, 1)

	if _, err := New(config.LoadBalancer{Algorithm: "nope"}, eps, rnd); !errors.Is(err, ErrUnknownAlgorithm) {
		t.Fatalf("unknown algorithm: got %v", err)
	}
	if _, err := New(config.LoadBalancer{Algorithm: "consistent_hash", VirtualNodes: -1}, eps, rnd); err == nil {
		t.Fatal("negative virtual nodes accepted")
	}
	if _, err := New(config.LoadBalancer{Algorithm: "consistent_hash", BoundedLoadFactor: 0.5}, eps, rnd); err == nil {
		t.Fatal("bounded load factor 0.5 accepted")
	}
	if _, err := New(config.LoadBalancer{Algorithm: "consistent_hash", BoundedLoadFactor: math.NaN()}, eps, rnd); err == nil {
		t.Fatal("NaN bounded load factor accepted")
	}
	if _, err := New(config.LoadBalancer{Algorithm: "consistent_hash", BoundedLoadFactor: math.Inf(1)}, eps, rnd); err == nil {
		t.Fatal("infinite bounded load factor accepted")
	}
	if _, err := New(config.LoadBalancer{}, eps, nil); err == nil {
		t.Fatal("p2c built without a random source")
	}
	if _, err := New(config.LoadBalancer{Algorithm: "random"}, eps, nil); err == nil {
		t.Fatal("random built without a random source")
	}
	if _, err := New(config.LoadBalancer{Algorithm: "consistent_hash"}, eps, nil); err == nil {
		t.Fatal("consistent_hash built without a random source")
	}
	if _, err := New(config.LoadBalancer{Algorithm: "round_robin"}, eps, nil); err != nil {
		t.Fatalf("round_robin needs no random source: %v", err)
	}
	if _, err := New(config.LoadBalancer{Algorithm: "weighted_round_robin"}, eps, nil); err != nil {
		t.Fatalf("weighted_round_robin needs no random source: %v", err)
	}

	b, err := New(config.LoadBalancer{}, eps, rnd)
	if err != nil {
		t.Fatalf("default algorithm: %v", err)
	}
	if _, ok := b.(*p2c); !ok {
		t.Fatalf("default algorithm is %T, want *p2c", b)
	}

	b, err = New(config.LoadBalancer{Algorithm: "consistent_hash"}, mkEps(0, 1), rnd)
	if err != nil {
		t.Fatalf("consistent_hash defaults: %v", err)
	}
	ch, ok := b.(*consistentHash)
	if !ok {
		t.Fatalf("consistent_hash is %T", b)
	}
	if ch.factor != 1.25 {
		t.Fatalf("default bounded load factor = %v, want 1.25", ch.factor)
	}
	if len(ch.points) != 160 {
		t.Fatalf("default ring has %d points, want 160 for one weight-1 endpoint", len(ch.points))
	}
}

func TestNewCopiesEndpoints(t *testing.T) {
	eps := mkEps(1, 1)
	b := mustNew(t, "round_robin", eps, 1)
	eps[0].Weight = 0
	eps[1].Weight = 0
	if _, ok := b.Pick(Input{}); !ok {
		t.Fatal("balancer observed caller's later modification of endpoints")
	}
}

func TestRoundRobin(t *testing.T) {
	b := mustNew(t, "round_robin", mkEps(1, 5, 0), 1)
	counts := tally(t, b, Input{}, 10000, 3)
	if counts[0] != 5000 || counts[1] != 5000 || counts[2] != 0 {
		t.Fatalf("weights {1,5,0}: counts %v, want [5000 5000 0]", counts)
	}

	b = mustNew(t, "round_robin", mkEps(1, 1, 1, 1), 1)
	in := Input{Eligible: func(i int) bool { return i != 2 }}
	counts = tally(t, b, in, 9999, 4)
	if counts[0] != 3333 || counts[1] != 3333 || counts[2] != 0 || counts[3] != 3333 {
		t.Fatalf("ineligible ep2: counts %v, want [3333 3333 0 3333]", counts)
	}
}

func TestSmoothWRR(t *testing.T) {
	for _, ws := range [][]int{{5, 1, 1}, {1, 1, 1}, {3, 2, 1, 0}, {10, 1}} {
		checkSmooth(t, ws, Input{})
		if len(ws) > 2 {
			checkSmooth(t, ws, Input{Eligible: func(i int) bool { return i != 1 }})
		}
	}
}

func checkSmooth(t *testing.T, ws []int, in Input) {
	t.Helper()
	const n = 10000
	eff := make([]int, len(ws))
	total := 0
	for i, w := range ws {
		if in.Eligible == nil || in.Eligible(i) {
			eff[i] = w
		}
		total += eff[i]
	}
	b := mustNew(t, "weighted_round_robin", mkEps(ws...), 1)
	seq := make([]int, n)
	counts := make([]int, len(ws))
	for k := range seq {
		seq[k] = mustPick(t, b, in)
		counts[seq[k]]++
	}
	for i, w := range eff {
		if d := math.Abs(float64(counts[i])/n - float64(w)/float64(total)); d > 0.01 {
			t.Errorf("weights %v: endpoint %d share %.4f, want %.4f", ws, i, float64(counts[i])/n, float64(w)/float64(total))
		}
		if w == 0 && counts[i] != 0 {
			t.Errorf("weights %v: zero-weight endpoint %d picked %d times", ws, i, counts[i])
		}
	}
	for start := 0; start+total <= n; start += total {
		blk := make([]int, len(ws))
		for _, i := range seq[start : start+total] {
			blk[i]++
		}
		for i, w := range eff {
			if blk[i] != w {
				t.Fatalf("weights %v: block at %d gives endpoint %d %d picks, want %d", ws, start, i, blk[i], w)
			}
		}
	}
	heaviest := 0
	for i, w := range eff {
		if w > eff[heaviest] {
			heaviest = i
		}
	}
	w := eff[heaviest]
	limit := (w+(total-w)-1)/(total-w) + 1
	longest, run := 0, 0
	for _, i := range seq {
		if i == heaviest {
			run++
			longest = max(longest, run)
		} else {
			run = 0
		}
	}
	if longest > limit {
		t.Errorf("weights %v: longest run of heaviest %d exceeds %d", ws, longest, limit)
	}
}

func TestP2C(t *testing.T) {
	b := mustNew(t, "p2c", mkEps(0, 3, 0), 1)
	for k := 0; k < 100; k++ {
		if i := mustPick(t, b, Input{}); i != 1 {
			t.Fatalf("single candidate: picked %d", i)
		}
	}

	b = mustNew(t, "p2c", mkEps(1, 1, 1), 1)
	in := Input{Eligible: func(i int) bool { return i == 2 }}
	for k := 0; k < 100; k++ {
		if i := mustPick(t, b, in); i != 2 {
			t.Fatalf("single eligible candidate: picked %d", i)
		}
	}

	b = mustNew(t, "p2c", mkInFlightEps([]int{1, 1}, []int64{5, 0}), 1)
	for k := 0; k < 200; k++ {
		if i := mustPick(t, b, Input{}); i != 1 {
			t.Fatalf("inflight {5,0}: picked %d, want 1", i)
		}
	}

	b = mustNew(t, "p2c", mkInFlightEps([]int{4, 1}, []int64{4, 2}), 1)
	for k := 0; k < 200; k++ {
		if i := mustPick(t, b, Input{}); i != 0 {
			t.Fatalf("weight 4/inflight 4 vs weight 1/inflight 2: picked %d, want 0", i)
		}
	}

	eps := mkEps(1, 2, 3, 4)
	b1 := mustNew(t, "p2c", eps, 7)
	b2 := mustNew(t, "p2c", eps, 7)
	for k := 0; k < 1000; k++ {
		if x, y := mustPick(t, b1, Input{}), mustPick(t, b2, Input{}); x != y {
			t.Fatalf("pick %d differs between seeded balancers: %d vs %d", k, x, y)
		}
	}

	b = mustNew(t, "p2c", mkEps(1, 1, 1, 1), 3)
	const n = 10000
	counts := tally(t, b, Input{}, n, 4)
	for i, c := range counts {
		if d := math.Abs(float64(c)/n - 0.25); d > 0.02 {
			t.Fatalf("equal weights: endpoint %d share %.4f, want 0.25", i, float64(c)/n)
		}
	}
}

func TestRandomWeighted(t *testing.T) {
	b := mustNew(t, "random", mkEps(5, 1, 1, 0), 42)
	const n = 10000
	counts := tally(t, b, Input{}, n, 4)
	want := []float64{5.0 / 7, 1.0 / 7, 1.0 / 7, 0}
	for i, w := range want {
		if d := math.Abs(float64(counts[i])/n - w); d > 0.02 {
			t.Errorf("endpoint %d share %.4f, want %.4f", i, float64(counts[i])/n, w)
		}
	}
	if counts[3] != 0 {
		t.Errorf("zero-weight endpoint picked %d times", counts[3])
	}
}

func pickID(t *testing.T, b Balancer, eps []Endpoint, key string) string {
	t.Helper()
	return eps[mustPick(t, b, Input{Key: key, HasKey: true})].ID
}

func ringShares(c *consistentHash) []float64 {
	const space = 18446744073709551616.0
	shares := make([]float64, len(c.eps))
	n := len(c.points)
	for k := range c.points {
		arc := c.points[k].hash - c.points[(k-1+n)%n].hash
		shares[c.points[k].ep] += float64(arc) / space
	}
	return shares
}

func TestConsistentHashRingBalance(t *testing.T) {
	families := []struct {
		name string
		id   func(i int) string
	}{
		{"zero-based", func(i int) string { return fmt.Sprintf("http://h%d:80", i) }},
		{"one-based", func(i int) string { return fmt.Sprintf("http://srv%d.mem:80", i+1) }},
	}
	weightSets := [][]int{{1, 1, 1, 1, 1}, {1, 1, 1, 1, 1, 1, 1, 1}, {3, 1, 1, 1, 1}}
	for _, fam := range families {
		for _, ws := range weightSets {
			eps := make([]Endpoint, len(ws))
			total := 0
			for i, w := range ws {
				eps[i] = Endpoint{ID: fam.id(i), Weight: w}
				total += w
			}
			b, err := New(config.LoadBalancer{Algorithm: "consistent_hash"}, eps, clock.NewRand(1))
			if err != nil {
				t.Fatal(err)
			}
			// tolerance is about 3x the 1/sqrt(160) relative deviation
			shares := ringShares(b.(*consistentHash))
			for i, w := range ws {
				want := float64(w) / float64(total)
				if shares[i] < 0.75*want || shares[i] > 1.25*want {
					t.Errorf("%s %v: endpoint %d ring share %.4f, want within [%.4f, %.4f]", fam.name, ws, i, shares[i], 0.75*want, 1.25*want)
				}
			}
		}
	}
}

func TestConsistentHashStability(t *testing.T) {
	const n = 10000
	cfg := config.LoadBalancer{Algorithm: "consistent_hash"}
	for _, size := range []int{5, 8} {
		full := mkURLEps(size)
		bFull, err := New(cfg, full, clock.NewRand(1))
		if err != nil {
			t.Fatal(err)
		}
		fullIDs := make([]string, n)
		for k := range fullIDs {
			key := fmt.Sprintf("key-%d", k)
			fullIDs[k] = pickID(t, bFull, full, key)
			if again := pickID(t, bFull, full, key); again != fullIDs[k] {
				t.Fatalf("size %d: key %s repeated pick %s, first %s", size, key, again, fullIDs[k])
			}
		}

		for r := range full {
			removed := full[r].ID
			less := make([]Endpoint, 0, size-1)
			for _, e := range full {
				if e.ID != removed {
					less = append(less, e)
				}
			}
			bLess, err := New(cfg, less, clock.NewRand(1))
			if err != nil {
				t.Fatal(err)
			}
			moved, unchanged := 0, 0
			for k := 0; k < n; k++ {
				key := fmt.Sprintf("key-%d", k)
				before := fullIDs[k]
				after := pickID(t, bLess, less, key)
				if before == removed {
					if after == removed {
						t.Fatalf("size %d: key %s still maps to removed host %s", size, key, removed)
					}
					moved++
					continue
				}
				if before != after {
					t.Fatalf("size %d: key %s moved from %s to %s though its host stayed", size, key, before, after)
				}
				unchanged++
			}
			if frac := float64(moved) / n; frac > 1.25/float64(size) {
				t.Fatalf("size %d: removing %s moved %.4f of keys, want <= %.4f", size, removed, frac, 1.25/float64(size))
			}
			if size == 8 {
				if frac := float64(unchanged) / n; frac < 0.80 {
					t.Fatalf("size %d: removing %s left only %.4f of keys unchanged", size, removed, frac)
				}
			}
		}

		other, err := New(cfg, full, clock.NewRand(99))
		if err != nil {
			t.Fatal(err)
		}
		for k := 0; k < n; k++ {
			key := fmt.Sprintf("key-%d", k)
			if a := pickID(t, other, full, key); a != fullIDs[k] {
				t.Fatalf("size %d: key %s maps to %s and %s across balancers", size, key, fullIDs[k], a)
			}
		}
	}

	five := mkURLEps(5)
	six := mkURLEps(6)
	bFive, err := New(cfg, five, clock.NewRand(1))
	if err != nil {
		t.Fatal(err)
	}
	bSix, err := New(cfg, six, clock.NewRand(1))
	if err != nil {
		t.Fatal(err)
	}
	const added = "http://h5:80"
	for k := 0; k < n; k++ {
		key := fmt.Sprintf("key-%d", k)
		before := pickID(t, bFive, five, key)
		after := pickID(t, bSix, six, key)
		if before != after && after != added {
			t.Fatalf("key %s moved from %s to %s on adding %s", key, before, after, added)
		}
	}
}

func TestConsistentHashSkipsIneligible(t *testing.T) {
	full := mkURLEps(5)
	cfg := config.LoadBalancer{Algorithm: "consistent_hash"}
	b, err := New(cfg, full, clock.NewRand(1))
	if err != nil {
		t.Fatal(err)
	}
	in := Input{Eligible: func(i int) bool { return i != 1 }}
	for k := 0; k < 10000; k++ {
		key := fmt.Sprintf("key-%d", k)
		before := mustPick(t, b, Input{Key: key, HasKey: true})
		after := mustPick(t, b, Input{Key: key, HasKey: true, Eligible: in.Eligible})
		if after == 1 {
			t.Fatalf("key %s picked the ineligible endpoint", key)
		}
		if before != 1 && before != after {
			t.Fatalf("key %s moved from %d to %d though its endpoint stayed eligible", key, before, after)
		}
	}
}

func TestConsistentHashBoundedLoad(t *testing.T) {
	const m = 5
	inflights := make([]atomic.Int64, m)
	eps := make([]Endpoint, m)
	for i := range eps {
		eps[i] = Endpoint{
			ID:       fmt.Sprintf("http://h%d:80", i),
			Weight:   1,
			InFlight: func() int64 { return inflights[i].Load() },
		}
	}
	b, err := New(config.LoadBalancer{Algorithm: "consistent_hash"}, eps, clock.NewRand(3))
	if err != nil {
		t.Fatal(err)
	}
	rnd := clock.NewRand(5)
	for it := 0; it < 2000; it++ {
		key := "hot"
		if rnd.Float64() >= 0.9 {
			key = fmt.Sprintf("cold-%d", it)
		}
		idx := mustPick(t, b, Input{Key: key, HasKey: true})
		inflights[idx].Add(1)
		var total int64
		for i := range inflights {
			total += inflights[i].Load()
		}
		bound := int64(math.Ceil(1.25 * float64(total) / float64(m)))
		for i := range inflights {
			if v := inflights[i].Load(); v > bound {
				t.Fatalf("iteration %d: endpoint %d in-flight %d exceeds bound %d (total %d)", it, i, v, bound, total)
			}
		}
		if rnd.Float64() < 0.5 {
			j := rnd.Int64N(m)
			if inflights[j].Load() > 0 {
				inflights[j].Add(-1)
			}
		}
	}
}

func TestConsistentHashNoKey(t *testing.T) {
	eps := mkEps(2, 0, 1)
	run := func() []int {
		b := mustNew(t, "consistent_hash", eps, 9)
		seq := make([]int, 1000)
		for k := range seq {
			seq[k] = mustPick(t, b, Input{})
		}
		return seq
	}
	first := run()
	for k, i := range first {
		if i == 1 {
			t.Fatalf("pick %d chose the zero-weight endpoint", k)
		}
	}
	second := run()
	for k := range first {
		if first[k] != second[k] {
			t.Fatalf("pick %d differs between seeded runs", k)
		}
	}
}

func TestNoCandidates(t *testing.T) {
	none := func(int) bool { return false }
	for _, algo := range algorithms {
		b := mustNew(t, algo, mkEps(0, 0, 0), 1)
		if _, ok := b.Pick(Input{Key: "k", HasKey: true}); ok {
			t.Errorf("%s: all weight 0 returned a candidate", algo)
		}
		b = mustNew(t, algo, mkEps(1, 1), 1)
		if _, ok := b.Pick(Input{Key: "k", HasKey: true, Eligible: none}); ok {
			t.Errorf("%s: all ineligible returned a candidate", algo)
		}
		if _, ok := b.Pick(Input{Eligible: none}); ok {
			t.Errorf("%s: all ineligible without key returned a candidate", algo)
		}
	}
}

type phased struct {
	snap, later func(int) bool
	calls       []int
}

func newPhased(n int, snap, later func(int) bool) *phased {
	return &phased{snap: snap, later: later, calls: make([]int, n)}
}

func (p *phased) eligible(i int) bool {
	p.calls[i]++
	if p.calls[i] == 1 {
		return p.snap(i)
	}
	return p.later(i)
}

func (p *phased) assertOnce(t *testing.T, label string) {
	t.Helper()
	for i, c := range p.calls {
		if c > 1 {
			t.Fatalf("%s: Eligible(%d) called %d times in one Pick, want at most 1", label, i, c)
		}
	}
}

func only(set ...int) func(int) bool {
	return func(i int) bool {
		for _, s := range set {
			if s == i {
				return true
			}
		}
		return false
	}
}

func TestShrinkingEligibility(t *testing.T) {
	none := func(int) bool { return false }

	t.Run("sets", func(t *testing.T) {
		for _, algo := range algorithms {
			for _, hasKey := range []bool{false, true} {
				for seed := 0; seed < 200; seed++ {
					label := fmt.Sprintf("%s hasKey=%v seed=%d", algo, hasKey, seed)
					b := mustNew(t, algo, mkEps(1, 1, 1, 1), uint64(seed))
					key := fmt.Sprintf("k-%d", seed)

					p := newPhased(4, only(0, 1, 2, 3), only(0))
					i, ok := b.Pick(Input{Key: key, HasKey: hasKey, Eligible: p.eligible})
					p.assertOnce(t, label+" shrink")
					if !ok || i < 0 || i >= 4 {
						t.Fatalf("%s shrink: got (%d, %v), want index in [0,4) and true", label, i, ok)
					}
					if i, ok := b.Pick(Input{Key: key, HasKey: hasKey, Eligible: only(0)}); !ok || i != 0 {
						t.Fatalf("%s after shrink to {0}: got (%d, %v), want (0, true)", label, i, ok)
					}
					if i, ok := b.Pick(Input{Key: key, HasKey: hasKey, Eligible: none}); ok {
						t.Fatalf("%s after shrink to none: got (%d, true), want (-1, false)", label, i)
					}

					p = newPhased(4, only(1, 2), only(0, 3))
					i, ok = b.Pick(Input{Key: key, HasKey: hasKey, Eligible: p.eligible})
					p.assertOnce(t, label+" grow")
					if !ok || (i != 1 && i != 2) {
						t.Fatalf("%s grow: got (%d, %v), want index in {1,2} and true", label, i, ok)
					}
					if i, ok := b.Pick(Input{Key: key, HasKey: hasKey, Eligible: only(0, 3)}); !ok || (i != 0 && i != 3) {
						t.Fatalf("%s after grow to {0,3}: got (%d, %v), want index in {0,3} and true", label, i, ok)
					}

					p = newPhased(4, only(2), none)
					i, ok = b.Pick(Input{Key: key, HasKey: hasKey, Eligible: p.eligible})
					p.assertOnce(t, label+" vanish")
					if !ok || i != 2 {
						t.Fatalf("%s vanish: got (%d, %v), want (2, true)", label, i, ok)
					}
					if i, ok := b.Pick(Input{Key: key, HasKey: hasKey, Eligible: none}); ok {
						t.Fatalf("%s after vanish: got (%d, true), want (-1, false)", label, i)
					}
				}
			}
		}
	})

	t.Run("round_robin sequence", func(t *testing.T) {
		b := mustNew(t, "round_robin", mkEps(1, 1, 1), 1)
		p := newPhased(3, only(0, 1, 2), only(0))
		first, _ := b.Pick(Input{Eligible: p.eligible})
		p.assertOnce(t, "round_robin first")
		second, _ := b.Pick(Input{Eligible: only(0)})
		third, _ := b.Pick(Input{})
		if got := [3]int{first, second, third}; got != [3]int{0, 0, 2} {
			t.Fatalf("round_robin sequence: got %v, want [0 0 2]", got)
		}
	})

	t.Run("weighted_round_robin sequence", func(t *testing.T) {
		b := mustNew(t, "weighted_round_robin", mkEps(1, 1, 1), 1)
		p := newPhased(3, only(0, 1, 2), only(0))
		first, _ := b.Pick(Input{Eligible: p.eligible})
		p.assertOnce(t, "weighted_round_robin first")
		second, _ := b.Pick(Input{Eligible: only(0)})
		third, _ := b.Pick(Input{})
		if got := [3]int{first, second, third}; got != [3]int{0, 0, 1} {
			t.Fatalf("weighted_round_robin sequence: got %v, want [0 0 1]", got)
		}
	})

	t.Run("p2c", func(t *testing.T) {
		for seed := 0; seed < 200; seed++ {
			label := fmt.Sprintf("p2c seed=%d", seed)
			b := mustNew(t, "p2c", mkInFlightEps([]int{1, 1}, []int64{5, 0}), uint64(seed))

			p := newPhased(2, only(0, 1), only(0))
			i, ok := b.Pick(Input{Eligible: p.eligible})
			p.assertOnce(t, label+" snapshot all")
			if !ok || i != 1 {
				t.Fatalf("%s snapshot all: got (%d, %v), want (1, true)", label, i, ok)
			}
			if i, ok := b.Pick(Input{Eligible: only(0)}); !ok || i != 0 {
				t.Fatalf("%s static {0}: got (%d, %v), want (0, true)", label, i, ok)
			}

			p = newPhased(2, only(0), only(0, 1))
			i, ok = b.Pick(Input{Eligible: p.eligible})
			p.assertOnce(t, label+" snapshot {0}")
			if !ok || i != 0 {
				t.Fatalf("%s snapshot {0}: got (%d, %v), want (0, true)", label, i, ok)
			}
		}
	})

	t.Run("consistent_hash bound", func(t *testing.T) {
		ch := mustNew(t, "consistent_hash", mkInFlightEps([]int{1, 1}, []int64{0, 10}), 1)
		key := Input{Key: "k", HasKey: true}

		p := newPhased(2, only(1), only(0, 1))
		in := key
		in.Eligible = p.eligible
		i, ok := ch.Pick(in)
		p.assertOnce(t, "consistent_hash snapshot {1}")
		if !ok || i != 1 {
			t.Fatalf("consistent_hash snapshot {1}: got (%d, %v), want (1, true)", i, ok)
		}

		in = key
		in.Eligible = only(0, 1)
		if i, ok := ch.Pick(in); !ok || i != 0 {
			t.Fatalf("consistent_hash static all: got (%d, %v), want (0, true)", i, ok)
		}

		p = newPhased(2, only(0, 1), only(1))
		in = key
		in.Eligible = p.eligible
		i, ok = ch.Pick(in)
		p.assertOnce(t, "consistent_hash snapshot all")
		if !ok || i != 0 {
			t.Fatalf("consistent_hash snapshot all: got (%d, %v), want (0, true)", i, ok)
		}

		p = newPhased(2, none, only(0, 1))
		in = key
		in.Eligible = p.eligible
		if i, ok := ch.Pick(in); ok {
			t.Fatalf("consistent_hash snapshot none: got (%d, true), want (-1, false)", i)
		}
		p.assertOnce(t, "consistent_hash snapshot none")
	})
}

func TestPickAllocs(t *testing.T) {
	ws := make([]int, 16)
	for i := range ws {
		ws[i] = 1 + i%3
	}
	eps := mkEps(ws...)
	for i := range eps {
		eps[i].InFlight = zeroInFlight
	}
	in := Input{Eligible: func(i int) bool { return i != 3 }, Key: "user-1", HasKey: true}
	for _, algo := range algorithms {
		b := mustNew(t, algo, eps, 1)
		allocs := testing.AllocsPerRun(1000, func() {
			_, _ = b.Pick(in)
		})
		if allocs != 0 {
			t.Errorf("%s: %v allocs per pick, want 0", algo, allocs)
		}
	}
}

func TestConcurrentPick(t *testing.T) {
	ws := []int{1, 2, 3, 4, 5, 6, 7, 8}
	eps := mkEps(ws...)
	for _, algo := range algorithms {
		b := mustNew(t, algo, eps, 1)
		var wg sync.WaitGroup
		for g := 0; g < 64; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				in := Input{Key: fmt.Sprintf("key-%d", g), HasKey: true, Eligible: func(i int) bool { return i != 5 }}
				for k := 0; k < 1000; k++ {
					i, ok := b.Pick(in)
					if !ok || i < 0 || i >= len(eps) {
						t.Errorf("%s: pick returned (%d, %v)", algo, i, ok)
						return
					}
				}
			}(g)
		}
		wg.Wait()
	}
}

func TestConstructorTakesNoSharedHandle(t *testing.T) {
	ft := reflect.TypeOf(New)
	if ft.NumIn() != 3 {
		t.Fatalf("New takes %d parameters, want 3", ft.NumIn())
	}
	if got, want := ft.In(0), reflect.TypeOf(config.LoadBalancer{}); got != want {
		t.Errorf("parameter 0 is %v, want %v", got, want)
	}
	if got, want := ft.In(1), reflect.TypeOf([]Endpoint(nil)); got != want {
		t.Errorf("parameter 1 is %v, want %v", got, want)
	}
	if got, want := ft.In(2), reflect.TypeOf((*clock.Rand)(nil)).Elem(); got != want {
		t.Errorf("parameter 2 is %v, want %v", got, want)
	}
}
