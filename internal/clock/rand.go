package clock

import (
	"math/rand/v2"
	"sync"
)

// Rand is a source of pseudo-random numbers that is safe for concurrent use.
type Rand interface {
	// Int64N returns a pseudo-random value in [0, n). It panics if n <= 0.
	Int64N(n int64) int64
	// Float64 returns a pseudo-random value in [0.0, 1.0).
	Float64() float64
}

// NewRand returns a Rand that produces the same sequence for the same seed.
func NewRand(seed uint64) Rand {
	return &lockedRand{r: rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}

type lockedRand struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (l *lockedRand) Int64N(n int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Int64N(n)
}

func (l *lockedRand) Float64() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Float64()
}
