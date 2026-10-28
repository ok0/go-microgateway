package retry

import (
	"sync"
	"time"

	"github.com/ok0/lychgate/internal/clock"
)

const bucketCount = 10

type budgetBucket struct {
	idx      int64
	requests int64
	retries  int64
}

// Budget limits retries to a fraction of recent requests plus a per-second floor over a sliding window.
type Budget struct {
	mu           sync.Mutex
	clk          clock.Clock
	origin       time.Time
	ratio        float64
	minPerSecond float64
	window       time.Duration
	width        time.Duration
	buckets      []budgetBucket
}

// NewBudget returns a Budget that allows retries up to ratio of the requests seen in window,
// plus minPerSecond over the window.
func NewBudget(ratio, minPerSecond float64, window time.Duration, clk clock.Clock) *Budget {
	n := bucketCount
	width := window / bucketCount
	if width == 0 {
		n = 1
		width = window
	}
	return &Budget{
		clk:          clk,
		origin:       clk.Now(),
		ratio:        ratio,
		minPerSecond: minPerSecond,
		window:       window,
		width:        width,
		buckets:      make([]budgetBucket, n),
	}
}

// Deposit records one request.
func (b *Budget) Deposit() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.current(b.index(b.clk.Now())).requests++
}

// Withdraw reserves one retry. When allowed, it returns the slot of the bucket that recorded
// the reservation, which the caller passes to Refund.
func (b *Budget) Withdraw() (slot int64, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur := b.index(b.clk.Now())
	requests, retries := b.sumLocked(cur)
	allowed := b.minPerSecond*b.window.Seconds() + b.ratio*float64(requests)
	if float64(retries+1) > allowed {
		return 0, false
	}
	b.current(cur).retries++
	return cur, true
}

// Refund returns the reservation recorded in slot by a successful Withdraw. A reservation whose
// bucket has aged out of the window is not counted, so Refund is a no-op for it. Refund at most
// once per successful Withdraw.
func (b *Budget) Refund(slot int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur := b.index(b.clk.Now())
	n := int64(len(b.buckets))
	bk := &b.buckets[((slot%n)+n)%n]
	if bk.idx == slot && cur-slot < n && bk.retries > 0 {
		bk.retries--
	}
}

// Stats returns the request and retry counts inside the window.
func (b *Budget) Stats() (requests, retries int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sumLocked(b.index(b.clk.Now()))
}

func (b *Budget) index(now time.Time) int64 {
	return int64(now.Sub(b.origin) / b.width)
}

func (b *Budget) current(cur int64) *budgetBucket {
	n := int64(len(b.buckets))
	bk := &b.buckets[((cur%n)+n)%n]
	if bk.idx != cur {
		*bk = budgetBucket{idx: cur}
	}
	return bk
}

func (b *Budget) forEachInWindow(cur int64, fn func(*budgetBucket)) {
	n := int64(len(b.buckets))
	for i := range b.buckets {
		bk := &b.buckets[i]
		if bk.idx <= cur && cur-bk.idx < n {
			fn(bk)
		}
	}
}

func (b *Budget) sumLocked(cur int64) (requests, retries int64) {
	b.forEachInWindow(cur, func(bk *budgetBucket) {
		requests += bk.requests
		retries += bk.retries
	})
	return requests, retries
}
