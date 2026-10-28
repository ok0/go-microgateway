package lb

import (
	"fmt"
	"testing"
)

type flipper struct {
	first func(i int) bool
	calls []int
}

func newFlipper(n int, first func(i int) bool) *flipper {
	return &flipper{first: first, calls: make([]int, n)}
}

func (f *flipper) eligible(i int) bool {
	f.calls[i]++
	return f.calls[i] == 1 && f.first(i)
}

func (f *flipper) checkOnce(t *testing.T, algo string) {
	t.Helper()
	for i, c := range f.calls {
		if c > 1 {
			t.Fatalf("%s: Eligible(%d) called %d times in one Pick, want at most 1", algo, i, c)
		}
	}
}

func TestPickUsesSingleEligibilitySnapshot(t *testing.T) {
	const n = 6
	for _, algo := range algorithms {
		for _, hasKey := range []bool{true, false} {
			for seed := 0; seed < 200; seed++ {
				eps := mkEps(1, 1, 1, 1, 1, 1)
				b := mustNew(t, algo, eps, uint64(seed))
				f := newFlipper(n, func(i int) bool { return i%2 == 0 })
				in := Input{Key: "user-1", HasKey: hasKey, Eligible: f.eligible}
				i, ok := b.Pick(in)
				if !ok {
					t.Fatalf("%s (hasKey=%v, seed=%d): Pick found no candidate although endpoints were eligible at first evaluation", algo, hasKey, seed)
				}
				if i < 0 || i >= n || i%2 != 0 {
					t.Fatalf("%s (hasKey=%v, seed=%d): Pick returned %d, want an endpoint eligible at first evaluation", algo, hasKey, seed, i)
				}
				f.checkOnce(t, algo)
			}
		}
	}
}

func TestPickSnapshotBeyondInlineWords(t *testing.T) {
	const n = 300
	ws := make([]int, n)
	for i := range ws {
		ws[i] = 1
	}
	eps := mkEps(ws...)
	for _, algo := range algorithms {
		for _, hasKey := range []bool{true, false} {
			b := mustNew(t, algo, eps, 3)
			f := newFlipper(n, func(i int) bool { return i%3 == 0 })
			in := Input{Key: "user-2", HasKey: hasKey, Eligible: f.eligible}
			i, ok := b.Pick(in)
			if !ok {
				t.Fatalf("%s (hasKey=%v): Pick found no candidate among %d endpoints", algo, hasKey, n)
			}
			if i%3 != 0 {
				t.Fatalf("%s (hasKey=%v): Pick returned %d, want an endpoint eligible at first evaluation", algo, hasKey, i)
			}
			f.checkOnce(t, algo)
		}
	}
}

func TestPickSnapshotFirstEvaluationOnlyCandidate(t *testing.T) {
	for _, algo := range algorithms {
		eps := mkEps(1, 1, 1, 1)
		b := mustNew(t, algo, eps, 9)
		f := newFlipper(len(eps), func(i int) bool { return i == 3 })
		in := Input{Key: fmt.Sprintf("k-%s", algo), HasKey: true, Eligible: f.eligible}
		if i, ok := b.Pick(in); !ok || i != 3 {
			t.Fatalf("%s: got (%d, %v), want (3, true)", algo, i, ok)
		}
		f.checkOnce(t, algo)
	}
}
