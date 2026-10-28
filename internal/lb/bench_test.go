package lb

import (
	"testing"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
)

func BenchmarkPick(b *testing.B) {
	ws := make([]int, 16)
	for i := range ws {
		ws[i] = 1 + i%4
	}
	eps := mkEps(ws...)
	for i := range eps {
		eps[i].InFlight = zeroInFlight
	}
	for _, algo := range algorithms {
		b.Run(algo, func(b *testing.B) {
			bal, err := New(config.LoadBalancer{Algorithm: algo}, eps, clock.NewRand(1))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				in := Input{Key: "user-1", HasKey: true}
				for pb.Next() {
					if _, ok := bal.Pick(in); !ok {
						b.Error("no candidate")
					}
				}
			})
		})
	}
}
