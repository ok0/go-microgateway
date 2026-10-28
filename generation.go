package microgateway

import (
	"log/slog"
	"sync/atomic"

	"github.com/ok0/go-microgateway/internal/runtime"
)

// generation is one installed configuration: an immutable snapshot and the state lease that keeps its components open.
type generation struct {
	snap        *runtime.Snapshot
	fingerprint string       // config fingerprint combined with participant digests
	state       atomic.Int64 // bit 0: retired; the remaining bits count requests in flight, in steps of 2
	release     func() error // runs exactly once, when retired and drained
	logger      *slog.Logger
}

func (gen *generation) enter() bool {
	for {
		s := gen.state.Load()
		if s&1 != 0 {
			return false
		}
		if gen.state.CompareAndSwap(s, s+2) {
			return true
		}
	}
}

func (gen *generation) leave() {
	if gen.state.Add(-2) == 1 {
		if err := gen.release(); err != nil {
			gen.logger.Warn("releasing retired configuration failed", "error", err)
		}
	}
}

func (gen *generation) retire() error {
	if gen.state.Or(1) == 0 {
		return gen.release()
	}
	return nil
}
