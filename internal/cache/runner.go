package cache

import (
	"context"
	"sync"
)

// Runner runs the background refreshes of the cache stages of one gateway instance: at most max at once, at most one
// per key. It owns every refresh goroutine.
type Runner struct {
	mu      sync.Mutex
	max     int
	running int
	keys    map[string]struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	closed  bool
}

// NewRunner returns a Runner that runs at most max refreshes at once. A max below 1 is treated as 1.
func NewRunner(max int) *Runner {
	if max < 1 {
		max = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Runner{max: max, keys: make(map[string]struct{}), ctx: ctx, cancel: cancel}
}

// Go starts fn in its own goroutine under key and reports true. It reports false, starting nothing, when the runner
// is closed, when max refreshes are running or when key is already in flight. It never queues. The context passed to
// fn ends when the runner is closed.
func (r *Runner) Go(key string, fn func(ctx context.Context)) bool {
	return r.start(key, fn, nil)
}

// start is Go with an after callback. Like Go, it runs fn in its own goroutine under key. after runs in that goroutine
// once the refresh has left the runner (slot, key and join released), so after may close the runner without waiting
// on itself.
func (r *Runner) start(key string, fn func(ctx context.Context), after func()) bool {
	r.mu.Lock()
	if r.closed || r.running >= r.max {
		r.mu.Unlock()
		return false
	}
	if _, ok := r.keys[key]; ok {
		r.mu.Unlock()
		return false
	}
	r.keys[key] = struct{}{}
	r.running++
	r.wg.Add(1)
	r.mu.Unlock()
	go func() {
		defer func() {
			r.mu.Lock()
			delete(r.keys, key)
			r.running--
			r.mu.Unlock()
			r.wg.Done()
			if after != nil {
				after()
			}
		}()
		fn(r.ctx)
	}()
	return true
}

// Running reports the refreshes that were started and have not returned.
func (r *Runner) Running() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.running
}

// Close cancels and joins every refresh function; later Go calls start nothing. A scope's idle callback runs after its
// refresh left the runner, so it may call Close. It is idempotent and always returns nil.
func (r *Runner) Close() error {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.cancel()
	r.wg.Wait()
	return nil
}

// Scope holds the refreshes started by the cache stages of one gateway generation. They share the bounds of the
// runner, and the generation's resources are released only once its scope is idle.
type Scope struct {
	r       *Runner
	mu      sync.Mutex
	n       int
	ctx     context.Context
	cancel  context.CancelFunc
	stopped bool
	retired bool
	onIdle  func()
}

// Scope returns a new scope of r. Its refreshes end when the scope is cancelled or when r is closed.
func (r *Runner) Scope() *Scope {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scope{r: r, ctx: ctx, cancel: cancel}
}

// Go starts fn as a refresh of the scope under key, with the rules of Runner.Go. It also reports false once the scope
// was cancelled or retired. The context passed to fn ends when the runner closes or the scope is cancelled.
func (s *Scope) Go(key string, fn func(ctx context.Context)) bool {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false
	}
	s.n++
	s.mu.Unlock()
	ok := s.r.start(key, func(rctx context.Context) {
		ctx, cancel := context.WithCancel(s.ctx)
		defer cancel()
		stop := context.AfterFunc(rctx, cancel)
		defer stop()
		fn(ctx)
	}, s.done)
	if !ok {
		s.done()
	}
	return ok
}

// done releases one reservation of the scope and runs the idle callback when the scope becomes idle.
func (s *Scope) done() {
	s.mu.Lock()
	s.n--
	var f func()
	if s.n == 0 && s.onIdle != nil {
		f, s.onIdle = s.onIdle, nil
	}
	s.mu.Unlock()
	if f != nil {
		f()
	}
}

// Running reports the refreshes of the scope whose function has not returned, including those reserved by Go and not
// yet started.
func (s *Scope) Running() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// Cancel ends the context of every refresh of the scope; later Go calls start nothing. It is idempotent.
func (s *Scope) Cancel() {
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	s.cancel()
}

// Retire stops the scope from starting refreshes. It reports true when refreshes are running; then onIdle, when non-nil,
// runs once in the goroutine of the last refresh after its function returned and after it left the runner, so onIdle
// may close the runner. It reports false, without calling onIdle, when no refresh runs or Retire was called before.
func (s *Scope) Retire(onIdle func()) bool {
	s.mu.Lock()
	if s.retired {
		s.mu.Unlock()
		return false
	}
	s.retired, s.stopped = true, true
	if s.n == 0 {
		s.mu.Unlock()
		return false
	}
	s.onIdle = onIdle
	s.mu.Unlock()
	return true
}
