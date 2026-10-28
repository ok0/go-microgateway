package retry

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"

	"github.com/ok0/lychgate/internal/clock"
)

// Hooks are called when a retry is made or the budget refuses one. Nil hooks are skipped.
type Hooks struct {
	OnRetry, OnExhausted func()
}

// Request carries the retry state of one proxied request.
type Request struct {
	Policy                 *Policy
	Budget                 *Budget
	Replayable, HasIdemKey bool
	body                   []byte
	hooks                  Hooks
}

// NewBody returns a fresh reader over the buffered body, or nil when the body was not buffered.
func (q *Request) NewBody() io.ReadCloser {
	if q.body == nil {
		return nil
	}
	return io.NopCloser(bytes.NewReader(q.body))
}

// Retried invokes the OnRetry hook.
func (q *Request) Retried() {
	if q.hooks.OnRetry != nil {
		q.hooks.OnRetry()
	}
}

// Exhausted invokes the OnExhausted hook.
func (q *Request) Exhausted() {
	if q.hooks.OnExhausted != nil {
		q.hooks.OnExhausted()
	}
}

type requestKey struct{}

// WithRequest returns a copy of ctx that carries q.
func WithRequest(ctx context.Context, q *Request) context.Context {
	return context.WithValue(ctx, requestKey{}, q)
}

// FromContext returns the Request carried by ctx, or nil when there is none.
func FromContext(ctx context.Context) *Request {
	q, _ := ctx.Value(requestKey{}).(*Request)
	return q
}

// Middleware counts each request against the budget, prepares its body for replay and
// attaches its Request to the context passed to the next handler.
func Middleware(p *Policy, b *Budget, h Hooks) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b.Deposit()
			q := &Request{Policy: p, Budget: b, hooks: h, Replayable: true}
			q.HasIdemKey = p.IdempotencyHeader != "" && r.Header.Get(p.IdempotencyHeader) != ""
			if p.Buffers(r.Method, q.HasIdemKey) {
				q.body, q.Replayable = BufferBody(r, p.MaxBody)
			} else if !bodyless(r) {
				q.Replayable = false
			}
			next.ServeHTTP(w, r.WithContext(WithRequest(r.Context(), q)))
		})
	}
}

// Sleep waits for d on clk. It returns the context error if ctx ends first.
func Sleep(ctx context.Context, clk clock.Clock, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	done := make(chan struct{})
	t := clk.AfterFunc(d, func() { close(done) })
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		t.Stop()
		return ctx.Err()
	}
}

// Fits reports whether a wait of d on clk would finish before the deadline of ctx.
// Without a deadline it always reports true.
func Fits(ctx context.Context, clk clock.Clock, d time.Duration) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return deadline.Sub(clk.Now()) >= d
}
