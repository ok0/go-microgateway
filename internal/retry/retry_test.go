package retry

import (
	"bytes"
	"context"
	"errors"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

var testStart = time.Unix(1000, 0)

const kib = 1 << 10

var errBoom = errors.New("boom")

func defaultPolicy() *Policy {
	return &Policy{
		Attempts:          3,
		Base:              25 * time.Millisecond,
		Max:               250 * time.Millisecond,
		Statuses:          map[int]bool{502: true, 503: true, 504: true},
		Methods:           map[string]bool{"GET": true, "HEAD": true, "OPTIONS": true, "PUT": true, "DELETE": true},
		IdempotencyHeader: "Idempotency-Key",
		MaxBody:           64 * kib,
	}
}

func payload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251)
	}
	return b
}

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

type trackCloser struct {
	io.Reader
	closed bool
}

func (c *trackCloser) Close() error {
	c.closed = true
	return nil
}

type failingReader struct {
	prefix []byte
	sent   bool
}

func (f *failingReader) Read(p []byte) (int, error) {
	if !f.sent {
		f.sent = true
		return copy(p, f.prefix), nil
	}
	return 0, errBoom
}

func bodyRequest(method string, rc io.ReadCloser, cl int64) *http.Request {
	r, _ := http.NewRequest(method, "http://example.test/", nil)
	r.Body = rc
	r.ContentLength = cl
	return r
}

func waitForGoroutines(t *testing.T, want int) {
	t.Helper()
	for i := 0; i < 100000 && runtime.NumGoroutine() > want; i++ {
		runtime.Gosched()
	}
	if got := runtime.NumGoroutine(); got > want {
		t.Fatalf("goroutines = %d, want <= %d", got, want)
	}
}

func TestShouldRetry(t *testing.T) {
	p := defaultPolicy()

	optOut := defaultPolicy()
	optOut.IdempotencyHeader = ""

	listed := defaultPolicy()
	listed.Methods["POST"] = true

	listed429 := defaultPolicy()
	listed429.Statuses[429] = true

	no504 := defaultPolicy()
	delete(no504.Statuses, 504)

	tests := []struct {
		name   string
		policy *Policy
		a      Attempt
		want   bool
	}{
		{"GET 502 first attempt", p, Attempt{N: 1, Method: http.MethodGet, Status: 502, Replayable: true}, true},
		{"GET connect error", p, Attempt{N: 1, Method: http.MethodGet, ConnectErr: true, Replayable: true}, true},
		{"GET header timeout", p, Attempt{N: 1, Method: http.MethodGet, HeaderTimeout: true, Replayable: true}, true},
		{"GET 502 at last attempt", p, Attempt{N: 3, Method: http.MethodGet, Status: 502, Replayable: true}, false},
		{"GET 404", p, Attempt{N: 1, Method: http.MethodGet, Status: 404, Replayable: true}, false},
		{"GET 101", p, Attempt{N: 1, Method: http.MethodGet, Status: 101, Replayable: true}, false},
		{"GET 200", p, Attempt{N: 1, Method: http.MethodGet, Status: 200, Replayable: true}, false},
		{"GET 429 unlisted", p, Attempt{N: 1, Method: http.MethodGet, Status: 429, Replayable: true}, false},
		{"GET 429 listed", listed429, Attempt{N: 1, Method: http.MethodGet, Status: 429, Replayable: true}, true},
		{"GET 502 canceled", p, Attempt{N: 1, Method: http.MethodGet, Status: 502, Replayable: true, Canceled: true}, false},
		{"POST 503", p, Attempt{N: 1, Method: http.MethodPost, Status: 503, Replayable: true}, false},
		{"POST connect error not sent", p, Attempt{N: 1, Method: http.MethodPost, ConnectErr: true, Replayable: true}, true},
		{"POST connect error sent", p, Attempt{N: 1, Method: http.MethodPost, ConnectErr: true, Sent: true, Replayable: true}, false},
		{"POST 503 with key", p, Attempt{N: 1, Method: http.MethodPost, Status: 503, HasIdemKey: true, Replayable: true}, true},
		{"POST 503 with key opt-out", optOut, Attempt{N: 1, Method: http.MethodPost, Status: 503, HasIdemKey: true, Replayable: true}, false},
		{"PATCH 503", p, Attempt{N: 1, Method: http.MethodPatch, Status: 503, Replayable: true}, false},
		{"PATCH connect error not sent", p, Attempt{N: 1, Method: http.MethodPatch, ConnectErr: true, Replayable: true}, true},
		{"POST listed 503", listed, Attempt{N: 1, Method: http.MethodPost, Status: 503, Replayable: true}, true},
		{"PUT 503 not replayable", p, Attempt{N: 1, Method: http.MethodPut, Status: 503}, false},
		{"PUT connect error not sent not replayable", p, Attempt{N: 1, Method: http.MethodPut, ConnectErr: true}, true},
		{"GET header timeout without 504", no504, Attempt{N: 1, Method: http.MethodGet, HeaderTimeout: true, Replayable: true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.policy.ShouldRetry(tt.a); got != tt.want {
				t.Fatalf("ShouldRetry() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuffers(t *testing.T) {
	p := defaultPolicy()
	if !p.Buffers(http.MethodPut, false) {
		t.Fatal("Buffers(PUT) = false, want true")
	}
	if p.Buffers(http.MethodPost, false) {
		t.Fatal("Buffers(POST without key) = true, want false")
	}
	if !p.Buffers(http.MethodPost, true) {
		t.Fatal("Buffers(POST with key) = false, want true")
	}
	p.IdempotencyHeader = ""
	if p.Buffers(http.MethodPost, true) {
		t.Fatal("Buffers(POST with key, opt-out) = true, want false")
	}
}

func TestBackoffBounds(t *testing.T) {
	p := &Policy{Base: 25 * time.Millisecond, Max: 250 * time.Millisecond}
	for attempt := 2; attempt <= 12; attempt++ {
		window := min(p.Max, p.Base<<(attempt-1))
		if attempt == 2 && window != 50*time.Millisecond {
			t.Fatalf("attempt 2 window = %v, want 50ms", window)
		}
		if attempt == 3 && window != 100*time.Millisecond {
			t.Fatalf("attempt 3 window = %v, want 100ms", window)
		}
		if attempt == 4 && window != 200*time.Millisecond {
			t.Fatalf("attempt 4 window = %v, want 200ms", window)
		}
		if attempt >= 5 && window != 250*time.Millisecond {
			t.Fatalf("attempt %d window = %v, want 250ms", attempt, window)
		}

		lo, hi := window, time.Duration(0)
		for seed := uint64(1); seed <= 200; seed++ {
			v := p.Backoff(attempt, clock.NewRand(seed))
			if v < 0 || v >= window {
				t.Fatalf("attempt %d seed %d: Backoff() = %v, outside [0, %v)", attempt, seed, v, window)
			}
			lo = min(lo, v)
			hi = max(hi, v)
		}
		if hi <= window*9/10 {
			t.Fatalf("attempt %d: largest backoff %v not above 90%% of %v", attempt, hi, window)
		}
		if lo >= window/10 {
			t.Fatalf("attempt %d: smallest backoff %v not below 10%% of %v", attempt, lo, window)
		}
	}

	for seed := uint64(1); seed <= 200; seed++ {
		v := p.Backoff(70, clock.NewRand(seed))
		if v < 0 || v >= p.Max {
			t.Fatalf("attempt 70 seed %d: Backoff() = %v, outside [0, %v)", seed, v, p.Max)
		}
	}
}

func allow(b *Budget) bool {
	_, ok := b.Withdraw()
	return ok
}

func TestBudget(t *testing.T) {
	fk := clock.NewFake(testStart)
	b := NewBudget(0.1, 1, 10*time.Second, fk)
	for i := 0; i < 100; i++ {
		b.Deposit()
	}
	for i := 0; i < 20; i++ {
		if !allow(b) {
			t.Fatalf("Withdraw %d denied, want allowed", i+1)
		}
	}
	if allow(b) {
		t.Fatal("Withdraw 21 allowed, want denied")
	}

	fk.Advance(11 * time.Second)
	if req, ret := b.Stats(); req != 0 || ret != 0 {
		t.Fatalf("Stats() after window = (%d, %d), want (0, 0)", req, ret)
	}
	for i := 0; i < 10; i++ {
		if !allow(b) {
			t.Fatalf("Withdraw %d after window denied, want allowed", i+1)
		}
	}
	if allow(b) {
		t.Fatal("Withdraw 11 after window allowed, want denied")
	}

	r := NewBudget(0.1, 1, 10*time.Second, fk)
	for i := 0; i < 100; i++ {
		r.Deposit()
	}
	for i := 0; i < 19; i++ {
		allow(r)
	}
	s, _ := r.Withdraw()
	if allow(r) {
		t.Fatal("Withdraw after 20 allowed, want denied")
	}
	r.Refund(s)
	if !allow(r) {
		t.Fatal("Withdraw after Refund denied, want allowed")
	}
	if allow(r) {
		t.Fatal("second Withdraw after Refund allowed, want denied")
	}
	r.Refund(s)
	r.Refund(s)
	if req, ret := r.Stats(); req != 100 || ret != 18 {
		t.Fatalf("Stats() = (%d, %d), want (100, 18)", req, ret)
	}

	z := NewBudget(0, 0, 10*time.Second, fk)
	for i := 0; i < 100; i++ {
		z.Deposit()
	}
	if allow(z) {
		t.Fatal("zero budget allowed a retry")
	}

	c := NewBudget(0.1, 1, 10*time.Second, clock.NewFake(testStart))
	var wg sync.WaitGroup
	var granted atomic.Int64
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				c.Deposit()
				if allow(c) {
					granted.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if got := granted.Load(); got > 10+640 {
		t.Fatalf("concurrent successes = %d, want <= 650", got)
	}
}

func TestBudgetRefundOwnBucket(t *testing.T) {
	fk := clock.NewFake(testStart)
	b := NewBudget(0, 1, 10*time.Second, fk)
	var s0 int64
	for i := 0; i < 5; i++ {
		s, ok := b.Withdraw()
		if !ok {
			t.Fatalf("Withdraw %d at t0 denied, want allowed", i+1)
		}
		s0 = s
	}
	fk.Advance(5 * time.Second)
	for i := 0; i < 5; i++ {
		if !allow(b) {
			t.Fatalf("Withdraw %d at t5 denied, want allowed", i+1)
		}
	}
	if allow(b) {
		t.Fatal("Withdraw 11 allowed, want denied")
	}
	b.Refund(s0)
	if _, ret := b.Stats(); ret != 9 {
		t.Fatalf("Stats() retries after refund = %d, want 9", ret)
	}
	fk.Advance(5 * time.Second)
	if _, ret := b.Stats(); ret != 5 {
		t.Fatalf("Stats() retries after s0 aged out = %d, want 5", ret)
	}
	for i := 0; i < 5; i++ {
		if !allow(b) {
			t.Fatalf("Withdraw %d after s0 aged out denied, want allowed", i+1)
		}
	}
	if allow(b) {
		t.Fatal("Withdraw 6 after s0 aged out allowed, want denied")
	}
	b.Refund(s0)
	if _, ret := b.Stats(); ret != 10 {
		t.Fatalf("Stats() retries after refund of aged-out slot = %d, want 10", ret)
	}
}

func TestBudgetSingleBucketWindow(t *testing.T) {
	fk := clock.NewFake(testStart)
	b := NewBudget(0.5, 0, time.Nanosecond, fk)
	b.Deposit()
	b.Deposit()
	if !allow(b) {
		t.Fatal("Withdraw denied, want allowed")
	}
	fk.Advance(time.Nanosecond)
	if req, ret := b.Stats(); req != 0 || ret != 0 {
		t.Fatalf("Stats() = (%d, %d), want (0, 0)", req, ret)
	}
}

func TestBufferBody(t *testing.T) {
	t.Run("bodyless GET", func(t *testing.T) {
		r, _ := http.NewRequest(http.MethodGet, "http://example.test/", nil)
		buf, ok := BufferBody(r, 64*kib)
		if buf != nil || !ok {
			t.Fatalf("BufferBody() = (%v, %v), want (nil, true)", buf, ok)
		}
	})

	t.Run("no body", func(t *testing.T) {
		r := bodyRequest(http.MethodPost, http.NoBody, 0)
		buf, ok := BufferBody(r, 64*kib)
		if buf != nil || !ok {
			t.Fatalf("BufferBody() = (%v, %v), want (nil, true)", buf, ok)
		}
	})

	t.Run("known length within limit", func(t *testing.T) {
		src := payload(kib)
		cr := &countingReader{r: bytes.NewReader(src)}
		r := bodyRequest(http.MethodPost, io.NopCloser(cr), int64(len(src)))
		buf, ok := BufferBody(r, 64*kib)
		if !ok || !bytes.Equal(buf, src) {
			t.Fatal("BufferBody() did not return the identical bytes as replayable")
		}
		got, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(got, src) {
			t.Fatal("buffered body does not read back identically")
		}
	})

	t.Run("known length over limit is not read", func(t *testing.T) {
		src := payload(100 * kib)
		cr := &countingReader{r: bytes.NewReader(src)}
		r := bodyRequest(http.MethodPost, io.NopCloser(cr), int64(len(src)))
		buf, ok := BufferBody(r, 64*kib)
		if buf != nil || ok {
			t.Fatalf("BufferBody() = (%d bytes, %v), want (nil, false)", len(buf), ok)
		}
		if cr.n != 0 {
			t.Fatalf("read %d bytes of an over-limit body, want 0", cr.n)
		}
		got, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(got, src) {
			t.Fatal("untouched body does not read back identically")
		}
	})

	t.Run("unknown length within limit", func(t *testing.T) {
		src := payload(kib)
		cr := &countingReader{r: bytes.NewReader(src)}
		r := bodyRequest(http.MethodPost, io.NopCloser(cr), -1)
		buf, ok := BufferBody(r, 64*kib)
		if !ok || !bytes.Equal(buf, src) {
			t.Fatal("BufferBody() did not return the identical bytes as replayable")
		}
		if r.ContentLength != -1 {
			t.Fatalf("ContentLength = %d, want -1", r.ContentLength)
		}
		got, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(got, src) {
			t.Fatal("buffered body does not read back identically")
		}
	})

	t.Run("unknown length exactly at limit", func(t *testing.T) {
		src := payload(64 * kib)
		r := bodyRequest(http.MethodPost, io.NopCloser(bytes.NewReader(src)), -1)
		buf, ok := BufferBody(r, 64*kib)
		if !ok || !bytes.Equal(buf, src) {
			t.Fatal("BufferBody() at exactly max did not return replayable bytes")
		}
	})

	t.Run("unknown length over limit", func(t *testing.T) {
		src := payload(100 * kib)
		cr := &countingReader{r: bytes.NewReader(src)}
		closer := &trackCloser{Reader: cr}
		r := bodyRequest(http.MethodPost, closer, -1)
		buf, ok := BufferBody(r, 64*kib)
		if buf != nil || ok {
			t.Fatalf("BufferBody() = (%d bytes, %v), want (nil, false)", len(buf), ok)
		}
		if cr.n > 64*kib+1 {
			t.Fatalf("consumed %d bytes, want at most %d", cr.n, 64*kib+1)
		}
		got, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(got, src) {
			t.Fatal("unbuffered stream is not byte-exact")
		}
		if err := r.Body.Close(); err != nil || !closer.closed {
			t.Fatal("Close did not close the original body")
		}
	})

	t.Run("max zero with body", func(t *testing.T) {
		src := payload(kib)
		cr := &countingReader{r: bytes.NewReader(src)}
		r := bodyRequest(http.MethodPost, io.NopCloser(cr), int64(len(src)))
		buf, ok := BufferBody(r, 0)
		if buf != nil || ok {
			t.Fatalf("BufferBody() = (%d bytes, %v), want (nil, false)", len(buf), ok)
		}
		if cr.n != 0 {
			t.Fatalf("read %d bytes with max 0, want 0", cr.n)
		}
		got, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(got, src) {
			t.Fatal("untouched body does not read back identically")
		}
	})

	t.Run("read error midway with known length", func(t *testing.T) {
		r := bodyRequest(http.MethodPost, io.NopCloser(&failingReader{prefix: []byte("hello")}), 10)
		buf, ok := BufferBody(r, 64*kib)
		if buf != nil || ok {
			t.Fatalf("BufferBody() = (%d bytes, %v), want (nil, false)", len(buf), ok)
		}
		got, err := io.ReadAll(r.Body)
		if !errors.Is(err, errBoom) || string(got) != "hello" {
			t.Fatalf("prefix = %q, err = %v; want \"hello\" and boom", got, err)
		}
	})

	t.Run("read error midway with unknown length", func(t *testing.T) {
		r := bodyRequest(http.MethodPost, io.NopCloser(&failingReader{prefix: []byte("hello")}), -1)
		buf, ok := BufferBody(r, 64*kib)
		if buf != nil || ok {
			t.Fatalf("BufferBody() = (%d bytes, %v), want (nil, false)", len(buf), ok)
		}
		got, err := io.ReadAll(r.Body)
		if !errors.Is(err, errBoom) || string(got) != "hello" {
			t.Fatalf("prefix = %q, err = %v; want \"hello\" and boom", got, err)
		}
	})
}

func TestBufferBodyAllocBound(t *testing.T) {
	const max = 64 * kib
	src := payload(20 * kib)
	r := bodyRequest(http.MethodPost, io.NopCloser(bytes.NewReader(src)), -1)
	buf, ok := BufferBody(r, max)
	if !ok {
		t.Fatal("BufferBody() not replayable")
	}
	if cap(buf) > max+1 {
		t.Fatalf("cap(buf) = %d, want <= %d", cap(buf), max+1)
	}
}

func TestMiddleware(t *testing.T) {
	fk := clock.NewFake(testStart)

	t.Run("deposit counted per request", func(t *testing.T) {
		b := NewBudget(0.1, 1, 10*time.Second, fk)
		h := Middleware(defaultPolicy(), b, Hooks{})(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		for i := 0; i < 3; i++ {
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		}
		if req, _ := b.Stats(); req != 3 {
			t.Fatalf("requests = %d, want 3", req)
		}
	})

	t.Run("request set in context", func(t *testing.T) {
		p := defaultPolicy()
		var got *Request
		h := Middleware(p, NewBudget(0.1, 1, 10*time.Second, fk), Hooks{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = FromContext(r.Context())
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		if got == nil || got.Policy != p || !got.Replayable || got.HasIdemKey || got.NewBody() != nil {
			t.Fatalf("unexpected request state: %+v", got)
		}
	})

	t.Run("FromContext without request", func(t *testing.T) {
		if FromContext(context.Background()) != nil {
			t.Fatal("FromContext on plain context returned non-nil")
		}
	})

	t.Run("post without key is not buffered", func(t *testing.T) {
		cr := &countingReader{r: bytes.NewReader(payload(kib))}
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Body = io.NopCloser(cr)
		req.ContentLength = kib
		var got *Request
		h := Middleware(defaultPolicy(), NewBudget(0.1, 1, 10*time.Second, fk), Hooks{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = FromContext(r.Context())
		}))
		h.ServeHTTP(httptest.NewRecorder(), req)
		if cr.n != 0 {
			t.Fatalf("read %d bytes, want 0", cr.n)
		}
		if got.Replayable || got.NewBody() != nil {
			t.Fatal("unbuffered POST reported replayable or produced a body")
		}
	})

	t.Run("post with key and opt-in is buffered", func(t *testing.T) {
		src := payload(kib)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(src))
		req.Header.Set("Idempotency-Key", "k1")
		var got *Request
		var read []byte
		h := Middleware(defaultPolicy(), NewBudget(0.1, 1, 10*time.Second, fk), Hooks{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = FromContext(r.Context())
			read, _ = io.ReadAll(got.NewBody())
		}))
		h.ServeHTTP(httptest.NewRecorder(), req)
		if !got.HasIdemKey || !got.Replayable {
			t.Fatalf("HasIdemKey = %v, Replayable = %v; want true, true", got.HasIdemKey, got.Replayable)
		}
		if !bytes.Equal(read, src) {
			t.Fatal("buffered body does not read back identically")
		}
	})

	t.Run("hooks fire", func(t *testing.T) {
		var retried, exhausted int
		hooks := Hooks{
			OnRetry:     func() { retried++ },
			OnExhausted: func() { exhausted++ },
		}
		h := Middleware(defaultPolicy(), NewBudget(0.1, 1, 10*time.Second, fk), hooks)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			q := FromContext(r.Context())
			q.Retried()
			q.Retried()
			q.Exhausted()
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		if retried != 2 || exhausted != 1 {
			t.Fatalf("retried = %d, exhausted = %d; want 2, 1", retried, exhausted)
		}
	})

	t.Run("nil hooks are skipped", func(t *testing.T) {
		h := Middleware(defaultPolicy(), NewBudget(0.1, 1, 10*time.Second, fk), Hooks{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			q := FromContext(r.Context())
			q.Retried()
			q.Exhausted()
		}))
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	})
}

func TestSleep(t *testing.T) {
	t.Run("returns after advance", func(t *testing.T) {
		fk := clock.NewFake(testStart)
		errc := make(chan error, 1)
		go func() { errc <- Sleep(context.Background(), fk, time.Second) }()
		fk.BlockUntil(1)
		fk.Advance(time.Second)
		if err := <-errc; err != nil {
			t.Fatalf("Sleep() = %v, want nil", err)
		}
	})

	t.Run("cancel during wait", func(t *testing.T) {
		fk := clock.NewFake(testStart)
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() { errc <- Sleep(ctx, fk, time.Second) }()
		fk.BlockUntil(1)
		cancel()
		if err := <-errc; !errors.Is(err, context.Canceled) {
			t.Fatalf("Sleep() = %v, want context.Canceled", err)
		}
		if n := fk.Waiters(); n != 0 {
			t.Fatalf("Waiters() = %d, want 0", n)
		}
	})

	t.Run("zero duration is immediate", func(t *testing.T) {
		fk := clock.NewFake(testStart)
		if err := Sleep(context.Background(), fk, 0); err != nil {
			t.Fatalf("Sleep(0) = %v, want nil", err)
		}
		if n := fk.Waiters(); n != 0 {
			t.Fatalf("Waiters() = %d, want 0", n)
		}
	})

	t.Run("already canceled", func(t *testing.T) {
		fk := clock.NewFake(testStart)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := Sleep(ctx, fk, time.Second); !errors.Is(err, context.Canceled) {
			t.Fatalf("Sleep() = %v, want context.Canceled", err)
		}
		if n := fk.Waiters(); n != 0 {
			t.Fatalf("Waiters() = %d, want 0", n)
		}
	})

	t.Run("no goroutines left behind", func(t *testing.T) {
		fk := clock.NewFake(testStart)
		before := runtime.NumGoroutine()
		for i := 0; i < 100; i++ {
			errc := make(chan error, 1)
			go func() { errc <- Sleep(context.Background(), fk, time.Millisecond) }()
			fk.BlockUntil(1)
			fk.Advance(time.Millisecond)
			if err := <-errc; err != nil {
				t.Fatalf("Sleep() = %v, want nil", err)
			}
		}
		waitForGoroutines(t, before)
	})
}

func TestFits(t *testing.T) {
	fk := clock.NewFake(time.Unix(1000, 0))
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1000, 0).Add(5*time.Second))
	defer cancel()
	if !Fits(ctx, fk, 4*time.Second) {
		t.Fatal("Fits(4s) = false, want true")
	}
	if !Fits(ctx, fk, 5*time.Second) {
		t.Fatal("Fits(5s) = false, want true")
	}
	if Fits(ctx, fk, 6*time.Second) {
		t.Fatal("Fits(6s) = true, want false")
	}
	if !Fits(context.Background(), fk, time.Hour) {
		t.Fatal("Fits without deadline = false, want true")
	}
}

func TestFitsInjectedClockDeadline(t *testing.T) {
	fk := clock.NewFake(testStart)
	ctx, cancel := clock.WithTimeout(context.Background(), fk, 5*time.Second)
	defer cancel()
	if !Fits(ctx, fk, 4*time.Second) {
		t.Fatal("Fits(4s) = false, want true")
	}
	if !Fits(ctx, fk, 5*time.Second) {
		t.Fatal("Fits(5s) = false, want true")
	}
	if Fits(ctx, fk, 6*time.Second) {
		t.Fatal("Fits(6s) = true, want false")
	}
	fk.Advance(2 * time.Second)
	if !Fits(ctx, fk, 3*time.Second) {
		t.Fatal("after Advance(2s): Fits(3s) = false, want true")
	}
	if Fits(ctx, fk, 4*time.Second) {
		t.Fatal("after Advance(2s): Fits(4s) = true, want false")
	}
}

func fieldNames(t reflect.Type) []string {
	names := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		names = append(names, t.Field(i).Name)
	}
	return names
}

func TestConstructorsTakeNoSharedHandle(t *testing.T) {
	clockType := reflect.TypeOf((*clock.Clock)(nil)).Elem()
	wantBudget := []reflect.Type{
		reflect.TypeOf(float64(0)),
		reflect.TypeOf(float64(0)),
		reflect.TypeOf(time.Duration(0)),
		clockType,
	}
	budgetFn := reflect.TypeOf(NewBudget)
	if budgetFn.NumIn() != len(wantBudget) {
		t.Fatalf("NewBudget has %d inputs, want %d", budgetFn.NumIn(), len(wantBudget))
	}
	for i, want := range wantBudget {
		if budgetFn.In(i) != want {
			t.Fatalf("NewBudget input %d = %v, want %v", i, budgetFn.In(i), want)
		}
	}

	wantMiddleware := []reflect.Type{
		reflect.TypeOf((*Policy)(nil)),
		reflect.TypeOf((*Budget)(nil)),
		reflect.TypeOf(Hooks{}),
	}
	middlewareFn := reflect.TypeOf(Middleware)
	if middlewareFn.NumIn() != len(wantMiddleware) {
		t.Fatalf("Middleware has %d inputs, want %d", middlewareFn.NumIn(), len(wantMiddleware))
	}
	for i, want := range wantMiddleware {
		if middlewareFn.In(i) != want {
			t.Fatalf("Middleware input %d = %v, want %v", i, middlewareFn.In(i), want)
		}
	}

	if got := fieldNames(reflect.TypeOf(Hooks{})); !slices.Equal(got, []string{"OnRetry", "OnExhausted"}) {
		t.Fatalf("Hooks fields = %v", got)
	}
	if got := fieldNames(reflect.TypeOf(Policy{})); !slices.Equal(got, []string{"Attempts", "Base", "Max", "Statuses", "Methods", "IdempotencyHeader", "MaxBody"}) {
		t.Fatalf("Policy fields = %v", got)
	}
}

func TestImports(t *testing.T) {
	forbidden := map[string]bool{
		"net":                true,
		"net/http/httptrace": true,
		"log":                true,
		"log/slog":           true,
		"math/rand":          true,
		"math/rand/v2":       true,
		"github.com/ok0/go-microgateway/internal/metrics": true,
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			if forbidden[path] || strings.HasPrefix(path, "math/rand") || strings.Contains(path, "/store") || strings.Contains(path, "memstore") {
				t.Errorf("%s imports forbidden package %q", name, path)
			}
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		for _, banned := range []string{"time.Now(", "time.Since(", "time.Sleep("} {
			if strings.Contains(string(src), banned) {
				t.Errorf("%s contains %q", name, banned)
			}
		}
	}
}

func TestNoSleepInResilienceCode(t *testing.T) {
	for _, dir := range []string{".", "../breaker", "../upstream", "../proxy"} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			src, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(src), "time.Sleep(") {
				t.Errorf("%s contains time.Sleep(", filepath.Join(dir, name))
			}
		}
	}
}

func BenchmarkShouldRetry(b *testing.B) {
	p := defaultPolicy()
	a := Attempt{N: 1, Method: http.MethodGet, Status: 503, Replayable: true}
	for i := 0; i < b.N; i++ {
		if !p.ShouldRetry(a) {
			b.Fatal("ShouldRetry() = false, want true")
		}
	}
}

func BenchmarkBackoff(b *testing.B) {
	p := defaultPolicy()
	rnd := clock.NewRand(1)
	for i := 0; i < b.N; i++ {
		_ = p.Backoff(2+i%10, rnd)
	}
}

func BenchmarkBudgetWithdraw(b *testing.B) {
	bud := NewBudget(0.1, 1, 10*time.Second, clock.NewFake(testStart))
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bud.Deposit()
			bud.Withdraw()
		}
	})
}
