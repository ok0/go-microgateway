package microgateway

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

type reEnv struct {
	g      *Gateway
	s      *testServer
	client *http.Client
}

func reServe(t *testing.T, n testNetwork, fake *clock.Fake, doc rsDoc, opts ...Option) reEnv {
	t.Helper()
	opts = append([]Option{withClock(fake)}, opts...)
	g := n.gateway(t, doc.yaml(), opts...)
	return reEnv{g: g, s: n.serve(t, g), client: n.client(t, nil)}
}

func reSingle(hosts []string, extra string) rsDoc {
	return rsDoc{hosts: hosts, routes: []rsRoute{{name: "all", path: "/", extra: extra}}}
}

type reDriver struct {
	fake   *clock.Fake
	max    time.Duration
	done   chan struct{}
	exited chan struct{}
}

// reDrive advances the fake clock past each backoff wait of up to max until stop is called.
func reDrive(t *testing.T, fake *clock.Fake, max time.Duration) *reDriver {
	t.Helper()
	d := &reDriver{fake: fake, max: max, done: make(chan struct{}), exited: make(chan struct{})}
	go func() {
		defer close(d.exited)
		for {
			select {
			case <-d.done:
				return
			default:
			}
			rsAdvanceBackoff(t, fake, max)
		}
	}()
	return d
}

func (d *reDriver) stop() {
	close(d.done)
	wake := d.fake.NewTicker(d.max)
	<-d.exited
	wake.Stop()
}

type reResult struct {
	code int
	hdr  http.Header
	body []byte
	err  error
}

func reNewGet(t *testing.T, ctx context.Context, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func reDoAsync(client *http.Client, req *http.Request) <-chan reResult {
	ch := make(chan reResult, 1)
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			ch <- reResult{err: err}
			return
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		ch <- reResult{code: resp.StatusCode, hdr: resp.Header, body: data, err: err}
	}()
	return ch
}

func reAwait(t *testing.T, ch <-chan reResult) reResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("request did not finish within 5s")
	}
	return reResult{}
}

func reAssertIdle(t *testing.T, g *Gateway) {
	t.Helper()
	eventually(t, func() bool {
		for _, h := range rsStatus(t, g).Hosts {
			if h.InFlight != 0 {
				return false
			}
		}
		return true
	})
}

func reRandomBytes(size int) []byte {
	r := rand.New(rand.NewPCG(11, 13))
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func reLines(r io.Reader) <-chan string {
	lines := make(chan string, 8)
	go func() {
		defer close(lines)
		reader := bufio.NewReader(r)
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				lines <- line
			}
			if err != nil {
				return
			}
		}
	}()
	return lines
}

func runRetryBasics(t *testing.T, n testNetwork) {
	t.Helper()
	fake := clock.NewFake(time.Now())

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete, http.MethodOptions} {
		t.Run("method "+method, func(t *testing.T) {
			be := newRSBackend(t, n, rsStatusSeq(502, 200))
			env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
			before := rsMetric(t, env.g, "microgateway_retries_total", "route", "all")
			d := reDrive(t, fake, 250*time.Millisecond)
			code, _, body := rsGet(t, env.client, env.s.URL+"/", method, nil, nil)
			d.stop()
			want := "r2"
			if method == http.MethodHead {
				want = ""
			}
			if code != http.StatusOK || string(body) != want {
				t.Fatalf("response = %d %q, want 200 %q", code, body, want)
			}
			if got := be.hitCount(); got != 2 {
				t.Fatalf("hits = %d, want 2", got)
			}
			if got := rsMetric(t, env.g, "microgateway_retries_total", "route", "all") - before; got != 1 {
				t.Fatalf("retries_total increase = %v, want 1", got)
			}
			reAssertIdle(t, env.g)
		})
	}

	t.Run("connect error", func(t *testing.T) {
		refused := newRSBackend(t, n, nil)
		n.refuse(t, refused.srv)
		healthy := newRSBackend(t, n, nil)
		env := reServe(t, n, fake, reSingle([]string{refused.URL, healthy.URL}, "retry: {}"))
		before := rsMetric(t, env.g, "microgateway_retries_total", "route", "all")
		d := reDrive(t, fake, 250*time.Millisecond)
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodGet, nil, nil)
		d.stop()
		if code != http.StatusOK {
			t.Fatalf("status = %d, want 200", code)
		}
		if got := rsMetric(t, env.g, "microgateway_retries_total", "route", "all") - before; got != 1 {
			t.Fatalf("retries_total increase = %v, want 1", got)
		}
		reAssertIdle(t, env.g)
	})

	t.Run("response header timeout", func(t *testing.T) {
		be := newRSBackend(t, n, func(seq int64, w http.ResponseWriter, r *http.Request) {
			if seq == 1 {
				<-r.Context().Done()
				return
			}
			_, _ = io.WriteString(w, "ok")
		})
		doc := rsDoc{
			hosts:    []string{be.URL},
			timeouts: "connect: 1h\nresponse_header: 5s\n",
			routes:   []rsRoute{{name: "all", path: "/", extra: "retry: {}"}},
		}
		env := reServe(t, n, fake, doc)
		d := reDrive(t, fake, 250*time.Millisecond)
		res := reDoAsync(env.client, reNewGet(t, context.Background(), env.s.URL+"/"))
		fake.BlockUntilDue(1, 5*time.Second)
		fake.Advance(5 * time.Second)
		r := reAwait(t, res)
		d.stop()
		if r.err != nil || r.code != http.StatusOK || string(r.body) != "ok" {
			t.Fatalf("response = %d %q (err %v), want 200 \"ok\"", r.code, r.body, r.err)
		}
		if got := be.hitCount(); got != 2 {
			t.Fatalf("hits = %d, want 2", got)
		}
		reAssertIdle(t, env.g)
	})

	t.Run("attempts exhausted", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(503))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry:\n      attempts: 3\n"))
		d := reDrive(t, fake, 250*time.Millisecond)
		code, hdr, body := rsGet(t, env.client, env.s.URL+"/", http.MethodGet, nil, nil)
		d.stop()
		if code != http.StatusServiceUnavailable || string(body) != "r3" || hdr.Get("X-Attempt") != "3" {
			t.Fatalf("response = %d %q X-Attempt=%q, want 503 \"r3\" X-Attempt=3", code, body, hdr.Get("X-Attempt"))
		}
		if got := be.hitCount(); got != 3 {
			t.Fatalf("hits = %d, want 3", got)
		}
		reAssertIdle(t, env.g)
	})
}

func TestRetryBasics(t *testing.T) {
	runRetryBasics(t, newMemNetwork())
}

func TestRetryConnectFailureSameHost(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	be := newRSBackend(t, n, nil)
	n.failDials(t, be.srv, 1)
	env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
	d := reDrive(t, fake, 250*time.Millisecond)
	code, _, body := rsGet(t, env.client, env.s.URL+"/", http.MethodGet, nil, nil)
	d.stop()
	if code != http.StatusOK || string(body) != "ok" {
		t.Fatalf("response = %d %q, want 200 \"ok\"", code, body)
	}
	if got := n.dialCount(be.srv.Addr); got != 2 {
		t.Fatalf("dialCount = %d, want 2", got)
	}
}

func runRetryIdempotency(t *testing.T, n testNetwork) {
	t.Helper()
	fake := clock.NewFake(time.Now())

	for _, method := range []string{http.MethodPost, http.MethodPatch} {
		t.Run(method+" 503 not retried", func(t *testing.T) {
			be := newRSBackend(t, n, rsStatusSeq(503, 200))
			env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
			code, _, _ := rsGet(t, env.client, env.s.URL+"/", method, strings.NewReader("payload"), nil)
			if code != http.StatusServiceUnavailable || be.hitCount() != 1 {
				t.Fatalf("status = %d, hits = %d, want 503 and 1 hit", code, be.hitCount())
			}
			reAssertIdle(t, env.g)
		})

		t.Run(method+" reset not retried", func(t *testing.T) {
			be := newRSBackend(t, n, rsReset)
			env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
			code, _, _ := rsGet(t, env.client, env.s.URL+"/", method, strings.NewReader("payload"), nil)
			if code != http.StatusBadGateway || be.hitCount() != 1 {
				t.Fatalf("status = %d, hits = %d, want 502 and 1 hit", code, be.hitCount())
			}
		})

		t.Run(method+" connect error retried with same body", func(t *testing.T) {
			refused := newRSBackend(t, n, nil)
			n.refuse(t, refused.srv)
			healthy := newRSBackend(t, n, nil)
			env := reServe(t, n, fake, reSingle([]string{refused.URL, healthy.URL}, "retry: {}"))
			d := reDrive(t, fake, 250*time.Millisecond)
			code, _, _ := rsGet(t, env.client, env.s.URL+"/", method, strings.NewReader("payload"), nil)
			d.stop()
			if code != http.StatusOK {
				t.Fatalf("status = %d, want 200", code)
			}
			if healthy.hitCount() != 1 || string(healthy.body(0)) != "payload" {
				t.Fatalf("healthy hits = %d body = %q, want 1 hit with \"payload\"", healthy.hitCount(), healthy.body(0))
			}
			reAssertIdle(t, env.g)
		})

		t.Run(method+" idempotency key retried", func(t *testing.T) {
			be := newRSBackend(t, n, rsStatusSeq(503, 200))
			env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry:\n      idempotency_key:\n        enabled: true\n"))
			d := reDrive(t, fake, 250*time.Millisecond)
			code, _, body := rsGet(t, env.client, env.s.URL+"/", method, strings.NewReader("payload"), http.Header{"Idempotency-Key": {"k1"}})
			d.stop()
			if code != http.StatusOK || string(body) != "r2" || be.hitCount() != 2 {
				t.Fatalf("status = %d %q, hits = %d, want 200 \"r2\" and 2 hits", code, body, be.hitCount())
			}
			reAssertIdle(t, env.g)
		})

		t.Run(method+" idempotency key absent not retried", func(t *testing.T) {
			be := newRSBackend(t, n, rsStatusSeq(503, 200))
			env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry:\n      idempotency_key:\n        enabled: true\n"))
			code, _, _ := rsGet(t, env.client, env.s.URL+"/", method, strings.NewReader("payload"), nil)
			if code != http.StatusServiceUnavailable || be.hitCount() != 1 {
				t.Fatalf("status = %d, hits = %d, want 503 and 1 hit", code, be.hitCount())
			}
		})

		t.Run(method+" idempotency key disabled not retried", func(t *testing.T) {
			be := newRSBackend(t, n, rsStatusSeq(503, 200))
			env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry:\n      idempotency_key:\n        enabled: false\n"))
			code, _, _ := rsGet(t, env.client, env.s.URL+"/", method, strings.NewReader("payload"), http.Header{"Idempotency-Key": {"k1"}})
			if code != http.StatusServiceUnavailable || be.hitCount() != 1 {
				t.Fatalf("status = %d, hits = %d, want 503 and 1 hit", code, be.hitCount())
			}
		})
	}

	t.Run("methods list retries POST", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(503, 200))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry:\n      methods: [GET, POST]\n"))
		d := reDrive(t, fake, 250*time.Millisecond)
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodPost, strings.NewReader("payload"), nil)
		d.stop()
		if code != http.StatusOK || be.hitCount() != 2 {
			t.Fatalf("status = %d, hits = %d, want 200 and 2 hits", code, be.hitCount())
		}
	})

	t.Run("GET 404 not retried", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(404, 200))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodGet, nil, nil)
		if code != http.StatusNotFound || be.hitCount() != 1 {
			t.Fatalf("status = %d, hits = %d, want 404 and 1 hit", code, be.hitCount())
		}
	})

	t.Run("GET 429 not retried", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(429, 200))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodGet, nil, nil)
		if code != http.StatusTooManyRequests || be.hitCount() != 1 {
			t.Fatalf("status = %d, hits = %d, want 429 and 1 hit", code, be.hitCount())
		}
	})

	t.Run("client cancel stops retries", func(t *testing.T) {
		arrived := make(chan struct{}, 1)
		release := make(chan struct{})
		be := newRSBackend(t, n, func(_ int64, w http.ResponseWriter, r *http.Request) {
			select {
			case arrived <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, env.s.URL+"/", nil)
		if err != nil {
			t.Fatal(err)
		}
		res := reDoAsync(env.client, req)
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			t.Fatal("backend did not receive the request")
		}
		cancel()
		r := reAwait(t, res)
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("client error = %v, want context.Canceled", r.err)
		}
		close(release)
		fake.Advance(250 * time.Millisecond)
		if got := be.hitCount(); got != 1 {
			t.Fatalf("hits = %d, want 1", got)
		}
	})
}

func TestRetryIdempotency(t *testing.T) {
	runRetryIdempotency(t, newMemNetwork())
}

func TestRetryBackoffSchedule(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	be := newRSBackend(t, n, rsStatusSeq(503))
	env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry:\n      attempts: 4\n      base_delay: 100ms\n      max_delay: 250ms\n"), withRand(clock.NewRand(7)))

	want := clock.NewRand(7)
	waits := []time.Duration{
		time.Duration(want.Int64N(int64(200 * time.Millisecond))),
		time.Duration(want.Int64N(int64(250 * time.Millisecond))),
		time.Duration(want.Int64N(int64(250 * time.Millisecond))),
	}

	res := reDoAsync(env.client, reNewGet(t, context.Background(), env.s.URL+"/"))
	for i, w := range waits {
		hits := int64(i + 1)
		if w > 0 {
			fake.BlockUntilDue(1, 250*time.Millisecond)
			fake.Advance(w - 1)
			if got := be.hitCount(); got != hits {
				t.Fatalf("hits before backoff %d end = %d, want %d", i+2, got, hits)
			}
			if due := fake.Due(250 * time.Millisecond); due != 1 {
				t.Fatalf("due waiters before backoff %d end = %d, want 1", i+2, due)
			}
			fake.Advance(1)
		}
		eventually(t, func() bool { return be.hitCount() == hits+1 })
	}

	r := reAwait(t, res)
	if r.err != nil || r.code != http.StatusServiceUnavailable || string(r.body) != "r4" {
		t.Fatalf("response = %d %q (err %v), want 503 \"r4\"", r.code, r.body, r.err)
	}
	if got := be.hitCount(); got != 4 {
		t.Fatalf("hits = %d, want 4", got)
	}
}

func TestRetryBackoffCanceled(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	if clock.NewRand(7).Int64N(int64(50*time.Millisecond)) <= 0 {
		t.Fatal("seed 7 yields a zero first backoff")
	}
	be := newRSBackend(t, n, rsStatusSeq(503))
	env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"), withRand(clock.NewRand(7)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := reDoAsync(env.client, reNewGet(t, ctx, env.s.URL+"/"))
	fake.BlockUntilDue(1, 250*time.Millisecond)
	baseline := runtime.NumGoroutine()
	cancel()

	r := reAwait(t, res)
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("client error = %v, want context.Canceled", r.err)
	}
	if got := be.hitCount(); got != 1 {
		t.Fatalf("hits = %d, want 1", got)
	}
	eventually(t, func() bool { return fake.Due(250*time.Millisecond) == 0 })
	eventually(t, func() bool { return runtime.NumGoroutine() <= baseline })
}

func TestRetryDeadlineShorterThanBackoff(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	const seed = 7
	if clock.NewRand(seed).Int64N(int64(10*time.Minute)) <= int64(time.Minute) {
		t.Fatal("seed 7 first backoff does not exceed the 1m deadline")
	}
	be := newRSBackend(t, n, rsStatusSeq(503))
	doc := reSingle([]string{be.URL}, "timeout: 1m\n    retry:\n      base_delay: 10m\n      max_delay: 10m\n")
	env := reServe(t, n, fake, doc, withRand(clock.NewRand(seed)))

	code, _, body := rsGet(t, env.client, env.s.URL+"/", http.MethodGet, nil, nil)
	if code != http.StatusServiceUnavailable || string(body) != "r1" {
		t.Fatalf("response = %d %q, want 503 \"r1\"", code, body)
	}
	if got := be.hitCount(); got != 1 {
		t.Fatalf("hits = %d, want 1", got)
	}
	if due := fake.Due(10 * time.Minute); due != 0 {
		t.Fatalf("armed waiters = %d, want 0", due)
	}
}

func runRetryBudget(t *testing.T, n testNetwork) {
	t.Helper()
	fake := clock.NewFake(time.Now())

	const burst = 100
	release := make(chan struct{})
	closeRelease := sync.OnceFunc(func() { close(release) })
	arrived := make(chan struct{}, burst)
	hits := map[string]*atomic.Int64{"/a": {}, "/b": {}, "/c": {}}
	be := newRSBackend(t, n, func(_ int64, w http.ResponseWriter, r *http.Request) {
		seq := hits[r.URL.Path].Add(1)
		if r.URL.Path == "/a" && seq <= burst {
			arrived <- struct{}{}
			<-release
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	t.Cleanup(closeRelease)

	budget := "retry:\n      attempts: 2\n      base_delay: 1ns\n      max_delay: 1ns\n      budget:\n        ratio: 0.1\n        min_per_second: 1\n        window: 10s\n"
	env := reServe(t, n, fake, rsDoc{
		hosts: []string{be.URL},
		routes: []rsRoute{
			{name: "a", path: "/a", extra: budget},
			{name: "b", path: "/b", extra: budget},
			{name: "c", path: "/c", extra: budget},
		},
	})

	const stress = 100
	cResults := make([]<-chan reResult, stress)
	for i := range cResults {
		cResults[i] = reDoAsync(env.client, reNewGet(t, context.Background(), env.s.URL+"/c"))
	}
	for _, ch := range cResults {
		if r := reAwait(t, ch); r.err != nil || r.code != http.StatusServiceUnavailable {
			t.Fatalf("stress response = %d (err %v), want 503", r.code, r.err)
		}
	}
	cRetries := rsMetric(t, env.g, "microgateway_retries_total", "route", "c")
	if cRetries < 10 || cRetries > 20 {
		t.Fatalf("retries(c) = %v, want within [10, 20]", cRetries)
	}
	if got := float64(hits["/c"].Load()); got != stress+cRetries {
		t.Fatalf("hits(/c) = %v, want %v", got, stress+cRetries)
	}
	if got := rsMetric(t, env.g, "microgateway_retry_budget_exhausted_total", "route", "c"); got != stress-cRetries {
		t.Fatalf("exhausted(c) = %v, want %v", got, stress-cRetries)
	}

	aResults := make([]<-chan reResult, burst)
	for i := range aResults {
		aResults[i] = reDoAsync(env.client, reNewGet(t, context.Background(), env.s.URL+"/a"))
	}
	for range burst {
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			t.Fatal("burst request did not arrive within 5s")
		}
	}
	closeRelease()
	for _, ch := range aResults {
		if r := reAwait(t, ch); r.err != nil || r.code != http.StatusServiceUnavailable {
			t.Fatalf("burst response = %d (err %v), want 503", r.code, r.err)
		}
	}
	if got := rsMetric(t, env.g, "microgateway_retries_total", "route", "a"); got != 20 {
		t.Fatalf("retries(a) = %v, want 20", got)
	}
	if got := rsMetric(t, env.g, "microgateway_retry_budget_exhausted_total", "route", "a"); got != 80 {
		t.Fatalf("exhausted(a) = %v, want 80", got)
	}
	if got := hits["/a"].Load(); got != 120 {
		t.Fatalf("hits(/a) = %d, want 120", got)
	}

	hitsBefore := hits["/a"].Load()
	code, _, _ := rsGet(t, env.client, env.s.URL+"/a", http.MethodGet, nil, nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("post-burst status = %d, want 503", code)
	}
	if got := hits["/a"].Load() - hitsBefore; got != 1 {
		t.Fatalf("post-burst hits(/a) increase = %d, want 1", got)
	}
	if got := rsMetric(t, env.g, "microgateway_retries_total", "route", "a"); got != 20 {
		t.Fatalf("retries(a) after post-burst GET = %v, want 20", got)
	}
	if got := rsMetric(t, env.g, "microgateway_retry_budget_exhausted_total", "route", "a"); got != 81 {
		t.Fatalf("exhausted(a) after post-burst GET = %v, want 81", got)
	}

	code, _, _ = rsGet(t, env.client, env.s.URL+"/b", http.MethodGet, nil, nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("route b status = %d, want 503", code)
	}
	if got := hits["/b"].Load(); got != 2 {
		t.Fatalf("hits(/b) = %d, want 2", got)
	}
	if got := rsMetric(t, env.g, "microgateway_retries_total", "route", "b"); got != 1 {
		t.Fatalf("retries(b) = %v, want 1", got)
	}
	if got := rsMetric(t, env.g, "microgateway_retry_budget_exhausted_total", "route", "b"); got != 0 {
		t.Fatalf("exhausted(b) = %v, want 0", got)
	}

	fake.Advance(11 * time.Second)
	hitsBefore = hits["/a"].Load()
	code, _, _ = rsGet(t, env.client, env.s.URL+"/a", http.MethodGet, nil, nil)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("status after window = %d, want 503", code)
	}
	if got := hits["/a"].Load() - hitsBefore; got != 2 {
		t.Fatalf("hits(/a) increase after window = %d, want 2", got)
	}
	if got := rsMetric(t, env.g, "microgateway_retries_total", "route", "a"); got != 21 {
		t.Fatalf("retries(a) after window = %v, want 21", got)
	}
}

func TestRetryBudget(t *testing.T) {
	runRetryBudget(t, newMemNetwork())
}

func runRetryBodyReplay(t *testing.T, n testNetwork) {
	t.Helper()
	fake := clock.NewFake(time.Now())

	t.Run("buffered body replayed", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(503, 200))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
		sent := reRandomBytes(1024)
		d := reDrive(t, fake, 250*time.Millisecond)
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodPut, bytes.NewReader(sent), nil)
		d.stop()
		if code != http.StatusOK || be.hitCount() != 2 {
			t.Fatalf("status = %d, hits = %d, want 200 and 2 hits", code, be.hitCount())
		}
		if !bytes.Equal(be.body(0), sent) || !bytes.Equal(be.body(1), sent) {
			t.Fatal("replayed body differs from the sent bytes")
		}
		reAssertIdle(t, env.g)
	})

	t.Run("oversized body with content length not retried", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(503, 200))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
		sent := reRandomBytes(100 * 1024)
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodPut, bytes.NewReader(sent), nil)
		if code != http.StatusServiceUnavailable || be.hitCount() != 1 {
			t.Fatalf("status = %d, hits = %d, want 503 and 1 hit", code, be.hitCount())
		}
		if !bytes.Equal(be.body(0), sent) {
			t.Fatal("upstream body differs from the sent bytes")
		}
	})

	t.Run("oversized chunked body not retried", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(503, 200))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
		sent := reRandomBytes(100 * 1024)
		pr, pw := io.Pipe()
		go func() {
			_, _ = pw.Write(sent)
			_ = pw.Close()
		}()
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodPut, pr, nil)
		if code != http.StatusServiceUnavailable || be.hitCount() != 1 {
			t.Fatalf("status = %d, hits = %d, want 503 and 1 hit", code, be.hitCount())
		}
		if !bytes.Equal(be.body(0), sent) {
			t.Fatal("upstream body differs from the sent bytes")
		}
	})

	t.Run("max_body_bytes zero disables buffering", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(503, 200))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry:\n      max_body_bytes: 0\n"))
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodPut, bytes.NewReader([]byte("0123456789")), nil)
		if code != http.StatusServiceUnavailable || be.hitCount() != 1 {
			t.Fatalf("status = %d, hits = %d, want 503 and 1 hit", code, be.hitCount())
		}
	})

	t.Run("bodyless DELETE retried", func(t *testing.T) {
		be := newRSBackend(t, n, rsStatusSeq(503, 200))
		env := reServe(t, n, fake, reSingle([]string{be.URL}, "retry: {}"))
		d := reDrive(t, fake, 250*time.Millisecond)
		code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodDelete, nil, nil)
		d.stop()
		if code != http.StatusOK || be.hitCount() != 2 {
			t.Fatalf("status = %d, hits = %d, want 200 and 2 hits", code, be.hitCount())
		}
	})
}

func TestRetryBodyReplay(t *testing.T) {
	runRetryBodyReplay(t, newMemNetwork())
}

func runRetryStreaming(t *testing.T, n testNetwork) {
	t.Helper()
	fake := clock.NewFake(time.Now())

	t.Run("websocket", func(t *testing.T) {
		be := newRSBackend(t, n, func(_ int64, w http.ResponseWriter, r *http.Request) {
			wsEchoHandler().ServeHTTP(w, r)
		})
		doc := rsDoc{
			hosts:   []string{be.URL},
			breaker: rsBreaker,
			routes:  []rsRoute{{name: "all", path: "/", extra: "retry: {}"}},
		}
		env := reServe(t, n, fake, doc)
		conn := n.dial(t, env.s)
		if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		_, err := io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: gw.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
		if err != nil {
			t.Fatal(err)
		}
		br := bufio.NewReader(conn)
		resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("status = %d, want 101", resp.StatusCode)
		}
		if _, err := io.WriteString(conn, "hello\n"); err != nil {
			t.Fatal(err)
		}
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line != "hello\n" {
			t.Fatalf("echo = %q, want \"hello\\n\"", line)
		}
		if got := be.hitCount(); got != 1 {
			t.Fatalf("hits = %d, want 1", got)
		}
		h := rsHost(t, env.g, be.URL)
		if h.Breaker == nil {
			t.Fatal("host breaker status missing")
		}
		if h.Breaker.Requests != 1 || h.Breaker.Failures != 0 {
			t.Fatalf("breaker requests = %d failures = %d, want 1 and 0", h.Breaker.Requests, h.Breaker.Failures)
		}
	})

	t.Run("sse incremental", func(t *testing.T) {
		be := newSSEBackend(t, n)
		doc := rsDoc{
			hosts:   []string{be.URL},
			breaker: rsBreaker,
			routes:  []rsRoute{{name: "all", path: "/", extra: "retry: {}"}},
		}
		env := reServe(t, n, fake, doc)
		resp, err := env.client.Get(env.s.URL + "/events")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		lines := reLines(resp.Body)
		if got := recvLine(t, lines); got != "data: one\n" {
			t.Fatalf("first line = %q, want \"data: one\"", got)
		}
		be.Release()
		if got := recvLine(t, lines); got != "\n" {
			t.Fatalf("blank line = %q, want newline", got)
		}
		if got := recvLine(t, lines); got != "data: two\n" {
			t.Fatalf("second event = %q, want \"data: two\"", got)
		}
		h := rsHost(t, env.g, be.URL)
		if h.Breaker == nil || h.Breaker.Requests != 1 {
			t.Fatalf("breaker requests = %v, want 1", h.Breaker)
		}
	})

	t.Run("aborted body after headers", func(t *testing.T) {
		be := newRSBackend(t, n, func(_ int64, w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "partial")
			rc := http.NewResponseController(w)
			_ = rc.Flush()
			conn, _, err := rc.Hijack()
			if err != nil {
				return
			}
			_ = conn.Close()
		})
		doc := rsDoc{
			hosts:   []string{be.URL},
			breaker: rsBreaker,
			routes:  []rsRoute{{name: "all", path: "/", extra: "retry: {}"}},
		}
		env := reServe(t, n, fake, doc)
		resp, err := env.client.Get(env.s.URL + "/")
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(data) != "partial" || readErr == nil {
			t.Fatalf("body = %q (err %v), want \"partial\" followed by an error", data, readErr)
		}
		if got := be.hitCount(); got != 1 {
			t.Fatalf("hits = %d, want 1", got)
		}
	})
}

func TestRetryStreaming(t *testing.T) {
	runRetryStreaming(t, newMemNetwork())
}

func TestSingleAttemptWithoutRetry(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())

	be := newRSBackend(t, n, rsStatusSeq(503))
	env := reServe(t, n, fake, reSingle([]string{be.URL}, ""))
	code, _, _ := rsGet(t, env.client, env.s.URL+"/", http.MethodGet, nil, nil)
	if code != http.StatusServiceUnavailable || be.hitCount() != 1 {
		t.Fatalf("status = %d, hits = %d, want 503 and 1 hit", code, be.hitCount())
	}

	refused := newRSBackend(t, n, nil)
	n.refuse(t, refused.srv)
	env = reServe(t, n, fake, reSingle([]string{refused.URL}, ""))
	code, _, _ = rsGet(t, env.client, env.s.URL+"/", http.MethodGet, nil, nil)
	if code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", code)
	}
	if got := n.dialCount(refused.srv.Addr); got != 1 {
		t.Fatalf("dialCount = %d, want 1", got)
	}
}
