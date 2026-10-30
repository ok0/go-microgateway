package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
)

const (
	// real-clock limit for hung peers; with a healthy peer the limit runs on the fake clock.
	otLimit    = 50 * time.Millisecond
	otRequests = 200
	otWorkers  = 8
)

const otRetry = "retry:\n      attempts: 2\n      base_delay: 1ns\n      max_delay: 1ns\n      on_statuses: [504]\n      budget:\n        ratio: 1\n        min_per_second: 1000\n        window: 10s\n"

const otHeaderTimeouts = "connect: 1h\nresponse_header: 5s\n"

type otEnv struct {
	g      *microgateway.Gateway
	s      *testServer
	client *http.Client
	gw     *reTracker
	logs   *logBuf
	done   atomic.Int64
}

func otServe(t *testing.T, n testNetwork, doc rsDoc, opts ...microgateway.Option) *otEnv {
	t.Helper()
	logs := &logBuf{}
	g := n.gateway(t, doc.yaml(), append([]microgateway.Option{microgateway.WithLogger(newJSONLogger(logs))}, opts...)...)
	gw := &reTracker{h: g}
	return &otEnv{
		g:  g,
		s:  n.serve(t, gw),
		gw: gw,
		client: n.client(t, func(tr *http.Transport) {
			tr.MaxIdleConnsPerHost = otWorkers
		}),
		logs: logs,
	}
}

// state describes the completed requests, the gateway handlers in progress and each host's in-flight count.
func (e *otEnv) state(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "completed=%d gateway handlers in progress=%d", e.done.Load(), e.gw.inProgress())
	for _, h := range rsStatus(t, e.g).Hosts {
		fmt.Fprintf(&b, "\n%s in_flight=%d", h.URL, h.InFlight)
	}
	return b.String()
}

// runRounds sends n GET requests in rounds of otWorkers on fake. In each round it waits until every request of the
// round has completed or is parked on stalled with its limit armed and no other attempt in progress, advances fake by
// otLimit so that exactly the parked limits fire, and collects the round's results. Retry backoffs (1ns) are
// advanced by a driver that never reaches otLimit.
func (e *otEnv) runRounds(t *testing.T, n int, fake *clock.Fake, stalled string) []reResult {
	t.Helper()
	drv := reDrive(t, fake, time.Nanosecond)
	defer drv.stop()
	out := make([]reResult, 0, n)
	for len(out) < n {
		size := min(otWorkers, n-len(out))
		pending := make([]<-chan reResult, 0, size)
		for range size {
			pending = append(pending, reDoAsync(e.client, reNewGet(t, context.Background(), e.s.URL+"/")))
		}
		var round []reResult
		var parked int64
		eventually(t, fmt.Sprintf("round of %d requests to complete or park on %s", size, stalled), func() bool {
			for i, ch := range pending {
				if ch == nil {
					continue
				}
				select {
				case r := <-ch:
					round = append(round, r)
					pending[i] = nil
					e.done.Add(1)
				default:
				}
			}
			parked = rsHost(t, e.g, stalled).InFlight
			return len(round)+int(parked) == size && fake.Due(otLimit) == int(parked)
		}, func() string {
			return fmt.Sprintf("round results=%d parked=%d due=%d\n%s", len(round), parked, fake.Due(otLimit), e.state(t))
		})
		if parked > 0 {
			fake.Advance(otLimit)
		}
		for i, ch := range pending {
			if ch == nil {
				continue
			}
			r, _ := reRecv(t, "round result after the parked limits fired", ch, func() string { return e.state(t) })
			e.done.Add(1)
			round = append(round, r)
			pending[i] = nil
		}
		out = append(out, round...)
	}
	reRecv(t, "gateway handlers to return", e.gw.idleCh(), func() string { return e.state(t) })
	return out
}

// run sends n GET requests through e.client with otWorkers in flight and returns their results in arrival order.
func (e *otEnv) run(t *testing.T, n int) []reResult {
	t.Helper()
	jobs := make(chan *http.Request, n)
	for range n {
		jobs <- reNewGet(t, context.Background(), e.s.URL+"/")
	}
	close(jobs)
	results := make(chan reResult, n)
	for range otWorkers {
		go func() {
			for req := range jobs {
				r := <-reDoAsync(e.client, req)
				e.done.Add(1)
				results <- r
			}
		}()
	}
	out := make([]reResult, 0, n)
	for i := range n {
		r, _ := reRecv(t, fmt.Sprintf("result %d of %d", i+1, n), results, func() string { return e.state(t) })
		out = append(out, r)
	}
	reRecv(t, "gateway handlers to return", e.gw.idleCh(), func() string { return e.state(t) })
	return out
}

func otHang(_ int64, _ http.ResponseWriter, r *http.Request) {
	<-r.Context().Done()
}

func otText(body string) func(int64, http.ResponseWriter, *http.Request) {
	return func(_ int64, w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	}
}

func otDoc(hosts []string, timeouts, extra string) rsDoc {
	return rsDoc{hosts: hosts, timeouts: timeouts, routes: []rsRoute{{name: "all", path: "/", extra: extra}}}
}

func otAttempts(t *testing.T, g *microgateway.Gateway, host, outcome string) float64 {
	t.Helper()
	return rsMetricOr0(t, g, "microgateway_upstream_attempts_total", "upstream", "up", "host", host, "outcome", outcome)
}

func otRetries(t *testing.T, g *microgateway.Gateway) float64 {
	t.Helper()
	return rsMetricOr0(t, g, "microgateway_retries_total", "route", "all")
}

func otWant(t *testing.T, what string, got, want float64) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", what, got, want)
	}
}

func otErrorCode(body []byte) string {
	var env struct {
		Error apiError `json:"error"`
	}
	_ = json.Unmarshal(body, &env)
	return env.Error.Code
}

func otCheckAllOK(t *testing.T, results []reResult) {
	t.Helper()
	for i, r := range results {
		if r.err != nil || r.code != http.StatusOK || string(r.body) != "ok" {
			t.Fatalf("result %d = %+v, want 200 with body \"ok\"", i, r)
		}
	}
}

func otCheckAllInFlightZero(t *testing.T, g *microgateway.Gateway) {
	t.Helper()
	for _, h := range rsStatus(t, g).Hosts {
		if h.InFlight != 0 {
			t.Fatalf("host %s in_flight = %d after all requests, want 0", h.URL, h.InFlight)
		}
	}
}

func TestOneTimerPerLimit(t *testing.T) {
	n := newMemNetwork()

	t.Run("header timeout retried on the next host", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		hang := newRSBackend(t, n, otHang)
		ok := newRSBackend(t, n, nil)
		e := otServe(t, n, otDoc([]string{hang.URL, ok.URL}, fmt.Sprintf("connect: 1h\nresponse_header: %s\n", otLimit), otRetry), withClock(fake))
		before := otRetries(t, e.g)

		results := e.runRounds(t, otRequests, fake, hang.URL)
		otCheckAllOK(t, results)

		h1 := otAttempts(t, e.g, hang.srv.Addr, "failure")
		if h1 <= 0 {
			t.Fatalf("failure attempts on %s = %v, want > 0", hang.srv.Addr, h1)
		}
		otWant(t, "success attempts on the hanging host", otAttempts(t, e.g, hang.srv.Addr, "success"), 0)
		otWant(t, "ignored attempts on the hanging host", otAttempts(t, e.g, hang.srv.Addr, "ignored"), 0)
		otWant(t, "retries", otRetries(t, e.g)-before, h1)
		otWant(t, "success attempts on the healthy host", otAttempts(t, e.g, ok.srv.Addr, "success"), otRequests)
		otWant(t, "failure attempts on the healthy host", otAttempts(t, e.g, ok.srv.Addr, "failure"), 0)
		otWant(t, "healthy host hits", float64(ok.hitCount()), otRequests)
		otCheckAllInFlightZero(t, e.g)
	})

	t.Run("connect timeout retried on the next host", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		hole := n.blackhole(t)
		ok := newRSBackend(t, n, nil)
		e := otServe(t, n, otDoc([]string{"http://" + hole, ok.URL}, fmt.Sprintf("connect: %s\nresponse_header: 1h\n", otLimit), otRetry), withClock(fake))
		before := otRetries(t, e.g)

		results := e.runRounds(t, otRequests, fake, "http://"+hole)
		otCheckAllOK(t, results)

		h1 := otAttempts(t, e.g, hole, "failure")
		if h1 <= 0 {
			t.Fatalf("failure attempts on %s = %v, want > 0", hole, h1)
		}
		otWant(t, "retries", otRetries(t, e.g)-before, h1)
		otWant(t, "dials to the black-holed host", float64(n.dialCount(hole)), h1)
		otWant(t, "success attempts on the healthy host", otAttempts(t, e.g, ok.srv.Addr, "success"), otRequests)
		if recs := e.logs.records(t, "upstream error"); len(recs) != 0 {
			t.Fatalf("upstream error records = %v, want none", recs)
		}
	})

	t.Run("header timeout without retry is 504", func(t *testing.T) {
		hang := newRSBackend(t, n, otHang)
		// Real clock: the hung peer's response-header limit has to fire on the production clock.
		e := otServe(t, n, otDoc([]string{hang.URL}, fmt.Sprintf("connect: 1h\nresponse_header: %s\n", otLimit), ""),
			withClock(clock.Real()))

		results := e.run(t, otRequests)
		for i, r := range results {
			if r.err != nil || r.code != http.StatusGatewayTimeout || otErrorCode(r.body) != "gateway_timeout" {
				t.Fatalf("result %d = %+v, want 504 gateway_timeout", i, r)
			}
		}
		otWant(t, "failure attempts", otAttempts(t, e.g, hang.srv.Addr, "failure"), otRequests)
		recs := e.logs.records(t, "upstream error")
		for _, rec := range recs {
			if rec["error"] != "upstream response header timeout" {
				t.Fatalf("upstream error record = %v, want error \"upstream response header timeout\"", rec)
			}
		}
		otWant(t, "upstream error records", float64(len(recs)), otRequests)
	})

	t.Run("connect timeout without retry is 502", func(t *testing.T) {
		hole := n.blackhole(t)
		// Real clock: the black-holed peer's connect limit has to fire on the production clock.
		e := otServe(t, n, otDoc([]string{"http://" + hole}, fmt.Sprintf("connect: %s\nresponse_header: 1h\n", otLimit), ""),
			withClock(clock.Real()))

		results := e.run(t, otRequests)
		for i, r := range results {
			if r.err != nil || r.code != http.StatusBadGateway || otErrorCode(r.body) != "bad_gateway" {
				t.Fatalf("result %d = %+v, want 502 bad_gateway", i, r)
			}
		}
		otWant(t, "failure attempts", otAttempts(t, e.g, hole, "failure"), otRequests)
		recs := e.logs.records(t, "upstream error")
		for _, rec := range recs {
			if rec["error"] != "dial tcp: connect timeout" {
				t.Fatalf("upstream error record = %v, want error \"dial tcp: connect timeout\"", rec)
			}
		}
		otWant(t, "upstream error records", float64(len(recs)), otRequests)
	})
}

// otSignal is a slog.Handler that closes ch once it receives a record with message msg.
type otSignal struct {
	msg  string
	once sync.Once
	ch   chan struct{}
}

func (s *otSignal) Enabled(context.Context, slog.Level) bool { return true }

func (s *otSignal) Handle(_ context.Context, r slog.Record) error {
	if r.Message == s.msg {
		s.once.Do(func() { close(s.ch) })
	}
	return nil
}

func (s *otSignal) WithAttrs([]slog.Attr) slog.Handler { return s }

func (s *otSignal) WithGroup(string) slog.Handler { return s }

func otPrecedenceEnv(t *testing.T, n testNetwork, fake *clock.Fake, be *rsBackend, extra string, logs *logBuf) reEnv {
	t.Helper()
	doc := otDoc([]string{be.URL}, otHeaderTimeouts, extra)
	return reServe(t, n, fake, doc, microgateway.WithLogger(newJSONLogger(logs)))
}

func TestHeaderTimeoutPrecedence(t *testing.T) {
	const header = 5 * time.Second

	t.Run("response before the header timer", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		n := newMemNetwork()
		logs := &logBuf{}
		be := newRSBackend(t, n, otText("first"))
		env := otPrecedenceEnv(t, n, fake, be, otRetry, logs)
		before := otRetries(t, env.g)

		r := reAwait(t, env, reDoAsync(env.client, reNewGet(t, context.Background(), env.s.URL+"/")))
		if r.err != nil || r.code != http.StatusOK || string(r.body) != "first" {
			t.Fatalf("reply = %d %q (err %v), want 200 \"first\"", r.code, r.body, r.err)
		}
		if w := fake.Waiters(); w != 0 {
			t.Fatalf("fake waiters after the response = %d, want 0 (header timer stopped)", w)
		}
		fake.Advance(header)
		reAwaitIdle(t, env)

		otWant(t, "backend hits", float64(be.hitCount()), 1)
		otWant(t, "retries", otRetries(t, env.g)-before, 0)
		otWant(t, "success attempts", otAttempts(t, env.g, be.srv.Addr, "success"), 1)
		otWant(t, "failure attempts", otAttempts(t, env.g, be.srv.Addr, "failure"), 0)
		if recs := logs.records(t, "upstream error"); len(recs) != 0 {
			t.Fatalf("upstream error records = %v, want none", recs)
		}
	})

	t.Run("header timer before the response", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		n := newMemNetwork()
		logs := &logBuf{}
		be := newRSBackend(t, n, func(seq int64, w http.ResponseWriter, r *http.Request) {
			if seq == 1 {
				otHang(seq, w, r)
				return
			}
			_, _ = io.WriteString(w, "ok")
		})
		env := otPrecedenceEnv(t, n, fake, be, otRetry, logs)
		before := otRetries(t, env.g)

		res := reDoAsync(env.client, reNewGet(t, context.Background(), env.s.URL+"/"))
		reAwaitHits(t, env, be, 1)
		reWaitDue(t, env, 1, header, "response-header timer armed")
		fake.Advance(header)
		d := reDrive(t, fake, 250*time.Millisecond)
		r := reAwait(t, env, res)
		d.stop()
		reAwaitIdle(t, env)

		if r.err != nil || r.code != http.StatusOK || string(r.body) != "ok" {
			t.Fatalf("reply = %d %q (err %v), want 200 \"ok\"", r.code, r.body, r.err)
		}
		otWant(t, "backend hits", float64(be.hitCount()), 2)
		otWant(t, "retries", otRetries(t, env.g)-before, 1)
		otWant(t, "failure attempts", otAttempts(t, env.g, be.srv.Addr, "failure"), 1)
		otWant(t, "success attempts", otAttempts(t, env.g, be.srv.Addr, "success"), 1)
		if recs := logs.records(t, "upstream error"); len(recs) != 0 {
			t.Fatalf("upstream error records = %v, want none", recs)
		}
	})

	t.Run("route deadline equal to the header timer", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		n := newMemNetwork()
		logs := &logBuf{}
		be := newRSBackend(t, n, otHang)
		env := otPrecedenceEnv(t, n, fake, be, "timeout: 5s\n    "+otRetry, logs)
		before := otRetries(t, env.g)

		res := reDoAsync(env.client, reNewGet(t, context.Background(), env.s.URL+"/"))
		reAwaitHits(t, env, be, 1)
		reWaitDue(t, env, 2, header, "route deadline and response-header timer armed")
		fake.Advance(header)
		r := reAwait(t, env, res)
		reAwaitIdle(t, env)

		if r.err != nil || r.code != http.StatusGatewayTimeout || otErrorCode(r.body) != "gateway_timeout" {
			t.Fatalf("reply = %d %q (err %v), want 504 gateway_timeout", r.code, r.body, r.err)
		}
		otWant(t, "backend hits", float64(be.hitCount()), 1)
		otWant(t, "retries", otRetries(t, env.g)-before, 0)
		recs := logs.records(t, "upstream error")
		if len(recs) != 1 || recs[0]["error"] != "context deadline exceeded" || recs[0]["status"] != float64(http.StatusGatewayTimeout) {
			t.Fatalf("upstream error records = %v, want exactly one with error \"context deadline exceeded\" and status 504", recs)
		}
		for _, rec := range logs.records(t, "upstream error") {
			if rec["error"] == "upstream response header timeout" {
				t.Fatalf("upstream error record = %v, want no response-header timeout", rec)
			}
		}
	})

	t.Run("client cancel during the header wait", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		n := newMemNetwork()
		logs := &logBuf{}
		be := newRSBackend(t, n, otHang)
		env := otPrecedenceEnv(t, n, fake, be, otRetry, logs)
		before := otRetries(t, env.g)
		canceledBefore := rsMetricOr0(t, env.g, "microgateway_requests_total", "route", "all", "method", "GET", "code", "499")

		ctx, cancel := context.WithCancel(context.Background())
		res := reDoAsync(env.client, reNewGet(t, ctx, env.s.URL+"/"))
		reAwaitHits(t, env, be, 1)
		reWaitDue(t, env, 1, header, "response-header timer armed")
		cancel()
		r := reAwait(t, env, res)
		reAwaitIdle(t, env)

		if r.err == nil {
			t.Fatalf("reply = %d %q, want a client error after cancel", r.code, r.body)
		}
		if w := fake.Waiters(); w != 0 {
			t.Fatalf("fake waiters after cancel = %d, want 0 (header timer stopped)", w)
		}
		fake.Advance(header)

		otWant(t, "499 requests delta", rsMetricOr0(t, env.g, "microgateway_requests_total", "route", "all", "method", "GET", "code", "499")-canceledBefore, 1)
		otWant(t, "ignored attempts", otAttempts(t, env.g, be.srv.Addr, "ignored"), 1)
		otWant(t, "failure attempts", otAttempts(t, env.g, be.srv.Addr, "failure"), 0)
		otWant(t, "retries", otRetries(t, env.g)-before, 0)
		otWant(t, "backend hits", float64(be.hitCount()), 1)
		canceled := logs.records(t, "client canceled request")
		if len(canceled) != 1 || canceled[0]["error"] != "context canceled" {
			t.Fatalf("client canceled request records = %v, want one with error \"context canceled\"", canceled)
		}
		if recs := logs.records(t, "upstream error"); len(recs) != 0 {
			t.Fatalf("upstream error records = %v, want none", recs)
		}
	})
}

func TestActiveProbeBoundedWithoutTransportHeaderLimit(t *testing.T) {
	n := newMemNetwork()
	gate := newHoldGate()
	t.Cleanup(gate.open)
	s := newHostStub(t, n, "b0", "/health", gate)
	s.setProbeHold(true)

	doc := upstreamDoc{
		name: "up",
		urls: []string{s.URL},
		active: "{path: /health, interval: 100ms, jitter: 0, timeout: 50ms, healthy_threshold: 1, " +
			"unhealthy_threshold: 1, expected_statuses: [\"200-399\"]}",
	}
	sig := &otSignal{msg: "upstream host failed active health check", ch: make(chan struct{})}
	// Real clock: the hung probe's timeout has to fire on the production clock.
	g := n.gateway(t, doc.yaml(), microgateway.WithLogger(slog.New(sig)), withClock(clock.Real()))

	reRecv(t, "hung probe to reach the host", s.probeArrived, func() string {
		return fmt.Sprintf("probes=%d", s.probeCount())
	})
	reRecv(t, "active check to fail at its timeout", sig.ch, func() string {
		return fmt.Sprintf("%+v", upstreamStatus(t, g, "up").Hosts)
	})

	h := hostByURL(upstreamStatus(t, g, "up"), s.URL)
	if h.ActiveCheck == nil || h.ActiveCheck.Status != "unhealthy" || h.Healthy {
		t.Fatalf("host status = %+v, want unhealthy active check", h)
	}
	failed := func() float64 {
		return rsMetricOr0(t, g, "microgateway_upstream_active_checks_total", "upstream", "up", "host", s.label(), "result", "fail")
	}
	eventually(t, "a failed active check to be counted", func() bool { return failed() >= 1 }, func() string {
		return fmt.Sprintf("failed active checks for %s = %v", s.label(), failed())
	})
}
