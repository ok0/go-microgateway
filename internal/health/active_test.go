package health

import (
	"io"
	"math"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

const probeInterval = 10 * time.Second

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type trackedBody struct {
	r       io.Reader
	drained bool
	closed  bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if err == io.EOF {
		b.drained = true
	}
	return n, err
}

func (b *trackedBody) Close() error {
	b.closed = true
	return nil
}

type probeEvent struct {
	key  string
	pass bool
	err  error
}

type changeEvent struct {
	key     string
	healthy bool
	st      ActiveStatus
}

type harness struct {
	clk     *clock.Fake
	c       *Checker
	probed  chan probeEvent
	changes chan changeEvent
}

func floatp(v float64) *float64 {
	return &v
}

func activeCfg(unhealthy, healthy int) config.ActiveHealth {
	return config.ActiveHealth{
		Path:               "/health",
		Interval:           config.Duration(probeInterval),
		Timeout:            config.Duration(2 * time.Second),
		Jitter:             floatp(0),
		HealthyThreshold:   intp(healthy),
		UnhealthyThreshold: intp(unhealthy),
		ExpectedStatuses:   []string{"200-399"},
	}
}

func newHarness(t *testing.T, cfg config.ActiveHealth, seed uint64) *harness {
	t.Helper()
	h := &harness{
		clk:     clock.NewFake(healthStart),
		probed:  make(chan probeEvent, 64),
		changes: make(chan changeEvent, 64),
	}
	c, err := NewChecker(cfg, CheckerDeps{
		Clock: h.clk,
		Rand:  clock.NewRand(seed),
		OnProbe: func(key string, pass bool, err error) {
			h.probed <- probeEvent{key: key, pass: pass, err: err}
		},
		OnChange: func(key string, healthy bool, st ActiveStatus) {
			h.changes <- changeEvent{key: key, healthy: healthy, st: st}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.c = c
	t.Cleanup(func() { _ = c.Close() })
	return h
}

func (h *harness) start(rt http.RoundTripper, ts ...Target) {
	h.c.SetTargets(ts, rt)
}

func (h *harness) tick(t *testing.T, n int) []probeEvent {
	t.Helper()
	h.clk.BlockUntil(n)
	h.clk.Advance(probeInterval)
	out := make([]probeEvent, 0, n)
	for i := 0; i < n; i++ {
		select {
		case ev := <-h.probed:
			out = append(out, ev)
		case <-time.After(10 * time.Second):
			t.Fatal("probe did not complete")
		}
	}
	return out
}

func (h *harness) step(t *testing.T) probeEvent {
	t.Helper()
	return h.tick(t, 1)[0]
}

func (h *harness) noChange(t *testing.T) {
	t.Helper()
	select {
	case ch := <-h.changes:
		t.Fatalf("unexpected state change: %+v", ch)
	default:
	}
}

func (h *harness) expectChange(t *testing.T, key string, healthy bool) {
	t.Helper()
	select {
	case ch := <-h.changes:
		if ch.key != key || ch.healthy != healthy {
			t.Fatalf("state change = %+v, want %s healthy=%v", ch, key, healthy)
		}
	default:
		t.Fatal("expected a state change")
	}
}

func statusRT(code *atomic.Int32) rtFunc {
	return func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: int(code.Load()), Body: http.NoBody}, nil
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestHysteresis(t *testing.T) {
	var code atomic.Int32
	code.Store(500)
	h := newHarness(t, activeCfg(3, 2), 1)
	h.start(statusRT(&code), Target{Key: "h1", URL: mustURL(t, "http://h1:8080")})
	st := h.c.Status("h1")
	if st == nil {
		t.Fatal("status for h1 is nil")
	}

	h.step(t)
	h.step(t)
	if !st.Healthy() || st.Snapshot().State != "unknown" {
		t.Fatalf("after two failures: healthy=%v state=%s, want true unknown", st.Healthy(), st.Snapshot().State)
	}
	h.noChange(t)

	h.step(t)
	if st.Healthy() || st.Snapshot().State != "unhealthy" {
		t.Fatalf("after third failure: healthy=%v state=%s, want false unhealthy", st.Healthy(), st.Snapshot().State)
	}
	h.expectChange(t, "h1", false)

	code.Store(200)
	h.step(t)
	if st.Healthy() || st.Snapshot().State != "unhealthy" {
		t.Fatal("one pass must not restore an unhealthy host")
	}
	h.noChange(t)

	code.Store(500)
	h.step(t)
	h.noChange(t)
	code.Store(200)
	h.step(t)
	if st.Healthy() {
		t.Fatal("a failure must reset the pass streak")
	}

	h.step(t)
	if !st.Healthy() || st.Snapshot().State != "healthy" {
		t.Fatalf("after second pass: healthy=%v state=%s, want true healthy", st.Healthy(), st.Snapshot().State)
	}
	h.expectChange(t, "h1", true)
	h.noChange(t)
}

func TestProbeRequest(t *testing.T) {
	var code atomic.Int32
	code.Store(200)
	var got *http.Request
	var body *trackedBody
	rt := rtFunc(func(r *http.Request) (*http.Response, error) {
		got = r
		body = &trackedBody{r: strings.NewReader("payload")}
		return &http.Response{StatusCode: int(code.Load()), Body: body}, nil
	})
	cfg := activeCfg(3, 2)
	cfg.ExpectedStatuses = []string{"204-299"}
	h := newHarness(t, cfg, 1)
	h.start(rt, Target{Key: "http://backend:8080", URL: mustURL(t, "http://backend:8080/base")})

	ev := h.step(t)
	if ev.key != "http://backend:8080" || ev.pass || ev.err != nil {
		t.Fatalf("200 probe = %+v, want fail", ev)
	}
	if got.Method != http.MethodGet || got.URL.String() != "http://backend:8080/health" {
		t.Fatalf("request = %s %s, want GET http://backend:8080/health", got.Method, got.URL)
	}
	if got.Host != "backend:8080" {
		t.Fatalf("host = %q, want backend:8080", got.Host)
	}
	if got.Header.Get("User-Agent") != "microgateway-health-check" {
		t.Fatalf("user agent = %q", got.Header.Get("User-Agent"))
	}
	if !body.drained || !body.closed {
		t.Fatalf("body drained=%v closed=%v, want both true", body.drained, body.closed)
	}

	code.Store(204)
	ev = h.step(t)
	if !ev.pass {
		t.Fatalf("204 probe = %+v, want pass", ev)
	}
	if !body.drained || !body.closed {
		t.Fatalf("body drained=%v closed=%v, want both true", body.drained, body.closed)
	}
	h.noChange(t)
}

func TestProbeTimeout(t *testing.T) {
	rt := rtFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	h := newHarness(t, activeCfg(3, 2), 1)
	h.start(rt, Target{Key: "h1", URL: mustURL(t, "http://h1:8080")})

	h.clk.BlockUntil(1)
	h.clk.Advance(probeInterval)
	h.clk.BlockUntil(1)
	h.clk.Advance(2 * time.Second)

	select {
	case ev := <-h.probed:
		if ev.pass {
			t.Fatal("timed out probe must not pass")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("probe did not time out")
	}
}

func TestProbeBadTargetURL(t *testing.T) {
	rt := rtFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("round tripper must not be called for an invalid request")
		return nil, nil
	})
	h := newHarness(t, activeCfg(3, 2), 1)
	h.start(rt, Target{Key: "bad", URL: &url.URL{Scheme: "http", Host: "bad host"}})

	ev := h.step(t)
	if ev.pass || ev.err == nil {
		t.Fatalf("invalid target probe = %+v, want fail with error", ev)
	}
}

func TestOneProbeInFlight(t *testing.T) {
	var inflight, maxInflight, calls atomic.Int32
	rt := rtFunc(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		n := inflight.Add(1)
		defer inflight.Add(-1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	h := newHarness(t, activeCfg(3, 2), 1)
	h.start(rt, Target{Key: "h1", URL: mustURL(t, "http://h1:8080")})

	for i := 0; i < 10; i++ {
		h.clk.BlockUntil(1)
		h.clk.Advance(probeInterval)
	}
	h.c.Close()
	if calls.Load() == 0 {
		t.Fatal("no probe was issued")
	}
	if max := maxInflight.Load(); max != 1 {
		t.Fatalf("max concurrent probes = %d, want 1", max)
	}
}

func TestNextDelayRange(t *testing.T) {
	const interval = 10 * time.Second
	const jitter = 0.2
	rnd := clock.NewRand(1)
	lo := time.Duration(float64(interval) * (1 - jitter))
	hi := time.Duration(float64(interval) * (1 + jitter))
	slack := time.Microsecond
	var sum float64
	for i := 0; i < 10000; i++ {
		d := nextDelay(interval, jitter, rnd)
		if d < lo-slack || d > hi+slack {
			t.Fatalf("delay %v outside [%v, %v]", d, lo, hi)
		}
		sum += float64(d)
	}
	mean := sum / 10000
	if math.Abs(mean-float64(interval)) > 0.02*float64(interval) {
		t.Fatalf("mean delay %v differs from interval %v by more than 2%%", time.Duration(mean), interval)
	}
}

func TestJitter(t *testing.T) {
	const seed = 7
	const jitter = 0.2
	const interval = probeInterval
	const probes = 20
	step := interval / 100

	cfg := activeCfg(3, 2)
	cfg.Jitter = floatp(jitter)
	h := newHarness(t, cfg, seed)
	var code atomic.Int32
	code.Store(200)
	h.start(statusRT(&code), Target{Key: "h1", URL: mustURL(t, "http://h1:8080")})

	ref := clock.NewRand(seed)
	last := h.clk.Now()
	var intervals []time.Duration
	for k := 0; k < probes; k++ {
		h.clk.BlockUntil(1)
		deadline := h.clk.Now().Add(nextDelay(interval, jitter, ref))
		for h.clk.Now().Before(deadline) {
			h.clk.Advance(step)
		}
		select {
		case <-h.probed:
		case <-time.After(10 * time.Second):
			t.Fatal("probe did not complete")
		}
		now := h.clk.Now()
		intervals = append(intervals, now.Sub(last))
		last = now
	}

	lo := time.Duration(float64(interval)*(1-jitter)) - step
	hi := time.Duration(float64(interval)*(1+jitter)) + step
	varied := false
	for _, d := range intervals {
		if d < lo || d > hi {
			t.Fatalf("probe interval %v outside [%v, %v]", d, lo, hi)
		}
		if d != interval {
			varied = true
		}
	}
	if !varied {
		t.Fatal("jitter did not vary any probe interval")
	}
}

func TestSetTargets(t *testing.T) {
	var code atomic.Int32
	code.Store(200)
	rt := statusRT(&code)
	h1 := Target{Key: "h1", URL: mustURL(t, "http://h1:8080")}
	h2 := Target{Key: "h2", URL: mustURL(t, "http://h2:8080")}

	h := newHarness(t, activeCfg(3, 2), 1)
	h.start(rt, h1, h1)
	s1 := h.c.Status("h1")
	if s1 == nil {
		t.Fatal("probe for h1 did not start")
	}
	if ev := h.step(t); ev.key != "h1" {
		t.Fatalf("probe key = %s, want h1", ev.key)
	}
	if s1.Snapshot().ConsecutivePasses != 1 {
		t.Fatalf("passes = %d, want 1", s1.Snapshot().ConsecutivePasses)
	}

	h.c.SetTargets([]Target{h1, h2}, rt)
	if h.c.Status("h1") != s1 {
		t.Fatal("unchanged key must keep the same status")
	}
	if s1.Snapshot().ConsecutivePasses != 1 {
		t.Fatalf("passes after add = %d, want 1", s1.Snapshot().ConsecutivePasses)
	}
	if h.c.Status("h2") == nil {
		t.Fatal("added key must have a status")
	}
	for _, ev := range h.tick(t, 2) {
		if ev.key != "h1" && ev.key != "h2" {
			t.Fatalf("unexpected probe key %s", ev.key)
		}
	}
	if s1.Snapshot().ConsecutivePasses != 2 {
		t.Fatalf("passes after shared tick = %d, want 2", s1.Snapshot().ConsecutivePasses)
	}

	h.c.SetTargets([]Target{h2, h2}, rt)
	if h.c.Status("h1") != nil {
		t.Fatal("removed key must have no status")
	}
	h.clk.BlockUntil(1)
	if n := h.clk.Waiters(); n != 1 {
		t.Fatalf("pending timers after removal = %d, want 1", n)
	}
	h.clk.Advance(probeInterval)
	select {
	case ev := <-h.probed:
		if ev.key != "h2" {
			t.Fatalf("probe key = %s, want h2", ev.key)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("remaining target did not probe")
	}
	select {
	case ev := <-h.probed:
		t.Fatalf("removed target probed: %+v", ev)
	default:
	}

	if err := h.c.Close(); err != nil {
		t.Fatal(err)
	}
	h.c.SetTargets([]Target{{Key: "h3", URL: mustURL(t, "http://h3:8080")}}, rt)
	if h.c.Status("h3") != nil {
		t.Fatal("SetTargets after Close must not add targets")
	}
}

func TestCloseJoins(t *testing.T) {
	base := runtime.NumGoroutine()
	entered := make(chan struct{}, 4)
	rt := rtFunc(func(r *http.Request) (*http.Response, error) {
		entered <- struct{}{}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	h := newHarness(t, activeCfg(3, 2), 1)
	h.start(rt, Target{Key: "h1", URL: mustURL(t, "http://h1:8080")})

	h.clk.BlockUntil(1)
	h.clk.Advance(probeInterval)
	<-entered

	if err := h.c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.c.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100000 && runtime.NumGoroutine() > base; i++ {
		runtime.Gosched()
	}
	if n := runtime.NumGoroutine(); n > base {
		t.Fatalf("goroutines = %d after Close, baseline %d", n, base)
	}

	var ok atomic.Int32
	ok.Store(200)
	idle := newHarness(t, activeCfg(3, 2), 1)
	idle.start(statusRT(&ok),
		Target{Key: "h1", URL: mustURL(t, "http://h1:8080")},
		Target{Key: "h2", URL: mustURL(t, "http://h2:8080")},
		Target{Key: "h3", URL: mustURL(t, "http://h3:8080")},
	)
	idle.clk.BlockUntil(3)
	if err := idle.c.Close(); err != nil {
		t.Fatal(err)
	}
	if n := idle.clk.Waiters(); n != 0 {
		t.Fatalf("pending timers after Close = %d, want 0", n)
	}
}

func TestNewCheckerValidation(t *testing.T) {
	if _, err := NewChecker(activeCfg(3, 2), CheckerDeps{Clock: clock.NewFake(healthStart)}); err == nil {
		t.Fatal("nil Rand must be rejected")
	}
	for _, s := range []string{"2xx", "99", "1000", "200-", "300-200", "600", "20", "2000-2001"} {
		cfg := activeCfg(3, 2)
		cfg.ExpectedStatuses = []string{s}
		if _, err := NewChecker(cfg, CheckerDeps{Rand: clock.NewRand(1)}); err == nil {
			t.Fatalf("expected status %q must be rejected", s)
		}
	}

	cfg := activeCfg(3, 2)
	cfg.Jitter = nil
	c, err := NewChecker(cfg, CheckerDeps{Rand: clock.NewRand(1)})
	if err != nil {
		t.Fatal(err)
	}
	if c.jitter != 0.1 {
		t.Fatalf("default jitter = %v, want 0.1", c.jitter)
	}
	if c.clk == nil {
		t.Fatal("nil Clock must default to the real clock")
	}

	cfg = activeCfg(3, 2)
	cfg.ExpectedStatuses = []string{"204", "300-399"}
	c, err = NewChecker(cfg, CheckerDeps{Rand: clock.NewRand(1)})
	if err != nil {
		t.Fatal(err)
	}
	if len(c.expected) != 2 || !c.expects(204) || c.expects(200) || !c.expects(350) {
		t.Fatalf("expected ranges = %+v", c.expected)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStatusNilForUnknownKey(t *testing.T) {
	c, err := NewChecker(activeCfg(3, 2), CheckerDeps{Rand: clock.NewRand(1)})
	if err != nil {
		t.Fatal(err)
	}
	if c.Status("missing") != nil {
		t.Fatal("unknown key must have no status")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
