package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/upstream"
)

var healthEpoch = time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)

const (
	stubAnswer int32 = iota
	stubHangHead
	stubHoldBody
)

const (
	passiveHourFlow    = "{consecutive_failures: 3, base_ejection_time: 1h, max_ejection_time: 1h, max_ejection_percent: 50}"
	passiveTwoHourFlow = "{consecutive_failures: 3, base_ejection_time: 2h, max_ejection_time: 2h, max_ejection_percent: 50}"
)

func passiveFlow(percent int) string {
	return fmt.Sprintf("{consecutive_failures: 3, base_ejection_time: 30s, max_ejection_time: 120s, max_ejection_percent: %d}", percent)
}

func activeFlow(path, expected string) string {
	return fmt.Sprintf("{path: %s, interval: 10s, jitter: 0, timeout: 2s, healthy_threshold: 2, unhealthy_threshold: 3, expected_statuses: [%q]}", path, expected)
}

type holdGate struct {
	arrived chan struct{}
	release chan struct{}
	once    sync.Once
}

func newHoldGate() *holdGate {
	return &holdGate{arrived: make(chan struct{}, 16), release: make(chan struct{})}
}

func (g *holdGate) open() {
	g.once.Do(func() { close(g.release) })
}

func pulse(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

type hostStub struct {
	name      string
	URL       string
	srv       *testServer
	probePath string
	gate      *holdGate

	status      atomic.Int32
	mode        atomic.Int32
	hitCount    atomic.Int64
	probeStatus atomic.Int32
	probeHold   atomic.Bool

	mu           sync.Mutex
	probes       int
	probeActive  int
	probePeakVal int

	probeArrived chan struct{}
}

func newHostStub(t *testing.T, n testNetwork, name, probePath string, gate *holdGate) *hostStub {
	t.Helper()
	s := &hostStub{
		name:         name,
		probePath:    probePath,
		gate:         gate,
		probeArrived: make(chan struct{}, 16),
	}
	s.status.Store(http.StatusOK)
	s.probeStatus.Store(http.StatusOK)
	s.srv = n.serve(t, s)
	s.URL = s.srv.URL
	return s
}

func (s *hostStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.probePath != "" && r.URL.Path == s.probePath {
		s.serveProbe(w, r)
		return
	}
	s.hitCount.Add(1)
	switch s.mode.Load() {
	case stubHangHead:
		pulse(s.gate.arrived)
		if !s.wait(r) {
			return
		}
	case stubHoldBody:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = http.NewResponseController(w).Flush()
		pulse(s.gate.arrived)
		if !s.wait(r) {
			return
		}
		_, _ = io.WriteString(w, "{}\n")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(int(s.status.Load()))
	_ = json.NewEncoder(w).Encode(echo{Backend: s.name, Path: r.URL.Path})
}

func (s *hostStub) wait(r *http.Request) bool {
	select {
	case <-s.gate.release:
		return true
	case <-r.Context().Done():
		return false
	}
}

func (s *hostStub) serveProbe(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.probes++
	s.probeActive++
	s.probePeakVal = max(s.probePeakVal, s.probeActive)
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.probeActive--
		s.mu.Unlock()
	}()
	if s.probeHold.Load() {
		pulse(s.probeArrived)
		<-r.Context().Done()
		return
	}
	w.WriteHeader(int(s.probeStatus.Load()))
}

func (s *hostStub) setStatus(code int) { s.status.Store(int32(code)) }

func (s *hostStub) setMode(m int32) { s.mode.Store(m) }

func (s *hostStub) setProbeStatus(code int) {
	s.probeStatus.Store(int32(code))
}

func (s *hostStub) setProbeHold(on bool) { s.probeHold.Store(on) }

func (s *hostStub) hits() int64 { return s.hitCount.Load() }

func (s *hostStub) probeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probes
}

func (s *hostStub) probePeak() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probePeakVal
}

func (s *hostStub) probeInFlight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probeActive
}

func (s *hostStub) label() string {
	return strings.TrimPrefix(s.URL, "http://")
}

func urlsOf(hosts []*hostStub) []string {
	out := make([]string, len(hosts))
	for i, h := range hosts {
		out[i] = h.URL
	}
	return out
}

type upstreamDoc struct {
	name          string
	urls          []string
	weights       []int
	passive       string
	active        string
	addRoute      bool
	badMiddleware bool
}

func (d upstreamDoc) yaml() string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	b.WriteString(listenersYAML)
	b.WriteString("upstreams:\n")
	fmt.Fprintf(&b, "  - name: %s\n    hosts:\n", d.name)
	for i, u := range d.urls {
		fmt.Fprintf(&b, "      - url: %q\n", u)
		if i < len(d.weights) && d.weights[i] > 0 {
			fmt.Fprintf(&b, "        weight: %d\n", d.weights[i])
		}
	}
	b.WriteString("    load_balancer: {algorithm: round_robin}\n")
	b.WriteString("    timeouts: {connect: 1h, response_header: 1h}\n")
	if d.passive != "" {
		fmt.Fprintf(&b, "    passive_health: %s\n", d.passive)
	}
	if d.active != "" {
		fmt.Fprintf(&b, "    active_health: %s\n", d.active)
	}
	b.WriteString("routes:\n  - name: api\n    match:\n      path: /api\n")
	fmt.Fprintf(&b, "    upstream: %s\n", d.name)
	if d.badMiddleware {
		b.WriteString("    middlewares: [m]\n")
	}
	if d.addRoute {
		b.WriteString("  - name: other\n    match:\n      path: /other\n")
		fmt.Fprintf(&b, "    upstream: %s\n", d.name)
	}
	if d.badMiddleware {
		b.WriteString("middlewares:\n  - name: m\n    type: nope\n")
	}
	return b.String()
}

type healthRig struct {
	t      *testing.T
	fake   *clock.Fake
	logs   *logBuf
	g      *microgateway.Gateway
	url    string
	client *http.Client
	hosts  []*hostStub
	gate   *holdGate
	probes float64
}

func newHealthRig(t *testing.T, n testNetwork, doc upstreamDoc, hosts []*hostStub, gate *holdGate) *healthRig {
	t.Helper()
	fake := clock.NewFake(healthEpoch)
	logs := &logBuf{}
	g := n.gateway(t, doc.yaml(), withClock(fake), microgateway.WithLogger(newJSONLogger(logs)))
	ts := n.serve(t, g)
	t.Cleanup(gate.open)
	return &healthRig{
		t:      t,
		fake:   fake,
		logs:   logs,
		g:      g,
		url:    ts.URL,
		client: n.client(t, nil),
		hosts:  hosts,
		gate:   gate,
	}
}

func newPassiveRig(t *testing.T, n testNetwork, count, percent int) (*healthRig, []*hostStub) {
	t.Helper()
	gate := newHoldGate()
	hosts := make([]*hostStub, count)
	for i := range hosts {
		hosts[i] = newHostStub(t, n, fmt.Sprintf("b%d", i), "", gate)
	}
	doc := upstreamDoc{name: "up", urls: urlsOf(hosts), passive: passiveFlow(percent)}
	return newHealthRig(t, n, doc, hosts, gate), hosts
}

func newActiveRig(t *testing.T, n testNetwork, count int, path, expected string) (*healthRig, []*hostStub) {
	t.Helper()
	gate := newHoldGate()
	hosts := make([]*hostStub, count)
	for i := range hosts {
		hosts[i] = newHostStub(t, n, fmt.Sprintf("b%d", i), path, gate)
	}
	doc := upstreamDoc{name: "up", urls: urlsOf(hosts), passive: passiveHourFlow, active: activeFlow(path, expected)}
	return newHealthRig(t, n, doc, hosts, gate), hosts
}

func (r *healthRig) get(path string) (int, []byte) {
	r.t.Helper()
	resp, err := r.client.Get(r.url + path)
	if err != nil {
		r.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		r.t.Fatal(err)
	}
	return resp.StatusCode, body
}

func (r *healthRig) expect(want int) {
	r.t.Helper()
	if got, _ := r.get("/api/x"); got != want {
		r.t.Fatalf("status = %d, want %d", got, want)
	}
}

func (r *healthRig) getCanceled(path string) (context.CancelFunc, <-chan error) {
	r.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url+path, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		resp, err := r.client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
		}
		done <- err
	}()
	return cancel, done
}

func (r *healthRig) status() upstream.Status {
	return upstreamStatus(r.t, r.g, "up")
}

func (r *healthRig) host(s *hostStub) upstream.HostStatus {
	return hostByURL(r.status(), s.URL)
}

func (r *healthRig) metrics() string {
	return metricsText(r.t, r.g)
}

func (r *healthRig) metric(name string, labels ...string) float64 {
	return metricValue(r.t, r.metrics(), series(name, labels...))
}

func (r *healthRig) attempts(s *hostStub) float64 {
	total := 0.0
	for _, o := range []string{"success", "failure", "ignored"} {
		total += r.metric("microgateway_upstream_attempts_total", "upstream", "up", "host", s.label(), "outcome", o)
	}
	return total
}

func (r *healthRig) activeChecks() float64 {
	return metricSum(r.t, r.metrics(), `microgateway_upstream_active_checks_total{upstream="up",`)
}

func (r *healthRig) reload(doc upstreamDoc) {
	r.t.Helper()
	if err := r.g.ReloadYAML([]byte(doc.yaml())); err != nil {
		r.t.Fatal(err)
	}
}

func (r *healthRig) tick(live, other int) {
	r.t.Helper()
	r.fake.BlockUntil(live + other)
	if got := r.fake.Waiters(); got != live+other {
		r.t.Fatalf("pending timers = %d, want %d", got, live+other)
	}
	r.fake.Advance(10 * time.Second)
	r.probes += float64(live)
	waitProbes(r.t, r.fake, r.activeChecks, r.probes, live)
}

func (r *healthRig) driveUntilEjected(s *hostStub) {
	r.t.Helper()
	for range 40 {
		if r.host(s).Ejected {
			return
		}
		r.get("/api/x")
	}
	r.t.Fatal("host was not ejected within 40 requests")
}

func (r *healthRig) driveUntilConsecutive(s *hostStub, n int) {
	r.t.Helper()
	for range 40 {
		if r.host(s).ConsecutiveFailures == n {
			return
		}
		r.get("/api/x")
	}
	r.t.Fatalf("consecutive failures did not reach %d within 40 requests", n)
}

func metricSum(t *testing.T, body, prefix string) float64 {
	t.Helper()
	total := 0.0
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
		if err != nil {
			t.Fatal(err)
		}
		total += v
	}
	return total
}

func checkActive(t *testing.T, st upstream.HostStatus, state string, passes, failures int, healthy bool, last time.Time) {
	t.Helper()
	ac := st.ActiveCheck
	if ac == nil {
		t.Fatal("active_check is null")
	}
	if ac.Status != state || ac.ConsecutivePasses != passes || ac.ConsecutiveFailures != failures || st.Healthy != healthy {
		t.Fatalf("active state = {status %s passes %d failures %d healthy %v}, want {status %s passes %d failures %d healthy %v}",
			ac.Status, ac.ConsecutivePasses, ac.ConsecutiveFailures, st.Healthy, state, passes, failures, healthy)
	}
	switch {
	case last.IsZero() && ac.LastResult != nil:
		t.Fatalf("last_result = %v, want null", *ac.LastResult)
	case !last.IsZero() && (ac.LastResult == nil || !ac.LastResult.Equal(last)):
		t.Fatalf("last_result = %v, want %v", ac.LastResult, last)
	}
}

func runPassiveEjection(t *testing.T, n testNetwork) {
	t.Run("ejects after three server errors", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 50)
		bad := hosts[0]
		bad.setStatus(http.StatusServiceUnavailable)
		for i := range 12 {
			want := http.StatusOK
			if i%4 == 0 {
				want = http.StatusServiceUnavailable
			}
			r.expect(want)
		}
		st := r.host(bad)
		if !st.Ejected || st.Healthy || st.EjectionCount != 1 || st.EjectionRemainingSeconds != 30 {
			t.Fatalf("bad host = %+v, want ejected once for 30s and unhealthy", st)
		}
		if got := bad.hits(); got != 3 {
			t.Fatalf("bad host hits = %d, want 3", got)
		}
		if v := r.metric("microgateway_upstream_ejections_total", "upstream", "up", "host", bad.label()); v != 1 {
			t.Fatalf("ejections_total = %v, want 1", v)
		}
		if v := r.metric("microgateway_upstream_host_healthy", "upstream", "up", "host", bad.label()); v != 0 {
			t.Fatalf("host_healthy = %v, want 0", v)
		}
		recs := r.logs.records(t, "upstream host ejected")
		if len(recs) != 1 || recs[0]["upstream"] != "up" || recs[0]["host"] != bad.label() {
			t.Fatalf("ejection log records = %v, want one for up/%s", recs, bad.label())
		}
		for range 1000 {
			r.expect(http.StatusOK)
		}
		if got := bad.hits(); got != 3 {
			t.Fatalf("bad host hits during ejection = %d, want 3", got)
		}
	})

	t.Run("client errors and canceled requests never eject", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 50)
		bad := hosts[0]
		bad.setStatus(http.StatusNotFound)
		for i := range 16 {
			want := http.StatusOK
			if i%4 == 0 {
				want = http.StatusNotFound
			}
			r.expect(want)
		}
		bad.setStatus(http.StatusOK)
		ignored := func() float64 {
			return r.metric("microgateway_upstream_attempts_total", "upstream", "up", "host", bad.label(), "outcome", "ignored")
		}
		base := ignored()
		for k := range 3 {
			bad.setMode(stubHangHead)
			cancel, done := r.getCanceled("/api/x")
			awaitSignal(t, r.gate.arrived, "canceled request to reach the backend")
			cancel()
			if err := <-done; err == nil {
				t.Fatal("canceled request completed without error")
			}
			eventually(t, fmt.Sprintf("ignored attempt %d to be counted", k+1), func() bool { return ignored() == base+float64(k+1) }, func() string { return fmt.Sprint(ignored()) })
			bad.setMode(stubAnswer)
			for range 3 {
				r.expect(http.StatusOK)
			}
		}
		st := r.host(bad)
		if st.Ejected || st.EjectionCount != 0 {
			t.Fatalf("bad host = %+v, want never ejected", st)
		}
		if recs := r.logs.records(t, "upstream host ejected"); len(recs) != 0 {
			t.Fatalf("ejection log records = %d, want 0", len(recs))
		}
	})

	t.Run("a success resets the failure run", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 50)
		bad := hosts[0]
		for _, code := range []int{503, 503, 200, 503, 503} {
			bad.setStatus(code)
			r.expect(code)
			for range 3 {
				r.expect(http.StatusOK)
			}
		}
		st := r.host(bad)
		if st.Ejected || st.EjectionCount != 0 || st.ConsecutiveFailures != 2 {
			t.Fatalf("bad host = %+v, want not ejected with 2 consecutive failures", st)
		}
		if got := bad.hits(); got != 5 {
			t.Fatalf("bad host hits = %d, want 5", got)
		}
		if recs := r.logs.records(t, "upstream host ejected"); len(recs) != 0 {
			t.Fatalf("ejection log records = %d, want 0", len(recs))
		}
	})

	t.Run("refused dials eject after three bad gateway responses", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 50)
		bad := hosts[0]
		n.refuse(t, bad.srv)
		for i := range 12 {
			want := http.StatusOK
			if i%4 == 0 {
				want = http.StatusBadGateway
			}
			r.expect(want)
		}
		st := r.host(bad)
		if !st.Ejected || st.EjectionCount != 1 {
			t.Fatalf("bad host = %+v, want ejected once", st)
		}
		if v := r.metric("microgateway_upstream_ejections_total", "upstream", "up", "host", bad.label()); v != 1 {
			t.Fatalf("ejections_total = %v, want 1", v)
		}
	})

	t.Run("hung backend ejects after three gateway timeouts", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 50)
		bad := hosts[0]
		bad.setMode(stubHangHead)
		for range 3 {
			ch := startGet(r.client, r.url+"/api/x")
			awaitSignal(t, r.gate.arrived, "hung request to reach the backend")
			r.fake.BlockUntil(1)
			r.fake.Advance(time.Hour)
			if got := awaitGet(t, ch).status; got != http.StatusGatewayTimeout {
				t.Fatalf("hung request status = %d, want 504", got)
			}
			for range 3 {
				r.expect(http.StatusOK)
			}
		}
		st := r.host(bad)
		if !st.Ejected || st.EjectionCount != 1 || st.EjectionRemainingSeconds != 30 {
			t.Fatalf("bad host = %+v, want ejected once for 30s", st)
		}
	})

	t.Run("ejection time grows and is capped", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 50)
		bad := hosts[0]
		bad.setStatus(http.StatusServiceUnavailable)
		for cycle, secs := range []int{30, 60, 90, 120, 120} {
			r.driveUntilEjected(bad)
			st := r.host(bad)
			if st.EjectionCount != cycle+1 || st.EjectionRemainingSeconds != float64(secs) {
				t.Fatalf("cycle %d: bad host = %+v, want count %d and %ds remaining", cycle+1, st, cycle+1, secs)
			}
			r.fake.Advance(time.Duration(secs) * time.Second)
		}
	})

	t.Run("ejection count decays by one per max ejection time", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 50)
		bad := hosts[0]
		bad.setStatus(http.StatusServiceUnavailable)
		r.driveUntilEjected(bad)
		r.fake.Advance(150 * time.Second)
		st := r.host(bad)
		if st.Ejected || st.EjectionCount != 0 {
			t.Fatalf("bad host = %+v, want expired with count 0", st)
		}
		r.driveUntilEjected(bad)
		st = r.host(bad)
		if st.EjectionCount != 1 || st.EjectionRemainingSeconds != 30 {
			t.Fatalf("bad host = %+v, want count 1 and 30s remaining", st)
		}
	})

	t.Run("ejection stops at the percentage cap", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 50)
		for _, h := range hosts[:3] {
			h.setStatus(http.StatusServiceUnavailable)
		}
		for range 40 {
			r.get("/api/x")
		}
		ejected := 0
		for _, h := range hosts[:3] {
			if r.host(h).Ejected {
				ejected++
			}
		}
		if ejected != 2 {
			t.Fatalf("ejected hosts = %d, want 2", ejected)
		}
		if r.host(hosts[3]).Ejected {
			t.Fatal("healthy host was ejected")
		}
		if v := r.metric("microgateway_upstream_ejections_refused_total", "upstream", "up"); v <= 0 {
			t.Fatalf("ejections_refused_total = %v, want > 0", v)
		}
	})

	t.Run("zero percent never ejects", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 4, 0)
		bad := hosts[0]
		bad.setStatus(http.StatusServiceUnavailable)
		for i := range 40 {
			want := http.StatusOK
			if i%4 == 0 {
				want = http.StatusServiceUnavailable
			}
			r.expect(want)
		}
		st := r.host(bad)
		if st.Ejected || st.EjectionCount != 0 {
			t.Fatalf("bad host = %+v, want never ejected", st)
		}
		if v := r.metric("microgateway_upstream_ejections_total", "upstream", "up", "host", bad.label()); v != 0 {
			t.Fatalf("ejections_total = %v, want 0", v)
		}
	})

	t.Run("panic mode serves from all hosts and recovers", func(t *testing.T) {
		r, hosts := newPassiveRig(t, n, 2, 100)
		for _, h := range hosts {
			h.setStatus(http.StatusServiceUnavailable)
		}
		for range 6 {
			r.expect(http.StatusServiceUnavailable)
		}
		for _, h := range hosts {
			if !r.host(h).Ejected {
				t.Fatalf("host %s not ejected", h.label())
			}
		}
		code, body := r.get("/api/x")
		var e echo
		if err := json.Unmarshal(body, &e); err != nil {
			t.Fatal(err)
		}
		if code != http.StatusServiceUnavailable || e.Backend == "" {
			t.Fatalf("panic-mode response = %d backend %q, want a backend answer with 503", code, e.Backend)
		}
		if !r.status().PanicMode {
			t.Fatal("panic_mode = false, want true")
		}
		if v := r.metric("microgateway_upstream_panic_mode", "upstream", "up"); v != 1 {
			t.Fatalf("panic_mode gauge = %v, want 1", v)
		}
		if recs := r.logs.records(t, "upstream entered panic mode"); len(recs) != 1 {
			t.Fatalf("entered panic mode records = %d, want 1", len(recs))
		}
		r.fake.Advance(31 * time.Second)
		r.expect(http.StatusServiceUnavailable)
		if r.status().PanicMode {
			t.Fatal("panic_mode = true after expiry, want false")
		}
		if v := r.metric("microgateway_upstream_panic_mode", "upstream", "up"); v != 0 {
			t.Fatalf("panic_mode gauge = %v, want 0", v)
		}
		if recs := r.logs.records(t, "upstream left panic mode"); len(recs) != 1 {
			t.Fatalf("left panic mode records = %d, want 1", len(recs))
		}
	})
}

func runActiveHealth(t *testing.T, n testNetwork) {
	t.Run("transitions follow the thresholds", func(t *testing.T) {
		r, hosts := newActiveRig(t, n, 2, "/health", "200-399")
		a, b := hosts[0], hosts[1]
		at := func(k int) time.Time { return healthEpoch.Add(time.Duration(k) * 10 * time.Second) }
		a.setProbeStatus(http.StatusServiceUnavailable)

		r.tick(2, 0)
		checkActive(t, r.host(a), "unknown", 0, 1, true, time.Time{})
		checkActive(t, r.host(b), "healthy", 1, 0, true, at(1))

		r.tick(2, 0)
		checkActive(t, r.host(a), "unknown", 0, 2, true, time.Time{})
		checkActive(t, r.host(b), "healthy", 2, 0, true, at(2))

		r.tick(2, 0)
		checkActive(t, r.host(a), "unhealthy", 0, 3, false, time.Time{})
		checkActive(t, r.host(b), "healthy", 3, 0, true, at(3))
		failed := r.logs.records(t, "upstream host failed active health check")
		if len(failed) != 1 || failed[0]["host"] != a.label() || failed[0]["consecutive_failures"] != float64(3) {
			t.Fatalf("failed-check records = %v, want one for %s after 3 failures", failed, a.label())
		}

		a.setProbeStatus(http.StatusOK)
		r.tick(2, 0)
		checkActive(t, r.host(a), "unhealthy", 1, 0, false, at(4))

		r.tick(2, 0)
		checkActive(t, r.host(a), "healthy", 2, 0, true, at(5))
		checkActive(t, r.host(b), "healthy", 5, 0, true, at(5))
		passed := r.logs.records(t, "upstream host passed active health check")
		if len(passed) != 1 || passed[0]["host"] != a.label() || passed[0]["consecutive_passes"] != float64(2) {
			t.Fatalf("passed-check records = %v, want one for %s after 2 passes", passed, a.label())
		}
		if got := len(r.logs.records(t, "active health probe")); got != 10 {
			t.Fatalf("probe debug records = %d, want 10", got)
		}
	})

	t.Run("probe path and expected statuses are honored", func(t *testing.T) {
		r, hosts := newActiveRig(t, n, 1, "/ready", "204")
		s := hosts[0]
		s.setProbeStatus(http.StatusOK)
		r.tick(1, 0)
		checkActive(t, r.host(s), "unknown", 0, 1, true, time.Time{})
		r.tick(1, 0)
		checkActive(t, r.host(s), "unknown", 0, 2, true, time.Time{})
		r.tick(1, 0)
		checkActive(t, r.host(s), "unhealthy", 0, 3, false, time.Time{})
		s.setProbeStatus(http.StatusNoContent)
		r.tick(1, 0)
		checkActive(t, r.host(s), "unhealthy", 1, 0, false, healthEpoch.Add(40*time.Second))
		r.tick(1, 0)
		checkActive(t, r.host(s), "healthy", 2, 0, true, healthEpoch.Add(50*time.Second))
		if got := s.hits(); got != 0 {
			t.Fatalf("ordinary hits from probes = %d, want 0", got)
		}
		if got := s.probeCount(); got != 5 {
			t.Fatalf("probes at /ready = %d, want 5", got)
		}
	})

	t.Run("hung probe fails at the probe timeout without touching traffic metrics", func(t *testing.T) {
		r, hosts := newActiveRig(t, n, 2, "/health", "200-399")
		a, b := hosts[0], hosts[1]
		r.expect(http.StatusOK)
		eventually(t, "one request log record", func() bool {
			return len(r.logs.records(t, "request")) == 1
		}, func() string { return fmt.Sprint(len(r.logs.records(t, "request"))) })
		before := r.attempts(a)

		a.setProbeHold(true)
		r.fake.BlockUntil(2)
		r.fake.Advance(10 * time.Second)
		awaitSignal(t, a.probeArrived, "hung probe to reach the host")
		waitProbes(t, r.fake, r.activeChecks, 1, 2)
		r.probes = 1

		if st := r.host(a); st.InFlight != 0 {
			t.Fatalf("in_flight during hung probe = %d, want 0", st.InFlight)
		}
		if got := r.attempts(a); got != before {
			t.Fatalf("attempts during hung probe = %v, want %v", got, before)
		}
		for _, rec := range r.logs.records(t, "request") {
			if rec["path"] == "/health" {
				t.Fatal("access log recorded the probe path")
			}
		}

		r.fake.Advance(2 * time.Second)
		r.probes = 2
		waitProbes(t, r.fake, r.activeChecks, r.probes, 2)
		checkActive(t, r.host(a), "unknown", 0, 1, true, time.Time{})

		eventually(t, "hung probe handler to observe the cancelled request", func() bool {
			return a.probeInFlight() == 0
		}, func() string { return fmt.Sprint(a.probeInFlight()) })
		a.setProbeHold(false)
		r.tick(2, 0)
		if st := r.host(a); st.ActiveCheck == nil || st.ActiveCheck.Status != "healthy" {
			t.Fatalf("host after recovered probe = %+v, want healthy", st.ActiveCheck)
		}
		if a.probePeak() > 1 || b.probePeak() > 1 {
			t.Fatalf("concurrent probes per host = %d, %d, want at most 1", a.probePeak(), b.probePeak())
		}
	})

	t.Run("recovery clears a passive ejection before it expires", func(t *testing.T) {
		r, hosts := newActiveRig(t, n, 2, "/health", "200-399")
		a := hosts[0]
		a.setStatus(http.StatusServiceUnavailable)
		for i := range 5 {
			want := http.StatusOK
			if i%2 == 0 {
				want = http.StatusServiceUnavailable
			}
			r.expect(want)
		}
		if st := r.host(a); !st.Ejected || st.EjectionRemainingSeconds != 3600 {
			t.Fatalf("host = %+v, want passively ejected for 3600s", st)
		}

		a.setProbeStatus(http.StatusServiceUnavailable)
		for range 3 {
			r.tick(2, 0)
		}
		if st := r.host(a); !st.Ejected || st.Healthy {
			t.Fatalf("host = %+v, want still ejected and unhealthy", st)
		}

		a.setProbeStatus(http.StatusOK)
		r.tick(2, 0)
		r.tick(2, 0)
		st := r.host(a)
		if st.Ejected || st.EjectionRemainingSeconds != 0 {
			t.Fatalf("host = %+v, want passive ejection cleared", st)
		}
		ended := r.logs.records(t, "upstream host ejection ended")
		if len(ended) != 1 || ended[0]["reason"] != "active_check" {
			t.Fatalf("ejection ended records = %v, want one with reason active_check", ended)
		}

		a.setStatus(http.StatusOK)
		before := a.hits()
		for range 4 {
			r.expect(http.StatusOK)
		}
		if a.hits() == before {
			t.Fatal("recovered host received no traffic")
		}
	})
}

func TestPassiveEjection(t *testing.T) {
	runPassiveEjection(t, newMemNetwork())
}

func TestActiveHealth(t *testing.T) {
	runActiveHealth(t, newMemNetwork())
}

func TestUpstreamsAdminAndMetrics(t *testing.T) {
	n := newMemNetwork()
	gate := newHoldGate()
	a := newHostStub(t, n, "a", "/health", gate)
	b := newHostStub(t, n, "b", "/health", gate)
	doc := upstreamDoc{
		name:    "up",
		urls:    []string{a.URL, b.URL},
		weights: []int{2},
		passive: passiveFlow(50),
		active:  activeFlow("/health", "200-399"),
	}
	r := newHealthRig(t, n, doc, []*hostStub{a, b}, gate)

	raw := adminBody(t, r.g, "/upstreams")
	var list []map[string]any
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("upstreams = %d, want 1", len(list))
	}
	for _, k := range []string{"name", "algorithm", "panic_mode", "hosts"} {
		if _, ok := list[0][k]; !ok {
			t.Fatalf("upstream JSON lacks %q", k)
		}
	}
	if list[0]["name"] != "up" || list[0]["algorithm"] != "round_robin" || list[0]["panic_mode"] != false {
		t.Fatalf("upstream JSON = %v", list[0])
	}
	hostsJSON, ok := list[0]["hosts"].([]any)
	if !ok || len(hostsJSON) != 2 {
		t.Fatalf("hosts JSON = %v, want 2 entries", list[0]["hosts"])
	}
	hj, ok := hostsJSON[0].(map[string]any)
	if !ok {
		t.Fatalf("host JSON = %v", hostsJSON[0])
	}
	for _, k := range []string{"url", "weight", "healthy", "ejected", "ejection_remaining_seconds", "ejection_count", "consecutive_failures", "active_check", "breaker_state", "in_flight"} {
		if _, ok := hj[k]; !ok {
			t.Fatalf("host JSON lacks %q", k)
		}
	}
	ac, ok := hj["active_check"].(map[string]any)
	if !ok {
		t.Fatalf("active_check JSON = %v", hj["active_check"])
	}
	for _, k := range []string{"status", "consecutive_passes", "consecutive_failures", "last_result"} {
		if _, ok := ac[k]; !ok {
			t.Fatalf("active_check JSON lacks %q", k)
		}
	}
	if hj["url"] != a.URL || hj["weight"] != float64(2) || hj["breaker_state"] != "closed" {
		t.Fatalf("host JSON = %v", hj)
	}

	body := r.metrics()
	for _, f := range []string{
		"microgateway_upstream_attempts_total",
		"microgateway_upstream_ejections_total",
		"microgateway_upstream_ejections_refused_total",
		"microgateway_upstream_host_healthy",
		"microgateway_upstream_panic_mode",
		"microgateway_upstream_active_checks_total",
	} {
		if !strings.Contains(body, "\n"+f+"{") {
			t.Fatalf("metrics lack a sample line for %s before any request", f)
		}
	}
	for _, h := range []*hostStub{a, b} {
		for _, line := range []string{
			series("microgateway_upstream_host_healthy", "upstream", "up", "host", h.label()) + " 1",
			series("microgateway_upstream_ejections_total", "upstream", "up", "host", h.label()) + " 0",
			series("microgateway_upstream_active_checks_total", "upstream", "up", "host", h.label(), "result", "pass") + " 0",
			series("microgateway_upstream_active_checks_total", "upstream", "up", "host", h.label(), "result", "fail") + " 0",
			series("microgateway_upstream_attempts_total", "upstream", "up", "host", h.label(), "outcome", "success") + " 0",
		} {
			if !metricsHasLine(body, line) {
				t.Fatalf("metrics lack line %q before any request", line)
			}
		}
	}
	if !metricsHasLine(body, series("microgateway_upstream_panic_mode", "upstream", "up")+" 0") {
		t.Fatal("metrics lack panic_mode line before any request")
	}

	a.setStatus(http.StatusServiceUnavailable)
	for i := range 5 {
		want := http.StatusOK
		if i%2 == 0 {
			want = http.StatusServiceUnavailable
		}
		r.expect(want)
	}
	st := r.host(a)
	if !st.Ejected || st.Healthy || st.EjectionRemainingSeconds != 30 || st.EjectionCount != 1 {
		t.Fatalf("admin host a = %+v, want ejected once for 30s", st)
	}
	if v := r.metric("microgateway_upstream_host_healthy", "upstream", "up", "host", a.label()); v != 0 {
		t.Fatalf("host_healthy for a = %v, want 0", v)
	}
	if v := r.metric("microgateway_upstream_ejections_total", "upstream", "up", "host", a.label()); v != 1 {
		t.Fatalf("ejections_total for a = %v, want 1", v)
	}
	if v := r.metric("microgateway_upstream_attempts_total", "upstream", "up", "host", a.label(), "outcome", "failure"); v != 3 {
		t.Fatalf("attempts failure for a = %v, want 3", v)
	}
	if v := r.metric("microgateway_upstream_attempts_total", "upstream", "up", "host", b.label(), "outcome", "success"); v != 2 {
		t.Fatalf("attempts success for b = %v, want 2", v)
	}

	doc.weights = []int{5}
	r.reload(doc)
	if st := r.host(a); st.Weight != 5 || !st.Ejected {
		t.Fatalf("after successful reload host a = %+v, want weight 5 and still ejected", st)
	}

	before := adminBody(t, r.g, "/upstreams")
	if err := r.g.ReloadYAML([]byte("version: 2\n")); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	if after := adminBody(t, r.g, "/upstreams"); after != before {
		t.Fatalf("admin state changed after failed reload:\n%s\n%s", before, after)
	}

	rec := httptest.NewRecorder()
	r.g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upstreams", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET /upstreams on data handler = %d, want 404", rec.Code)
	}

	t.Run("host credentials are not reported", func(t *testing.T) {
		cred := newHostStub(t, n, "cred", "/health", gate)
		credURL := strings.Replace(cred.URL, "http://", "http://user:s3cret@", 1)
		doc := upstreamDoc{
			name:    "up",
			urls:    []string{credURL},
			passive: passiveFlow(50),
			active:  activeFlow("/health", "200-399"),
		}
		cr := newHealthRig(t, n, doc, []*hostStub{cred}, gate)
		cred.setStatus(http.StatusServiceUnavailable)
		cr.expect(http.StatusServiceUnavailable)
		cr.tick(1, 0)
		if got := upstreamStatus(t, cr.g, "up").Hosts[0].URL; got != "http://REDACTED@"+cred.label() {
			t.Fatalf("host url = %q, want %q", got, "http://REDACTED@"+cred.label())
		}
		for _, path := range []string{"/upstreams", "/config", "/metrics"} {
			if out := adminBody(t, cr.g, path); strings.Contains(out, "s3cret") {
				t.Errorf("%s reports the host credential", path)
			}
		}
		if out := cr.logs.String(); strings.Contains(out, "s3cret") {
			t.Error("log output reports the host credential")
		}
	})
}

func stateDoc(hosts []*hostStub) upstreamDoc {
	return upstreamDoc{
		name:    "up",
		urls:    urlsOf(hosts),
		passive: passiveHourFlow,
		active:  activeFlow("/health", "200-399"),
	}
}

func reloadState(t *testing.T, n testNetwork) (*healthRig, <-chan getResult, []*hostStub) {
	t.Helper()
	gate := newHoldGate()
	hosts := make([]*hostStub, 5)
	for i := range hosts {
		hosts[i] = newHostStub(t, n, string(rune('a'+i)), "/health", gate)
	}
	r := newHealthRig(t, n, stateDoc(hosts), hosts, gate)

	hosts[0].setMode(stubHoldBody)
	held := startGet(r.client, r.url+"/api/held")
	awaitSignal(t, gate.arrived, "held request to reach the backend")
	hosts[0].setMode(stubAnswer)

	hosts[2].setStatus(http.StatusServiceUnavailable)
	r.driveUntilEjected(hosts[2])
	hosts[2].setStatus(http.StatusOK)

	hosts[1].setStatus(http.StatusServiceUnavailable)
	r.driveUntilConsecutive(hosts[1], 2)
	hosts[1].setStatus(http.StatusOK)

	hosts[3].setProbeStatus(http.StatusServiceUnavailable)
	for range 3 {
		r.tick(5, 0)
	}
	return r, held, hosts
}

func assertReloadState(t *testing.T, r *healthRig, hosts []*hostStub, remaining float64) {
	t.Helper()
	c := r.host(hosts[2])
	if !c.Ejected || c.EjectionCount != 1 || c.EjectionRemainingSeconds != remaining {
		t.Fatalf("ejected host = %+v, want ejected once with %v seconds remaining", c, remaining)
	}
	if b := r.host(hosts[1]); b.ConsecutiveFailures != 2 {
		t.Fatalf("passive consecutive failures = %d, want 2", b.ConsecutiveFailures)
	}
	if d := r.host(hosts[3]); d.ActiveCheck == nil || d.ActiveCheck.Status != "unhealthy" || d.Healthy {
		t.Fatalf("active host = %+v, want unhealthy", d)
	}
	if a := r.host(hosts[0]); a.InFlight != 1 {
		t.Fatalf("held host in_flight = %d, want 1", a.InFlight)
	}
}

func releaseHeld(t *testing.T, gate *holdGate, held <-chan getResult) {
	t.Helper()
	gate.open()
	if got := awaitGet(t, held).status; got != http.StatusOK {
		t.Fatalf("held status = %d, want 200", got)
	}
}

func TestReloadKeepsHealthState(t *testing.T) {
	n := newMemNetwork()

	t.Run("host weight change", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		doc := stateDoc(hosts)
		doc.weights = []int{0, 0, 0, 0, 2}
		r.reload(doc)
		assertReloadState(t, r, hosts, remaining)
		r.tick(5, 0)
		releaseHeld(t, r.gate, held)
	})

	t.Run("route added", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		doc := stateDoc(hosts)
		doc.addRoute = true
		r.reload(doc)
		assertReloadState(t, r, hosts, remaining)
		r.tick(5, 0)
		releaseHeld(t, r.gate, held)
	})

	t.Run("host added", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		added := newHostStub(t, n, "f", "/health", r.gate)
		doc := stateDoc(append(slices.Clone(hosts), added))
		r.reload(doc)
		assertReloadState(t, r, hosts, remaining)
		r.tick(6, 0)
		releaseHeld(t, r.gate, held)
	})

	t.Run("host removed", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		r.reload(stateDoc(hosts[:4]))
		assertReloadState(t, r, hosts, remaining)
		if got := len(r.status().Hosts); got != 4 {
			t.Fatalf("hosts after removal = %d, want 4", got)
		}
		r.tick(4, 0)
		releaseHeld(t, r.gate, held)
	})
}

func TestReloadHealthDefinitionChange(t *testing.T) {
	n := newMemNetwork()

	t.Run("passive change resets ejection only", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		doc := stateDoc(hosts)
		doc.passive = passiveTwoHourFlow
		r.reload(doc)
		if c := r.host(hosts[2]); c.Ejected || c.EjectionCount != 0 {
			t.Fatalf("host after passive change = %+v, want not ejected", c)
		}
		if d := r.host(hosts[3]); d.ActiveCheck == nil || d.ActiveCheck.Status != "unhealthy" || d.Healthy {
			t.Fatalf("active state after passive change = %+v, want unhealthy", d)
		}
		if a := r.host(hosts[0]); a.InFlight != 1 {
			t.Fatalf("held host in_flight = %d, want 1", a.InFlight)
		}
		releaseHeld(t, r.gate, held)
	})

	t.Run("active change resets active status only", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		doc := stateDoc(hosts)
		doc.active = activeFlow("/healthz", "200-399")
		r.reload(doc)
		if c := r.host(hosts[2]); !c.Ejected || c.EjectionRemainingSeconds != remaining {
			t.Fatalf("ejection after active change = %+v, want unchanged", c)
		}
		if d := r.host(hosts[3]); d.ActiveCheck == nil || d.ActiveCheck.Status != "unknown" || !d.Healthy {
			t.Fatalf("active state after active change = %+v, want unknown and healthy", d)
		}
		if b := r.host(hosts[1]); b.ConsecutiveFailures != 2 {
			t.Fatalf("passive consecutive failures = %d, want 2", b.ConsecutiveFailures)
		}
		if a := r.host(hosts[0]); a.InFlight != 1 {
			t.Fatalf("held host in_flight = %d, want 1", a.InFlight)
		}
		releaseHeld(t, r.gate, held)
	})

	t.Run("rename resets all", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		doc := stateDoc(hosts)
		doc.name = "up2"
		r.reload(doc)
		st := upstreamStatus(t, r.g, "up2")
		if c := hostByURL(st, hosts[2].URL); c.Ejected || c.EjectionCount != 0 {
			t.Fatalf("host after rename = %+v, want not ejected", c)
		}
		if d := hostByURL(st, hosts[3].URL); d.ActiveCheck == nil || d.ActiveCheck.Status != "unknown" || !d.Healthy {
			t.Fatalf("active state after rename = %+v, want unknown and healthy", d)
		}
		if b := hostByURL(st, hosts[1].URL); b.ConsecutiveFailures != 0 {
			t.Fatalf("passive consecutive failures after rename = %d, want 0", b.ConsecutiveFailures)
		}
		if a := hostByURL(st, hosts[0].URL); a.InFlight != 0 {
			t.Fatalf("in_flight after rename = %d, want 0", a.InFlight)
		}
		releaseHeld(t, r.gate, held)
	})
}

func TestFailedReloadKeepsHealth(t *testing.T) {
	n := newMemNetwork()
	r, held, hosts := reloadState(t, n)
	before := adminBody(t, r.g, "/upstreams")

	if err := r.g.ReloadYAML([]byte("version: 2\n")); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	doc := stateDoc(hosts)
	doc.badMiddleware = true
	if err := r.g.ReloadYAML([]byte(doc.yaml())); err == nil {
		t.Fatal("reload with an unknown middleware type succeeded")
	}

	if after := adminBody(t, r.g, "/upstreams"); after != before {
		t.Fatalf("admin state changed after failed reloads:\n%s\n%s", before, after)
	}
	r.tick(5, 0)
	releaseHeld(t, r.gate, held)
}

func TestHundredReloadsNoGoroutineLeak(t *testing.T) {
	pre := snapGoroutines()
	n := newMemNetwork()
	gate := newHoldGate()
	hosts := []*hostStub{
		newHostStub(t, n, "a", "/health", gate),
		newHostStub(t, n, "b", "/health", gate),
	}
	doc := upstreamDoc{name: "up", urls: urlsOf(hosts), active: activeFlow("/health", "200-399")}
	r := newHealthRig(t, n, doc, hosts, gate)
	r.tick(2, 0)
	baseline := snapGoroutines()

	for i := range 100 {
		path := "/health"
		if i%2 == 0 {
			path = "/healthz"
		}
		doc.active = activeFlow(path, "200-399")
		r.reload(doc)
	}
	awaitGoroutinesSettled(t, "goroutines of retired probers to exit after 100 reloads", pre, baseline)
	r.fake.BlockUntil(len(hosts))
	if got := r.fake.Waiters(); got != len(hosts) {
		t.Fatalf("pending timers after reloads = %d, want %d live probers", got, len(hosts))
	}
	r.tick(2, 0)
}

const (
	hgHealthy = "microgateway_upstream_host_healthy"
	hgBreaker = "microgateway_circuit_breaker_state"
)

func hgDoc(upstreamName string, hosts ...string) string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	b.WriteString(listenersYAML)
	fmt.Fprintf(&b, "upstreams:\n  - name: %s\n", upstreamName)
	b.WriteString(miTimeouts + miRR + "    circuit_breaker:\n" + rsBreaker)
	b.WriteString("    hosts:\n")
	for _, h := range hosts {
		fmt.Fprintf(&b, "      - url: %q\n", h)
	}
	fmt.Fprintf(&b, "routes:\n  - name: all\n    upstream: %s\n", upstreamName)
	return b.String()
}

func hgSeries(t *testing.T, g *microgateway.Gateway, upstreamName string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(metricsText(t, g), "\n") {
		for _, family := range []string{hgHealthy, hgBreaker} {
			rest, ok := strings.CutPrefix(line, family+`{upstream="`+upstreamName+`",host="`)
			if ok && !strings.HasPrefix(rest, `"`) {
				out = append(out, line)
			}
		}
	}
	return out
}

func hgHas(t *testing.T, g *microgateway.Gateway, family, host string) bool {
	t.Helper()
	return metricsHasPrefix(metricsText(t, g), series(family, "upstream", "up", "host", host)+" ")
}

func metricsHasPrefix(body, prefix string) bool {
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, prefix) {
			return true
		}
	}
	return false
}

func hostLabel(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}

func hgReload(t *testing.T, g *microgateway.Gateway, doc string) {
	t.Helper()
	if err := g.ReloadYAML([]byte(doc)); err != nil {
		t.Fatal(err)
	}
}

func hgLiveOne(t *testing.T, g *microgateway.Gateway) {
	t.Helper()
	eventually(t, "one live generation", func() bool { return inspect(g).LiveGenerations == 1 }, func() string {
		return fmt.Sprintf("LiveGenerations=%d", inspect(g).LiveGenerations)
	})
}

func TestHostGaugesPrunedAfterDrain(t *testing.T) {
	n := newMemNetwork()
	a := countedBackendFor(t, n, "a")
	b := countedBackendFor(t, n, "b")
	aHost := hostLabel(t, a.URL)
	bHost := hostLabel(t, b.URL)
	docAB := hgDoc("up", a.URL, b.URL)
	docA := hgDoc("up", a.URL)
	fake := clock.NewFake(healthEpoch)
	g := n.gateway(t, docAB, withClock(fake))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	if got := statusOf(client, ts.URL+"/one"); got != http.StatusOK {
		t.Fatalf("/one status = %d, want 200", got)
	}
	held := startGet(client, ts.URL+"/hold")
	awaitSignal(t, b.arrived, "held request to reach b")

	hgReload(t, g, docA)
	if live := inspect(g).LiveGenerations; live != 2 {
		t.Fatalf("LiveGenerations = %d, want 2 while the held request runs", live)
	}
	for _, family := range []string{hgHealthy, hgBreaker} {
		if !hgHas(t, g, family, bHost) {
			t.Fatalf("%s for b is missing while its held request runs", family)
		}
	}
	if !hgHas(t, g, hgHealthy, bHost) || !hgHas(t, g, hgBreaker, bHost) {
		t.Fatal("b series are missing on the second scrape while its held request runs")
	}

	b.releaseHold()
	if got := awaitGet(t, held).status; got != http.StatusOK {
		t.Fatalf("held status = %d, want 200", got)
	}
	hgLiveOne(t, g)
	for _, family := range []string{hgHealthy, hgBreaker} {
		if hgHas(t, g, family, bHost) {
			t.Fatalf("%s for b survives the drain", family)
		}
		if !hgHas(t, g, family, aHost) {
			t.Fatalf("%s for a is missing after the drain", family)
		}
	}
	for _, line := range hgSeries(t, g, "up") {
		if strings.Contains(line, `host="`+bHost+`"`) {
			t.Fatalf("series for b survives on another scrape: %s", line)
		}
	}
	if v := metricValue(t, metricsText(t, g), series("microgateway_upstream_attempts_total", "upstream", "up", "host", bHost, "outcome", "success")); v != 1 {
		t.Fatalf("attempts success for b = %v, want 1", v)
	}

	hgReload(t, g, docAB)
	body := metricsText(t, g)
	if !metricsHasLine(body, series(hgHealthy, "upstream", "up", "host", bHost)+" 1") {
		t.Fatal("re-added b host_healthy is not 1")
	}
	if !metricsHasLine(body, series(hgBreaker, "upstream", "up", "host", bHost)+" 0") {
		t.Fatal("re-added b breaker_state is not 0")
	}

	hgReload(t, g, hgDoc("up2", a.URL))
	hgLiveOne(t, g)
	body = metricsText(t, g)
	for _, prefix := range []string{hgHealthy + `{upstream="up",`, hgBreaker + `{upstream="up",`, `microgateway_upstream_panic_mode{upstream="up"}`} {
		if metricsHasPrefix(body, prefix) {
			t.Fatalf("series %s survive the removal of upstream up", prefix)
		}
	}
	if !metricsHasPrefix(body, `microgateway_upstream_attempts_total{upstream="up",`) {
		t.Fatal("attempts counters of upstream up are gone")
	}
}

func TestHostGaugesPrunedOnDNSRefresh(t *testing.T) {
	n := newMemNetwork()
	for _, addr := range []string{"10.0.0.1:8080", "10.0.0.2:8080"} {
		n.serveAt(t, addr, echoHandler(addr))
	}
	fr := &fakeResolver{}
	fr.set([]string{"10.0.0.1", "10.0.0.2"})
	doc := "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n" + miTimeouts +
		"    circuit_breaker:\n" + rsBreaker +
		"    dns:\n      hostname: svc.internal\n      port: 8080\n      scheme: http\n      refresh_interval: 5s\n" +
		"routes:\n  - name: all\n    upstream: up\n"

	fake := clock.NewFake(time.Unix(0, 0))
	g := n.gateway(t, doc, withClock(fake), withResolver(fr))

	initial := []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080"}
	if got := miHostURLs(t, g); !slices.Equal(got, initial) {
		t.Fatalf("initial hosts = %v, want %v", got, initial)
	}
	for _, host := range []string{"10.0.0.1:8080", "10.0.0.2:8080"} {
		for _, family := range []string{hgHealthy, hgBreaker} {
			if !hgHas(t, g, family, host) {
				t.Fatalf("%s for %s is missing before the refresh", family, host)
			}
		}
	}

	fake.BlockUntil(1)
	fr.set([]string{"10.0.0.1"})
	fake.Advance(5 * time.Second)
	one := []string{"http://10.0.0.1:8080"}
	eventually(t, "the gateway to adopt the one-host DNS set", func() bool { return slices.Equal(miHostURLs(t, g), one) }, func() string { return fmt.Sprint(miHostURLs(t, g)) })
	eventually(t, "the series of the removed DNS host to be pruned", func() bool {
		return !hgHas(t, g, hgHealthy, "10.0.0.2:8080") && !hgHas(t, g, hgBreaker, "10.0.0.2:8080")
	}, func() string { return "series of 10.0.0.2:8080 still present" })
	for _, family := range []string{hgHealthy, hgBreaker} {
		if !hgHas(t, g, family, "10.0.0.1:8080") {
			t.Fatalf("%s for the remaining host 10.0.0.1:8080 is missing", family)
		}
	}
	if !metricsHasPrefix(metricsText(t, g), series("microgateway_upstream_attempts_total", "upstream", "up", "host", "10.0.0.2:8080", "outcome", "success")+" ") {
		t.Fatal("attempts counter of the removed host 10.0.0.2:8080 is gone")
	}

	fr.set([]string{"10.0.0.1", "10.0.0.2"})
	fake.BlockUntil(1)
	fake.Advance(5 * time.Second)
	eventually(t, "the gateway to adopt the two-host DNS set", func() bool { return slices.Equal(miHostURLs(t, g), initial) }, func() string { return fmt.Sprint(miHostURLs(t, g)) })
	eventually(t, "the re-added host to report healthy with a closed breaker", func() bool {
		body := metricsText(t, g)
		return metricsHasLine(body, series(hgHealthy, "upstream", "up", "host", "10.0.0.2:8080")+" 1") &&
			metricsHasLine(body, series(hgBreaker, "upstream", "up", "host", "10.0.0.2:8080")+" 0")
	}, func() string { return "series of 10.0.0.2:8080 not yet healthy and closed" })
}

func TestHostGaugesBoundedAcrossReloads(t *testing.T) {
	n := newMemNetwork()
	a := countedBackendFor(t, n, "a")
	b := countedBackendFor(t, n, "b")
	docAB := hgDoc("up", a.URL, b.URL)
	docA := hgDoc("up", a.URL)
	g := n.gateway(t, docAB, withClock(clock.NewFake(healthEpoch)))

	for i := range 1000 {
		if i%2 == 0 {
			hgReload(t, g, docA)
		} else {
			hgReload(t, g, docAB)
		}
	}
	hgLiveOne(t, g)
	if got := len(hgSeries(t, g, "up")); got != 4 {
		t.Fatalf("host series after 1000 reloads = %d, want 4", got)
	}

	hgReload(t, g, docA)
	hgLiveOne(t, g)
	if got := len(hgSeries(t, g, "up")); got != 2 {
		t.Fatalf("host series after the final reload = %d, want 2", got)
	}
}

func heapInUse() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapInuse
}

func TestHostGaugesChurnLoad(t *testing.T) {
	if testing.Short() || os.Getenv("MICROGATEWAY_LOAD") == "" {
		t.Skip("set MICROGATEWAY_LOAD=1")
	}
	n := newMemNetwork()
	a := countedBackendFor(t, n, "a")
	b := countedBackendFor(t, n, "b")
	docAB := hgDoc("up", a.URL, b.URL)
	docA := hgDoc("up", a.URL)
	g := n.gateway(t, docAB, withClock(clock.NewFake(healthEpoch)))

	var base uint64
	for i := range 100000 {
		if i%2 == 0 {
			hgReload(t, g, docA)
		} else {
			hgReload(t, g, docAB)
		}
		if i+1 == 10000 {
			base = heapInUse()
		}
	}
	hgLiveOne(t, g)
	if got := len(hgSeries(t, g, "up")); got != 4 {
		t.Fatalf("host series after 100000 reloads = %d, want 4", got)
	}
	limit := base*11/10 + 1<<20
	if end := heapInUse(); end > limit {
		t.Fatalf("HeapInuse after 100000 reloads = %d, want at most %d", end, limit)
	}
}
