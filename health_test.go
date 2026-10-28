package lychgate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/upstream"
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
func (s *hostStub) setMode(m int32)    { s.mode.Store(m) }
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
	g      *Gateway
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
	g := n.gateway(t, doc.yaml(), withClock(fake), WithLogger(newJSONLogger(logs)))
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
	defer resp.Body.Close()
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
			resp.Body.Close()
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
		total += r.metric("lychgate_upstream_attempts_total", "upstream", "up", "host", s.label(), "outcome", o)
	}
	return total
}

func (r *healthRig) activeChecks() float64 {
	return metricSum(r.t, r.metrics(), `lychgate_upstream_active_checks_total{upstream="up",`)
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

func series(name string, labels ...string) string {
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i := 0; i+1 < len(labels); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `%s="%s"`, labels[i], labels[i+1])
	}
	b.WriteByte('}')
	return b.String()
}

func metricValue(t *testing.T, body, name string) float64 {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if rest, ok := strings.CutPrefix(line, name+" "); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v
		}
	}
	t.Fatalf("metric %s not found", name)
	return 0
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
		if v := r.metric("lychgate_upstream_ejections_total", "upstream", "up", "host", bad.label()); v != 1 {
			t.Fatalf("ejections_total = %v, want 1", v)
		}
		if v := r.metric("lychgate_upstream_host_healthy", "upstream", "up", "host", bad.label()); v != 0 {
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
			return r.metric("lychgate_upstream_attempts_total", "upstream", "up", "host", bad.label(), "outcome", "ignored")
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
			eventually(t, func() bool { return ignored() == base+float64(k+1) })
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
		if v := r.metric("lychgate_upstream_ejections_total", "upstream", "up", "host", bad.label()); v != 1 {
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
		if v := r.metric("lychgate_upstream_ejections_refused_total", "upstream", "up"); v <= 0 {
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
		if v := r.metric("lychgate_upstream_ejections_total", "upstream", "up", "host", bad.label()); v != 0 {
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
		if v := r.metric("lychgate_upstream_panic_mode", "upstream", "up"); v != 1 {
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
		if v := r.metric("lychgate_upstream_panic_mode", "upstream", "up"); v != 0 {
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
		eventually(t, func() bool {
			return len(r.logs.records(t, "request")) == 1
		})
		before := r.attempts(a)

		a.setProbeHold(true)
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
		"lychgate_upstream_attempts_total",
		"lychgate_upstream_ejections_total",
		"lychgate_upstream_ejections_refused_total",
		"lychgate_upstream_host_healthy",
		"lychgate_upstream_panic_mode",
		"lychgate_upstream_active_checks_total",
	} {
		if !strings.Contains(body, "\n"+f+"{") {
			t.Fatalf("metrics lack a sample line for %s before any request", f)
		}
	}
	for _, h := range []*hostStub{a, b} {
		for _, line := range []string{
			series("lychgate_upstream_host_healthy", "upstream", "up", "host", h.label()) + " 1",
			series("lychgate_upstream_ejections_total", "upstream", "up", "host", h.label()) + " 0",
			series("lychgate_upstream_active_checks_total", "upstream", "up", "host", h.label(), "result", "pass") + " 0",
			series("lychgate_upstream_active_checks_total", "upstream", "up", "host", h.label(), "result", "fail") + " 0",
			series("lychgate_upstream_attempts_total", "upstream", "up", "host", h.label(), "outcome", "success") + " 0",
		} {
			if !metricsHasLine(body, line) {
				t.Fatalf("metrics lack line %q before any request", line)
			}
		}
	}
	if !metricsHasLine(body, series("lychgate_upstream_panic_mode", "upstream", "up")+" 0") {
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
	if v := r.metric("lychgate_upstream_host_healthy", "upstream", "up", "host", a.label()); v != 0 {
		t.Fatalf("host_healthy for a = %v, want 0", v)
	}
	if v := r.metric("lychgate_upstream_ejections_total", "upstream", "up", "host", a.label()); v != 1 {
		t.Fatalf("ejections_total for a = %v, want 1", v)
	}
	if v := r.metric("lychgate_upstream_attempts_total", "upstream", "up", "host", a.label(), "outcome", "failure"); v != 3 {
		t.Fatalf("attempts failure for a = %v, want 3", v)
	}
	if v := r.metric("lychgate_upstream_attempts_total", "upstream", "up", "host", b.label(), "outcome", "success"); v != 2 {
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
