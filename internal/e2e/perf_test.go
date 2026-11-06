package e2e

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
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
	"github.com/ok0/go-microgateway/internal/testwait"
)

const perfKey = "perf-key-1"

var perfOK = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, "ok")
})

var perfCacheable = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "max-age=600")
	_, _ = io.WriteString(w, "ok")
})

var soakCacheBackend = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "max-age=2")
	w.Header().Set("Surrogate-Key", "t"+strconv.Itoa(soakIndex(r)%8))
	_, _ = io.WriteString(w, "ok")
})

func heavy() bool { return os.Getenv("MICROGATEWAY_HEAVY") == "1" }

func p50(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	s := slices.Clone(d)
	slices.Sort(s)
	return s[(len(s)-1)/2]
}

func reportOverhead(b *testing.B, gw, base []time.Duration, baseName string, limit time.Duration) {
	gp, bp := p50(gw), p50(base)
	over := gp - bp
	b.ReportMetric(float64(gp.Nanoseconds()), "gateway-p50-ns")
	b.ReportMetric(float64(bp.Nanoseconds()), baseName+"-p50-ns")
	b.ReportMetric(float64(over.Nanoseconds()), "overhead-p50-ns")
	if heavy() && over >= limit {
		b.Fatalf("overhead p50 %v, target < %v", over, limit)
	}
}

func perfGateway(tb testing.TB, doc string) *microgateway.Gateway {
	tb.Helper()
	g, err := newGatewayFromYAML([]byte(doc), withClock(clock.Real()), microgateway.WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))))
	if err != nil {
		tb.Fatalf("gateway: %v", err)
	}
	tb.Cleanup(func() { _ = g.Close() })
	return g
}

func perfReq(path string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "http://gw.test"+path, nil)
	req.Header.Set("X-API-Key", perfKey)
	req.RemoteAddr = "192.0.2.1:1234"
	return req
}

func cacheStatus(rec *httptest.ResponseRecorder) string {
	return strings.Join(rec.Header().Values("Cache-Status"), ", ")
}

func perfChainDoc(a, b string) string {
	return "version: 1\n" + listenersYAML +
		"auth:\n  api_keys:\n    - name: keys1\n      keys:\n        - id: k1\n          hash: " + strconv.Quote(auHash(perfKey)) + "\n          consumer: alice\n" +
		"upstreams:\n  - name: chain\n    load_balancer:\n      algorithm: round_robin\n    pool:\n      max_idle_per_host: 8\n    circuit_breaker: {}\n    hosts:\n      - url: " + strconv.Quote(a) + "\n      - url: " + strconv.Quote(b) + "\n" +
		"routes:\n  - name: chain\n    match:\n      path: /chain/\n    upstream: chain\n    timeout: 5s\n    auth:\n      mode: apikey\n      providers: [keys1]\n    rate_limit:\n      rate: 1000000\n      per: 1s\n    circuit_breaker:\n      enabled: true\n    retry:\n      attempts: 2\n"
}

func soakChainDoc(a, b string, w int, idle time.Duration) string {
	return "version: 1\n" + listenersYAML +
		"auth:\n  api_keys:\n    - name: keys1\n      keys:\n        - id: k1\n          hash: " + strconv.Quote(auHash(perfKey)) + "\n          consumer: alice\n" +
		"upstreams:\n  - name: chain\n    load_balancer:\n      algorithm: round_robin\n" +
		fmt.Sprintf("    pool:\n      max_idle_per_host: %d\n      max_conns_per_host: %d\n    timeouts:\n      idle: %s\n", w, w, idle) +
		"    circuit_breaker: {}\n    hosts:\n      - url: " + strconv.Quote(a) + "\n      - url: " + strconv.Quote(b) + "\n" +
		"routes:\n  - name: chain\n    match:\n      path: /chain/\n    upstream: chain\n    timeout: 5s\n    auth:\n      mode: apikey\n      providers: [keys1]\n    rate_limit:\n      rate: 1000000\n      per: 1s\n    circuit_breaker:\n      enabled: true\n    retry:\n      attempts: 2\n"
}

func perfCacheDoc(backendURL string, cached bool) string {
	top := ""
	route := "  - name: plain\n    match:\n      path: /\n    upstream: up\n"
	if cached {
		top = "cache:\n  memory:\n    max_bytes: 8388608\n    shards: 1\n"
		route = "  - name: cached\n    match:\n      path: /\n    upstream: up\n    cache:\n      enabled: true\n      default_ttl: 60s\n"
	}
	return "version: 1\n" + listenersYAML + top +
		"upstreams:\n  - name: up\n    hosts:\n      - url: " + strconv.Quote(backendURL) + "\n" +
		"routes:\n" + route
}

func BenchmarkFullChain(b *testing.B) {
	be1 := httptest.NewServer(perfOK)
	be2 := httptest.NewServer(perfOK)
	b.Cleanup(be1.Close)
	b.Cleanup(be2.Close)
	g := perfGateway(b, perfChainDoc(be1.URL, be2.URL))
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 8}}
	b.Cleanup(client.CloseIdleConnections)

	for range 2 {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, perfReq("/chain/warm"))
		if rec.Code != http.StatusOK {
			b.Fatalf("warm-up gateway status %d", rec.Code)
		}
		resp, err := client.Get(be1.URL + "/warm")
		if err != nil {
			b.Fatalf("warm-up direct: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	gw := make([]time.Duration, b.N)
	direct := make([]time.Duration, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		req := perfReq("/chain/x")
		rec := httptest.NewRecorder()
		start := time.Now()
		g.ServeHTTP(rec, req)
		gw[i] = time.Since(start)
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			b.Fatalf("gateway status %d, body %q", rec.Code, rec.Body.String())
		}

		start = time.Now()
		resp, err := client.Get(be1.URL + "/direct")
		if err != nil {
			b.Fatalf("direct: %v", err)
		}
		_, err = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		direct[i] = time.Since(start)
		if err != nil {
			b.Fatalf("direct body: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("direct status %d", resp.StatusCode)
		}
	}
	b.StopTimer()
	reportOverhead(b, gw, direct, "direct", 150*time.Microsecond)
}

func BenchmarkCacheHit(b *testing.B) {
	be := httptest.NewServer(perfCacheable)
	b.Cleanup(be.Close)
	hits := perfGateway(b, perfCacheDoc(be.URL, true))
	pass := perfGateway(b, perfCacheDoc(be.URL, false))

	hits.ServeHTTP(httptest.NewRecorder(), perfReq("/cache/x"))
	primed := httptest.NewRecorder()
	hits.ServeHTTP(primed, perfReq("/cache/x"))
	if got := cacheStatus(primed); !strings.Contains(got, "hit") {
		b.Fatalf("second request Cache-Status %q, want a hit", got)
	}

	hitTimes := make([]time.Duration, b.N)
	passTimes := make([]time.Duration, b.N)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		req := perfReq("/cache/x")
		rec := httptest.NewRecorder()
		start := time.Now()
		hits.ServeHTTP(rec, req)
		hitTimes[i] = time.Since(start)
		if rec.Code != http.StatusOK || !strings.Contains(cacheStatus(rec), "hit") {
			b.Fatalf("iteration %d: status %d, Cache-Status %q", i, rec.Code, cacheStatus(rec))
		}

		req = perfReq("/cache/x")
		rec = httptest.NewRecorder()
		start = time.Now()
		pass.ServeHTTP(rec, req)
		passTimes[i] = time.Since(start)
		if rec.Code != http.StatusOK {
			b.Fatalf("iteration %d: passthrough status %d", i, rec.Code)
		}
	}
	b.StopTimer()
	reportOverhead(b, hitTimes, passTimes, "passthrough", 50*time.Microsecond)
}

type soakSample struct {
	goroutines int
	heap       uint64
}

func soakTrend(samples []soakSample) (m1, m2, m3, s float64) {
	n := len(samples)
	if n < 3 {
		return 0, 0, 0, 0
	}
	bounds := [4]int{0, n / 3, 2 * n / 3, n}
	var means [3]float64
	var ss float64
	for k := range 3 {
		part := samples[bounds[k]:bounds[k+1]]
		var sum float64
		for _, x := range part {
			sum += float64(x.heap)
		}
		means[k] = sum / float64(len(part))
		for _, x := range part {
			d := float64(x.heap) - means[k]
			ss += d * d
		}
	}
	if n > 3 {
		s = math.Sqrt(ss / float64(n-3))
	}
	return means[0], means[1], means[2], s
}

func soakVerdict(base soakSample, samples []soakSample) []string {
	if len(samples) == 0 {
		return []string{"no samples"}
	}
	var problems []string
	lo := float64(base.goroutines) * 0.9
	hi := float64(base.goroutines) * 1.1
	for i, s := range samples {
		g := float64(s.goroutines)
		if g < lo || g > hi {
			problems = append(problems, fmt.Sprintf("sample %d: %d goroutines outside [%.1f, %.1f] around baseline %d", i, s.goroutines, lo, hi, base.goroutines))
		}
	}
	last := samples[len(samples)-1]
	if float64(last.heap) > 1.25*float64(base.heap) {
		problems = append(problems, fmt.Sprintf("final heap in use %d bytes exceeds 1.25x baseline %d", last.heap, base.heap))
	}
	if len(samples) >= 3 {
		m1, m2, m3, s := soakTrend(samples)
		if m1 < m2 && m2 < m3 && m3-m1 > 2*s {
			problems = append(problems, "heap rises across the run")
		}
	}
	return problems
}

func flatGoroutines(heaps ...uint64) []soakSample {
	out := make([]soakSample, len(heaps))
	for i, h := range heaps {
		out[i] = soakSample{goroutines: 100, heap: h}
	}
	return out
}

func TestSoakVerdict(t *testing.T) {
	base := soakSample{goroutines: 100, heap: 1000}
	linear := make([]uint64, 30)
	for i := range linear {
		linear[i] = 1000 + uint64(i)
	}
	tests := []struct {
		name     string
		samples  []soakSample
		problems int
	}{
		{"flat", []soakSample{{100, 1000}, {100, 1000}, {100, 1000}}, 0},
		{"goroutines +11%", []soakSample{{100, 1000}, {111, 1000}}, 1},
		{"goroutines -11%", []soakSample{{100, 1000}, {89, 1000}}, 1},
		{"goroutines +10%", []soakSample{{110, 1000}}, 0},
		{"final heap 1.3x", []soakSample{{100, 1300}}, 1},
		{"heap rises by thirds", []soakSample{{100, 1000}, {100, 1100}, {100, 1200}}, 1},
		{"linear growth", flatGoroutines(linear...), 1},
		{"zero-spread small rise", flatGoroutines(1000, 1008, 1016), 1},
		{"noise with ordered means", flatGoroutines(1000, 1200, 1000, 1200, 1010, 1210, 1010, 1210, 1020, 1220, 1020, 1220), 0},
		{"plateau after a step", flatGoroutines(1000, 1000, 1000, 1100, 1100, 1100, 1100, 1100, 1100), 0},
		{"falling", flatGoroutines(1200, 1100, 1000), 0},
		{"empty", nil, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := soakVerdict(base, tc.samples)
			if len(got) != tc.problems {
				t.Fatalf("problems = %v, want %d", got, tc.problems)
			}
		})
	}
}

func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	v := os.Getenv("MICROGATEWAY_SOAK")
	if v == "" {
		t.Skip("soak skipped: set MICROGATEWAY_SOAK=<duration>, for example 60s")
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		t.Fatalf("MICROGATEWAY_SOAK %q: %v", v, err)
	}
	if d <= 0 {
		t.Fatalf("MICROGATEWAY_SOAK %q must be positive", v)
	}
	return d
}

type soakFailures struct {
	n    atomic.Int64
	once sync.Once
	msg  string
}

func (f *soakFailures) fail(format string, args ...any) {
	f.n.Add(1)
	f.once.Do(func() { f.msg = fmt.Sprintf(format, args...) })
}

func (f *soakFailures) report(t *testing.T) {
	if n := f.n.Load(); n > 0 {
		t.Errorf("%d failed operations; first: %s", n, f.msg)
	}
}

func soakSnapshot(gate *sync.RWMutex) soakSample {
	gate.Lock()
	defer gate.Unlock()
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return soakSample{goroutines: runtime.NumGoroutine(), heap: ms.HeapInuse}
}

func soakIndex(r *http.Request) int {
	s := r.URL.Query().Get("k")
	if s == "" {
		s = strings.TrimPrefix(r.URL.Path, "/hot/")
	}
	n, _ := strconv.Atoi(s)
	return n
}

func soakWorkers() int { return min(4, runtime.GOMAXPROCS(0)) }

type primeGate struct {
	arrived atomic.Int32
	release chan struct{}
	on      atomic.Bool
}

func newPrimeGate() *primeGate {
	return &primeGate{release: make(chan struct{})}
}

func (p *primeGate) wrap(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p.on.Load() {
			p.arrived.Add(1)
			select {
			case <-p.release:
			case <-r.Context().Done():
			}
		}
		h.ServeHTTP(w, r)
	})
}

func primeConns(t *testing.T, perBackend int, backends []*primeGate, send func(i int)) {
	t.Helper()
	for _, p := range backends {
		p.on.Store(true)
	}
	var wg sync.WaitGroup
	for i := range perBackend * len(backends) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			send(i)
		}()
	}
	testwait.Until(t, "every pooled upstream connection opened", func() bool {
		for _, p := range backends {
			if int(p.arrived.Load()) != perBackend {
				return false
			}
		}
		return true
	}, func() string {
		var b strings.Builder
		for k, p := range backends {
			fmt.Fprintf(&b, "backend %d arrivals %d of %d; ", k, p.arrived.Load(), perBackend)
		}
		return b.String()
	})
	for _, p := range backends {
		p.on.Store(false)
		close(p.release)
	}
	wg.Wait()
}

func runSoak(t *testing.T, d, minWarm time.Duration, do func(i int)) {
	t.Helper()
	workers := soakWorkers()
	var gate sync.RWMutex
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk := time.NewTicker(10 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-stop:
					return
				case <-tk.C:
					gate.RLock()
					do(w)
					gate.RUnlock()
				}
			}
		}()
	}

	warm := time.NewTimer(max(minWarm, min(max(d/6, 500*time.Millisecond), 10*time.Second)))
	<-warm.C
	base := soakSnapshot(&gate)

	interval := max(d/30, 100*time.Millisecond)
	var samples []soakSample
	end := time.NewTimer(d)
	tick := time.NewTicker(interval)
sampling:
	for {
		select {
		case <-end.C:
			break sampling
		case <-tick.C:
			samples = append(samples, soakSnapshot(&gate))
		}
	}
	tick.Stop()
	end.Stop()
	close(stop)
	wg.Wait()

	last := base
	minG, maxG := base.goroutines, base.goroutines
	if len(samples) > 0 {
		last = samples[len(samples)-1]
	}
	for _, s := range samples {
		minG = min(minG, s.goroutines)
		maxG = max(maxG, s.goroutines)
	}
	m1, m2, m3, s := soakTrend(samples)
	t.Logf("baseline goroutines %d, heap in use %d; %d samples; goroutines min %d max %d; final heap in use %d; heap third means %.0f, %.0f, %.0f; spread %.1f",
		base.goroutines, base.heap, len(samples), minG, maxG, last.heap, m1, m2, m3, s)
	for _, p := range soakVerdict(base, samples) {
		t.Error(p)
	}
}

func TestSoakFullChain(t *testing.T) {
	d := soakDuration(t)
	pre := runtime.NumGoroutine()
	w := soakWorkers()
	gate1, gate2 := newPrimeGate(), newPrimeGate()
	be1 := httptest.NewServer(gate1.wrap(perfOK))
	be2 := httptest.NewServer(gate2.wrap(perfOK))
	t.Cleanup(be1.Close)
	t.Cleanup(be2.Close)
	idle := d + 10*time.Second + time.Minute
	g := perfGateway(t, soakChainDoc(be1.URL, be2.URL, w, idle))

	primeConns(t, w, []*primeGate{gate1, gate2}, func(int) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, perfReq("/chain/prime"))
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Errorf("prime: status %d, body %q", rec.Code, rec.Body.String())
		}
	})

	var fails soakFailures
	runSoak(t, d, 500*time.Millisecond, func(int) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, perfReq("/chain/soak"))
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			fails.fail("status %d, body %q", rec.Code, rec.Body.String())
		}
	})
	fails.report(t)

	_ = g.Close()
	be1.CloseClientConnections()
	be2.CloseClientConnections()
	be1.Close()
	be2.Close()
	testwait.Until(t, "gateway goroutines gone after Close", func() bool { return runtime.NumGoroutine() <= pre },
		func() string { return fmt.Sprintf("goroutines %d, before the test %d", runtime.NumGoroutine(), pre) })
}

func TestSoakCacheMixed(t *testing.T) {
	d := soakDuration(t)
	pre := runtime.NumGoroutine()
	w := soakWorkers()
	gate := newPrimeGate()
	be := httptest.NewServer(gate.wrap(soakCacheBackend))
	t.Cleanup(be.Close)
	idle := d + 10*time.Second + time.Minute
	doc := "version: 1\n" + listenersYAML + "cache:\n  memory:\n    max_bytes: 8388608\n    shards: 1\n" +
		"upstreams:\n  - name: up\n" +
		fmt.Sprintf("    pool:\n      max_idle_per_host: %d\n      max_conns_per_host: %d\n    timeouts:\n      idle: %s\n", w, w, idle) +
		"    hosts:\n      - url: " + strconv.Quote(be.URL) + "\n" +
		"routes:\n  - name: mix\n    match:\n      path: /\n    upstream: up\n    cache:\n      enabled: true\n      default_ttl: 2s\n"
	g := perfGateway(t, doc)

	primeConns(t, w, []*primeGate{gate}, func(i int) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, perfReq("/mix/prime?k="+strconv.Itoa(i)))
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			t.Errorf("prime %d: status %d, body %q", i, rec.Code, rec.Body.String())
		}
	})

	var fails soakFailures
	get := func(path string) {
		rec := httptest.NewRecorder()
		g.ServeHTTP(rec, perfReq(path))
		if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
			fails.fail("GET %s: status %d", path, rec.Code)
		}
	}
	purge := func(c int) {
		var req microgateway.PurgeRequest
		hot := fmt.Sprintf("http://gw.test/hot/%d", c%50)
		switch (c / 10) % 4 {
		case 0:
			req = microgateway.PurgeRequest{URLs: []string{hot}}
		case 1:
			req = microgateway.PurgeRequest{Tags: []string{fmt.Sprintf("t%d", c%8)}}
		case 2:
			req = microgateway.PurgeRequest{All: true}
		default:
			req = microgateway.PurgeRequest{URLs: []string{hot}, Soft: true}
		}
		if _, err := g.Purge(context.Background(), req); err != nil {
			fails.fail("purge %+v: %v", req, err)
		}
	}

	counters := make([]int, w)
	runSoak(t, d, 5*time.Second, func(i int) {
		c := counters[i]
		counters[i]++
		switch {
		case c%10 < 6:
			get(fmt.Sprintf("/hot/%d", c%50))
		case c%10 < 9:
			get(fmt.Sprintf("/mix/miss?k=%d", c%100))
		default:
			purge(c)
		}
	})
	fails.report(t)

	_ = g.Close()
	be.CloseClientConnections()
	be.Close()
	testwait.Until(t, "gateway goroutines gone after Close", func() bool { return runtime.NumGoroutine() <= pre },
		func() string { return fmt.Sprintf("goroutines %d, before the test %d", runtime.NumGoroutine(), pre) })
}
