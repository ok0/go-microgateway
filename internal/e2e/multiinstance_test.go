package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/store"
)

const (
	miActive = "    active_health:\n      path: /health\n      interval: 10s\n      timeout: 2s\n      jitter: 0\n      healthy_threshold: 2\n      unhealthy_threshold: 3\n"
)

func miDoc(upstreamBody string, hosts []string) string {
	doc := "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n" + upstreamBody
	if len(hosts) > 0 {
		doc += "    hosts:\n"
		for _, h := range hosts {
			doc += fmt.Sprintf("      - url: %q\n", h)
		}
	}
	return doc + "routes:\n  - name: all\n    upstream: up\n"
}

func miActiveChecks(t *testing.T, gs ...*microgateway.Gateway) func() float64 {
	return func() float64 {
		var sum float64
		for _, g := range gs {
			for _, line := range strings.Split(metricsText(t, g), "\n") {
				if !strings.HasPrefix(line, "microgateway_upstream_active_checks_total{") {
					continue
				}
				v, err := strconv.ParseFloat(line[strings.LastIndexByte(line, ' ')+1:], 64)
				if err != nil {
					t.Fatal(err)
				}
				sum += v
			}
		}
		return sum
	}
}

func miHostURLs(t *testing.T, g *microgateway.Gateway) []string {
	t.Helper()
	var out []string
	for _, h := range upstreamStatus(t, g, "up").Hosts {
		out = append(out, h.URL)
	}
	return out
}

func miBackend(t *testing.T, client *http.Client, url string) string {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", url, resp.StatusCode)
	}
	var e echo
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatal(err)
	}
	return e.Backend
}

func TestInstancesIsolateEjection(t *testing.T) {
	n := newMemNetwork()
	var xHits atomic.Int64
	x := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		xHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-Fail") == "1" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(echo{Backend: "x", Path: r.URL.Path})
	}))
	y := n.serve(t, echoHandler("y"))
	z := n.serve(t, echoHandler("z"))
	doc := miDoc(miTimeouts+miRR+miPassive, []string{x.URL, y.URL, z.URL})

	gA := n.gateway(t, doc, withClock(clock.NewFake(time.Unix(0, 0))))
	gB := n.gateway(t, doc, withClock(clock.NewFake(time.Unix(0, 0))))
	gC := n.gateway(t, doc, withClock(clock.NewFake(time.Unix(0, 0))))

	for range 4 {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-Fail", "1")
		balDo(gA, req)
	}
	xA := hostByURL(upstreamStatus(t, gA, "up"), x.URL)
	if !xA.Ejected {
		t.Fatal("host x not ejected on A")
	}
	for _, g := range []*microgateway.Gateway{gB, gC} {
		if h := hostByURL(upstreamStatus(t, g, "up"), x.URL); h.Ejected || h.EjectionCount != 0 {
			t.Fatalf("host x ejected=%v ejection_count=%d, want not ejected", h.Ejected, h.EjectionCount)
		}
	}

	for _, g := range []*microgateway.Gateway{gB, gC} {
		before := xHits.Load()
		for i := range 30 {
			if code, _ := balDo(g, httptest.NewRequest(http.MethodGet, "/", nil)); code != http.StatusOK {
				t.Fatalf("request %d status = %d, want 200", i, code)
			}
		}
		if got := xHits.Load() - before; got != 10 {
			t.Fatalf("requests reaching x = %d, want 10 of 30", got)
		}
	}
}

func TestInstancesIsolateActiveHealth(t *testing.T) {
	n := newMemNetwork()
	p := failingBackend(t, n, "p")
	p.Health.Store(http.StatusInternalServerError)
	doc := miDoc(miTimeouts+miRR+miActive, []string{p.URL})

	fakeA := clock.NewFake(time.Unix(0, 0))
	fakeB := clock.NewFake(time.Unix(0, 0))
	gA := n.gateway(t, doc, withClock(fakeA))
	gB := n.gateway(t, doc, withClock(fakeB))

	completedA := miActiveChecks(t, gA)
	waitProbes(t, fakeA, completedA, 0, 1)
	for i := 1; i <= 3; i++ {
		fakeA.Advance(10 * time.Second)
		waitProbes(t, fakeA, completedA, float64(i), 1)
	}

	hA := hostByURL(upstreamStatus(t, gA, "up"), p.URL)
	if hA.ActiveCheck == nil || hA.ActiveCheck.Status != "unhealthy" || hA.Healthy {
		t.Fatalf("A host = %+v, want unhealthy and not healthy", hA)
	}
	hB := hostByURL(upstreamStatus(t, gB, "up"), p.URL)
	if hB.ActiveCheck == nil || hB.ActiveCheck.Status != "unknown" || !hB.Healthy {
		t.Fatalf("B host = %+v, want unknown and healthy", hB)
	}
}

func TestInstancesIsolateCursorAndInFlight(t *testing.T) {
	n := newMemNetwork()
	h1 := countedBackendFor(t, n, "h1")
	h2 := countedBackendFor(t, n, "h2")
	doc := miDoc(miTimeouts+miRR, []string{h1.URL, h2.URL})
	fake := clock.NewFake(time.Unix(0, 0))
	gA := n.gateway(t, doc, withClock(fake))
	gB := n.gateway(t, doc, withClock(fake))
	tsA := n.serve(t, gA)
	tsB := n.serve(t, gB)
	client := n.client(t, nil)

	if got := miBackend(t, client, tsA.URL+"/one"); got != "h1" {
		t.Fatalf("A first request served by %s, want h1", got)
	}
	if got := miBackend(t, client, tsA.URL+"/two"); got != "h2" {
		t.Fatalf("A second request served by %s, want h2", got)
	}
	if got := miBackend(t, client, tsB.URL+"/one"); got != "h1" {
		t.Fatalf("B first request served by %s, want h1", got)
	}

	held := startGet(client, tsA.URL+"/hold")
	awaitSignal(t, h1.arrived, "held request to reach h1")

	stA := upstreamStatus(t, gA, "up")
	if got := hostByURL(stA, h1.URL).InFlight; got != 1 {
		t.Fatalf("A in_flight on h1 = %d, want 1", got)
	}
	if got := hostByURL(stA, h2.URL).InFlight; got != 0 {
		t.Fatalf("A in_flight on h2 = %d, want 0", got)
	}
	stB := upstreamStatus(t, gB, "up")
	if got := hostByURL(stB, h1.URL).InFlight + hostByURL(stB, h2.URL).InFlight; got != 0 {
		t.Fatalf("B in_flight total = %d, want 0", got)
	}

	h1.releaseHold()
	if r := awaitGet(t, held); r.status != http.StatusOK {
		t.Fatalf("held status = %d, want 200", r.status)
	}
	eventually(t, "in-flight on h1 of instance A to return to 0", func() bool { return hostByURL(upstreamStatus(t, gA, "up"), h1.URL).InFlight == 0 }, func() string {
		return fmt.Sprintf("in_flight=%d", hostByURL(upstreamStatus(t, gA, "up"), h1.URL).InFlight)
	})
}

func TestInstancesIsolateDNS(t *testing.T) {
	n := newMemNetwork()
	for _, addr := range []string{"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"} {
		n.serveAt(t, addr, echoHandler(addr))
	}
	fr := &fakeResolver{}
	fr.set([]string{"10.0.0.1", "10.0.0.2"})
	doc := "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n" + miTimeouts +
		"    dns:\n      hostname: svc.internal\n      port: 8080\n      scheme: http\n      refresh_interval: 5s\n" +
		"routes:\n  - name: all\n    upstream: up\n"

	fakeA := clock.NewFake(time.Unix(0, 0))
	fakeB := clock.NewFake(time.Unix(0, 0))
	gA := n.gateway(t, doc, withClock(fakeA), withResolver(fr))
	gB := n.gateway(t, doc, withClock(fakeB), withResolver(fr))

	initial := []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080"}
	if got := miHostURLs(t, gA); !slices.Equal(got, initial) {
		t.Fatalf("A initial hosts = %v, want %v", got, initial)
	}
	if got := miHostURLs(t, gB); !slices.Equal(got, initial) {
		t.Fatalf("B initial hosts = %v, want %v", got, initial)
	}

	fakeA.BlockUntil(1)
	fr.set([]string{"10.0.0.1", "10.0.0.3"})
	fakeA.Advance(5 * time.Second)

	changed := []string{"http://10.0.0.1:8080", "http://10.0.0.3:8080"}
	eventually(t, "instance A to adopt the changed DNS host set", func() bool { return slices.Equal(miHostURLs(t, gA), changed) }, func() string { return fmt.Sprint(miHostURLs(t, gA)) })
	if got := miHostURLs(t, gB); !slices.Equal(got, initial) {
		t.Fatalf("B hosts after A refresh = %v, want %v", got, initial)
	}
}

func TestEachInstanceProbes(t *testing.T) {
	n := newMemNetwork()
	h1 := failingBackend(t, n, "h1")
	h2 := failingBackend(t, n, "h2")
	doc := miDoc(miTimeouts+miRR+miActive, []string{h1.URL, h2.URL})

	fake := clock.NewFake(time.Unix(0, 0))
	gs := []*microgateway.Gateway{
		n.gateway(t, doc, withClock(fake)),
		n.gateway(t, doc, withClock(fake)),
		n.gateway(t, doc, withClock(fake)),
	}
	completed := miActiveChecks(t, gs...)

	waitProbes(t, fake, completed, 0, 6)
	fake.Advance(10 * time.Second)
	waitProbes(t, fake, completed, 6, 6)
	if got := completed(); got != 6 {
		t.Fatalf("probes after one interval = %v, want 6", got)
	}
}

func miSpyRoutes() string {
	return stRoute("rl", smRate("spy", 1000, 1000)) + stRoute("plain", "")
}

func miStoreCalls(t *testing.T, spy *teststore.Spy, calls *atomic.Int64, gateways int) {
	t.Helper()
	if got := spy.Count("take"); got != gateways {
		t.Fatalf("spy store takes = %d, want %d", got, gateways)
	}
	if got := calls.Load(); got != int64(gateways) {
		t.Fatalf("store factory calls = %d, want %d", got, gateways)
	}
}

func miWire(t *testing.T, spy *teststore.Spy, calls *atomic.Int64, gs ...*microgateway.Gateway) {
	t.Helper()
	for _, g := range gs {
		if rec := stDo(t, g, "/rl", smClient); rec.Code != http.StatusOK {
			t.Fatalf("/rl status = %d, want 200", rec.Code)
		}
	}
	miStoreCalls(t, spy, calls, len(gs))
}

func miProbeHandler(code int, name string) http.Handler {
	h := echoHandler(name)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(code)
			return
		}
		h.ServeHTTP(w, r)
	})
}

func TestInstancesIsolateBreakerState(t *testing.T) {
	n := newMemNetwork()
	be := newRSBackend(t, n, rsStatusSeq(503, 503, 503, 200))
	spy := teststore.NewSpy()
	shared := stShared(t, rlFake())
	var calls atomic.Int64
	opt := miSpyOption(spy, store.CapRateLimit, shared, nil, &calls)
	doc := smDoc(stStore("spy", ""), miTimeouts+miRR+"    circuit_breaker:\n"+rsBreaker, []string{be.URL}, miSpyRoutes())
	gs, fs := miInstances(t, n, 2, doc, time.Unix(rlStart, 0), time.Hour, opt)
	gA, gB := gs[0], gs[1]
	fA, fB := fs[0], fs[1]
	before := len(spy.Calls())

	for range 3 {
		if rec := stDo(t, gA, "/plain", smClient); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("A status = %d, want 503 from backend", rec.Code)
		}
	}
	if got := be.hitCount(); got != 3 {
		t.Fatalf("backend hits after A failures = %d, want 3", got)
	}
	bkCircuitOpen(t, stDo(t, gA, "/plain", smClient).Body.Bytes())
	if got := be.hitCount(); got != 3 {
		t.Fatalf("backend hits after short-circuit = %d, want 3", got)
	}
	if st := hostByURL(upstreamStatus(t, gA, "up"), be.URL); st.BreakerState != "open" {
		t.Fatalf("A breaker_state = %q, want open", st.BreakerState)
	}
	if v := bkGauge(t, gA, ""); v != 2 {
		t.Fatalf("A aggregate breaker gauge = %v, want 2", v)
	}

	if st := hostByURL(upstreamStatus(t, gB, "up"), be.URL); st.BreakerState != "closed" {
		t.Fatalf("B breaker_state = %q, want closed", st.BreakerState)
	}
	if v := bkGauge(t, gB, ""); v != 0 {
		t.Fatalf("B aggregate breaker gauge = %v, want 0", v)
	}
	if rec := stDo(t, gB, "/plain", smClient); rec.Code != http.StatusOK {
		t.Fatalf("B status = %d, want 200", rec.Code)
	}
	if got := be.hitCount(); got != 4 {
		t.Fatalf("backend hits after B request = %d, want 4", got)
	}

	fA.Advance(30*time.Second - time.Nanosecond)
	bkCircuitOpen(t, stDo(t, gA, "/plain", smClient).Body.Bytes())
	if got := be.hitCount(); got != 4 {
		t.Fatalf("backend hits before open timeout = %d, want 4", got)
	}
	if st := hostByURL(upstreamStatus(t, gA, "up"), be.URL); st.BreakerState != "open" {
		t.Fatalf("A breaker_state before open timeout = %q, want open", st.BreakerState)
	}

	fB.Advance(time.Hour)
	if st := hostByURL(upstreamStatus(t, gA, "up"), be.URL); st.BreakerState != "open" {
		t.Fatalf("A breaker_state after B advanced = %q, want open", st.BreakerState)
	}

	fA.Advance(time.Nanosecond)
	if st := hostByURL(upstreamStatus(t, gA, "up"), be.URL); st.BreakerState != "half_open" {
		t.Fatalf("A breaker_state at open timeout = %q, want half_open", st.BreakerState)
	}
	if rec := stDo(t, gA, "/plain", smClient); rec.Code != http.StatusOK {
		t.Fatalf("A first probe status = %d, want 200", rec.Code)
	}
	if got := be.hitCount(); got != 5 {
		t.Fatalf("backend hits after first probe = %d, want 5", got)
	}

	fA.Advance(time.Nanosecond)
	if rec := stDo(t, gA, "/plain", smClient); rec.Code != http.StatusOK {
		t.Fatalf("A second probe status = %d, want 200", rec.Code)
	}
	if got := be.hitCount(); got != 6 {
		t.Fatalf("backend hits after second probe = %d, want 6", got)
	}
	if st := hostByURL(upstreamStatus(t, gA, "up"), be.URL); st.BreakerState != "closed" {
		t.Fatalf("A breaker_state after second probe = %q, want closed", st.BreakerState)
	}

	if got := len(miSpyDelta(spy, before)); got != 0 {
		t.Fatalf("spy store calls during isolation = %d, want 0", got)
	}
	stDo(t, gA, "/rl", smClient)
	stDo(t, gB, "/rl", smClient)
	miStoreCalls(t, spy, &calls, 2)
}

func TestInstancesIsolatePassiveEjection(t *testing.T) {
	n := newMemNetwork()
	x := failingBackend(t, n, "x")
	y := failingBackend(t, n, "y")
	spy := teststore.NewSpy()
	shared := stShared(t, rlFake())
	var calls atomic.Int64
	opt := miSpyOption(spy, store.CapRateLimit, shared, nil, &calls)
	doc := smDoc(stStore("spy", ""), miTimeouts+miRR+miPassive, []string{x.URL, y.URL}, miSpyRoutes())
	gs, fs := miInstances(t, n, 2, doc, time.Unix(rlStart, 0), time.Hour, opt)
	gA, gB := gs[0], gs[1]
	fA, fB := fs[0], fs[1]
	miWire(t, spy, &calls, gA, gB)
	before := len(spy.Calls())
	x.Status.Store(http.StatusServiceUnavailable)

	for range 4 {
		stDo(t, gA, "/plain", smClient)
	}
	if h := hostByURL(upstreamStatus(t, gA, "up"), x.URL); !h.Ejected {
		t.Fatal("host x not ejected on A")
	}
	if h := hostByURL(upstreamStatus(t, gB, "up"), x.URL); h.Ejected || h.EjectionCount != 0 {
		t.Fatalf("host x on B ejected=%v ejection_count=%d, want not ejected", h.Ejected, h.EjectionCount)
	}

	fA.Advance(30*time.Second - time.Nanosecond)
	if h := hostByURL(upstreamStatus(t, gA, "up"), x.URL); !h.Ejected {
		t.Fatal("host x not ejected on A before base ejection time")
	}
	xHits := x.Hits.Load()
	for range 2 {
		stDo(t, gA, "/plain", smClient)
	}
	if got := x.Hits.Load() - xHits; got != 0 {
		t.Fatalf("requests reaching x on A while ejected = %d, want 0", got)
	}

	fB.Advance(time.Hour)
	if h := hostByURL(upstreamStatus(t, gA, "up"), x.URL); !h.Ejected {
		t.Fatal("host x not ejected on A after B advanced")
	}
	xHits = x.Hits.Load()
	for range 4 {
		stDo(t, gB, "/plain", smClient)
	}
	if got := x.Hits.Load() - xHits; got != 2 {
		t.Fatalf("requests reaching x on B = %d, want 2 of 4", got)
	}

	fA.Advance(time.Nanosecond)
	if h := hostByURL(upstreamStatus(t, gA, "up"), x.URL); h.Ejected {
		t.Fatal("host x still ejected on A at base ejection time")
	}
	fA.Advance(time.Nanosecond)
	if h := hostByURL(upstreamStatus(t, gA, "up"), x.URL); h.Ejected {
		t.Fatal("host x ejected on A after base ejection time")
	}
	xHits = x.Hits.Load()
	for range 2 {
		stDo(t, gA, "/plain", smClient)
	}
	if got := x.Hits.Load() - xHits; got != 1 {
		t.Fatalf("requests reaching x on A after readmission = %d, want 1 of 2", got)
	}

	if got := len(miSpyDelta(spy, before)); got != 0 {
		t.Fatalf("spy store calls during isolation = %d, want 0", got)
	}
}

func TestInstancesIsolateActiveHealthState(t *testing.T) {
	const (
		addr = "10.0.0.9:8080"
		host = "http://10.0.0.9:8080"
	)
	nA, nB := newMemNetwork(), newMemNetwork()
	nA.serveAt(t, addr, miProbeHandler(http.StatusInternalServerError, "a"))
	nB.serveAt(t, addr, miProbeHandler(http.StatusOK, "b"))
	spy := teststore.NewSpy()
	shared := stShared(t, rlFake())
	var calls atomic.Int64
	opt := miSpyOption(spy, store.CapRateLimit, shared, nil, &calls)
	doc := smDoc(stStore("spy", ""), miTimeouts+miRR+miActive, []string{host}, miSpyRoutes())
	fakeA := clock.NewFake(time.Unix(rlStart, 0))
	fakeB := clock.NewFake(time.Unix(rlStart, 0))
	gA := nA.gateway(t, doc, opt, withClock(fakeA))
	gB := nB.gateway(t, doc, opt, withClock(fakeB))
	miWire(t, spy, &calls, gA, gB)
	before := len(spy.Calls())

	completedA := miActiveChecks(t, gA)
	completedB := miActiveChecks(t, gB)
	waitProbes(t, fakeA, completedA, 0, 1)
	waitProbes(t, fakeB, completedB, 0, 1)
	for i := 1; i <= 3; i++ {
		fakeA.Advance(10 * time.Second)
		waitProbes(t, fakeA, completedA, float64(i), 1)
		fakeB.Advance(10 * time.Second)
		waitProbes(t, fakeB, completedB, float64(i), 1)
	}

	hA := hostByURL(upstreamStatus(t, gA, "up"), host)
	if hA.ActiveCheck == nil || hA.ActiveCheck.Status != "unhealthy" || hA.Healthy {
		t.Fatalf("A host = %+v, want unhealthy and not healthy", hA)
	}
	if v := rsMetric(t, gA, "microgateway_upstream_host_healthy", "upstream", "up", "host", "10.0.0.9:8080"); v != 0 {
		t.Fatalf("A host_healthy = %v, want 0", v)
	}
	hB := hostByURL(upstreamStatus(t, gB, "up"), host)
	if hB.ActiveCheck == nil || hB.ActiveCheck.Status != "healthy" || !hB.Healthy {
		t.Fatalf("B host = %+v, want healthy", hB)
	}
	if v := rsMetric(t, gB, "microgateway_upstream_host_healthy", "upstream", "up", "host", "10.0.0.9:8080"); v != 1 {
		t.Fatalf("B host_healthy = %v, want 1", v)
	}

	if got := len(miSpyDelta(spy, before)); got != 0 {
		t.Fatalf("spy store calls during probes = %d, want 0", got)
	}
}

func TestInstancesIsolateReload(t *testing.T) {
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	spy := teststore.NewSpy()
	shared := stShared(t, rlFake())
	var calls atomic.Int64
	opt := miSpyOption(spy, store.CapRateLimit, shared, nil, &calls)
	stores := stStore("spy", "")
	rl := stRoute("rl", smRate("spy", 1000, 1000))
	doc0 := stDoc(be.URL, stores, rl)
	doc1 := stDoc(be.URL, stores, rl+stRoute("extra1", ""))
	doc2 := stDoc(be.URL, stores, rl+stRoute("extra1", "")+stRoute("extra2", ""))
	doc3 := stDoc(be.URL, stores, rl+stRoute("extra1", "")+stRoute("extra2", "")+stRoute("extra3", ""))

	gs, _ := miInstances(t, n, 2, doc0, time.Unix(rlStart, 0), time.Hour, opt)
	gA, gB := gs[0], gs[1]
	fpA := gA.Config().Fingerprint()
	fpB := gB.Config().Fingerprint()
	miWire(t, spy, &calls, gA, gB)
	bReloads := inspect(gB).ReloadSuccess

	prev := fpA
	afterA := func(step, route string) {
		t.Helper()
		fp := gA.Config().Fingerprint()
		if fp == prev {
			t.Fatalf("%s: A fingerprint unchanged", step)
		}
		prev = fp
		if rec := stDo(t, gA, "/"+route, smClient); rec.Code != http.StatusOK {
			t.Fatalf("%s: A /%s status = %d, want 200", step, route, rec.Code)
		}
		if got := gB.Config().Fingerprint(); got != fpB {
			t.Fatalf("%s: B fingerprint = %q, want %q", step, got, fpB)
		}
		rec := stDo(t, gB, "/"+route, smClient)
		if e := decodeErr(t, rec.Result()); rec.Code != http.StatusNotFound || e.Code != "route_not_found" {
			t.Fatalf("%s: B /%s status = %d code %q, want 404 route_not_found", step, route, rec.Code, e.Code)
		}
		if rec := stDo(t, gB, "/rl", smClient); rec.Code != http.StatusOK {
			t.Fatalf("%s: B /rl status = %d, want 200", step, rec.Code)
		}
		if got := inspect(gB).ReloadSuccess; got != bReloads {
			t.Fatalf("%s: B reload success = %v, want %v", step, got, bReloads)
		}
	}

	cfg1, err := config.Parse([]byte(doc1))
	if err != nil {
		t.Fatal(err)
	}
	if err := gA.Reload(cfg1); err != nil {
		t.Fatal(err)
	}
	afterA("Reload", "extra1")

	if err := gA.ReloadYAML([]byte(doc2)); err != nil {
		t.Fatal(err)
	}
	afterA("ReloadYAML", "extra2")

	path := filepath.Join(t.TempDir(), "gw.yaml")
	if err := os.WriteFile(path, []byte(doc3), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := gA.ReloadFile(path); err != nil {
		t.Fatal(err)
	}
	afterA("ReloadFile", "extra3")

	fpD := gA.Config().Fingerprint()
	if err := gA.ReloadYAML([]byte("version: 1\nnot_a_key: 1\n")); err == nil {
		t.Fatal("invalid ReloadYAML succeeded")
	}
	if got := inspect(gA).ReloadFailure; got != 1 {
		t.Fatalf("A reload failures = %v, want 1", got)
	}
	if got := inspect(gB).ReloadFailure; got != 0 {
		t.Fatalf("B reload failures = %v, want 0", got)
	}
	if got := gA.Config().Fingerprint(); got != fpD {
		t.Fatal("A fingerprint changed by invalid reload")
	}

	p2 := filepath.Join(t.TempDir(), "shared.yaml")
	if err := os.WriteFile(p2, []byte(doc0), 0o600); err != nil {
		t.Fatal(err)
	}
	pair, _ := miInstances(t, n, 2, doc0, time.Unix(rlStart, 0), time.Hour, opt)
	gC, gD := pair[0], pair[1]
	if err := os.WriteFile(p2, []byte(doc1), 0o600); err != nil {
		t.Fatal(err)
	}
	dReloads := inspect(gD).ReloadSuccess
	if err := gC.ReloadFile(p2); err != nil {
		t.Fatal(err)
	}
	if rec := stDo(t, gC, "/extra1", smClient); rec.Code != http.StatusOK {
		t.Fatalf("C /extra1 status = %d, want 200", rec.Code)
	}
	if rec := stDo(t, gD, "/extra1", smClient); rec.Code != http.StatusNotFound {
		t.Fatalf("D /extra1 status = %d, want 404", rec.Code)
	}
	if got := inspect(gD).ReloadSuccess; got != dReloads {
		t.Fatalf("D reload success after C reload = %v, want %v", got, dReloads)
	}
	if err := gD.ReloadFile(p2); err != nil {
		t.Fatal(err)
	}
	if rec := stDo(t, gD, "/extra1", smClient); rec.Code != http.StatusOK {
		t.Fatalf("D /extra1 status after its reload = %d, want 200", rec.Code)
	}
	if got := inspect(gD).ReloadSuccess; got != dReloads+1 {
		t.Fatalf("D reload success = %v, want %v", got, dReloads+1)
	}
}

func TestInstancesIsolateCoalescing(t *testing.T) {
	n := newMemNetwork()
	origin := newFlightOrigin()
	origin.set(http.StatusOK, coalesceHot(), "x")
	origin.hold()
	be := n.serve(t, origin)
	sc := teststore.NewSharedCache(clock.NewFake(cacheStart), 1<<20)
	t.Cleanup(func() { _ = sc.Close() })
	spy := teststore.NewSpy()
	opt := miSpyOption(spy, store.CapCache, nil, sc, nil)
	doc := cacheGatewayDoc(be.URL, "stores:\n  - name: sc\n    type: test\ncache:\n  store: sc\n", cacheRouteYAML("r"))
	gs, _ := miInstances(t, n, 2, doc, cacheStart, time.Hour, opt)
	gA, gB := gs[0], gs[1]
	const target = "http://example.com/co"

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	leaderA := coalesceAsync(context.Background(), gA, target, nil)
	awaitSignal(t, origin.arrived, "leader on A to reach the origin")
	var waitersA []<-chan *httptest.ResponseRecorder
	for range 18 {
		waitersA = append(waitersA, coalesceAsync(context.Background(), gA, target, nil))
	}
	cancelledA := coalesceAsync(ctxA, gA, target, nil)
	awaitWaiters(t, gA, 19)

	leaderB := coalesceAsync(context.Background(), gB, target, nil)
	awaitSignal(t, origin.arrived, "leader on B to reach the origin")
	var waitersB []<-chan *httptest.ResponseRecorder
	for range 19 {
		waitersB = append(waitersB, coalesceAsync(context.Background(), gB, target, nil))
	}
	awaitWaiters(t, gB, 19)

	if calls, _, _ := origin.counts(); calls != 2 {
		t.Fatalf("origin calls before release = %d, want 2", calls)
	}
	cancelA()
	coalesceRecv(t, cancelledA)
	awaitWaiters(t, gA, 18)
	if w := coalesceStats(gB).Waiters; w != 19 {
		t.Fatalf("B coalesce waiters = %d, want 19", w)
	}
	if calls, _, _ := origin.counts(); calls != 2 {
		t.Fatalf("origin calls after A cancel = %d, want 2", calls)
	}

	origin.release()
	coalesceOK(t, coalesceRecv(t, leaderA), "x")
	for _, ch := range waitersA {
		coalesceOK(t, coalesceRecv(t, ch), "x")
	}
	coalesceOK(t, coalesceRecv(t, leaderB), "x")
	for _, ch := range waitersB {
		coalesceOK(t, coalesceRecv(t, ch), "x")
	}
	if calls, _, _ := origin.counts(); calls != 2 {
		t.Fatalf("origin calls after release = %d, want 2", calls)
	}
}
