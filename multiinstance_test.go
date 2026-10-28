package microgateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

const (
	miTimeouts = "    timeouts:\n      connect: 1h\n      response_header: 1h\n"
	miRR       = "    load_balancer:\n      algorithm: round_robin\n"
	miPassive  = "    passive_health:\n      consecutive_failures: 2\n      base_ejection_time: 30s\n      max_ejection_time: 120s\n"
	miActive   = "    active_health:\n      path: /health\n      interval: 10s\n      timeout: 2s\n      jitter: 0\n      healthy_threshold: 2\n      unhealthy_threshold: 3\n"
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

func miActiveChecks(t *testing.T, gs ...*Gateway) func() float64 {
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

func miHostURLs(t *testing.T, g *Gateway) []string {
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
	for _, g := range []*Gateway{gB, gC} {
		if h := hostByURL(upstreamStatus(t, g, "up"), x.URL); h.Ejected || h.EjectionCount != 0 {
			t.Fatalf("host x ejected=%v ejection_count=%d, want not ejected", h.Ejected, h.EjectionCount)
		}
	}

	for _, g := range []*Gateway{gB, gC} {
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
	eventually(t, func() bool { return hostByURL(upstreamStatus(t, gA, "up"), h1.URL).InFlight == 0 })
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
	eventually(t, func() bool { return slices.Equal(miHostURLs(t, gA), changed) })
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
	gs := []*Gateway{
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
