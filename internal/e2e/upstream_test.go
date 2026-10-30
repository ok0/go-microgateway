package e2e

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/internal/upstream"
)

func getOK(t *testing.T, client *http.Client, url string) {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", url, resp.StatusCode)
	}
}

func upstreamHosts(t *testing.T, g *microgateway.Gateway) map[string][]upstream.HostStatus {
	t.Helper()
	var ups []upstream.Status
	if err := json.Unmarshal([]byte(adminBody(t, g, "/upstreams")), &ups); err != nil {
		t.Fatal(err)
	}
	out := make(map[string][]upstream.HostStatus, len(ups))
	for _, u := range ups {
		out[u.Name] = u.Hosts
	}
	return out
}

func TestCarryOverPooledConnection(t *testing.T) {
	runCarryOverPooledConnection(t, newMemNetwork())
}

func runCarryOverPooledConnection(t *testing.T, n testNetwork) {
	b1 := countedBackendFor(t, n, "b1")
	b2 := countedBackendFor(t, n, "b2")
	docA := fmt.Sprintf(`version: 1
%supstreams:
  - name: u1
    hosts:
      - url: %q
  - name: u2
    hosts:
      - url: %q
routes:
  - name: one
    match:
      path: /one
    upstream: u1
  - name: two
    match:
      path: /two
    upstream: u2
`, listenersYAML, b1.URL, b2.URL)
	docB := docA + "  - name: three\n    match:\n      path: /three\n    upstream: u2\n"

	g := n.gateway(t, docA)
	ts := n.serve(t, g)
	client := n.client(t, nil)

	getOK(t, client, ts.URL+"/one")
	getOK(t, client, ts.URL+"/two")
	if b1.srv.New.Load() != 1 || b2.srv.New.Load() != 1 {
		t.Fatalf("warm-up connections = %d, %d, want 1, 1", b1.srv.New.Load(), b2.srv.New.Load())
	}

	if err := g.ReloadYAML([]byte(docB)); err != nil {
		t.Fatal(err)
	}
	getOK(t, client, ts.URL+"/one")
	if got := b1.srv.New.Load(); got != 1 {
		t.Fatalf("u1 new connections after reload = %d, want 1", got)
	}
}

func TestCarryOverInFlight(t *testing.T) {
	runCarryOverInFlight(t, newMemNetwork())
}

func runCarryOverInFlight(t *testing.T, n testNetwork) {
	b1 := countedBackendFor(t, n, "b1")
	docA := fmt.Sprintf(`version: 1
%supstreams:
  - name: u1
    hosts:
      - url: %q
routes:
  - name: hold
    match:
      path: /hold
    upstream: u1
`, listenersYAML, b1.URL)
	docB := docA + "  - name: extra\n    match:\n      path: /extra\n    upstream: u1\n"

	g := n.gateway(t, docA)
	ts := n.serve(t, g)
	client := n.client(t, nil)

	held := startGet(client, ts.URL+"/hold")
	awaitSignal(t, b1.arrived, "held request to reach u1")

	if err := g.ReloadYAML([]byte(docB)); err != nil {
		t.Fatal(err)
	}
	if got := upstreamHosts(t, g)["u1"][0].InFlight; got != 1 {
		t.Fatalf("in_flight after reload = %d, want 1", got)
	}

	b1.releaseHold()
	if r := awaitGet(t, held); r.status != http.StatusOK {
		t.Fatalf("held status = %d, want 200", r.status)
	}
	eventually(t, "in-flight on u1 to return to 0", func() bool { return upstreamHosts(t, g)["u1"][0].InFlight == 0 }, func() string {
		return fmt.Sprintf("in_flight=%d", upstreamHosts(t, g)["u1"][0].InFlight)
	})
}

func TestTransportRecreatedOnTimeoutChange(t *testing.T) {
	runTransportRecreatedOnTimeoutChange(t, newMemNetwork())
}

func runTransportRecreatedOnTimeoutChange(t *testing.T, n testNetwork) {
	b1 := countedBackendFor(t, n, "b1")
	b2 := countedBackendFor(t, n, "b2")
	docA := fmt.Sprintf(`version: 1
%supstreams:
  - name: u1
    hosts:
      - url: %q
  - name: u2
    hosts:
      - url: %q
routes:
  - name: one
    match:
      path: /one
    upstream: u1
  - name: two
    match:
      path: /two
    upstream: u2
`, listenersYAML, b1.URL, b2.URL)
	docB := fmt.Sprintf(`version: 1
%supstreams:
  - name: u1
    hosts:
      - url: %q
    timeouts:
      connect: 7s
  - name: u2
    hosts:
      - url: %q
routes:
  - name: one
    match:
      path: /one
    upstream: u1
  - name: two
    match:
      path: /two
    upstream: u2
`, listenersYAML, b1.URL, b2.URL)

	g := n.gateway(t, docA)
	ts := n.serve(t, g)
	client := n.client(t, nil)

	getOK(t, client, ts.URL+"/one")
	getOK(t, client, ts.URL+"/two")
	if b1.srv.New.Load() != 1 || b2.srv.New.Load() != 1 {
		t.Fatalf("warm-up connections = %d, %d, want 1, 1", b1.srv.New.Load(), b2.srv.New.Load())
	}

	if err := g.ReloadYAML([]byte(docB)); err != nil {
		t.Fatal(err)
	}
	getOK(t, client, ts.URL+"/one")
	if got := b1.srv.New.Load(); got != 2 {
		t.Fatalf("u1 new connections after timeout change = %d, want 2", got)
	}
	getOK(t, client, ts.URL+"/two")
	if got := b2.srv.New.Load(); got != 1 {
		t.Fatalf("u2 new connections after u1 change = %d, want 1", got)
	}
}

func TestRenameResetsState(t *testing.T) {
	runRenameResetsState(t, newMemNetwork())
}

func runRenameResetsState(t *testing.T, n testNetwork) {
	b1 := countedBackendFor(t, n, "b1")
	docA := fmt.Sprintf(`version: 1
%supstreams:
  - name: u1
    hosts:
      - url: %q
routes:
  - name: hold
    match:
      path: /hold
    upstream: u1
  - name: one
    match:
      path: /one
    upstream: u1
`, listenersYAML, b1.URL)
	docB := fmt.Sprintf(`version: 1
%supstreams:
  - name: u1b
    hosts:
      - url: %q
routes:
  - name: hold
    match:
      path: /hold
    upstream: u1b
  - name: one
    match:
      path: /one
    upstream: u1b
`, listenersYAML, b1.URL)

	g := n.gateway(t, docA)
	ts := n.serve(t, g)
	client := n.client(t, nil)

	getOK(t, client, ts.URL+"/one")
	if got := b1.srv.New.Load(); got != 1 {
		t.Fatalf("warm-up connections = %d, want 1", got)
	}

	held := startGet(client, ts.URL+"/hold")
	awaitSignal(t, b1.arrived, "held request to reach u1")

	if err := g.ReloadYAML([]byte(docB)); err != nil {
		t.Fatal(err)
	}
	hosts := upstreamHosts(t, g)
	if _, ok := hosts["u1"]; ok {
		t.Fatal("upstream u1 still listed after rename")
	}
	if got := hosts["u1b"][0].InFlight; got != 0 {
		t.Fatalf("u1b in_flight after rename = %d, want 0", got)
	}

	getOK(t, client, ts.URL+"/one")
	if got := b1.srv.New.Load(); got != 2 {
		t.Fatalf("connections after rename = %d, want 2", got)
	}

	b1.releaseHold()
	if r := awaitGet(t, held); r.status != http.StatusOK {
		t.Fatalf("held status = %d, want 200", r.status)
	}
}

const dnsRefreshInterval = 5 * time.Second

func dnsDoc() string {
	return "version: 1\n" + listenersYAML + `upstreams:
  - name: dns
    dns:
      hostname: backends.test
      port: 8080
      scheme: http
      refresh_interval: 5s
    load_balancer:
      algorithm: round_robin
    timeouts:
      connect: 1h
      response_header: 1h
    passive_health:
      consecutive_failures: 3
      base_ejection_time: 1h
      max_ejection_time: 2h
      max_ejection_percent: 100
routes:
  - name: all
    upstream: dns
`
}

type dnsBackend struct {
	status  atomic.Int32
	hits    atomic.Int64
	release chan struct{}
	once    sync.Once
}

func (b *dnsBackend) releaseHold() {
	b.once.Do(func() { close(b.release) })
}

func serveDNSBackend(t *testing.T, n *memNetwork, addr string) *dnsBackend {
	t.Helper()
	b := &dnsBackend{release: make(chan struct{})}
	b.status.Store(http.StatusOK)
	n.serveAt(t, addr, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.hits.Add(1)
		if r.URL.Path == "/hold" {
			select {
			case <-b.release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(int(b.status.Load()))
		_ = json.NewEncoder(w).Encode(echo{Backend: addr, Path: r.URL.Path})
	}))
	t.Cleanup(b.releaseHold)
	return b
}

func dnsHostURLs(st upstream.Status) []string {
	out := make([]string, 0, len(st.Hosts))
	for _, h := range st.Hosts {
		out = append(out, h.URL)
	}
	return out
}

func dnsFieldError(t *testing.T, err error, path, msg string) {
	t.Helper()
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *config.ValidationError", err)
	}
	for _, fe := range ve.Errors {
		if fe.Path == path && fe.Msg == msg && fe.Line > 0 {
			return
		}
	}
	t.Fatalf("errors %v lack %q at %s with a line", ve.Errors, msg, path)
}

func TestDNSRefreshKeepsStateOfUnchangedHosts(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC))
	res := &fakeResolver{}
	res.set([]string{"10.0.0.3", "10.0.0.1", "10.0.0.2"})
	b1 := serveDNSBackend(t, n, "10.0.0.1:8080")
	b2 := serveDNSBackend(t, n, "10.0.0.2:8080")
	b3 := serveDNSBackend(t, n, "10.0.0.3:8080")
	b4 := serveDNSBackend(t, n, "10.0.0.4:8080")
	logs := &logBuf{}
	g := n.gateway(t, dnsDoc(), withClock(fake), withResolver(res), microgateway.WithLogger(newJSONLogger(logs)))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	three := []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080", "http://10.0.0.3:8080"}
	if got := dnsHostURLs(upstreamStatus(t, g, "dns")); !slices.Equal(got, three) {
		t.Fatalf("initial hosts = %v, want %v", got, three)
	}

	b1.status.Store(http.StatusServiceUnavailable)
	failed := 0
	for range 9 {
		if statusOf(client, ts.URL+"/") == http.StatusServiceUnavailable {
			failed++
		}
	}
	if failed != 3 || b1.hits.Load() != 3 {
		t.Fatalf("503 responses = %d, backend 1 hits = %d, want 3 and 3", failed, b1.hits.Load())
	}
	if h := hostByURL(upstreamStatus(t, g, "dns"), "http://10.0.0.1:8080"); !h.Ejected || h.EjectionCount != 1 {
		t.Fatalf("backend 1 ejected = %v, count = %d, want true and 1", h.Ejected, h.EjectionCount)
	}

	heldA := startGet(client, ts.URL+"/hold")
	heldB := startGet(client, ts.URL+"/hold")
	eventually(t, "one held request in flight on 10.0.0.2 and on 10.0.0.3", func() bool {
		st := upstreamStatus(t, g, "dns")
		return hostByURL(st, "http://10.0.0.2:8080").InFlight == 1 && hostByURL(st, "http://10.0.0.3:8080").InFlight == 1
	}, nil)

	fake.BlockUntil(3)
	res.set([]string{"10.0.0.4", "10.0.0.3", "10.0.0.1"})
	fake.Advance(dnsRefreshInterval)
	wantAfter := []string{"http://10.0.0.1:8080", "http://10.0.0.3:8080", "http://10.0.0.4:8080"}
	eventually(t, "refreshed DNS host set and two change records", func() bool {
		return slices.Equal(dnsHostURLs(upstreamStatus(t, g, "dns")), wantAfter) && len(logs.records(t, "upstream dns host set changed")) == 2
	}, func() string { return fmt.Sprint(dnsHostURLs(upstreamStatus(t, g, "dns"))) })

	st := upstreamStatus(t, g, "dns")
	if h := hostByURL(st, "http://10.0.0.1:8080"); !h.Ejected || h.EjectionCount != 1 {
		t.Fatalf("backend 1 after refresh ejected = %v, count = %d, want true and 1", h.Ejected, h.EjectionCount)
	}
	if got := hostByURL(st, "http://10.0.0.3:8080").InFlight; got != 1 {
		t.Fatalf("backend 3 in_flight after refresh = %d, want 1", got)
	}
	changes := logs.records(t, "upstream dns host set changed")
	change := changes[len(changes)-1]
	if change["added"] != float64(1) || change["removed"] != float64(1) || change["hosts"] != float64(3) {
		t.Fatalf("host set change log = %v, want added 1, removed 1, hosts 3", change)
	}

	b2.releaseHold()
	b3.releaseHold()
	if r := awaitGet(t, heldA); r.status != http.StatusOK {
		t.Fatalf("held request A status = %d, want 200", r.status)
	}
	if r := awaitGet(t, heldB); r.status != http.StatusOK {
		t.Fatalf("held request B status = %d, want 200", r.status)
	}

	removedHits := b2.hits.Load()
	for range 6 {
		if got := statusOf(client, ts.URL+"/"); got != http.StatusOK {
			t.Fatalf("request after refresh status = %d, want 200", got)
		}
	}
	if got := b2.hits.Load(); got != removedHits {
		t.Fatalf("removed backend hits changed from %d to %d", removedHits, got)
	}
	if got := b1.hits.Load(); got != 3 {
		t.Fatalf("ejected backend hits = %d, want 3", got)
	}
	if got := b4.hits.Load(); got == 0 {
		t.Fatal("added backend received no requests")
	}
	if got := b3.hits.Load(); got == 0 {
		t.Fatal("kept backend received no requests")
	}

	fake.BlockUntil(1)
	res.fail(errors.New("servfail"))
	fake.Advance(dnsRefreshInterval)
	eventually(t, "DNS refresh error counted and logged", func() bool {
		return metricsHasLine(metricsText(t, g), `microgateway_upstream_dns_refresh_total{upstream="dns",result="error"} 1`) &&
			len(logs.records(t, "upstream dns lookup failed")) == 1
	}, func() string { return fmt.Sprint(len(logs.records(t, "upstream dns lookup failed"))) })
	if got := dnsHostURLs(upstreamStatus(t, g, "dns")); !slices.Equal(got, wantAfter) {
		t.Fatalf("hosts after error answer = %v, want %v", got, wantAfter)
	}
	if cnt := len(logs.records(t, "upstream dns lookup failed")); cnt != 1 {
		t.Fatalf("lookup failed logs = %d, want 1", cnt)
	}

	fake.BlockUntil(1)
	res.set(nil)
	fake.Advance(dnsRefreshInterval)
	eventually(t, "empty DNS answer counted and logged", func() bool {
		return metricsHasLine(metricsText(t, g), `microgateway_upstream_dns_refresh_total{upstream="dns",result="empty"} 1`) &&
			len(logs.records(t, "upstream dns lookup returned no addresses")) == 1
	}, func() string { return fmt.Sprint(len(logs.records(t, "upstream dns lookup returned no addresses"))) })
	if got := dnsHostURLs(upstreamStatus(t, g, "dns")); !slices.Equal(got, wantAfter) {
		t.Fatalf("hosts after empty answer = %v, want %v", got, wantAfter)
	}
	if cnt := len(logs.records(t, "upstream dns lookup returned no addresses")); cnt != 1 {
		t.Fatalf("no addresses logs = %d, want 1", cnt)
	}
}

func TestDNSFirstLookupFailure(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC))
	res := &fakeResolver{}
	res.fail(errors.New("nxdomain"))
	n.serveAt(t, "10.0.0.1:8080", echoHandler("b1"))
	g := n.gateway(t, dnsDoc(), withClock(fake), withResolver(res))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	resp, err := client.Get(ts.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status before first successful lookup = %d, want 503", resp.StatusCode)
	}
	if e := decodeErr(t, resp); e.Code != "no_healthy_upstream" {
		t.Fatalf("error code = %q, want no_healthy_upstream", e.Code)
	}

	res.set([]string{"10.0.0.1"})
	fake.BlockUntil(1)
	fake.Advance(dnsRefreshInterval)
	eventually(t, "host set to be 10.0.0.1 only", func() bool {
		return slices.Equal(dnsHostURLs(upstreamStatus(t, g, "dns")), []string{"http://10.0.0.1:8080"})
	}, func() string { return fmt.Sprint(dnsHostURLs(upstreamStatus(t, g, "dns"))) })
	getOK(t, client, ts.URL+"/")
}

func TestDNSAnswerLimit(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Date(2026, 8, 9, 10, 11, 12, 0, time.UTC))
	answers := make([]string, 0, 300)
	for i := range 300 {
		answers = append(answers, fmt.Sprintf("10.1.%d.%d", i/250, i%250+1))
	}
	res := &fakeResolver{}
	res.set(answers)
	logs := &logBuf{}
	g := n.gateway(t, dnsDoc(), withClock(fake), withResolver(res), microgateway.WithLogger(newJSONLogger(logs)))

	if got := len(upstreamStatus(t, g, "dns").Hosts); got != 256 {
		t.Fatalf("hosts = %d, want 256", got)
	}
	truncated := logs.records(t, "upstream dns answer truncated")
	if len(truncated) != 1 {
		t.Fatalf("truncated logs = %d, want 1", len(truncated))
	}
	if truncated[0]["addresses"] != float64(300) || truncated[0]["limit"] != float64(256) || truncated[0]["hostname"] != "backends.test" {
		t.Fatalf("truncated log = %v, want addresses 300, limit 256, hostname backends.test", truncated[0])
	}
	changed := logs.records(t, "upstream dns host set changed")
	if len(changed) == 0 {
		t.Fatal("no host set change logged")
	}
	last := changed[len(changed)-1]
	if last["added"] != float64(256) || last["removed"] != float64(0) || last["hosts"] != float64(256) {
		t.Fatalf("host set change log = %v, want added 256, removed 0, hosts 256", last)
	}
}

func TestDNSHostsAndDNSExclusive(t *testing.T) {
	both := "version: 1\n" + listenersYAML + `upstreams:
  - name: dns
    hosts:
      - url: "http://10.0.0.1:8080"
    dns:
      hostname: backends.test
      port: 8080
routes:
  - name: all
    upstream: dns
`
	neither := "version: 1\n" + listenersYAML + `upstreams:
  - name: dns
routes:
  - name: all
    upstream: dns
`

	_, err := newGatewayFromYAML([]byte(both))
	dnsFieldError(t, err, "upstreams[0]", "hosts and dns are mutually exclusive")
	_, err = newGatewayFromYAML([]byte(neither))
	dnsFieldError(t, err, "upstreams[0]", "either hosts or dns is required")

	g := gwWith(t, dnsDoc(), withResolver(&fakeResolver{}), withClock(clock.NewFake(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))))
	dnsFieldError(t, g.ReloadYAML([]byte(both)), "upstreams[0]", "hosts and dns are mutually exclusive")
	dnsFieldError(t, g.ReloadYAML([]byte(neither)), "upstreams[0]", "either hosts or dns is required")
}

func churnAnswers(i int) []string {
	all := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	out := []string{all[i%3]}
	if i%2 == 0 {
		out = append(out, all[(i+1)%3])
	}
	return out
}

func churnGet(t *testing.T, client *http.Client, url string, want int) {
	resp, err := client.Get(url)
	if err != nil {
		t.Errorf("GET %s: %v", url, err)
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if want != 0 && resp.StatusCode != want {
		t.Errorf("GET %s status = %d, want %d", url, resp.StatusCode, want)
	}
}

func checkChurnAdmin(t *testing.T, g *microgateway.Gateway) {
	t.Helper()
	for name, hosts := range upstreamHosts(t, g) {
		ejected := 0
		for _, h := range hosts {
			if h.InFlight < 0 {
				t.Errorf("in_flight of %s in %s = %d, want >= 0", h.URL, name, h.InFlight)
			}
			if h.Ejected {
				ejected++
			}
		}
		if name == "flaky" && ejected > 1 {
			t.Errorf("ejected hosts in flaky = %d, want at most 1 (50%% of 2)", ejected)
		}
	}
}

func TestUpstreamsConcurrentChurn(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC))
	h1 := n.serve(t, echoHandler("h1"))
	h2 := n.serve(t, echoHandler("h2"))
	bad := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	good := n.serve(t, echoHandler("good"))
	for _, a := range []string{"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"} {
		n.serveAt(t, a, echoHandler(a))
	}
	res := &fakeResolver{}
	res.set(churnAnswers(0))

	doc := func(i int) string {
		healthy := fmt.Sprintf("      - url: %q\n        weight: %d\n", h1.URL, 1+i%3)
		if i%2 == 0 {
			healthy += fmt.Sprintf("      - url: %q\n        weight: 1\n", h2.URL)
		}
		routes := "  - name: h\n    match:\n      path: /h\n    upstream: healthy\n" +
			"  - name: f\n    match:\n      path: /f\n    upstream: flaky\n" +
			"  - name: d\n    match:\n      path: /d\n    upstream: dns\n"
		if i%3 == 0 {
			routes += "  - name: extra\n    match:\n      path: /extra\n    upstream: healthy\n"
		}
		return "version: 1\n" + listenersYAML + "upstreams:\n" +
			"  - name: healthy\n    hosts:\n" + healthy +
			"    load_balancer:\n      algorithm: round_robin\n" +
			"    timeouts:\n      connect: 1h\n      response_header: 1h\n" +
			"  - name: flaky\n    hosts:\n" +
			fmt.Sprintf("      - url: %q\n      - url: %q\n", bad.URL, good.URL) +
			"    load_balancer:\n      algorithm: round_robin\n" +
			"    timeouts:\n      connect: 1h\n      response_header: 1h\n" +
			"    passive_health:\n      consecutive_failures: 2\n      base_ejection_time: 1h\n      max_ejection_time: 2h\n      max_ejection_percent: 50\n" +
			"  - name: dns\n    dns:\n      hostname: svc.test\n      port: 8080\n      scheme: http\n      refresh_interval: 5s\n" +
			"    load_balancer:\n      algorithm: round_robin\n" +
			"    timeouts:\n      connect: 1h\n      response_header: 1h\n" +
			"routes:\n" + routes
	}

	g := n.gateway(t, doc(0), withClock(fake), withResolver(res))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(stop)
		wg.Wait()
	}()
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				churnGet(t, client, ts.URL+"/h", http.StatusOK)
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				churnGet(t, client, ts.URL+"/f", 0)
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				churnGet(t, client, ts.URL+"/d", 0)
				select {
				case <-stop:
					return
				default:
				}
			}
		}()
	}

	for i := 1; i <= 200; i++ {
		res.set(churnAnswers(i))
		fake.Advance(5 * time.Second)
		if err := g.ReloadYAML([]byte(doc(i))); err != nil {
			t.Fatalf("reload %d: %v", i, err)
		}
		checkChurnAdmin(t, g)
	}
	checkChurnAdmin(t, g)
}

var transportEpoch = time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)

const transportTimeouts = "    timeouts:\n      connect: 1h\n      response_header: 1h\n"

func oneUpstreamDoc(hostURL, fields string) string {
	return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
    load_balancer:
      algorithm: round_robin
`, hostURL) + fields + "routes:\n  - name: all\n    upstream: up\n"
}

type upstreamReply struct {
	status int
	code   string
	err    error
}

func startUpstreamReply(client *http.Client, req *http.Request) <-chan upstreamReply {
	out := make(chan upstreamReply, 1)
	go func() {
		resp, err := client.Do(req)
		if err != nil {
			out <- upstreamReply{err: err}
			return
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(resp.Body)
		if err != nil {
			out <- upstreamReply{err: err}
			return
		}
		var body struct {
			Error apiError `json:"error"`
		}
		_ = json.Unmarshal(raw, &body)
		out <- upstreamReply{status: resp.StatusCode, code: body.Error.Code}
	}()
	return out
}

func awaitUpstreamReply(t *testing.T, ch <-chan upstreamReply) upstreamReply {
	t.Helper()
	r, _ := testwait.Recv(t, "upstream reply", ch, nil)
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r
}

func awaitArmedOrReply(t *testing.T, fake *clock.Fake, ch <-chan upstreamReply, want int) (upstreamReply, bool) {
	t.Helper()
	var r upstreamReply
	replied := false
	eventually(t, fmt.Sprintf("%d armed fake timers or a reply", want), func() bool {
		select {
		case r = <-ch:
			replied = true
			return true
		default:
		}
		return fake.Waiters() >= want
	}, func() string { return fmt.Sprintf("fake waiters=%d", fake.Waiters()) })
	if replied && r.err != nil {
		t.Fatal(r.err)
	}
	return r, replied
}

func getRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func inFlightOf(t *testing.T, g *microgateway.Gateway, name string) int64 {
	t.Helper()
	return upstreamStatus(t, g, name).Hosts[0].InFlight
}

func startInFlightSampler(t *testing.T, g *microgateway.Gateway) (stop func()) {
	t.Helper()
	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		for {
			select {
			case <-done:
				return
			default:
			}
			var ups []upstream.Status
			rec := httptest.NewRecorder()
			g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upstreams", nil))
			if err := json.NewDecoder(rec.Body).Decode(&ups); err != nil {
				t.Errorf("sampling /upstreams: %v", err)
				return
			}
			for _, u := range ups {
				for _, h := range u.Hosts {
					if h.InFlight < 0 {
						t.Errorf("in_flight of %s in %s = %d, want >= 0", h.URL, u.Name, h.InFlight)
					}
				}
			}
			runtime.Gosched()
		}
	}()
	return func() {
		close(done)
		<-exited
	}
}

func TestTransportLimits(t *testing.T) {
	runTransportLimits(t, newMemNetwork())
}

func runTransportLimits(t *testing.T, n testNetwork) {
	t.Run("response header timeout", func(t *testing.T) {
		fake := clock.NewFake(transportEpoch)
		hold := holdBackend(t, n)
		g := n.gateway(t, oneUpstreamDoc(hold.URL, transportTimeouts), withClock(fake))
		ts := n.serve(t, g)
		client := n.client(t, nil)

		ch := startUpstreamReply(client, getRequest(t, ts.URL+"/"))
		awaitSignal(t, hold.arrived, "request to reach the held backend")
		fake.BlockUntil(1)
		fake.Advance(time.Hour)
		if r := awaitUpstreamReply(t, ch); r.status != http.StatusGatewayTimeout || r.code != "gateway_timeout" {
			t.Fatalf("reply = %d %q, want 504 gateway_timeout", r.status, r.code)
		}
	})

	t.Run("connect timeout", func(t *testing.T) {
		fake := clock.NewFake(transportEpoch)
		hole := n.blackhole(t)
		g := n.gateway(t, oneUpstreamDoc("http://"+hole, transportTimeouts), withClock(fake))
		ts := n.serve(t, g)
		client := n.client(t, nil)

		ch := startUpstreamReply(client, getRequest(t, ts.URL+"/"))
		if r, replied := awaitArmedOrReply(t, fake, ch, 1); replied {
			if _, ok := n.(tcpNetwork); ok {
				t.Skipf("dial to %s failed before the clock advance: %d %q", hole, r.status, r.code)
			}
			t.Fatalf("reply %d %q arrived before the clock advance", r.status, r.code)
		}
		fake.Advance(time.Hour)
		if r := awaitUpstreamReply(t, ch); r.status != http.StatusBadGateway || r.code != "bad_gateway" {
			t.Fatalf("reply = %d %q, want 502 bad_gateway", r.status, r.code)
		}
	})

	t.Run("max connections per host", func(t *testing.T) {
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
		t.Cleanup(releaseAll)
		srv := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, "{}\n")
		}))
		fields := "    pool:\n      max_conns_per_host: 2\n" + transportTimeouts
		g := n.gateway(t, oneUpstreamDoc(srv.URL, fields), withClock(clock.NewFake(transportEpoch)))
		ts := n.serve(t, g)
		client := n.client(t, nil)

		replies := make([]<-chan upstreamReply, 0, 6)
		for range 6 {
			replies = append(replies, startUpstreamReply(client, getRequest(t, ts.URL+"/")))
		}
		eventually(t, "6 requests in flight on up", func() bool { return inFlightOf(t, g, "up") == 6 }, func() string {
			return fmt.Sprintf("in_flight=%d", inFlightOf(t, g, "up"))
		})
		if p := srv.Peak.Load(); p > 2 {
			t.Fatalf("peak backend connections = %d, want at most 2", p)
		}
		releaseAll()
		for _, ch := range replies {
			if r := awaitUpstreamReply(t, ch); r.status != http.StatusOK {
				t.Fatalf("reply status = %d, want 200", r.status)
			}
		}
		if p := srv.Peak.Load(); p > 2 {
			t.Fatalf("peak backend connections = %d, want at most 2", p)
		}
	})

	t.Run("in-flight returns to zero", func(t *testing.T) {
		fake := clock.NewFake(transportEpoch)
		okSrv := n.serve(t, echoHandler("ok"))
		hold := holdBackend(t, n)
		bad := n.serve(t, echoHandler("bad"))
		n.refuse(t, bad)
		wsSrv := n.serve(t, wsEchoHandler())
		doc := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: ok
    hosts:
      - url: %q
    load_balancer:
      algorithm: round_robin
%s  - name: hold
    hosts:
      - url: %q
    load_balancer:
      algorithm: round_robin
%s  - name: refused
    hosts:
      - url: %q
    load_balancer:
      algorithm: round_robin
%s  - name: ws
    hosts:
      - url: %q
    load_balancer:
      algorithm: round_robin
%sroutes:
  - name: ok
    match:
      path: /ok
    upstream: ok
  - name: hold
    match:
      path: /hold
    upstream: hold
  - name: refused
    match:
      path: /refused
    upstream: refused
  - name: ws
    match:
      path: /ws
    upstream: ws
`, okSrv.URL, transportTimeouts, hold.URL, transportTimeouts, bad.URL, transportTimeouts, wsSrv.URL, transportTimeouts)
		g := n.gateway(t, doc, withClock(fake))
		ts := n.serve(t, g)
		client := n.client(t, nil)

		stop := startInFlightSampler(t, g)
		defer stop()

		getOK(t, client, ts.URL+"/ok")
		if got := inFlightOf(t, g, "ok"); got != 0 {
			t.Fatalf("ok in_flight after normal request = %d, want 0", got)
		}

		if got := statusOf(client, ts.URL+"/refused"); got != http.StatusBadGateway {
			t.Fatalf("refused status = %d, want 502", got)
		}
		eventually(t, "refused in-flight to return to 0", func() bool { return inFlightOf(t, g, "refused") == 0 }, func() string {
			return fmt.Sprintf("in_flight=%d", inFlightOf(t, g, "refused"))
		})

		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/hold", nil)
		if err != nil {
			t.Fatal(err)
		}
		canceled := startUpstreamReply(client, req)
		awaitSignal(t, hold.arrived, "held request to reach the backend")
		eventually(t, "held request in flight", func() bool { return inFlightOf(t, g, "hold") == 1 }, func() string {
			return fmt.Sprintf("in_flight=%d", inFlightOf(t, g, "hold"))
		})
		cancel()
		testwait.Recv(t, "canceled request to return", canceled, nil)
		eventually(t, "held in-flight to return to 0", func() bool { return inFlightOf(t, g, "hold") == 0 }, func() string {
			return fmt.Sprintf("in_flight=%d", inFlightOf(t, g, "hold"))
		})

		wsConn := n.dial(t, ts)
		if _, err := io.WriteString(wsConn, "GET /ws HTTP/1.1\r\nHost: gw.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		br := bufio.NewReader(wsConn)
		resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("upgrade status = %d, want 101", resp.StatusCode)
		}
		if _, err := io.WriteString(wsConn, "hello\n"); err != nil {
			t.Fatal(err)
		}
		if line, err := br.ReadString('\n'); err != nil || line != "hello\n" {
			t.Fatalf("echo = %q, %v", line, err)
		}
		if got := inFlightOf(t, g, "ws"); got != 1 {
			t.Fatalf("ws in_flight while open = %d, want 1", got)
		}
		_ = wsConn.Close()
		eventually(t, "ws in-flight to return to 0", func() bool { return inFlightOf(t, g, "ws") == 0 }, func() string {
			return fmt.Sprintf("in_flight=%d", inFlightOf(t, g, "ws"))
		})
	})
}

func TestIdleConnectionsClosed(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("idle"))
	g := n.gateway(t, oneUpstreamDoc(be.URL, "    timeouts:\n      idle: 50ms\n"))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	getOK(t, client, ts.URL+"/")
	if got := be.New.Load(); got != 1 {
		t.Fatalf("backend connections = %d, want 1", got)
	}
	eventually(t, "idle backend connection to close", func() bool { return be.Open.Load() == 0 }, func() string {
		return fmt.Sprintf("open=%d", be.Open.Load())
	})
}
