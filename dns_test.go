package microgateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/upstream"
)

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
	g := n.gateway(t, dnsDoc(), withClock(fake), withResolver(res), WithLogger(newJSONLogger(logs)))
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
	eventually(t, func() bool {
		st := upstreamStatus(t, g, "dns")
		return hostByURL(st, "http://10.0.0.2:8080").InFlight == 1 && hostByURL(st, "http://10.0.0.3:8080").InFlight == 1
	})

	fake.BlockUntil(3)
	res.set([]string{"10.0.0.4", "10.0.0.3", "10.0.0.1"})
	fake.Advance(dnsRefreshInterval)
	wantAfter := []string{"http://10.0.0.1:8080", "http://10.0.0.3:8080", "http://10.0.0.4:8080"}
	eventually(t, func() bool {
		return slices.Equal(dnsHostURLs(upstreamStatus(t, g, "dns")), wantAfter) && len(logs.records(t, "upstream dns host set changed")) == 2
	})

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
	eventually(t, func() bool {
		return metricsHasLine(metricsText(t, g), `microgateway_upstream_dns_refresh_total{upstream="dns",result="error"} 1`)
	})
	if got := dnsHostURLs(upstreamStatus(t, g, "dns")); !slices.Equal(got, wantAfter) {
		t.Fatalf("hosts after error answer = %v, want %v", got, wantAfter)
	}
	if cnt := len(logs.records(t, "upstream dns lookup failed")); cnt != 1 {
		t.Fatalf("lookup failed logs = %d, want 1", cnt)
	}

	fake.BlockUntil(1)
	res.set(nil)
	fake.Advance(dnsRefreshInterval)
	eventually(t, func() bool {
		return metricsHasLine(metricsText(t, g), `microgateway_upstream_dns_refresh_total{upstream="dns",result="empty"} 1`)
	})
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
	eventually(t, func() bool {
		return slices.Equal(dnsHostURLs(upstreamStatus(t, g, "dns")), []string{"http://10.0.0.1:8080"})
	})
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
	g := n.gateway(t, dnsDoc(), withClock(fake), withResolver(res), WithLogger(newJSONLogger(logs)))

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

	_, err := NewFromYAML([]byte(both))
	dnsFieldError(t, err, "upstreams[0]", "hosts and dns are mutually exclusive")
	_, err = NewFromYAML([]byte(neither))
	dnsFieldError(t, err, "upstreams[0]", "either hosts or dns is required")

	g := gwWith(t, dnsDoc(), withResolver(&fakeResolver{}), withClock(clock.NewFake(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))))
	dnsFieldError(t, g.ReloadYAML([]byte(both)), "upstreams[0]", "hosts and dns are mutually exclusive")
	dnsFieldError(t, g.ReloadYAML([]byte(neither)), "upstreams[0]", "either hosts or dns is required")
}
