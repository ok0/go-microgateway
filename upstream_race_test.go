package lychgate

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
)

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

func checkChurnAdmin(t *testing.T, g *Gateway) {
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
