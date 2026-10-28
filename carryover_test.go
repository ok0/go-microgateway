package microgateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

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

func upstreamHosts(t *testing.T, g *Gateway) map[string][]upstream.HostStatus {
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
	eventually(t, func() bool { return upstreamHosts(t, g)["u1"][0].InFlight == 0 })
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
