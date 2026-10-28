package microgateway

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type getResult struct {
	status int
	gen    string
	err    error
}

func startGet(client *http.Client, url string) <-chan getResult {
	out := make(chan getResult, 1)
	go func() {
		resp, err := client.Get(url)
		if err != nil {
			out <- getResult{err: err}
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		out <- getResult{status: resp.StatusCode, gen: resp.Header.Get("X-Gen")}
	}()
	return out
}

func awaitGet(t *testing.T, ch <-chan getResult) getResult {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for response")
	}
	return getResult{}
}

func TestFileEditUnderLoad(t *testing.T) {
	runFileEditUnderLoad(t, newMemNetwork())
}

func runFileEditUnderLoad(t *testing.T, n testNetwork) {
	be := n.serve(t, echoHandler("a"))
	docA := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: a
    match:
      path: /a
    upstream: up
`, be.URL)
	docB := docA + "  - name: b\n    match:\n      path: /b\n    upstream: up\n"

	dir := t.TempDir()
	path := writeConfig(t, dir, docA)
	g := n.gateway(t, docA)
	ts := n.serve(t, g)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchDone := make(chan error, 1)
	go func() { watchDone <- g.WatchFile(ctx, path) }()

	var stop atomic.Bool
	var total, failures atomic.Int64
	var wg sync.WaitGroup
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	for range 8 {
		client := n.client(t, nil)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer client.CloseIdleConnections()
			for !stop.Load() {
				total.Add(1)
				resp, err := client.Get(ts.URL + "/a")
				if err != nil {
					failures.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					failures.Add(1)
				}
			}
		}()
	}

	eventually(t, func() bool {
		_, _, u := counters(g)
		return u == 1
	})

	if err := os.WriteFile(path, []byte(docB), 0o600); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	probe := n.client(t, nil)
	eventually(t, func() bool { return statusOf(probe, ts.URL+"/b") == http.StatusOK })
	if elapsed := time.Since(start); elapsed >= 2*time.Second {
		t.Fatalf("new route live after %v, want < 2s", elapsed)
	}

	<-time.After(300 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d of %d requests failed under load", n, total.Load())
	}
	if total.Load() == 0 {
		t.Fatal("no load was generated")
	}
	if got := ts.New.Load(); got != 9 {
		t.Fatalf("front connections = %d, want 9 (8 workers and probe)", got)
	}
	cancel()
	if err := <-watchDone; err != nil {
		t.Fatalf("WatchFile = %v, want nil", err)
	}
}

func TestReloadStressConsistent(t *testing.T) {
	runReloadStressConsistent(t, newMemNetwork())
}

func runReloadStressConsistent(t *testing.T, n testNetwork) {
	beA := n.serve(t, echoHandler("A"))
	beB := n.serve(t, echoHandler("B"))
	genDoc := func(url, gen string) string {
		return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: all
    upstream: up
    response_headers:
      set:
        X-Gen: %q
`, url, gen)
	}
	docs := [2]string{genDoc(beA.URL, "A"), genDoc(beB.URL, "B")}
	invalid := "version: 1\nroutes: [\n"

	g := n.gateway(t, docs[1])
	ts := n.serve(t, g)

	var stop atomic.Bool
	var checked, bad atomic.Int64
	var wg sync.WaitGroup
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	for range 16 {
		client := n.client(t, nil)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer client.CloseIdleConnections()
			for !stop.Load() {
				resp, err := client.Get(ts.URL + "/")
				if err != nil {
					bad.Add(1)
					continue
				}
				var e echo
				derr := json.NewDecoder(resp.Body).Decode(&e)
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				checked.Add(1)
				if derr != nil || resp.StatusCode != http.StatusOK || resp.Header.Get("X-Gen") != e.Backend {
					bad.Add(1)
				}
			}
		}()
	}

	for i := range 100 {
		if err := g.ReloadYAML([]byte(docs[i%2])); err != nil {
			t.Fatalf("reload %d: %v", i, err)
		}
		if i%2 == 1 {
			if err := g.ReloadYAML([]byte(invalid)); err == nil {
				t.Fatalf("invalid reload after reload %d succeeded", i)
			}
		}
	}
	stop.Store(true)
	wg.Wait()

	if s, f, _ := counters(g); s != 100 || f != 50 {
		t.Fatalf("success, failure = %v, %v, want 100, 50", s, f)
	}
	if n := bad.Load(); n != 0 {
		t.Fatalf("%d of %d responses were not 200 with a matching X-Gen", n, checked.Load())
	}
	if checked.Load() == 0 {
		t.Fatal("no requests were checked")
	}
	if got := ts.New.Load(); got != 16 {
		t.Fatalf("front connections = %d, want 16", got)
	}
}

func TestInFlightSurvivesRemoval(t *testing.T) {
	runInFlightSurvivesRemoval(t, newMemNetwork())
}

func runInFlightSurvivesRemoval(t *testing.T, n testNetwork) {
	hold := holdBackend(t, n)
	sse := newSSEBackend(t, n)
	ws := n.serve(t, wsEchoHandler())
	other := n.serve(t, echoHandler("other"))

	doc1 := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: hold
    hosts:
      - url: %q
  - name: sse
    hosts:
      - url: %q
  - name: ws
    hosts:
      - url: %q
routes:
  - name: hold
    match:
      path: /hold
    upstream: hold
    response_headers:
      set:
        X-Gen: "1"
  - name: sse
    match:
      path: /sse
    upstream: sse
    response_headers:
      set:
        X-Gen: "1"
  - name: ws
    match:
      path: /ws
    upstream: ws
    response_headers:
      set:
        X-Gen: "1"
`, hold.URL, sse.URL, ws.URL)
	doc2 := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: other
    hosts:
      - url: %q
routes:
  - name: other
    match:
      path: /other
    upstream: other
`, other.URL)

	g := n.gateway(t, doc1)
	ref := n.gateway(t, doc2)
	refLen := ref.state.Len()

	ts := n.serve(t, g)

	heldClient := n.client(t, nil)
	heldDone := startGet(heldClient, ts.URL+"/hold")
	awaitSignal(t, hold.arrived, "held request to reach the upstream")

	sseClient := n.client(t, nil)
	sseResp, err := sseClient.Get(ts.URL + "/sse")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sseResp.Body.Close() })
	lines := make(chan string, 8)
	go func() {
		defer close(lines)
		reader := bufio.NewReader(sseResp.Body)
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				lines <- line
			}
			if err != nil {
				return
			}
		}
	}()
	if first := recvLine(t, lines); first != "data: one\n" {
		t.Fatalf("first SSE line = %q, want %q", first, "data: one\n")
	}

	conn := n.dial(t, ts)
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: gw.test\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	wsResp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if wsResp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade status = %d, want 101", wsResp.StatusCode)
	}
	wsEcho := func(msg string) {
		t.Helper()
		if _, err := io.WriteString(conn, msg); err != nil {
			t.Fatal(err)
		}
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line != msg {
			t.Fatalf("echo = %q, want %q", line, msg)
		}
	}
	wsEcho("hello\n")

	if err := g.ReloadYAML([]byte(doc2)); err != nil {
		t.Fatal(err)
	}
	probe := n.client(t, nil)
	if got := statusOf(probe, ts.URL+"/hold"); got != http.StatusNotFound {
		t.Fatalf("new /hold status = %d, want 404", got)
	}

	hold.Release()
	held := awaitGet(t, heldDone)
	if held.status != http.StatusOK || held.gen != "1" {
		t.Fatalf("held response status = %d X-Gen = %q, want 200 and 1", held.status, held.gen)
	}

	sse.Release()
	for recvLine(t, lines) != "data: two\n" {
	}

	wsEcho("again\n")

	sseResp.Body.Close()
	conn.Close()
	heldClient.CloseIdleConnections()
	sseClient.CloseIdleConnections()
	probe.CloseIdleConnections()

	eventually(t, func() bool { return g.live.Load() == 1 })
	if got := g.state.Len(); got != refLen {
		t.Fatalf("state entries = %d, want %d", got, refLen)
	}
}

func TestNoLeakAfterManyReloads(t *testing.T) {
	runNoLeakAfterManyReloads(t, newMemNetwork())
}

func runNoLeakAfterManyReloads(t *testing.T, n testNetwork) {
	be := n.serve(t, echoHandler("leak"))
	doc := func(name string) string {
		return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: %s
    hosts:
      - url: %q
routes:
  - name: all
    upstream: %s
`, name, be.URL, name)
	}
	g := n.gateway(t, doc("init"))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	if got := statusOf(client, ts.URL+"/x"); got != http.StatusOK {
		t.Fatalf("warm-up status = %d, want 200", got)
	}
	if got := be.New.Load(); got != 1 {
		t.Fatalf("backend connections after warm-up = %d, want 1", got)
	}
	base := runtime.NumGoroutine()

	names := []string{"alpha", "beta"}
	for i := range 100 {
		if err := g.ReloadYAML([]byte(doc(names[i%2]))); err != nil {
			t.Fatalf("reload %d: %v", i, err)
		}
		if got := statusOf(client, ts.URL+"/x"); got != http.StatusOK {
			t.Fatalf("request after reload %d: status = %d, want 200", i, got)
		}
	}
	client.CloseIdleConnections()

	if got := be.New.Load(); got != 101 {
		t.Fatalf("backend connections = %d, want 101", got)
	}
	if got := ts.New.Load(); got != 1 {
		t.Fatalf("front connections = %d, want 1", got)
	}

	eventually(t, func() bool { return runtime.NumGoroutine() <= base+base/10 })
	eventually(t, func() bool { return g.live.Load() == 1 })
	eventually(t, func() bool { return ts.Open.Load() == 0 })
	eventually(t, func() bool { return be.Open.Load() == 1 })
	if _, ok := n.openConns(); ok {
		eventually(t, func() bool {
			open, _ := n.openConns()
			return open == 2
		})
	}
}

func TestKeepAliveAcrossReload(t *testing.T) {
	runKeepAliveAcrossReload(t, newMemNetwork())
}

func runKeepAliveAcrossReload(t *testing.T, n testNetwork) {
	be := n.serve(t, echoHandler("ka"))
	be2 := n.serve(t, echoHandler("ka2"))
	initial := oneRouteDoc("", be.URL)
	success := oneRouteDoc("", be2.URL)
	g := n.gateway(t, initial)
	ts := n.serve(t, g)
	client := n.client(t, func(tr *http.Transport) { tr.MaxIdleConnsPerHost = 1 })

	get := func(step string, wantReused bool) {
		t.Helper()
		var info httptrace.GotConnInfo
		trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) { info = i }}
		req, err := http.NewRequestWithContext(httptrace.WithClientTrace(context.Background(), trace), http.MethodGet, ts.URL+"/k", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", step, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s: status = %d, want 200", step, resp.StatusCode)
		}
		if info.Reused != wantReused {
			t.Fatalf("%s: Reused = %v, want %v", step, info.Reused, wantReused)
		}
	}

	get("initial request", false)
	if err := g.ReloadYAML([]byte(success)); err != nil {
		t.Fatal(err)
	}
	get("request after successful reload", true)
	if err := g.ReloadYAML([]byte("version: 1\nroutes: [\n")); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	get("request after failed reload", true)
	if err := g.ReloadYAML([]byte(success)); err != nil {
		t.Fatal(err)
	}
	get("request after unchanged reload", true)
}
