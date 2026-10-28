package lychgate

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/upstream"
)

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
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for response")
	}
	return upstreamReply{}
}

func awaitArmedOrReply(t *testing.T, fake *clock.Fake, ch <-chan upstreamReply, want int) (upstreamReply, bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTimer(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatal(r.err)
			}
			return r, true
		default:
		}
		if fake.Waiters() >= want {
			return upstreamReply{}, false
		}
		select {
		case <-deadline.C:
			t.Fatal("neither an armed timer nor a reply within 5s")
		case <-tick.C:
			tick.Reset(5 * time.Millisecond)
		}
	}
}

func getRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func inFlightOf(t *testing.T, g *Gateway, name string) int64 {
	t.Helper()
	return upstreamStatus(t, g, name).Hosts[0].InFlight
}

func startInFlightSampler(t *testing.T, g *Gateway) (stop func()) {
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
		eventually(t, func() bool { return inFlightOf(t, g, "up") == 6 })
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
		eventually(t, func() bool { return inFlightOf(t, g, "refused") == 0 })

		ctx, cancel := context.WithCancel(context.Background())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/hold", nil)
		if err != nil {
			t.Fatal(err)
		}
		canceled := startUpstreamReply(client, req)
		awaitSignal(t, hold.arrived, "held request to reach the backend")
		eventually(t, func() bool { return inFlightOf(t, g, "hold") == 1 })
		cancel()
		select {
		case <-canceled:
		case <-time.After(5 * time.Second):
			t.Fatal("canceled request did not return")
		}
		eventually(t, func() bool { return inFlightOf(t, g, "hold") == 0 })

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
		eventually(t, func() bool { return inFlightOf(t, g, "ws") == 0 })
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
	eventually(t, func() bool { return be.Open.Load() == 0 })
}
