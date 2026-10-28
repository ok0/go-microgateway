package proxy

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/state"
	"github.com/ok0/lychgate/internal/upstream"
)

func newUpstream(t *testing.T, rawURL string) *upstream.Upstream {
	t.Helper()
	return newUpstreamWith(t, config.Upstream{Name: "u", Hosts: []config.Host{{URL: rawURL}}}, upstream.Deps{})
}

func newUpstreamWith(t *testing.T, cfg config.Upstream, d upstream.Deps) *upstream.Upstream {
	t.Helper()
	reg := state.NewRegistry(nil)
	tx := reg.Begin()
	d.State = tx
	up, err := upstream.Build(cfg, d)
	if err != nil {
		t.Fatal(err)
	}
	lease := tx.Commit()
	up.Activate()
	t.Cleanup(func() {
		reg.Close()
		_ = lease.Release()
	})
	return up
}

func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if cond() {
			return
		}
		tick := time.NewTimer(5 * time.Millisecond)
		select {
		case <-deadline.C:
			tick.Stop()
			t.Fatal("condition not met before deadline")
		case <-tick.C:
		}
	}
}

func TestRewritePath(t *testing.T) {
	tests := []struct {
		name     string
		from     string
		to       string
		path     string
		rawPath  string
		wantPath string
		wantRaw  string
	}{
		{"strip prefix root", "/orders", "", "/orders", "", "/", ""},
		{"strip prefix child", "/orders", "", "/orders/123", "", "/123", ""},
		{"strip keeps sibling", "/orders", "", "/ordersx", "", "/ordersx", ""},
		{"strip encoded slash", "/orders", "", "/orders/a/b", "/orders/a%2Fb", "/a/b", "/a%2Fb"},
		{"replace prefix child", "/v2", "/api/v2", "/v2/items", "", "/api/v2/items", ""},
		{"replace prefix exact", "/v2", "/api/v2", "/v2", "", "/api/v2", ""},
		{"replace trailing slash", "/old/", "/new/", "/old/x", "", "/new/x", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &url.URL{Path: tt.path, RawPath: tt.rawPath, RawQuery: "a=1"}
			rewritePath(u, tt.from, tt.to)
			if u.Path != tt.wantPath {
				t.Errorf("Path = %q, want %q", u.Path, tt.wantPath)
			}
			if u.RawPath != tt.wantRaw {
				t.Errorf("RawPath = %q, want %q", u.RawPath, tt.wantRaw)
			}
			if u.RawQuery != "a=1" {
				t.Errorf("RawQuery = %q, want %q", u.RawQuery, "a=1")
			}
		})
	}
}

func TestApplyHeaderOps(t *testing.T) {
	h := http.Header{}
	h.Set("X-Debug", "1")
	h.Set("X-Env", "wrong")
	h.Set("X-Gateway", "edge")
	applyHeaderOps(h, config.HeaderOps{
		Remove: []string{"X-Debug"},
		Set:    map[string]string{"X-Env": "example"},
		Add:    map[string]string{"X-Gateway": "lychgate"},
	})
	if got := h.Values("X-Debug"); len(got) != 0 {
		t.Errorf("X-Debug = %v, want removed", got)
	}
	if got := h.Values("X-Env"); !slices.Equal(got, []string{"example"}) {
		t.Errorf("X-Env = %v, want [example]", got)
	}
	if got := h.Values("X-Gateway"); !slices.Equal(got, []string{"edge", "lychgate"}) {
		t.Errorf("X-Gateway = %v, want [edge lychgate]", got)
	}
}

func TestPreserveHost(t *testing.T) {
	tests := []struct {
		name     string
		preserve bool
		useBack  bool
		want     string
	}{
		{"upstream host", false, true, ""},
		{"original host", true, false, "public.test"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hosts := make(chan string, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hosts <- r.Host
			}))
			defer backend.Close()
			want := tt.want
			if tt.useBack {
				want = strings.TrimPrefix(backend.URL, "http://")
			}
			up := newUpstream(t, backend.URL)
			gw := httptest.NewServer(New(RouteSpec{PreserveHost: tt.preserve}, up, Deps{}))
			defer gw.Close()

			req, err := http.NewRequest(http.MethodGet, gw.URL+"/", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Host = "public.test"
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()

			select {
			case got := <-hosts:
				if got != want {
					t.Errorf("backend Host = %q, want %q", got, want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("backend did not receive request")
			}
		})
	}
}

func TestInFlightUpgrade(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("response writer cannot hijack")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n"); err != nil {
			return
		}
		for {
			line, err := buf.ReadString('\n')
			if line != "" {
				if _, werr := io.WriteString(conn, line); werr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}))
	defer backend.Close()

	up := newUpstream(t, backend.URL)
	gw := httptest.NewServer(New(RouteSpec{}, up, Deps{}))
	defer gw.Close()

	conn, err := net.Dial("tcp", gw.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: gateway.test\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	if _, err := io.WriteString(conn, "ping\n"); err != nil {
		t.Fatal(err)
	}
	line, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "ping\n" {
		t.Errorf("echo = %q, want %q", line, "ping\n")
	}
	if got := up.Hosts()[0].InFlight(); got != 1 {
		t.Errorf("InFlight while open = %d, want 1", got)
	}

	_ = conn.Close()
	eventually(t, func() bool { return up.Hosts()[0].InFlight() == 0 })
}

func TestInFlightNormal(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()

	up := newUpstream(t, backend.URL)
	gw := httptest.NewServer(New(RouteSpec{}, up, Deps{}))
	defer gw.Close()

	resp, err := http.Get(gw.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}
	eventually(t, func() bool { return up.Hosts()[0].InFlight() == 0 })
}

func blockUntilDone(t *testing.T, started chan<- struct{}) string {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if started != nil {
			started <- struct{}{}
		}
		<-r.Context().Done()
	}))
	t.Cleanup(s.Close)
	return s.URL
}

func runAttempt(t *testing.T, ctx context.Context, attempts *metrics.CounterVec, rawURL, outcome string) {
	t.Helper()
	up := newUpstream(t, rawURL)
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	New(RouteSpec{}, up, Deps{Attempts: attempts}).ServeHTTP(httptest.NewRecorder(), req)
	host := strings.TrimPrefix(rawURL, "http://")
	if got := attempts.With(up.Name, host, outcome).Value(); got != 1 {
		t.Errorf("outcome %q count = %v, want 1", outcome, got)
	}
}

func TestAttemptOutcomes(t *testing.T) {
	reg := metrics.NewRegistry()
	attempts, err := reg.Counter("t_attempts", "h", "upstream", "host", "outcome")
	if err != nil {
		t.Fatal(err)
	}

	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(ok.Close)
	t.Run("success", func(t *testing.T) {
		runAttempt(t, context.Background(), attempts, ok.URL, "success")
	})

	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(unavailable.Close)
	t.Run("status_5xx_failure", func(t *testing.T) {
		runAttempt(t, context.Background(), attempts, unavailable.URL, "failure")
	})

	notFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(notFound.Close)
	t.Run("status_404_ignored", func(t *testing.T) {
		runAttempt(t, context.Background(), attempts, notFound.URL, "ignored")
	})

	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closed.URL
	closed.Close()
	t.Run("error", func(t *testing.T) {
		runAttempt(t, context.Background(), attempts, closedURL, "failure")
	})

	t.Run("canceled", func(t *testing.T) {
		started := make(chan struct{}, 1)
		backend := blockUntilDone(t, started)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			select {
			case <-started:
				cancel()
			case <-ctx.Done():
			}
		}()
		runAttempt(t, ctx, attempts, backend, "ignored")
	})

	t.Run("timeout", func(t *testing.T) {
		backend := blockUntilDone(t, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		runAttempt(t, ctx, attempts, backend, "failure")
	})
}

func silentDial(t *testing.T) upstream.DialFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = server.Close() })
		go func() { _, _ = io.Copy(io.Discard, server) }()
		return client, nil
	}
}

func TestResponseHeaderTimeoutFakeClock(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	up := newUpstreamWith(t, config.Upstream{
		Name:          "u",
		Hosts:         []config.Host{{URL: "http://127.0.0.1:9001"}},
		Timeouts:      config.Timeouts{ResponseHeader: config.Duration(time.Hour)},
		PassiveHealth: &config.PassiveHealth{},
	}, upstream.Deps{Dial: silentDial(t), Clock: fake})
	tr := &AttemptTransport{Upstream: up, Clock: fake}
	req, err := http.NewRequest(http.MethodGet, "http://upstream.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}

	errc := make(chan error, 1)
	go func() {
		resp, err := tr.RoundTrip(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		errc <- err
	}()
	fake.BlockUntil(1)
	fake.Advance(time.Hour)

	var got error
	select {
	case got = <-errc:
	case <-time.After(5 * time.Second):
		t.Fatal("RoundTrip did not return after the response header timeout")
	}
	if !errors.Is(got, ErrResponseHeaderTimeout) {
		t.Fatalf("err = %v, want ErrResponseHeaderTimeout", got)
	}
	if status, _ := httperr.FromError(got); status != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", status)
	}
	if n := up.Hosts()[0].InFlight(); n != 0 {
		t.Errorf("InFlight = %d, want 0", n)
	}
	if n := up.Status().Hosts[0].ConsecutiveFailures; n != 1 {
		t.Errorf("ConsecutiveFailures = %d, want 1", n)
	}
}

func TestHeaderTimerStopped(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(backend.Close)

	fake := clock.NewFake(time.Unix(0, 0))
	up := newUpstreamWith(t, config.Upstream{
		Name:     "u",
		Hosts:    []config.Host{{URL: backend.URL}},
		Timeouts: config.Timeouts{ResponseHeader: config.Duration(time.Hour)},
	}, upstream.Deps{Clock: fake})
	tr := &AttemptTransport{Upstream: up, Clock: fake}
	req, err := http.NewRequest(http.MethodGet, "http://upstream.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := tr.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if n := fake.Waiters(); n != 0 {
		t.Fatalf("Waiters = %d after response headers, want 0", n)
	}
	fake.Advance(2 * time.Hour)
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}
}
