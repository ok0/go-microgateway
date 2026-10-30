package proxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/breaker"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/retry"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/internal/upstream"
)

func newUpstream(t *testing.T, rawURL string, clk clock.Clock) *upstream.Upstream {
	t.Helper()
	return newUpstreamWith(t, config.Upstream{Name: "u", Hosts: []config.Host{{URL: config.NewSecretURL(rawURL)}}}, upstream.Deps{Clock: clk})
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

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	testwait.Until(t, what, cond, nil)
}

// blockUntil waits until fake has at least n pending timers, tickers or After channels.
func blockUntil(t *testing.T, fake *clock.Fake, n int, what string) {
	t.Helper()
	testwait.Until(t, what, func() bool { return fake.Waiters() >= n }, func() string {
		return fmt.Sprintf("fake waiters=%d, want >= %d", fake.Waiters(), n)
	})
}

// awaitDue waits until n timers of fake are due within the given duration.
func awaitDue(t *testing.T, fake *clock.Fake, n int, within time.Duration, what string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	due := make(chan struct{})
	go func() {
		defer close(due)
		_ = fake.WaitDue(ctx, n, within)
	}()
	testwait.Done(t, what, due, func() string {
		return fmt.Sprintf("fake waiters=%d due within %v=%d", fake.Waiters(), within, fake.Due(within))
	})
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
		Set:    map[string]config.Secret{"X-Env": config.NewSecret("example")},
		Add:    map[string]config.Secret{"X-Gateway": config.NewSecret("microgateway")},
	})
	if got := h.Values("X-Debug"); len(got) != 0 {
		t.Errorf("X-Debug = %v, want removed", got)
	}
	if got := h.Values("X-Env"); !slices.Equal(got, []string{"example"}) {
		t.Errorf("X-Env = %v, want [example]", got)
	}
	if got := h.Values("X-Gateway"); !slices.Equal(got, []string{"edge", "microgateway"}) {
		t.Errorf("X-Gateway = %v, want [edge microgateway]", got)
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
			fake := clock.NewFake(time.Unix(0, 0))
			up := newUpstream(t, backend.URL, fake)
			gw := httptest.NewServer(New(RouteSpec{PreserveHost: tt.preserve}, up, Deps{Clock: fake}))
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

			got, _ := testwait.Recv(t, "backend request", hosts, nil)
			if got != want {
				t.Errorf("backend Host = %q, want %q", got, want)
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

	fake := clock.NewFake(time.Unix(0, 0))
	up := newUpstream(t, backend.URL, fake)
	gw := httptest.NewServer(New(RouteSpec{}, up, Deps{Clock: fake}))
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
	eventually(t, "in-flight count back to 0", func() bool { return up.Hosts()[0].InFlight() == 0 })
}

func TestInFlightNormal(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer backend.Close()

	fake := clock.NewFake(time.Unix(0, 0))
	up := newUpstream(t, backend.URL, fake)
	gw := httptest.NewServer(New(RouteSpec{}, up, Deps{Clock: fake}))
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
	eventually(t, "in-flight count back to 0", func() bool { return up.Hosts()[0].InFlight() == 0 })
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

func runAttempt(t *testing.T, ctx context.Context, clk clock.Clock, attempts *metrics.CounterVec, rawURL, outcome string) {
	t.Helper()
	up := newUpstream(t, rawURL, clk)
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	New(RouteSpec{}, up, Deps{Attempts: attempts, Clock: clk}).ServeHTTP(httptest.NewRecorder(), req)
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
		runAttempt(t, context.Background(), clock.NewFake(time.Unix(0, 0)), attempts, ok.URL, "success")
	})

	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(unavailable.Close)
	t.Run("status_5xx_failure", func(t *testing.T) {
		runAttempt(t, context.Background(), clock.NewFake(time.Unix(0, 0)), attempts, unavailable.URL, "failure")
	})

	notFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(notFound.Close)
	t.Run("status_404_ignored", func(t *testing.T) {
		runAttempt(t, context.Background(), clock.NewFake(time.Unix(0, 0)), attempts, notFound.URL, "ignored")
	})

	closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	closedURL := closed.URL
	closed.Close()
	t.Run("error", func(t *testing.T) {
		runAttempt(t, context.Background(), clock.NewFake(time.Unix(0, 0)), attempts, closedURL, "failure")
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
		runAttempt(t, ctx, clock.NewFake(time.Unix(0, 0)), attempts, backend, "ignored")
	})

	t.Run("timeout", func(t *testing.T) {
		fake := clock.NewFake(time.Unix(0, 0))
		started := make(chan struct{}, 1)
		backend := blockUntilDone(t, started)
		ctx, cancel := clock.WithTimeout(context.Background(), fake, 50*time.Millisecond)
		defer cancel()
		go func() {
			select {
			case <-started:
				fake.Advance(50 * time.Millisecond)
			case <-ctx.Done():
			}
		}()
		runAttempt(t, ctx, fake, attempts, backend, "failure")
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
		Hosts:         []config.Host{{URL: config.NewSecretURL("http://127.0.0.1:9001")}},
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
	blockUntil(t, fake, 1, "response header timer")
	fake.Advance(time.Hour)

	got, _ := testwait.Recv(t, "RoundTrip after the response header timeout", errc, nil)
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
		Hosts:    []config.Host{{URL: config.NewSecretURL(backend.URL)}},
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

func intP(v int) *int { return &v }

func floatP(v float64) *float64 { return &v }

func cbConfig(consecutive int) *config.CircuitBreaker {
	return &config.CircuitBreaker{
		Window:              config.Duration(10 * time.Second),
		Buckets:             intP(10),
		MinRequests:         intP(100),
		FailureRatio:        floatP(1),
		ConsecutiveFailures: intP(consecutive),
		OpenTimeout:         config.Duration(30 * time.Second),
		HalfOpenProbes:      intP(1),
		SuccessThreshold:    intP(1),
	}
}

func upstreamMetrics(t *testing.T) *upstream.Metrics {
	t.Helper()
	reg := metrics.NewRegistry()
	var (
		m   upstream.Metrics
		err error
	)
	if m.Attempts, err = reg.Counter("t_upstream_attempts_total", "h", "upstream", "host", "outcome"); err != nil {
		t.Fatal(err)
	}
	if m.Ejections, err = reg.Counter("t_upstream_ejections_total", "h", "upstream", "host"); err != nil {
		t.Fatal(err)
	}
	if m.EjectionsRefused, err = reg.Counter("t_upstream_ejections_refused_total", "h", "upstream"); err != nil {
		t.Fatal(err)
	}
	if m.DNSRefresh, err = reg.Counter("t_upstream_dns_refresh_total", "h", "upstream", "result"); err != nil {
		t.Fatal(err)
	}
	if m.ActiveChecks, err = reg.Counter("t_upstream_active_checks_total", "h", "upstream", "host", "result"); err != nil {
		t.Fatal(err)
	}
	if m.HostHealthy, err = reg.Gauge("t_upstream_host_healthy", "h", "upstream", "host"); err != nil {
		t.Fatal(err)
	}
	if m.PanicMode, err = reg.Gauge("t_upstream_panic_mode", "h", "upstream"); err != nil {
		t.Fatal(err)
	}
	if m.BreakerState, err = reg.Gauge("t_circuit_breaker_state", "h", "upstream", "host"); err != nil {
		t.Fatal(err)
	}
	if m.BreakerTransitions, err = reg.Counter("t_circuit_breaker_transitions_total", "h", "upstream", "host", "from", "to"); err != nil {
		t.Fatal(err)
	}
	if m.BreakerRejected, err = reg.Counter("t_circuit_breaker_rejected_total", "h", "upstream", "host"); err != nil {
		t.Fatal(err)
	}
	return &m
}

func assertBreakerSnapshot(t *testing.T, b *breaker.Breaker, wantRequests, wantFailures int64) {
	t.Helper()
	s := b.Snapshot()
	if s.Requests != wantRequests || s.Failures != wantFailures {
		t.Errorf("breaker requests/failures = %d/%d, want %d/%d", s.Requests, s.Failures, wantRequests, wantFailures)
	}
	if s.InFlight != 0 {
		t.Errorf("breaker InFlight = %d, want 0", s.InFlight)
	}
}

func TestBreakerStageRejects(t *testing.T) {
	um := upstreamMetrics(t)
	up := newUpstreamWith(t, config.Upstream{
		Name:           "u",
		Hosts:          []config.Host{{URL: config.NewSecretURL("http://127.0.0.1:9001")}},
		CircuitBreaker: cbConfig(1),
	}, upstream.Deps{Clock: clock.NewFake(time.Unix(0, 0)), Metrics: um})

	tok, err := up.AllowAggregate()
	if err != nil {
		t.Fatal(err)
	}
	tok.Done(breaker.Failure)
	if got := up.Breaker.State(); got != breaker.Open {
		t.Fatalf("state = %v, want open", got)
	}

	called := false
	h := BreakerStage(up)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(reqctx.WithRequestID(context.Background(), "rid-1"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if called {
		t.Error("next handler was called while the circuit is open")
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body struct {
		Error struct {
			Code      string `json:"code"`
			RequestID string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != httperr.CodeCircuitOpen {
		t.Errorf("code = %q, want %q", body.Error.Code, httperr.CodeCircuitOpen)
	}
	if body.Error.RequestID != "rid-1" {
		t.Errorf("request_id = %q, want rid-1", body.Error.RequestID)
	}
	if got := um.BreakerRejected.With("u", "").Value(); got != 1 {
		t.Errorf("rejected = %v, want 1", got)
	}
}

func TestBreakerStageOutcomes(t *testing.T) {
	tests := []struct {
		name         string
		status       int
		write        bool
		markOpen     bool
		canceled     bool
		wantRequests int64
		wantFailures int64
	}{
		{name: "200 success", status: http.StatusOK, wantRequests: 1},
		{name: "302 success", status: http.StatusFound, wantRequests: 1},
		{name: "404 ignored", status: http.StatusNotFound},
		{name: "499 ignored", status: 499},
		{name: "500 failure", status: http.StatusInternalServerError, wantRequests: 1, wantFailures: 1},
		{name: "502 failure", status: http.StatusBadGateway, wantRequests: 1, wantFailures: 1},
		{name: "503 failure", status: http.StatusServiceUnavailable, wantRequests: 1, wantFailures: 1},
		{name: "504 failure", status: http.StatusGatewayTimeout, wantRequests: 1, wantFailures: 1},
		{name: "marked circuit open 503 ignored", status: http.StatusServiceUnavailable, markOpen: true},
		{name: "write without status success", write: true, wantRequests: 1},
		{name: "canceled without write ignored", canceled: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			up := newUpstreamWith(t, config.Upstream{
				Name:           "u",
				Hosts:          []config.Host{{URL: config.NewSecretURL("http://127.0.0.1:9001")}},
				CircuitBreaker: cbConfig(0),
			}, upstream.Deps{Clock: clock.NewFake(time.Unix(0, 0))})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.markOpen {
					markCircuitOpen(r.Context())
				}
				if tt.canceled {
					cancel()
				}
				if tt.status != 0 {
					w.WriteHeader(tt.status)
				}
				if tt.write {
					_, _ = w.Write([]byte("ok"))
				}
			})
			req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
			BreakerStage(up)(next).ServeHTTP(httptest.NewRecorder(), req)

			assertBreakerSnapshot(t, up.Breaker, tt.wantRequests, tt.wantFailures)
		})
	}

	t.Run("hijack success", func(t *testing.T) {
		up := newUpstreamWith(t, config.Upstream{
			Name:           "u",
			Hosts:          []config.Host{{URL: config.NewSecretURL("http://127.0.0.1:9001")}},
			CircuitBreaker: cbConfig(0),
		}, upstream.Deps{Clock: clock.NewFake(time.Unix(0, 0))})
		srv := httptest.NewServer(BreakerStage(up)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
		})))
		t.Cleanup(srv.Close)
		conn, err := net.Dial("tcp", srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: gateway.test\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(conn)
		assertBreakerSnapshot(t, up.Breaker, 1, 0)
	})

	t.Run("panic re-panics and is ignored", func(t *testing.T) {
		up := newUpstreamWith(t, config.Upstream{
			Name:           "u",
			Hosts:          []config.Host{{URL: config.NewSecretURL("http://127.0.0.1:9001")}},
			CircuitBreaker: cbConfig(0),
		}, upstream.Deps{Clock: clock.NewFake(time.Unix(0, 0))})
		h := BreakerStage(up)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			panic("boom")
		}))
		func() {
			defer func() {
				if got := recover(); got != "boom" {
					t.Errorf("recover = %v, want boom", got)
				}
			}()
			h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
		}()
		assertBreakerSnapshot(t, up.Breaker, 0, 0)
	})
}

func TestAttemptTokenCompleted(t *testing.T) {
	refused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	refusedURL := refused.URL
	refused.Close()

	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(ok.Close)

	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(unavailable.Close)

	upgrade := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	t.Cleanup(upgrade.Close)

	tests := []struct {
		name          string
		url           string
		upgrade       bool
		canceled      bool
		headerTimeout bool
		wantRequests  int64
		wantFailures  int64
	}{
		{name: "dial refused", url: refusedURL, wantRequests: 1, wantFailures: 1},
		{name: "response header timeout", url: "http://127.0.0.1:9001", headerTimeout: true, wantRequests: 1, wantFailures: 1},
		{name: "client canceled", url: ok.URL, canceled: true},
		{name: "200", url: ok.URL, wantRequests: 1},
		{name: "503", url: unavailable.URL, wantRequests: 1, wantFailures: 1},
		{name: "101 upgrade", url: upgrade.URL, upgrade: true, wantRequests: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := clock.NewFake(time.Unix(0, 0))
			cfg := config.Upstream{
				Name:           "u",
				Hosts:          []config.Host{{URL: config.NewSecretURL(tt.url)}},
				CircuitBreaker: cbConfig(0),
			}
			deps := upstream.Deps{Clock: fake}
			if tt.headerTimeout {
				cfg.Timeouts = config.Timeouts{ResponseHeader: config.Duration(time.Hour)}
				deps.Dial = silentDial(t)
			}
			up := newUpstreamWith(t, cfg, deps)
			tr := &AttemptTransport{Upstream: up, Clock: fake, HostBreakers: true}

			req, err := http.NewRequest(http.MethodGet, "http://upstream.invalid/", nil)
			if err != nil {
				t.Fatal(err)
			}
			if tt.upgrade {
				req.Header.Set("Connection", "Upgrade")
				req.Header.Set("Upgrade", "test")
			}
			if tt.canceled {
				ctx, cancel := context.WithCancel(req.Context())
				cancel()
				req = req.WithContext(ctx)
			}

			type result struct {
				resp *http.Response
				err  error
			}
			ch := make(chan result, 1)
			go func() {
				resp, err := tr.RoundTrip(req)
				ch <- result{resp, err}
			}()
			if tt.headerTimeout {
				awaitDue(t, fake, 1, time.Hour, "response header timer")
				fake.Advance(time.Hour)
			}
			got, _ := testwait.Recv(t, "RoundTrip to return", ch, nil)
			if got.resp != nil && got.resp.Body != nil {
				_ = got.resp.Body.Close()
			}

			assertBreakerSnapshot(t, up.Hosts()[0].Breaker, tt.wantRequests, tt.wantFailures)
		})
	}
}

func TestDepsFields(t *testing.T) {
	typ := reflect.TypeOf(Deps{})
	var names []string
	for i := range typ.NumField() {
		names = append(names, typ.Field(i).Name)
	}
	if want := []string{"Logger", "Attempts", "Clock", "Rand", "HostBreakers"}; !slices.Equal(names, want) {
		t.Errorf("Deps fields = %v, want %v", names, want)
	}
	stage := reflect.TypeOf(BreakerStage)
	if stage.NumIn() != 1 || stage.In(0) != reflect.TypeOf((*upstream.Upstream)(nil)) {
		t.Errorf("BreakerStage signature = %v, want a single *upstream.Upstream input", stage)
	}
}

type deadlineOnlyCtx struct {
	context.Context
	deadline time.Time
}

func (c deadlineOnlyCtx) Deadline() (time.Time, bool) { return c.deadline, true }

func TestHeaderTimerAtRequestDeadline(t *testing.T) {
	fake := clock.NewFake(time.Unix(0, 0))
	var dials atomic.Int32
	dial := silentDial(t)
	counting := func(ctx context.Context, network, addr string) (net.Conn, error) {
		dials.Add(1)
		return dial(ctx, network, addr)
	}
	up := newUpstreamWith(t, config.Upstream{
		Name: "u",
		Hosts: []config.Host{
			{URL: config.NewSecretURL("http://127.0.0.1:9001")},
			{URL: config.NewSecretURL("http://127.0.0.1:9002")},
		},
		Timeouts: config.Timeouts{Connect: config.Duration(time.Hour), ResponseHeader: config.Duration(time.Hour)},
	}, upstream.Deps{Dial: counting, Clock: fake})
	reg := metrics.NewRegistry()
	attempts, err := reg.Counter("t_attempts", "h", "upstream", "host", "outcome")
	if err != nil {
		t.Fatal(err)
	}
	tr := &AttemptTransport{Upstream: up, Attempts: attempts, Clock: fake}
	rr := &retry.Request{
		Policy:     &retry.Policy{Attempts: 2, Statuses: map[int]bool{504: true}, Methods: map[string]bool{"GET": true}},
		Budget:     retry.NewBudget(1, 1000, 10*time.Second, fake),
		Replayable: true,
	}
	ctx := retry.WithRequest(deadlineOnlyCtx{context.Background(), fake.Now().Add(time.Hour)}, rr)
	written := make(chan struct{}, 1)
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) {
		select {
		case written <- struct{}{}:
		default:
		}
	}})
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://upstream.invalid/", nil)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		resp *http.Response
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := tr.RoundTrip(req)
		ch <- result{resp, err}
	}()

	testwait.Done(t, "request written", written, nil)
	if w, d := fake.Waiters(), fake.Due(time.Hour); w != 1 || d != 1 {
		t.Fatalf("request written: fake waiters=%d due within 1h=%d, want only the response-header timer (connect timer stopped)", w, d)
	}
	fake.Advance(time.Hour)

	got, _ := testwait.Recv(t, "RoundTrip after the request deadline", ch, nil)
	if got.resp != nil && got.resp.Body != nil {
		_ = got.resp.Body.Close()
	}
	if !errors.Is(got.err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", got.err)
	}
	if errors.Is(got.err, ErrResponseHeaderTimeout) {
		t.Errorf("err = %v, want the request deadline rather than ErrResponseHeaderTimeout", got.err)
	}
	if status, _ := httperr.FromError(got.err); status != http.StatusGatewayTimeout {
		t.Errorf("status = %d, want 504", status)
	}
	if n := dials.Load(); n != 1 {
		t.Errorf("dials = %d, want 1", n)
	}
	var failures float64
	for _, h := range up.Hosts() {
		failures += attempts.With("u", h.URL.Host, "failure").Value()
	}
	if failures != 1 {
		t.Errorf("failure attempts = %v, want 1", failures)
	}
	for _, h := range up.Hosts() {
		if n := h.InFlight(); n != 0 {
			t.Errorf("InFlight(%s) = %d, want 0", h.URL.Host, n)
		}
	}
}
