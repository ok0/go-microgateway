package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/admin"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

type probeSrc struct {
	ready, metrics atomic.Bool
	reg            *metrics.Registry
}

func newProbeSrc() *probeSrc {
	p := &probeSrc{reg: metrics.NewRegistry()}
	p.ready.Store(true)
	return p
}

func (p *probeSrc) Ready() bool                { return p.ready.Load() }
func (p *probeSrc) ProbeMetrics() bool         { return p.metrics.Load() }
func (p *probeSrc) Metrics() *metrics.Registry { return p.reg }

var _ admin.ProbeSource = (*probeSrc)(nil)

func probeBody(s string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, s)
	})
}

func probeConfig(t *testing.T, mutate func(*config.Config)) *config.Config {
	t.Helper()
	return testConfig(t, func(c *config.Config) {
		c.Admin.Probes.Address = "127.0.0.1:0"
		if mutate != nil {
			mutate(c)
		}
	})
}

func startProbeServer(t *testing.T, cfg *config.Config, h, adminH, probeH http.Handler, ready func(bool), opts ...Option) *Server {
	t.Helper()
	srv, err := New(cfg, h, adminH, probeH, ready, nil, append([]Option{WithClock(clock.NewFake(serverStart))}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	cleanupShutdown(t, srv)
	return srv
}

func fetchStatus(t *testing.T, client *http.Client, url string) (int, string) {
	t.Helper()
	resp, body, err := fetch(client, url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp.StatusCode, body
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	return fetchStatus(t, &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}, url)
}

func TestProbeListenerServes(t *testing.T) {
	src := newProbeSrc()
	dataH := probeBody("data")
	adminH := probeBody("admin")
	srv := startProbeServer(t, probeConfig(t, nil), dataH, adminH, admin.ProbeHandler(src), nil)

	probeAddr := srv.ProbeAddr()
	if probeAddr == nil {
		t.Fatal("ProbeAddr returned nil with probes enabled")
	}
	if srv.AdminAddr() == nil || probeAddr.String() == srv.AdminAddr().String() {
		t.Fatalf("probe address %v must differ from admin address %v", probeAddr, srv.AdminAddr())
	}
	dataAddr := srv.Addrs()["public"].String()
	if probeAddr.String() == dataAddr {
		t.Fatalf("probe address %v equals data address", probeAddr)
	}
	base := "http://" + probeAddr.String()

	if st, _ := get(t, base+"/livez"); st != http.StatusOK {
		t.Fatalf("/livez status = %d, want 200", st)
	}
	if st, body := get(t, base+"/readyz"); st != http.StatusOK || !strings.Contains(body, `"ready"`) {
		t.Fatalf("/readyz status = %d, body = %q; want 200 with ready", st, body)
	}
	if st, _ := get(t, base+"/metrics"); st != http.StatusNotFound {
		t.Fatalf("/metrics status = %d, want 404 while probes.metrics is false", st)
	}
	src.metrics.Store(true)
	if st, _ := get(t, base+"/metrics"); st != http.StatusOK {
		t.Fatalf("/metrics status = %d, want 200 after probes.metrics is true", st)
	}
	for _, p := range []string{"/config", "/upstreams", "/cache/purge", "/"} {
		if st, _ := get(t, base+p); st != http.StatusNotFound {
			t.Fatalf("%s status = %d, want 404", p, st)
		}
	}
	req, err := http.NewRequest(http.MethodPost, base+"/readyz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Transport: &http.Transport{DisableKeepAlives: true}}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /readyz status = %d, want 405", resp.StatusCode)
	}

	if _, body := get(t, "http://"+dataAddr+"/livez"); body != "data" {
		t.Fatalf("data listener body = %q, want %q", body, "data")
	}
	if _, body := get(t, "http://"+srv.AdminAddr().String()+"/livez"); body != "admin" {
		t.Fatalf("admin listener body = %q, want %q", body, "admin")
	}

	requireHTTP2Off(t, srv.probe.srv)
	if want := (connLimits{header: 10 * time.Second, idle: 120 * time.Second}); srv.probe.lim != want {
		t.Errorf("probe limits = %+v, want %+v", srv.probe.lim, want)
	}
	if srv.probe.srv.ReadHeaderTimeout != 10*time.Second {
		t.Errorf("ReadHeaderTimeout = %v, want 10s", srv.probe.srv.ReadHeaderTimeout)
	}
	if srv.probe.srv.IdleTimeout != 120*time.Second {
		t.Errorf("IdleTimeout = %v, want 120s", srv.probe.srv.IdleTimeout)
	}
	if srv.probe.srv.TLSConfig != nil {
		t.Error("probe listener has a TLS config, want plain HTTP")
	}

	t.Run("tls data listener", func(t *testing.T) {
		certFile, keyFile, _ := testcert.Write(t, t.TempDir())
		cfg := probeConfig(t, func(c *config.Config) {
			c.Listeners[0].TLS = &config.TLS{CertFile: certFile, KeyFile: keyFile}
		})
		tlsSrv := startProbeServer(t, cfg, dataH, nil, admin.ProbeHandler(newProbeSrc()), nil)
		if st, _ := get(t, "http://"+tlsSrv.ProbeAddr().String()+"/livez"); st != http.StatusOK {
			t.Fatalf("plain http /livez on probe listener status = %d, want 200", st)
		}
	})
}

func TestProbeListenerAdminDisabled(t *testing.T) {
	off := false
	cfg := probeConfig(t, func(c *config.Config) {
		c.Admin.Enabled = &off
	})
	srv := startProbeServer(t, cfg, probeBody("data"), probeBody("admin"), admin.ProbeHandler(newProbeSrc()), nil)
	if srv.AdminAddr() != nil {
		t.Fatal("AdminAddr non-nil with admin disabled")
	}
	if srv.ProbeAddr() == nil {
		t.Fatal("ProbeAddr nil with admin disabled and probes enabled")
	}
	if st, _ := get(t, "http://"+srv.ProbeAddr().String()+"/readyz"); st != http.StatusOK {
		t.Fatalf("/readyz status = %d, want 200", st)
	}
}

func TestProbeListenerDisabled(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })

	t.Run("disabled", func(t *testing.T) {
		off := false
		cfg := probeConfig(t, func(c *config.Config) {
			c.Admin.Probes.Enabled = &off
			c.Admin.Probes.Address = held.Addr().String()
		})
		srv := startProbeServer(t, cfg, probeBody("data"), nil, admin.ProbeHandler(newProbeSrc()), nil)
		if srv.ProbeAddr() != nil {
			t.Fatal("ProbeAddr non-nil with probes disabled")
		}
	})

	t.Run("nil handler", func(t *testing.T) {
		cfg := probeConfig(t, func(c *config.Config) {
			c.Admin.Probes.Address = held.Addr().String()
		})
		srv := startProbeServer(t, cfg, probeBody("data"), nil, nil, nil)
		if srv.ProbeAddr() != nil {
			t.Fatal("ProbeAddr non-nil with a nil probe handler")
		}
	})
}

func TestProbeBindError(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	data := freePort(t)
	adm := freePort(t)
	cfg := probeConfig(t, func(c *config.Config) {
		c.Listeners = []config.Listener{{Name: "public", Address: data}}
		c.Admin.Address = adm
		c.Admin.Probes.Address = held.Addr().String()
	})
	srv, err := New(cfg, probeBody("data"), probeBody("admin"), admin.ProbeHandler(newProbeSrc()), nil, nil, WithClock(clock.NewFake(serverStart)))
	if err != nil {
		t.Fatal(err)
	}
	err = srv.Start()
	if err == nil {
		t.Fatal("Start returned nil error, want probe bind error")
	}
	if !strings.Contains(err.Error(), `listener "probe"`) || !strings.Contains(err.Error(), held.Addr().String()) {
		t.Fatalf("error = %v, want listener \"probe\" and address %s", err, held.Addr())
	}
	for _, a := range []string{data, adm} {
		ln, err := net.Listen("tcp", a)
		if err != nil {
			t.Fatalf("address %s not released after failed Start: %v", a, err)
		}
		_ = ln.Close()
	}
	conn, err := net.Dial("tcp", held.Addr().String())
	if err != nil {
		t.Fatalf("held listener closed: %v", err)
	}
	_ = conn.Close()
}

func TestProbeOutlivesDrain(t *testing.T) {
	cfg := probeConfig(t, func(c *config.Config) {
		c.Global.ShutdownDelay = durPtr(300 * time.Millisecond)
		c.Global.DrainTimeout = config.Duration(5 * time.Second)
	})
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			entered <- struct{}{}
			<-release
			_, _ = io.WriteString(w, "ok")
			return
		}
		_, _ = io.WriteString(w, "fast")
	})
	src := newProbeSrc()
	readyCh := make(chan bool, 4)
	fake := clock.NewFake(serverStart)
	srv := startProbeServer(t, cfg, h, nil, admin.ProbeHandler(src), func(v bool) {
		src.ready.Store(v)
		readyCh <- v
	}, WithClock(fake))
	dataAddr := srv.Addrs()["public"].String()
	probeAddr := srv.ProbeAddr().String()
	probeBase := "http://" + probeAddr

	keep := newClient(t, &http.Transport{})
	if st, _ := fetchStatus(t, keep, probeBase+"/livez"); st != http.StatusOK {
		t.Fatalf("/livez before shutdown status = %d, want 200", st)
	}

	type result struct {
		status int
		err    error
	}
	slowRes := make(chan result, 1)
	go func() {
		resp, _, err := fetch(&http.Client{Transport: &http.Transport{}}, "http://"+dataAddr+"/slow")
		if err != nil {
			slowRes <- result{err: err}
			return
		}
		slowRes <- result{status: resp.StatusCode}
	}()
	waitSignal(t, entered, "slow handler")

	shutdownErr := shutdownAsync(srv, context.Background())
	if v, _ := testwait.Recv(t, "ready(false)", readyCh, nil); v {
		t.Fatal("ready(true) reported during shutdown")
	}
	awaitDue(t, fake, 1, 300*time.Millisecond, "shutdown delay timer")

	if st, body := fetchStatus(t, keep, probeBase+"/readyz"); st != http.StatusServiceUnavailable || !strings.Contains(body, "not_ready") {
		t.Fatalf("/readyz during delay via keep-alive: status = %d, body = %q; want 503 not_ready", st, body)
	}
	if st, _ := fetchStatus(t, keep, probeBase+"/livez"); st != http.StatusOK {
		t.Fatalf("/livez during delay via keep-alive: status = %d, want 200", st)
	}
	if st, body := get(t, probeBase+"/readyz"); st != http.StatusServiceUnavailable || !strings.Contains(body, "not_ready") {
		t.Fatalf("/readyz during delay: status = %d, body = %q; want 503 not_ready", st, body)
	}
	if st, _ := get(t, probeBase+"/livez"); st != http.StatusOK {
		t.Fatalf("/livez during delay: status = %d, want 200", st)
	}

	fake.Advance(300 * time.Millisecond)
	awaitDue(t, fake, 1, 5*time.Second, "drain timer")
	testwait.Until(t, "data listener to stop accepting", func() bool {
		c, err := net.DialTimeout("tcp", dataAddr, time.Second)
		if err != nil {
			return true
		}
		_ = c.Close()
		return false
	}, nil)

	if st, body := get(t, probeBase+"/readyz"); st != http.StatusServiceUnavailable || !strings.Contains(body, "not_ready") {
		t.Fatalf("/readyz during drain: status = %d, body = %q; want 503 not_ready", st, body)
	}
	if st, _ := get(t, probeBase+"/livez"); st != http.StatusOK {
		t.Fatalf("/livez during drain: status = %d, want 200", st)
	}
	if st, _ := fetchStatus(t, keep, probeBase+"/livez"); st != http.StatusOK {
		t.Fatalf("/livez during drain via keep-alive: status = %d, want 200", st)
	}

	close(release)
	res, _ := testwait.Recv(t, "in-flight request to complete", slowRes, nil)
	if res.err != nil {
		t.Fatalf("in-flight request: %v", res.err)
	}
	if res.status != http.StatusOK {
		t.Fatalf("in-flight request status = %d, want 200", res.status)
	}

	err, _ := testwait.Recv(t, "Shutdown to return", shutdownErr, nil)
	if err != nil {
		t.Fatalf("Shutdown: %v, want nil (the idle probe connection must not extend the drain)", err)
	}

	if c, err := net.Dial("tcp", probeAddr); err == nil {
		_ = c.Close()
		t.Fatal("probe listener still accepts connections after Shutdown")
	}
}

func TestProbeClosedAfterForcedShutdown(t *testing.T) {
	cfg := probeConfig(t, func(c *config.Config) {
		c.Global.ShutdownDelay = durPtr(0)
		c.Global.DrainTimeout = config.Duration(time.Second)
	})
	entered := make(chan struct{}, 1)
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-block
	})
	fake := clock.NewFake(serverStart)
	src := newProbeSrc()
	srv := startProbeServer(t, cfg, h, nil, admin.ProbeHandler(src), func(v bool) { src.ready.Store(v) }, WithClock(fake))
	dataAddr := srv.Addrs()["public"].String()
	probeAddr := srv.ProbeAddr().String()

	go func() {
		_, _, _ = fetch(&http.Client{Transport: &http.Transport{}}, "http://"+dataAddr+"/hang")
	}()
	waitSignal(t, entered, "blocked handler")

	shutdownErr := shutdownAsync(srv, context.Background())
	awaitDue(t, fake, 1, time.Second, "drain timer")
	if st, _ := get(t, "http://"+probeAddr+"/readyz"); st != http.StatusServiceUnavailable {
		t.Fatalf("/readyz during drain status = %d, want 503", st)
	}

	fake.Advance(time.Second)
	err, _ := testwait.Recv(t, "Shutdown to return", shutdownErr, nil)
	if !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown error = %v, want ErrForcedClose", err)
	}
	if c, err := net.Dial("tcp", probeAddr); err == nil {
		_ = c.Close()
		t.Fatal("probe listener still accepts connections after forced Shutdown")
	}
}

func TestProbeListenersPerInstance(t *testing.T) {
	ready := newProbeSrc()
	notReady := newProbeSrc()
	notReady.ready.Store(false)
	a := startProbeServer(t, probeConfig(t, nil), probeBody("data"), nil, admin.ProbeHandler(ready), nil)
	b := startProbeServer(t, probeConfig(t, nil), probeBody("data"), nil, admin.ProbeHandler(notReady), nil)
	if a.ProbeAddr().String() == b.ProbeAddr().String() {
		t.Fatalf("two instances share probe address %s", a.ProbeAddr())
	}
	if st, _ := get(t, "http://"+a.ProbeAddr().String()+"/readyz"); st != http.StatusOK {
		t.Fatalf("first instance /readyz status = %d, want 200", st)
	}
	if st, _ := get(t, "http://"+b.ProbeAddr().String()+"/readyz"); st != http.StatusServiceUnavailable {
		t.Fatalf("second instance /readyz status = %d, want 503", st)
	}
}
