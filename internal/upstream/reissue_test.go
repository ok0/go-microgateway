package upstream_test

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/proxy"
	"github.com/ok0/go-microgateway/internal/retry"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/internal/upstream"
)

func reissueBackend(t *testing.T, h2 bool, h http.Handler) (string, *tls.Config) {
	t.Helper()
	if !h2 {
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)
		return srv.URL, nil
	}
	srv := httptest.NewUnstartedServer(h)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	return srv.URL, &tls.Config{RootCAs: pool}
}

func intPtr(v int) *int { return &v }

func floatPtr(v float64) *float64 { return &v }

func TestStaleHandoutNotARetry(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		name := "http1"
		if h2 {
			name = "h2"
		}
		t.Run(name, func(t *testing.T) {
			var count atomic.Int32
			backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				_, _ = io.WriteString(w, "ok")
			})
			base, tlsCfg := reissueBackend(t, h2, backend)

			fake := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			reg := state.NewRegistry(nil)
			t.Cleanup(reg.Close)
			tx := reg.Begin()
			cfg := config.Upstream{
				Name:  "u",
				Hosts: []config.Host{{URL: config.NewSecretURL(base)}},
				HTTP2: h2,
				Timeouts: config.Timeouts{
					Connect:        config.Duration(time.Hour),
					TLSHandshake:   config.Duration(time.Hour),
					ResponseHeader: config.Duration(time.Hour),
					Idle:           config.Duration(time.Minute),
				},
				CircuitBreaker: &config.CircuitBreaker{
					Window:              config.Duration(time.Hour),
					Buckets:             intPtr(10),
					MinRequests:         intPtr(100),
					FailureRatio:        floatPtr(1),
					ConsecutiveFailures: intPtr(0),
					OpenTimeout:         config.Duration(30 * time.Second),
					HalfOpenProbes:      intPtr(1),
					SuccessThreshold:    intPtr(1),
				},
			}
			u, err := upstream.Build(cfg, upstream.Deps{State: tx, Clock: fake})
			if err != nil {
				tx.Abort()
				t.Fatal(err)
			}
			lease := tx.Commit()
			t.Cleanup(func() { _ = lease.Release() })
			if tlsCfg != nil {
				upstream.SetClientTLS(u, tlsCfg)
			}
			u.Activate()

			mreg := metrics.NewRegistry()
			attempts, err := mreg.Counter("t_attempts", "h", "upstream", "host", "outcome")
			if err != nil {
				t.Fatal(err)
			}
			tr := &proxy.AttemptTransport{Upstream: u, Attempts: attempts, Clock: fake, HostBreakers: true}
			policy := &retry.Policy{
				Attempts: 3,
				Methods:  map[string]bool{"GET": true, "POST": true},
				Statuses: map[int]bool{502: true, 503: true, 504: true},
				MaxBody:  1 << 20,
			}
			budget := retry.NewBudget(0, 1, time.Second, fake)
			var retries, exhausted atomic.Int32
			hooks := retry.Hooks{
				OnRetry:     func() { retries.Add(1) },
				OnExhausted: func() { exhausted.Add(1) },
			}
			target, err := url.Parse("http://upstream.invalid/")
			if err != nil {
				t.Fatal(err)
			}
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				out := r.Clone(r.Context())
				out.RequestURI = ""
				out.URL = target
				resp, err := tr.RoundTrip(out)
				if err != nil {
					w.WriteHeader(http.StatusBadGateway)
					return
				}
				defer func() { _ = resp.Body.Close() }()
				w.WriteHeader(resp.StatusCode)
				_, _ = io.Copy(w, resp.Body)
			})
			h := retry.Middleware(policy, budget, hooks)(handler)

			settled := make(chan struct{}, 1)
			rec1 := httptest.NewRecorder()
			req1 := httptest.NewRequest("GET", "/", nil)
			if !h2 {
				req1 = req1.WithContext(httptrace.WithClientTrace(req1.Context(), &httptrace.ClientTrace{
					PutIdleConn: func(err error) {
						if err == nil {
							select {
							case settled <- struct{}{}:
							default:
							}
						}
					},
				}))
			}
			h.ServeHTTP(rec1, req1)
			if !h2 {
				testwait.Done(t, "request 1 returned to idle pool", settled, nil)
			}

			var armed atomic.Bool
			armed.Store(true)
			upstream.SetHandoutHook(u, func() {
				if armed.CompareAndSwap(true, false) {
					fake.Advance(time.Minute)
				}
			})

			rec2 := httptest.NewRecorder()
			h.ServeHTTP(rec2, httptest.NewRequest("POST", "/", strings.NewReader("payload")))

			if rec1.Code != http.StatusOK || rec2.Code != http.StatusOK {
				t.Fatalf("status = %d and %d, want 200 and 200", rec1.Code, rec2.Code)
			}
			if n := count.Load(); n != 2 {
				t.Fatalf("backend requests = %d, want 2", n)
			}
			host := u.Hosts()[0].URL.Host
			if got := attempts.With("u", host, "success").Value(); got != 2 {
				t.Fatalf("attempts success = %v, want 2", got)
			}
			if got := attempts.With("u", host, "failure").Value(); got != 0 {
				t.Fatalf("attempts failure = %v, want 0", got)
			}
			if got := attempts.With("u", host, "ignored").Value(); got != 0 {
				t.Fatalf("attempts ignored = %v, want 0", got)
			}
			if n := retries.Load(); n != 0 {
				t.Fatalf("retries = %d, want 0", n)
			}
			if n := exhausted.Load(); n != 0 {
				t.Fatalf("exhausted = %d, want 0", n)
			}
			if _, ok := budget.Withdraw(); !ok {
				t.Fatal("first Withdraw refused, want the single token available")
			}
			if _, ok := budget.Withdraw(); ok {
				t.Fatal("second Withdraw allowed, want refused")
			}
			st := u.Status()
			if len(st.Hosts) != 1 || st.Hosts[0].Breaker == nil {
				t.Fatalf("hosts = %+v, want one host with a breaker", st.Hosts)
			}
			if got := st.Hosts[0].Breaker.Requests; got != 2 {
				t.Fatalf("breaker requests = %d, want 2", got)
			}
			if got := st.Hosts[0].Breaker.Failures; got != 0 {
				t.Fatalf("breaker failures = %d, want 0", got)
			}
		})
	}
}
