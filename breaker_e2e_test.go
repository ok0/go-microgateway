package microgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/upstream"
)

func bkGet(t *testing.T, client *http.Client, url string, want int) []byte {
	t.Helper()
	code, _, body := rsGet(t, client, url, http.MethodGet, nil, nil)
	if code != want {
		t.Fatalf("GET %s status = %d, want %d; body %q", url, code, want, body)
	}
	return body
}

func bkCircuitOpen(t *testing.T, body []byte) {
	t.Helper()
	var e struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatal(err)
	}
	if e.Error.Status != http.StatusServiceUnavailable || e.Error.Code != "circuit_open" || !hexID.MatchString(e.Error.RequestID) {
		t.Fatalf("body = %s, want 503 circuit_open with request_id", body)
	}
}

func bkCheckBreaker(t *testing.T, scope, state string, got *upstream.BreakerStatus, want upstream.BreakerStatus) {
	t.Helper()
	if state != want.State {
		t.Fatalf("%s breaker_state = %q, want %q", scope, state, want.State)
	}
	if got == nil {
		t.Fatalf("%s breaker missing, want %+v", scope, want)
	}
	if *got != want {
		t.Fatalf("%s breaker = %+v, want %+v", scope, *got, want)
	}
}

func bkGauge(t *testing.T, g *Gateway, host string) float64 {
	t.Helper()
	return rsMetric(t, g, "microgateway_circuit_breaker_state", "upstream", "up", "host", host)
}

func bkTransitions(t *testing.T, g *Gateway, host, from, to string) float64 {
	t.Helper()
	return rsMetric(t, g, "microgateway_circuit_breaker_transitions_total", "upstream", "up", "host", host, "from", from, "to", to)
}

func bkCheckTransitions(t *testing.T, logs *captureHandler, want ...string) {
	t.Helper()
	got := rsTransitions(logs)
	slices.Sort(got)
	wantSorted := slices.Clone(want)
	slices.Sort(wantSorted)
	if !slices.Equal(got, wantSorted) {
		t.Fatalf("transitions = %q, want %q", got, wantSorted)
	}
}

func bkLogText(h *captureHandler) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var b strings.Builder
	for _, r := range h.records {
		b.WriteString(r.Message)
		for _, v := range recordAttrs(r) {
			b.WriteString(" " + v.String())
		}
	}
	return b.String()
}

func runBreakerOpenClose(t *testing.T, n testNetwork) {
	t.Helper()
	fake := clock.NewFake(time.Now())
	logs := &captureHandler{}
	var healthy atomic.Bool
	fail := rsStatusSeq(503)
	be := newRSBackend(t, n, func(seq int64, w http.ResponseWriter, r *http.Request) {
		if healthy.Load() {
			_, _ = io.WriteString(w, "ok")
			return
		}
		fail(seq, w, r)
	})
	g := n.gateway(t, rsDoc{hosts: []string{be.URL}, breaker: rsBreaker}.yaml(), withClock(fake), WithLogger(slog.New(logs)))
	ts := n.serve(t, g)
	client := n.client(t, nil)
	host := be.srv.Addr

	for range 3 {
		bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
	}
	if got := be.hitCount(); got != 3 {
		t.Fatalf("backend hits = %d, want 3", got)
	}
	for _, h := range []string{host, ""} {
		if v := bkGauge(t, g, h); v != 2 {
			t.Fatalf("state gauge host=%q = %v, want 2", h, v)
		}
		if v := bkTransitions(t, g, h, "closed", "open"); v != 1 {
			t.Fatalf("closed>open transitions host=%q = %v, want 1", h, v)
		}
	}
	bkCheckTransitions(t, logs,
		"host:"+host+":closed>open:consecutive",
		"aggregate::closed>open:consecutive",
	)

	conns := be.srv.New.Load()
	bkCircuitOpen(t, bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable))
	if got := be.hitCount(); got != 3 {
		t.Fatalf("backend hits after rejection = %d, want 3", got)
	}
	if got := be.srv.New.Load(); got != conns {
		t.Fatalf("connections grew from %d to %d on a rejected request", conns, got)
	}
	if v := rsMetric(t, g, "microgateway_circuit_breaker_rejected_total", "upstream", "up", "host", ""); v != 1 {
		t.Fatalf("rejected host=\"\" = %v, want 1", v)
	}

	fake.Advance(30 * time.Second)
	healthy.Store(true)
	for range 2 {
		if body := bkGet(t, client, ts.URL+"/", http.StatusOK); string(body) != "ok" {
			t.Fatalf("recovery body = %q, want ok", body)
		}
	}
	bkCheckTransitions(t, logs,
		"host:"+host+":closed>open:consecutive",
		"aggregate::closed>open:consecutive",
		"host:"+host+":open>half_open:timeout_elapsed",
		"aggregate::open>half_open:timeout_elapsed",
		"host:"+host+":half_open>closed:probes_succeeded",
		"aggregate::half_open>closed:probes_succeeded",
	)
	for _, h := range []string{host, ""} {
		if v := bkGauge(t, g, h); v != 0 {
			t.Fatalf("state gauge after recovery host=%q = %v, want 0", h, v)
		}
	}

	healthy.Store(false)
	for range 3 {
		bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
	}
	fake.Advance(30 * time.Second)
	bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
	bkCheckTransitions(t, logs,
		"host:"+host+":closed>open:consecutive",
		"aggregate::closed>open:consecutive",
		"host:"+host+":open>half_open:timeout_elapsed",
		"aggregate::open>half_open:timeout_elapsed",
		"host:"+host+":half_open>closed:probes_succeeded",
		"aggregate::half_open>closed:probes_succeeded",
		"host:"+host+":closed>open:consecutive",
		"aggregate::closed>open:consecutive",
		"host:"+host+":open>half_open:timeout_elapsed",
		"aggregate::open>half_open:timeout_elapsed",
		"host:"+host+":half_open>open:probe_failed",
		"aggregate::half_open>open:probe_failed",
	)

	fake.Advance(30*time.Second - time.Millisecond)
	before := be.hitCount()
	bkCircuitOpen(t, bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable))
	if got := be.hitCount(); got != before {
		t.Fatalf("backend hits = %d, want %d while open", got, before)
	}

	fake.Advance(time.Millisecond)
	body := bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
	if got := be.hitCount(); got != before+1 {
		t.Fatalf("backend hits = %d, want %d after admission", got, before+1)
	}
	if want := fmt.Sprintf("r%d", before+1); string(body) != want {
		t.Fatalf("admitted body = %q, want %q", body, want)
	}

	rsCheckExposition(t, metricsText(t, g))
}

func TestBreakerOpenClose(t *testing.T) {
	runBreakerOpenClose(t, newMemNetwork())
}

func TestBreakerIgnores4xxAndCancel(t *testing.T) {
	n := newMemNetwork()

	t.Run("client errors keep closed", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		be := newRSBackend(t, n, rsStatusSeq(404))
		g := n.gateway(t, rsDoc{hosts: []string{be.URL}, breaker: rsBreaker}.yaml(), withClock(fake))
		ts := n.serve(t, g)
		client := n.client(t, nil)
		for range 20 {
			bkGet(t, client, ts.URL+"/", http.StatusNotFound)
		}
		st := rsStatus(t, g)
		if st.BreakerState != "closed" || st.Breaker == nil || st.Breaker.State != "closed" || st.Breaker.Requests != 0 {
			t.Fatalf("aggregate breaker = %+v, want closed with 0 requests", st.Breaker)
		}
		host := rsHost(t, g, be.URL)
		if host.BreakerState != "closed" || host.Breaker == nil || host.Breaker.Requests != 0 {
			t.Fatalf("host breaker = %+v, want closed with 0 requests", host.Breaker)
		}
	})

	t.Run("canceled requests keep closed", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		arrived := make(chan struct{}, 16)
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		hold := rsHoldUntil(release)
		be := newRSBackend(t, n, func(seq int64, w http.ResponseWriter, r *http.Request) {
			select {
			case arrived <- struct{}{}:
			default:
			}
			hold(seq, w, r)
		})
		g := n.gateway(t, rsDoc{hosts: []string{be.URL}, breaker: rsBreaker}.yaml(), withClock(fake))
		ts := n.serve(t, g)
		client := n.client(t, nil)
		for range 10 {
			ctx, cancel := context.WithCancel(context.Background())
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/", nil)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				resp, err := client.Do(req)
				if resp != nil {
					_ = resp.Body.Close()
				}
				done <- err
			}()
			<-arrived
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("client error = %v, want context.Canceled", err)
			}
			eventually(t, func() bool { return rsStatus(t, g).Breaker.InFlight == 0 })
		}
		st := rsStatus(t, g)
		if st.BreakerState != "closed" {
			t.Fatalf("aggregate breaker_state = %q, want closed", st.BreakerState)
		}
		if host := rsHost(t, g, be.URL); host.BreakerState != "closed" {
			t.Fatalf("host breaker_state = %q, want closed", host.BreakerState)
		}
	})

	t.Run("client error probe keeps half open", func(t *testing.T) {
		fake := clock.NewFake(time.Now())
		be := newRSBackend(t, n, rsStatusSeq(503, 503, 503, 404))
		g := n.gateway(t, rsDoc{hosts: []string{be.URL}, breaker: rsBreaker}.yaml(), withClock(fake))
		ts := n.serve(t, g)
		client := n.client(t, nil)
		for range 3 {
			bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
		}
		if st := rsStatus(t, g); st.BreakerState != "open" {
			t.Fatalf("aggregate breaker_state = %q, want open", st.BreakerState)
		}
		fake.Advance(30 * time.Second)
		bkGet(t, client, ts.URL+"/", http.StatusNotFound)
		st := rsStatus(t, g)
		if st.BreakerState != "half_open" {
			t.Fatalf("aggregate breaker_state after 404 probe = %q, want half_open", st.BreakerState)
		}
		if host := rsHost(t, g, be.URL); host.BreakerState != "half_open" {
			t.Fatalf("host breaker_state after 404 probe = %q, want half_open", host.BreakerState)
		}
		if body := bkGet(t, client, ts.URL+"/", http.StatusNotFound); string(body) != "r5" {
			t.Fatalf("next admitted body = %q, want r5", body)
		}
		if got := be.hitCount(); got != 5 {
			t.Fatalf("backend hits = %d, want 5", got)
		}
	})
}

func TestBreakerSeriesAtStartup(t *testing.T) {
	n := newMemNetwork()
	be := newRSBackend(t, n, nil)
	fake := clock.NewFake(time.Now())
	g := n.gateway(t, rsDoc{hosts: []string{be.URL}, breaker: rsBreaker}.yaml(), withClock(fake))
	pairs := [][2]string{
		{"closed", "open"},
		{"open", "half_open"},
		{"half_open", "open"},
		{"half_open", "closed"},
	}
	for _, h := range []string{be.srv.Addr, ""} {
		if v := bkGauge(t, g, h); v != 0 {
			t.Fatalf("state gauge host=%q = %v, want 0", h, v)
		}
		for _, p := range pairs {
			if v := bkTransitions(t, g, h, p[0], p[1]); v != 0 {
				t.Fatalf("transitions %s>%s host=%q = %v, want 0", p[0], p[1], h, v)
			}
		}
		if v := rsMetric(t, g, "microgateway_circuit_breaker_rejected_total", "upstream", "up", "host", h); v != 0 {
			t.Fatalf("rejected host=%q = %v, want 0", h, v)
		}
	}
	rsCheckExposition(t, metricsText(t, g))
}

func TestBreakerGaugeLazyOnScrape(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	be := newRSBackend(t, n, rsStatusSeq(503))
	g := n.gateway(t, rsDoc{hosts: []string{be.URL}, breaker: rsBreaker}.yaml(), withClock(fake))
	ts := n.serve(t, g)
	client := n.client(t, nil)
	for range 3 {
		bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
	}
	if v := bkGauge(t, g, be.srv.Addr); v != 2 {
		t.Fatalf("state gauge before advance = %v, want 2", v)
	}
	fake.Advance(30 * time.Second)
	for _, h := range []string{be.srv.Addr, ""} {
		if v := bkGauge(t, g, h); v != 1 {
			t.Fatalf("state gauge host=%q after lazy scrape = %v, want 1", h, v)
		}
	}
}

func TestBreakerAdminStatus(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	be := newRSBackend(t, n, rsStatusSeq(503))
	doc := rsDoc{
		hosts:   []string{be.URL},
		breaker: rsBreaker,
		routes:  []rsRoute{{name: "api", path: "/api"}},
	}
	g := n.gateway(t, doc.yaml(), withClock(fake))
	ts := n.serve(t, g)
	client := n.client(t, nil)
	for range 2 {
		bkGet(t, client, ts.URL+"/api/x", http.StatusServiceUnavailable)
	}
	closed := upstream.BreakerStatus{State: "closed", Requests: 2, Failures: 2, ConsecutiveFailures: 2}
	st := rsStatus(t, g)
	bkCheckBreaker(t, "aggregate", st.BreakerState, st.Breaker, closed)
	host := rsHost(t, g, be.URL)
	bkCheckBreaker(t, "host", host.BreakerState, host.Breaker, closed)

	bkGet(t, client, ts.URL+"/api/x", http.StatusServiceUnavailable)
	open := upstream.BreakerStatus{State: "open", HalfOpenInSeconds: 30}
	st = rsStatus(t, g)
	bkCheckBreaker(t, "aggregate", st.BreakerState, st.Breaker, open)
	host = rsHost(t, g, be.URL)
	bkCheckBreaker(t, "host", host.BreakerState, host.Breaker, open)

	fake.Advance(10 * time.Second)
	open.HalfOpenInSeconds = 20
	st = rsStatus(t, g)
	bkCheckBreaker(t, "aggregate", st.BreakerState, st.Breaker, open)
	host = rsHost(t, g, be.URL)
	bkCheckBreaker(t, "host", host.BreakerState, host.Breaker, open)

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upstreams", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/upstreams through the gateway status = %d, want 404", rec.Code)
	}
	if e := decodeErr(t, rec.Result()); e.Code != "route_not_found" {
		t.Fatalf("/upstreams error code = %q, want route_not_found", e.Code)
	}
}

func TestBreakerCredentialsRedacted(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	logs := &captureHandler{}
	be := newRSBackend(t, n, rsStatusSeq(503))
	hostURL := "http://user:secret@" + be.srv.Addr + "/?token=topsecret"
	g := n.gateway(t, rsDoc{hosts: []string{hostURL}, breaker: rsBreaker}.yaml(), withClock(fake), WithLogger(slog.New(logs)))
	ts := n.serve(t, g)
	client := n.client(t, nil)
	for range 3 {
		bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
	}
	rejected := bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
	bkCircuitOpen(t, rejected)

	if text := bkLogText(logs); strings.Contains(text, "secret") || strings.Contains(text, "topsecret") {
		t.Fatalf("logs leak credentials: %s", text)
	}
	metrics := metricsText(t, g)
	if strings.Contains(metrics, "secret") || strings.Contains(metrics, "topsecret") {
		t.Fatal("metrics leak credentials")
	}
	admin := adminBody(t, g, "/upstreams")
	if strings.Contains(admin, "secret") || strings.Contains(admin, "topsecret") {
		t.Fatalf("/upstreams leaks credentials: %s", admin)
	}
	if got := rsStatus(t, g).Hosts[0].URL; got != config.RedactURL(hostURL) {
		t.Fatalf("/upstreams host url = %q, want %q", got, config.RedactURL(hostURL))
	}
	if strings.Contains(string(rejected), "secret") {
		t.Fatalf("503 body leaks credentials: %s", rejected)
	}

	bad := rsDoc{hosts: []string{hostURL}, breaker: strings.Replace(rsBreaker, "failure_ratio: 1", "failure_ratio: 2", 1)}.yaml()
	_, err := config.Parse([]byte(bad))
	if err == nil {
		t.Fatal("config with failure_ratio 2 parsed without error")
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "topsecret") {
		t.Fatalf("config error leaks credentials: %v", err)
	}
}

func runBreakerSelection(t *testing.T, n testNetwork) {
	t.Helper()
	fake := clock.NewFake(time.Now())
	a := newRSBackend(t, n, rsStatusSeq(503))
	b := newRSBackend(t, n, nil)
	c := newRSBackend(t, n, nil)
	doc := rsDoc{hosts: []string{a.URL, b.URL, c.URL}, breaker: rsBreaker}.yaml()
	weighted := strings.Replace(doc, fmt.Sprintf("      - url: %q\n", c.URL), fmt.Sprintf("      - url: %q\n        weight: 0\n", c.URL), 1)
	if weighted == doc {
		t.Fatal("weight-0 host was not added")
	}
	g := n.gateway(t, weighted, withClock(fake))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	for i := 0; rsHost(t, g, a.URL).BreakerState != "open"; i++ {
		if i == 12 {
			t.Fatal("host A breaker did not open within 12 requests")
		}
		code, _, _ := rsGet(t, client, ts.URL+"/", http.MethodGet, nil, nil)
		if code != http.StatusOK && code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 200 or 503", code)
		}
	}
	if st := rsStatus(t, g); st.BreakerState != "closed" || st.Breaker == nil || st.Breaker.Requests <= 0 {
		t.Fatalf("aggregate after host A opened = %q %+v, want closed with requests > 0", st.BreakerState, st.Breaker)
	}
	if got := c.hitCount(); got != 0 {
		t.Fatalf("weight-0 host hits = %d, want 0", got)
	}
	aHits, bHits := a.hitCount(), b.hitCount()
	for range 20 {
		bkGet(t, client, ts.URL+"/", http.StatusOK)
	}
	if got := a.hitCount(); got != aHits {
		t.Fatalf("host A hits = %d, want %d after opening", got, aHits)
	}
	if got := b.hitCount(); got != bHits+20 {
		t.Fatalf("host B hits = %d, want %d", got, bHits+20)
	}
	if got := c.hitCount(); got != 0 {
		t.Fatalf("weight-0 host hits = %d, want 0", got)
	}
	if st := rsStatus(t, g); st.BreakerState != "closed" {
		t.Fatalf("aggregate breaker_state after selection = %q, want closed", st.BreakerState)
	}

	runBreakerPanicSelection(t, n)
}

func runBreakerPanicSelection(t *testing.T, n testNetwork) {
	t.Helper()
	fake := clock.NewFake(time.Now())
	a := newRSBackend(t, n, rsStatusSeq(503))
	var bFail atomic.Bool
	b := newRSBackend(t, n, func(seq int64, w http.ResponseWriter, _ *http.Request) {
		if seq == 1 || bFail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "b-fail")
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	doc := rsDoc{
		hosts:   []string{a.URL, b.URL},
		breaker: rsBreaker,
		passive: "consecutive_failures: 1\nmax_ejection_percent: 100\nbase_ejection_time: 1h\nmax_ejection_time: 1h\n",
		routes: []rsRoute{
			{name: "all", path: "/", extra: "retry:\n      attempts: 3\n      base_delay: 1ns\n      max_delay: 1ns\n"},
		},
	}.yaml()
	g := n.gateway(t, doc, withClock(fake))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	for i := 0; rsHost(t, g, a.URL).BreakerState != "open"; i++ {
		if i == 10 {
			t.Fatal("host A breaker did not open within 10 requests")
		}
		code, _, _ := rsGet(t, client, ts.URL+"/", http.MethodGet, nil, nil)
		if code != http.StatusOK && code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 200 or 503", code)
		}
		if st := rsStatus(t, g); st.BreakerState != "closed" {
			t.Fatalf("aggregate breaker_state = %q, want closed", st.BreakerState)
		}
	}

	if host := rsHost(t, g, a.URL); !host.Ejected {
		t.Fatal("host A not ejected")
	}
	if host := rsHost(t, g, b.URL); !host.Ejected {
		t.Fatal("host B not ejected")
	}
	if v := rsMetric(t, g, "microgateway_upstream_panic_mode", "upstream", "up"); v != 1 {
		t.Fatalf("panic mode gauge = %v, want 1", v)
	}
	if host := rsHost(t, g, b.URL); host.BreakerState != "closed" {
		t.Fatalf("host B breaker_state = %q, want closed", host.BreakerState)
	}
	if st := rsStatus(t, g); st.BreakerState != "closed" {
		t.Fatalf("aggregate breaker_state = %q, want closed", st.BreakerState)
	}

	aHits, bHits := a.hitCount(), b.hitCount()
	retriesBefore := rsMetric(t, g, "microgateway_retries_total", "route", "all")
	for range 20 {
		if body := bkGet(t, client, ts.URL+"/", http.StatusOK); string(body) != "ok" {
			t.Fatalf("body = %q, want ok", body)
		}
	}
	if got := a.hitCount(); got != aHits {
		t.Fatalf("host A hits = %d, want %d", got, aHits)
	}
	if got := b.hitCount(); got != bHits+20 {
		t.Fatalf("host B hits = %d, want %d", got, bHits+20)
	}
	if got := rsMetric(t, g, "microgateway_retries_total", "route", "all"); got != retriesBefore {
		t.Fatalf("retries_total = %v, want %v (host A was never attempted)", got, retriesBefore)
	}
	if st := rsStatus(t, g); st.BreakerState != "closed" {
		t.Fatalf("aggregate breaker_state = %q, want closed", st.BreakerState)
	}

	bFail.Store(true)
	aHits, bHits = a.hitCount(), b.hitCount()
	if body := bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable); string(body) != "b-fail" {
		t.Fatalf("body = %q, want b-fail", body)
	}
	if got := b.hitCount(); got != bHits+3 {
		t.Fatalf("host B hits = %d, want %d", got, bHits+3)
	}
	if got := a.hitCount(); got != aHits {
		t.Fatalf("host A hits = %d, want %d", got, aHits)
	}
	if host := rsHost(t, g, b.URL); host.BreakerState != "open" {
		t.Fatalf("host B breaker_state = %q, want open", host.BreakerState)
	}
	if st := rsStatus(t, g); st.BreakerState != "closed" {
		t.Fatalf("aggregate breaker_state = %q, want closed", st.BreakerState)
	}

	aLabel, bLabel := a.srv.Addr, b.srv.Addr
	rejectedA := rsMetric(t, g, "microgateway_circuit_breaker_rejected_total", "upstream", "up", "host", aLabel)
	rejectedB := rsMetric(t, g, "microgateway_circuit_breaker_rejected_total", "upstream", "up", "host", bLabel)
	rejectedAgg := rsMetric(t, g, "microgateway_circuit_breaker_rejected_total", "upstream", "up", "host", "")
	aHits, bHits = a.hitCount(), b.hitCount()
	bkCircuitOpen(t, bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable))
	if got := a.hitCount(); got != aHits {
		t.Fatalf("host A hits = %d, want %d after all breakers open", got, aHits)
	}
	if got := b.hitCount(); got != bHits {
		t.Fatalf("host B hits = %d, want %d after all breakers open", got, bHits)
	}
	if v := rsMetric(t, g, "microgateway_circuit_breaker_rejected_total", "upstream", "up", "host", aLabel); v != rejectedA+1 {
		t.Fatalf("rejected host A = %v, want %v", v, rejectedA+1)
	}
	if v := rsMetric(t, g, "microgateway_circuit_breaker_rejected_total", "upstream", "up", "host", bLabel); v != rejectedB+1 {
		t.Fatalf("rejected host B = %v, want %v", v, rejectedB+1)
	}
	if v := rsMetric(t, g, "microgateway_circuit_breaker_rejected_total", "upstream", "up", "host", ""); v != rejectedAgg {
		t.Fatalf("rejected aggregate = %v, want %v", v, rejectedAgg)
	}
	if st := rsStatus(t, g); st.BreakerState != "closed" {
		t.Fatalf("aggregate breaker_state = %q, want closed", st.BreakerState)
	}
}

func TestBreakerSelection(t *testing.T) {
	runBreakerSelection(t, newMemNetwork())
}

func TestBreakerRouteDisabled(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	be := newRSBackend(t, n, rsStatusSeq(503))
	doc := rsDoc{
		hosts:   []string{be.URL},
		breaker: rsBreaker,
		routes: []rsRoute{
			{name: "a", path: "/a"},
			{name: "b", path: "/b", extra: "circuit_breaker:\n      enabled: false\n"},
		},
	}.yaml()
	g := n.gateway(t, doc, withClock(fake))
	ts := n.serve(t, g)
	client := n.client(t, nil)

	for range 3 {
		bkGet(t, client, ts.URL+"/a", http.StatusServiceUnavailable)
	}
	if host := rsHost(t, g, be.URL); host.BreakerState != "open" {
		t.Fatalf("host breaker_state after route a = %q, want open", host.BreakerState)
	}
	hostBefore := rsHost(t, g, be.URL).Breaker
	aggBefore := rsStatus(t, g).Breaker
	hitsBefore := be.hitCount()
	for range 3 {
		bkGet(t, client, ts.URL+"/b", http.StatusServiceUnavailable)
	}
	if got := be.hitCount(); got != hitsBefore+3 {
		t.Fatalf("backend hits via route b = %d, want %d", got, hitsBefore+3)
	}
	hostAfter := rsHost(t, g, be.URL).Breaker
	if hostAfter.Requests != hostBefore.Requests || hostAfter.Failures != hostBefore.Failures {
		t.Fatalf("host breaker counts changed by route b: before %+v after %+v", hostBefore, hostAfter)
	}
	aggAfter := rsStatus(t, g).Breaker
	if aggAfter.Requests != aggBefore.Requests || aggAfter.Failures != aggBefore.Failures {
		t.Fatalf("aggregate breaker counts changed by route b: before %+v after %+v", aggBefore, aggAfter)
	}
}

func TestBreakerPipelineWithoutPrerequisite(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	be := newRSBackend(t, n, rsStatusSeq(503))
	doc := rsDoc{
		hosts: []string{be.URL},
		routes: []rsRoute{
			{name: "all", path: "/", extra: "pipeline: [timeout, circuit_breaker, retry]\n"},
		},
	}.yaml()
	g := n.gateway(t, doc, withClock(fake))
	ts := n.serve(t, g)
	client := n.client(t, nil)
	bkGet(t, client, ts.URL+"/", http.StatusServiceUnavailable)
	if got := be.hitCount(); got != 1 {
		t.Fatalf("backend hits = %d, want 1", got)
	}
	for line := range strings.SplitSeq(metricsText(t, g), "\n") {
		if strings.HasPrefix(line, "microgateway_circuit_breaker_state") {
			t.Fatalf("unexpected breaker series: %q", line)
		}
	}
}
