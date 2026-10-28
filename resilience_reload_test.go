package lychgate

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/upstream"
)

type h3Result struct {
	code int
	body string
}

func h3Get(g *Gateway, path string) (int, string) {
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func h3Async(g *Gateway, path string) <-chan h3Result {
	ch := make(chan h3Result, 1)
	go func() {
		code, body := h3Get(g, path)
		ch <- h3Result{code: code, body: body}
	}()
	return ch
}

func h3GetUntil(t *testing.T, g *Gateway, path string, max int, done func() bool) {
	t.Helper()
	for range max {
		if done() {
			return
		}
		h3Get(g, path)
	}
	if !done() {
		t.Fatalf("condition not met after %d requests to %s", max, path)
	}
}

func h3Weights(doc string, urls []string, weights []int) string {
	for i, u := range urls {
		doc = strings.Replace(doc,
			fmt.Sprintf("      - url: %q\n", u),
			fmt.Sprintf("      - url: %q\n        weight: %d\n", u, weights[i]), 1)
	}
	return doc
}

func h3Retry(attempts int, base, max string) string {
	return fmt.Sprintf("retry:\n      attempts: %d\n      base_delay: %s\n      max_delay: %s\n", attempts, base, max)
}

func h3Budget(attempts int) string {
	return fmt.Sprintf("retry:\n      attempts: %d\n      base_delay: 1ns\n      max_delay: 1ns\n"+
		"      budget:\n        ratio: 0\n        min_per_second: 0.1\n        window: 10s\n", attempts)
}

func h3Switch(code *atomic.Int32) func(int64, http.ResponseWriter, *http.Request) {
	return func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(code.Load()))
		_, _ = io.WriteString(w, "ok")
	}
}

func h3Snap(t *testing.T, b *upstream.BreakerStatus) upstream.BreakerStatus {
	t.Helper()
	if b == nil {
		t.Fatal("breaker status missing")
	}
	return *b
}

func h3Once(f func()) func() {
	var done atomic.Bool
	return func() {
		if done.CompareAndSwap(false, true) {
			f()
		}
	}
}

func TestBreakerReloadRetention(t *testing.T) {
	n := newMemNetwork()
	var codeA atomic.Int32
	codeA.Store(http.StatusServiceUnavailable)
	a := newRSBackend(t, n, h3Switch(&codeA))
	b := newRSBackend(t, n, nil)
	fake := clock.NewFake(time.Now())
	hosts := []string{a.URL, b.URL}
	docBase := rsDoc{hosts: hosts, breaker: rsBreaker}.yaml()
	g := n.gateway(t, docBase, withClock(fake))

	// 1. Open A's breaker: three failures reach A.
	for range 10 {
		if rsHost(t, g, a.URL).BreakerState == "open" {
			break
		}
		h3Get(g, "/")
	}
	if got := a.hitCount(); got != 3 {
		t.Fatalf("hits on A = %d, want 3", got)
	}
	if st := rsHost(t, g, a.URL).BreakerState; st != "open" {
		t.Fatalf("A breaker_state = %q, want open", st)
	}

	// 2.
	fake.Advance(10 * time.Second)

	// 3. Adding a route keeps A open with 20s left.
	docRoutes := rsDoc{hosts: hosts, breaker: rsBreaker, routes: []rsRoute{{name: "all", path: "/"}, {name: "x", path: "/x"}}}
	if err := g.ReloadYAML([]byte(docRoutes.yaml())); err != nil {
		t.Fatal(err)
	}
	if st := rsHost(t, g, a.URL).BreakerState; st != "open" {
		t.Fatalf("after route reload A breaker_state = %q, want open", st)
	}
	if got := h3Snap(t, rsHost(t, g, a.URL).Breaker).HalfOpenInSeconds; got != 20 {
		t.Fatalf("after route reload half_open_in_seconds = %v, want 20", got)
	}

	// 4. Changing B's weight keeps A open with 20s left.
	docWeighted := h3Weights(docRoutes.yaml(), hosts, []int{1, 2})
	if err := g.ReloadYAML([]byte(docWeighted)); err != nil {
		t.Fatal(err)
	}
	if st := rsHost(t, g, a.URL).BreakerState; st != "open" {
		t.Fatalf("after weight reload A breaker_state = %q, want open", st)
	}
	if got := h3Snap(t, rsHost(t, g, a.URL).Breaker).HalfOpenInSeconds; got != 20 {
		t.Fatalf("after weight reload half_open_in_seconds = %v, want 20", got)
	}

	// 5. A probe to A succeeds: half-open, no probe in flight, success counted.
	fake.Advance(20 * time.Second)
	codeA.Store(http.StatusOK)
	hitsA := a.hitCount()
	h3GetUntil(t, g, "/", 10, func() bool { return a.hitCount() > hitsA })
	snap := h3Snap(t, rsHost(t, g, a.URL).Breaker)
	if snap.State != "half_open" {
		t.Fatalf("A breaker state after probe = %q, want half_open", snap.State)
	}
	if snap.ProbesInFlight != 0 {
		t.Fatalf("A probes_in_flight = %d, want 0", snap.ProbesInFlight)
	}

	docAddRoute := rsDoc{hosts: hosts, breaker: rsBreaker, routes: []rsRoute{
		{name: "all", path: "/"}, {name: "x", path: "/x"}, {name: "y", path: "/y"},
	}}
	docAddRouteW := h3Weights(docAddRoute.yaml(), hosts, []int{1, 2})
	if err := g.ReloadYAML([]byte(docAddRouteW)); err != nil {
		t.Fatal(err)
	}
	if st := rsHost(t, g, a.URL).BreakerState; st != "half_open" {
		t.Fatalf("after route reload A breaker_state = %q, want half_open", st)
	}
	hitsA = a.hitCount()
	h3GetUntil(t, g, "/", 10, func() bool { return a.hitCount() > hitsA })
	if st := rsHost(t, g, a.URL).BreakerState; st != "closed" {
		t.Fatalf("A breaker_state after second success = %q, want closed", st)
	}

	// 6. A changed open_timeout gives every breaker a fresh closed state.
	codeA.Store(http.StatusServiceUnavailable)
	h3GetUntil(t, g, "/", 10, func() bool { return rsHost(t, g, a.URL).BreakerState == "open" })
	hostA := rsHost(t, g, a.URL)
	if hostA.BreakerState != "open" {
		t.Fatalf("A breaker_state before reload = %q, want open", hostA.BreakerState)
	}
	if got := h3Snap(t, hostA.Breaker).HalfOpenInSeconds; got != 30 {
		t.Fatalf("A half_open_in_seconds before reload = %v, want 30", got)
	}
	if got := h3Snap(t, rsHost(t, g, b.URL).Breaker).Requests; got <= 0 {
		t.Fatalf("B requests before reload = %d, want > 0", got)
	}
	st := rsStatus(t, g)
	if st.BreakerState != "closed" {
		t.Fatalf("aggregate breaker_state before reload = %q, want closed", st.BreakerState)
	}
	if got := h3Snap(t, st.Breaker).Requests; got <= 0 {
		t.Fatalf("aggregate requests before reload = %d, want > 0", got)
	}

	docTimeout := h3Weights(docAddRoute.yaml(), hosts, []int{1, 2})
	docTimeout = strings.Replace(docTimeout, "open_timeout: 30s", "open_timeout: 40s", 1)
	if err := g.ReloadYAML([]byte(docTimeout)); err != nil {
		t.Fatal(err)
	}
	st = rsStatus(t, g)
	if st.BreakerState != "closed" {
		t.Fatalf("aggregate breaker_state after reload = %q, want closed", st.BreakerState)
	}
	if snap := h3Snap(t, st.Breaker); snap.Requests != 0 || snap.Failures != 0 || snap.ConsecutiveFailures != 0 {
		t.Fatalf("aggregate after reload requests=%d failures=%d consecutive_failures=%d, want 0 0 0",
			snap.Requests, snap.Failures, snap.ConsecutiveFailures)
	}
	for _, u := range hosts {
		host := rsHost(t, g, u)
		if host.BreakerState != "closed" {
			t.Fatalf("host %s breaker_state after reload = %q, want closed", u, host.BreakerState)
		}
		if snap := h3Snap(t, host.Breaker); snap.Requests != 0 || snap.Failures != 0 || snap.ConsecutiveFailures != 0 {
			t.Fatalf("host %s after reload requests=%d failures=%d consecutive_failures=%d, want 0 0 0",
				u, snap.Requests, snap.Failures, snap.ConsecutiveFailures)
		}
	}
	if got := rsMetric(t, g, "lychgate_circuit_breaker_state", "upstream", "up", "host", a.srv.Addr); got != 0 {
		t.Fatalf("circuit breaker state gauge for A after reload = %v, want 0", got)
	}
	hitsA = a.hitCount()
	h3GetUntil(t, g, "/", 3, func() bool { return a.hitCount() > hitsA })
	if got := a.hitCount(); got != hitsA+1 {
		t.Fatalf("hits on A after reload = %d, want %d", got, hitsA+1)
	}

	// 7. Reopen A, then failed reloads leave /upstreams unchanged.
	h3GetUntil(t, g, "/", 10, func() bool { return rsHost(t, g, a.URL).BreakerState == "open" })
	if got := h3Snap(t, rsHost(t, g, a.URL).Breaker).HalfOpenInSeconds; got != 40 {
		t.Fatalf("A half_open_in_seconds after reopen = %v, want 40", got)
	}
	before := adminBody(t, g, "/upstreams")
	invalidBreaker := strings.Replace(rsBreaker, "failure_ratio: 1\n", "failure_ratio: 2\n", 1)
	invalidYAML := []string{
		"version: [\n",
		rsDoc{hosts: hosts, breaker: invalidBreaker, routes: docAddRoute.routes}.yaml(),
	}
	for _, doc := range invalidYAML {
		if err := g.ReloadYAML([]byte(doc)); err == nil {
			t.Fatal("invalid reload succeeded")
		}
		if after := adminBody(t, g, "/upstreams"); after != before {
			t.Fatalf("/upstreams changed by failed reload:\nbefore: %s\nafter:  %s", before, after)
		}
	}
}

func TestBudgetReloadRetention(t *testing.T) {
	n := newMemNetwork()
	be := newRSBackend(t, n, rsStatusSeq(http.StatusServiceUnavailable))
	fake := clock.NewFake(time.Now())
	hosts := []string{be.URL}
	docA := rsDoc{hosts: hosts, routes: []rsRoute{
		{name: "a", path: "/a", extra: h3Budget(2)},
		{name: "b", path: "/b", extra: h3Budget(2)},
	}}.yaml()
	g := n.gateway(t, docA, withClock(fake))

	get := func(path, route string, hits int64, retries, exhausted float64) {
		t.Helper()
		before := be.hitCount()
		h3Get(g, path)
		if got := be.hitCount() - before; got != hits {
			t.Fatalf("GET %s backend hits +%d, want +%d", path, got, hits)
		}
		if got := rsMetric(t, g, "lychgate_retries_total", "route", route); got != retries {
			t.Fatalf("GET %s retries_total{%s} = %v, want %v", path, route, got, retries)
		}
		if got := rsMetric(t, g, "lychgate_retry_budget_exhausted_total", "route", route); got != exhausted {
			t.Fatalf("GET %s exhausted_total{%s} = %v, want %v", path, route, got, exhausted)
		}
	}

	get("/a", "a", 2, 1, 0)

	docB := rsDoc{hosts: hosts, routes: []rsRoute{
		{name: "a", path: "/a", extra: h3Budget(2)},
		{name: "b", path: "/b", extra: h3Budget(2)},
		{name: "c", path: "/c"},
	}}.yaml()
	if err := g.ReloadYAML([]byte(docB)); err != nil {
		t.Fatal(err)
	}
	get("/a", "a", 1, 1, 1)
	get("/b", "b", 2, 1, 0)

	docC := rsDoc{hosts: hosts, routes: []rsRoute{
		{name: "a", path: "/a", extra: h3Budget(3)},
		{name: "b", path: "/b", extra: h3Budget(2)},
		{name: "c", path: "/c"},
	}}.yaml()
	if err := g.ReloadYAML([]byte(docC)); err != nil {
		t.Fatal(err)
	}
	get("/a", "a", 2, 2, 2)
	get("/b", "b", 1, 1, 1)

	docD := rsDoc{hosts: hosts, routes: []rsRoute{
		{name: "a2", path: "/a2", extra: h3Budget(3)},
		{name: "b", path: "/b", extra: h3Budget(2)},
		{name: "c", path: "/c"},
	}}.yaml()
	if err := g.ReloadYAML([]byte(docD)); err != nil {
		t.Fatal(err)
	}
	get("/a2", "a2", 2, 1, 1)
}

func TestInFlightAcrossReloadRecordsOnce(t *testing.T) {
	n := newMemNetwork()
	arrived := make(chan struct{}, 4)
	release := make(chan struct{})
	releaseOnce := h3Once(func() { close(release) })
	t.Cleanup(releaseOnce)
	a := newRSBackend(t, n, func(_ int64, w http.ResponseWriter, r *http.Request) {
		arrived <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	b := newRSBackend(t, n, nil)
	hosts := []string{a.URL, b.URL}
	fake := clock.NewFake(time.Now())
	g := n.gateway(t, rsDoc{hosts: hosts, breaker: rsBreaker}.yaml(), withClock(fake))

	done := h3Async(g, "/")
	<-arrived
	if err := g.ReloadYAML([]byte(h3Weights(rsDoc{hosts: hosts, breaker: rsBreaker}.yaml(), hosts, []int{1, 2}))); err != nil {
		t.Fatal(err)
	}
	if got := rsHost(t, g, a.URL).InFlight; got != 1 {
		t.Fatalf("in_flight on A during reload = %d, want 1", got)
	}

	releaseOnce()
	res := <-done
	if res.code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", res.code)
	}
	host := rsHost(t, g, a.URL)
	snap := h3Snap(t, host.Breaker)
	if snap.Requests != 1 || snap.Failures != 1 {
		t.Fatalf("A breaker requests=%d failures=%d, want 1 and 1", snap.Requests, snap.Failures)
	}
	if snap.InFlight != 0 || host.InFlight != 0 {
		t.Fatalf("in_flight breaker=%d host=%d, want 0", snap.InFlight, host.InFlight)
	}
}

func TestResilienceReloadsNoLeak(t *testing.T) {
	n := newMemNetwork()
	be := newRSBackend(t, n, nil)
	fake := clock.NewFake(time.Now())
	doc := func(attempts int) string {
		return rsDoc{hosts: []string{be.URL}, breaker: rsBreaker, routes: []rsRoute{
			{name: "all", path: "/", extra: h3Retry(attempts, "1ns", "1ns")},
		}}.yaml()
	}
	g := n.gateway(t, doc(2), withClock(fake))
	base := runtime.NumGoroutine()
	for i := range 100 {
		if err := g.ReloadYAML([]byte(doc(2 + (i+1)%2))); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, func() bool {
		cur := runtime.NumGoroutine()
		diff := cur - base
		if diff < 0 {
			diff = -diff
		}
		return diff*10 <= base
	})
}
