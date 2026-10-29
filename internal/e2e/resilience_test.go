package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/upstream"
)

type rsLogs = captureHandler

type h3Result struct {
	code int
	body string
}

func h3Get(g *microgateway.Gateway, path string) (int, string) {
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code, rec.Body.String()
}

func h3Async(g *microgateway.Gateway, path string) <-chan h3Result {
	ch := make(chan h3Result, 1)
	go func() {
		code, body := h3Get(g, path)
		ch <- h3Result{code: code, body: body}
	}()
	return ch
}

func h3GetUntil(t *testing.T, g *microgateway.Gateway, path string, max int, done func() bool) {
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
	if got := rsMetric(t, g, "microgateway_circuit_breaker_state", "upstream", "up", "host", a.srv.Addr); got != 0 {
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
		if got := rsMetric(t, g, "microgateway_retries_total", "route", route); got != retries {
			t.Fatalf("GET %s retries_total{%s} = %v, want %v", path, route, got, retries)
		}
		if got := rsMetric(t, g, "microgateway_retry_budget_exhausted_total", "route", route); got != exhausted {
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
	pre := snapGoroutines()
	n := newMemNetwork()
	be := newRSBackend(t, n, nil)
	fake := clock.NewFake(time.Now())
	doc := func(attempts int) string {
		return rsDoc{hosts: []string{be.URL}, breaker: rsBreaker, routes: []rsRoute{
			{name: "all", path: "/", extra: h3Retry(attempts, "1ns", "1ns")},
		}}.yaml()
	}
	g := n.gateway(t, doc(2), withClock(fake))
	base := snapGoroutines()
	for i := range 100 {
		if err := g.ReloadYAML([]byte(doc(2 + (i+1)%2))); err != nil {
			t.Fatal(err)
		}
	}
	awaitGoroutinesSettled(t, "goroutines of retired generations to exit after 100 reloads", pre, base)
}

func h3Upstreams(g *microgateway.Gateway) ([]upstream.Status, error) {
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/upstreams", nil))
	if rec.Code != http.StatusOK {
		return nil, fmt.Errorf("/upstreams status = %d", rec.Code)
	}
	var list []upstream.Status
	if err := json.NewDecoder(rec.Body).Decode(&list); err != nil {
		return nil, err
	}
	return list, nil
}

func h3Violations(list []upstream.Status) []string {
	var out []string
	check := func(where string, b *upstream.BreakerStatus) {
		if b == nil {
			return
		}
		if b.ProbesInFlight > 2 {
			out = append(out, fmt.Sprintf("%s probes_in_flight = %d, want <= 2", where, b.ProbesInFlight))
		}
		if b.InFlight < 0 {
			out = append(out, fmt.Sprintf("%s in_flight = %d, want >= 0", where, b.InFlight))
		}
	}
	for _, st := range list {
		check(st.Name+" aggregate", st.Breaker)
		for _, h := range st.Hosts {
			check(st.Name+" host "+h.URL, h.Breaker)
		}
	}
	return out
}

func h3GetSeq(g *microgateway.Gateway, path, seq string) (int, string) {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("X-Rs-Seq", seq)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String()
}

func h3IsCircuitOpen(body string) bool {
	var e struct {
		Error apiError `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		return false
	}
	return e.Error.Code == "circuit_open"
}

type h3Req struct {
	seq  string
	path string
	code int
	body string
}

func TestResilienceConcurrency(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	var attemptsMu sync.Mutex
	attempts := make(map[string]int)
	var urls []string
	var backends []*rsBackend
	for range 3 {
		be := newRSBackend(t, n, func(seq int64, w http.ResponseWriter, r *http.Request) {
			attemptsMu.Lock()
			attempts[r.Header.Get("X-Rs-Seq")]++
			attemptsMu.Unlock()
			if seq%3 == 0 {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			_, _ = io.WriteString(w, "ok")
		})
		urls = append(urls, be.URL)
		backends = append(backends, be)
	}
	doc := func(weights []int) string {
		d := rsDoc{hosts: urls, breaker: rsBreaker, routes: []rsRoute{
			{name: "r", path: "/r", extra: "retry:\n      base_delay: 1ns\n      max_delay: 1ns\n      budget:\n        ratio: 0.1\n        min_per_second: 1\n        window: 10s\n"},
			{name: "p", path: "/p"},
		}}.yaml()
		return h3Weights(d, urls, weights)
	}
	g := n.gateway(t, doc([]int{1, 1, 1}), withClock(fake))

	const workers, perWorker = 64, 50
	reqs := make([][]h3Req, workers)
	var problemsMu sync.Mutex
	var problems []string
	addProblem := func(s string) {
		problemsMu.Lock()
		defer problemsMu.Unlock()
		problems = append(problems, s)
	}

	var workersWG sync.WaitGroup
	for w := range workers {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for j := range perWorker {
				path := "/p"
				if j%2 == 0 {
					path = "/r"
				}
				seq := fmt.Sprintf("%d-%d", w, j)
				code, body := h3GetSeq(g, path, seq)
				reqs[w] = append(reqs[w], h3Req{seq: seq, path: path, code: code, body: body})
			}
		}()
	}

	var reloaderWG sync.WaitGroup
	reloaderWG.Add(1)
	go func() {
		defer reloaderWG.Done()
		for k := range 20 {
			w := []int{1 + k%2, 2 - k%2, 1}
			if err := g.ReloadYAML([]byte(doc(w))); err != nil {
				addProblem(fmt.Sprintf("reload %d: %v", k, err))
			}
		}
	}()

	var clockWG sync.WaitGroup
	clockWG.Add(1)
	go func() {
		defer clockWG.Done()
		for range 40 {
			fake.Advance(5 * time.Second)
		}
	}()

	stop := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			list, err := h3Upstreams(g)
			if err != nil {
				addProblem(err.Error())
				return
			}
			for _, v := range h3Violations(list) {
				addProblem(v)
			}
		}
	}()

	workersWG.Wait()
	reloaderWG.Wait()
	clockWG.Wait()
	close(stop)
	samplerWG.Wait()

	problemsMu.Lock()
	if len(problems) > 0 {
		problemsMu.Unlock()
		t.Fatalf("concurrent invariant violations: %v", problems)
	}
	problemsMu.Unlock()

	var all []h3Req
	for _, rs := range reqs {
		all = append(all, rs...)
	}
	if len(all) != workers*perWorker {
		t.Fatalf("responses = %d, want %d", len(all), workers*perWorker)
	}

	st := rsStatus(t, g)
	if got := h3Snap(t, st.Breaker).InFlight; got != 0 {
		t.Fatalf("aggregate breaker in_flight = %d, want 0", got)
	}
	for _, h := range st.Hosts {
		if got := h3Snap(t, h.Breaker).InFlight; got != 0 {
			t.Fatalf("host %s breaker in_flight = %d, want 0", h.URL, got)
		}
		if h.InFlight != 0 {
			t.Fatalf("host %s in_flight = %d, want 0", h.URL, h.InFlight)
		}
	}

	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	sum := 0
	for _, c := range attempts {
		sum += c
	}
	var hits int64
	for _, be := range backends {
		hits += be.hitCount()
	}
	if int64(sum) != hits {
		t.Fatalf("sum of per-request attempts = %d, backend hits = %d", sum, hits)
	}

	wantRetries := 0
	for _, q := range all {
		if q.code != http.StatusOK && q.code != http.StatusServiceUnavailable {
			t.Fatalf("GET %s seq %s status = %d, want 200 or 503", q.path, q.seq, q.code)
		}
		limit := 3
		if q.path == "/p" {
			limit = 1
		}
		count := attempts[q.seq]
		if count > limit {
			t.Fatalf("GET %s seq %s attempts = %d, want <= %d", q.path, q.seq, count, limit)
		}
		if (count == 0) != h3IsCircuitOpen(q.body) {
			t.Fatalf("GET %s seq %s attempts = %d with body %q", q.path, q.seq, count, q.body)
		}
		if q.path == "/r" && count >= 1 {
			wantRetries += count - 1
		}
	}

	retries := rsMetric(t, g, "microgateway_retries_total", "route", "r")
	if retries != float64(wantRetries) {
		t.Fatalf("retries_total{r} = %v, want %d from per-request attempts", retries, wantRetries)
	}
	if got := rsMetricOr0(t, g, "microgateway_retries_total", "route", "p"); got != 0 {
		t.Fatalf("retries_total{p} = %v, want 0", got)
	}
	if retries <= 0 {
		t.Fatalf("retries_total{r} = %v, want > 0", retries)
	}
	if retries > 530 {
		t.Fatalf("retries_total{r} = %v, want <= 530", retries)
	}
}

func TestInstancesIsolateBreakers(t *testing.T) {
	fake := clock.NewFake(time.Now())
	var doc string
	gateways := make([]*microgateway.Gateway, 0, 3)
	backends := make([]*rsBackend, 0, 3)
	for i := range 3 {
		n := newMemNetwork()
		var script func(int64, http.ResponseWriter, *http.Request)
		if i == 0 {
			script = rsStatusSeq(http.StatusServiceUnavailable)
		}
		be := newRSBackend(t, n, script)
		d := rsDoc{hosts: []string{be.URL}, breaker: rsBreaker}.yaml()
		if i == 0 {
			doc = d
		} else if d != doc {
			t.Fatal("instance documents differ")
		}
		g := n.gateway(t, d, withClock(fake), withRand(clock.NewRand(uint64(i+1))))
		gateways = append(gateways, g)
		backends = append(backends, be)
	}
	gA, gB, gC := gateways[0], gateways[1], gateways[2]
	beA, beB, beC := backends[0], backends[1], backends[2]

	for range 3 {
		if code, _ := h3Get(gA, "/"); code != http.StatusServiceUnavailable {
			t.Fatalf("A status = %d, want 503", code)
		}
	}
	if st := rsStatus(t, gA); st.BreakerState != "open" || rsHost(t, gA, beA.URL).BreakerState != "open" {
		t.Fatalf("A aggregate = %q, want open", st.BreakerState)
	}
	for _, g := range []*microgateway.Gateway{gB, gC} {
		if st := rsStatus(t, g); st.BreakerState != "closed" {
			t.Fatalf("aggregate breaker_state = %q, want closed", st.BreakerState)
		}
	}
	for i, be := range []*rsBackend{beB, beC} {
		code, _ := h3Get(gateways[i+1], "/")
		if code != http.StatusOK {
			t.Fatalf("instance %d status = %d, want 200", i+1, code)
		}
		if got := be.hitCount(); got != 1 {
			t.Fatalf("instance %d backend hits = %d, want 1", i+1, got)
		}
	}
	if got := beA.hitCount(); got != 3 {
		t.Fatalf("A backend hits = %d, want 3", got)
	}
}

func TestInstancesIsolateBudget(t *testing.T) {
	fake := clock.NewFake(time.Now())
	doc := rsDoc{routes: []rsRoute{{name: "a", path: "/a", extra: h3Budget(2)}}}
	var gs [2]*microgateway.Gateway
	for i := range gs {
		n := newMemNetwork()
		be := newRSBackend(t, n, rsStatusSeq(http.StatusServiceUnavailable))
		d := doc
		d.hosts = []string{be.URL}
		gs[i] = n.gateway(t, d.yaml(), withClock(fake))
	}
	gA, gB := gs[0], gs[1]

	h3Get(gA, "/a")
	h3Get(gA, "/a")
	if got := rsMetric(t, gA, "microgateway_retries_total", "route", "a"); got != 1 {
		t.Fatalf("A retries_total{a} = %v, want 1", got)
	}
	if got := rsMetric(t, gA, "microgateway_retry_budget_exhausted_total", "route", "a"); got != 1 {
		t.Fatalf("A exhausted_total{a} = %v, want 1", got)
	}

	h3Get(gB, "/a")
	if got := rsMetric(t, gB, "microgateway_retries_total", "route", "a"); got != 1 {
		t.Fatalf("B retries_total{a} = %v, want 1", got)
	}
	if got := rsMetricOr0(t, gB, "microgateway_retry_budget_exhausted_total", "route", "a"); got != 0 {
		t.Fatalf("B exhausted_total{a} = %v, want 0", got)
	}
}

func TestInstancesBackoffRandomnessIndependent(t *testing.T) {
	fake := clock.NewFake(time.Now())
	const window = int64(200 * time.Millisecond)
	type instance struct {
		be *rsBackend
		g  *microgateway.Gateway
	}
	build := func() instance {
		n := newMemNetwork()
		be := newRSBackend(t, n, rsStatusSeq(http.StatusServiceUnavailable))
		doc := rsDoc{hosts: []string{be.URL}, routes: []rsRoute{
			{name: "all", path: "/", extra: h3Retry(2, "100ms", "250ms")},
		}}.yaml()
		g := n.gateway(t, doc, withClock(fake), withRand(clock.NewRand(7)))
		return instance{be: be, g: g}
	}
	a := build()
	b := build()

	for i := range 3 {
		res := h3Async(a.g, "/")
		rsAdvanceBackoff(t, fake, 250*time.Millisecond)
		if got := <-res; got.code != http.StatusServiceUnavailable {
			t.Fatalf("A request %d status = %d, want 503", i, got.code)
		}
	}
	if got := a.be.hitCount(); got != 6 {
		t.Fatalf("A backend hits = %d, want 6", got)
	}

	want := clock.NewRand(7).Int64N(window)
	if want <= 0 {
		t.Fatalf("precondition: first backoff draw = %d, want > 0", want)
	}
	res := h3Async(b.g, "/")
	fake.BlockUntilDue(1, 250*time.Millisecond)
	fake.Advance(time.Duration(want - 1))
	if got := b.be.hitCount(); got != 1 {
		t.Fatalf("B backend hits before backoff elapsed = %d, want 1", got)
	}
	if got := fake.Due(250 * time.Millisecond); got != 1 {
		t.Fatalf("B pending backoff waiters = %d, want 1", got)
	}
	fake.Advance(1)
	eventually(t, "second hit on B", func() bool { return b.be.hitCount() == 2 }, func() string { return fmt.Sprint(b.be.hitCount()) })
	if got := (<-res).code; got != http.StatusServiceUnavailable {
		t.Fatalf("B status = %d, want 503", got)
	}
}

func TestInstancesProbeIndependently(t *testing.T) {
	n := newMemNetwork()
	fake := clock.NewFake(time.Now())
	var fail atomic.Bool
	fail.Store(true)
	var held, peak atomic.Int64
	release := make(chan struct{})
	releaseOnce := h3Once(func() { close(release) })
	t.Cleanup(releaseOnce)
	be := n.serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		cur := held.Add(1)
		for {
			p := peak.Load()
			if cur <= p || peak.CompareAndSwap(p, cur) {
				break
			}
		}
		defer held.Add(-1)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	doc := rsDoc{hosts: []string{be.URL}, breaker: rsBreaker}.yaml()
	gs := make([]*microgateway.Gateway, 3)
	for i := range gs {
		gs[i] = n.gateway(t, doc, withClock(fake))
	}

	for _, g := range gs {
		for range 3 {
			h3Get(g, "/")
		}
		if st := rsStatus(t, g); st.BreakerState != "open" {
			t.Fatalf("aggregate breaker_state = %q, want open", st.BreakerState)
		}
	}
	fake.Advance(30 * time.Second)
	fail.Store(false)

	results := make(chan h3Result, 30)
	for _, g := range gs {
		for range 10 {
			go func(g *microgateway.Gateway) {
				code, body := h3Get(g, "/")
				results <- h3Result{code: code, body: body}
			}(g)
		}
	}
	for range 24 {
		res := <-results
		if res.code != http.StatusServiceUnavailable || !strings.Contains(res.body, "circuit_open") {
			t.Fatalf("rejected request: status = %d body = %s, want 503 circuit_open", res.code, res.body)
		}
	}
	eventually(t, "6 held requests", func() bool { return held.Load() == 6 }, func() string { return fmt.Sprint(held.Load()) })
	if got := peak.Load(); got != 6 {
		t.Fatalf("peak concurrently held requests = %d, want 6", got)
	}
	releaseOnce()
	for range 6 {
		res := <-results
		if res.code != http.StatusOK {
			t.Fatalf("admitted probe status = %d, want 200", res.code)
		}
	}
	if got := peak.Load(); got != 6 {
		t.Fatalf("peak concurrently held requests after release = %d, want 6", got)
	}
}
