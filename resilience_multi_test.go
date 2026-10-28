package lychgate

import (
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/clock"
)

func TestInstancesIsolateBreakers(t *testing.T) {
	fake := clock.NewFake(time.Now())
	var doc string
	gateways := make([]*Gateway, 0, 3)
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
	for _, g := range []*Gateway{gB, gC} {
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
	var gs [2]*Gateway
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
	if got := rsMetric(t, gA, "lychgate_retries_total", "route", "a"); got != 1 {
		t.Fatalf("A retries_total{a} = %v, want 1", got)
	}
	if got := rsMetric(t, gA, "lychgate_retry_budget_exhausted_total", "route", "a"); got != 1 {
		t.Fatalf("A exhausted_total{a} = %v, want 1", got)
	}

	h3Get(gB, "/a")
	if got := rsMetric(t, gB, "lychgate_retries_total", "route", "a"); got != 1 {
		t.Fatalf("B retries_total{a} = %v, want 1", got)
	}
	if got := rsMetricOr0(t, gB, "lychgate_retry_budget_exhausted_total", "route", "a"); got != 0 {
		t.Fatalf("B exhausted_total{a} = %v, want 0", got)
	}
}

func TestInstancesBackoffRandomnessIndependent(t *testing.T) {
	fake := clock.NewFake(time.Now())
	const window = int64(200 * time.Millisecond)
	type instance struct {
		be *rsBackend
		g  *Gateway
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
	eventually(t, func() bool { return b.be.hitCount() == 2 })
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
	gs := make([]*Gateway, 3)
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
			go func(g *Gateway) {
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
	eventually(t, func() bool { return held.Load() == 6 })
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
