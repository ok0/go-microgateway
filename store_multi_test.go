package lychgate

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/lychgate/internal/ratelimit"
	"github.com/ok0/lychgate/internal/teststore"
	"github.com/ok0/lychgate/store"
)

const smClient = "192.0.2.10:4000"

func smRate(ref string, rate, burst int) string {
	var b strings.Builder
	b.WriteString("    rate_limit:\n")
	fmt.Fprintf(&b, "      rate: %d\n      per: 1s\n      burst: %d\n", rate, burst)
	if ref != "" {
		fmt.Fprintf(&b, "      store: %s\n", ref)
	}
	return b.String()
}

func smDoc(stores, upstreamBody string, hosts []string, routes string) string {
	var b strings.Builder
	b.WriteString("version: 1\n")
	if stores != "" {
		b.WriteString("stores:\n" + stores)
	}
	b.WriteString(listenersYAML + "upstreams:\n  - name: up\n" + upstreamBody)
	if len(hosts) > 0 {
		b.WriteString("    hosts:\n")
		for _, h := range hosts {
			fmt.Fprintf(&b, "      - url: %q\n", h)
		}
	}
	b.WriteString("routes:\n" + routes)
	return b.String()
}

func TestStoreSharedAcrossGateways(t *testing.T) {
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	shared := stShared(t, rlFake())
	var factoryCalls atomic.Int64
	opt := stOption(shared, &factoryCalls)
	doc := stDoc(be.URL, stStore("shared", ""), stRoute("rl", smRate("shared", 5, 3)))
	gA := n.gateway(t, doc, withClock(rlFake()), opt)
	gB := n.gateway(t, doc, withClock(rlFake()), opt)
	if got := factoryCalls.Load(); got != 2 {
		t.Fatalf("store factory calls = %d, want 2", got)
	}

	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "2")
	rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "1")
	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "0")
	rlExpectLimited(t, stDo(t, gB, "/rl", smClient), "1")
	rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")

	lim := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	key := ratelimit.PolicyID("rl", lim) + ":ip:192.0.2.10"
	calls := shared.Calls()
	if len(calls) != 5 {
		t.Fatalf("shared store calls = %d, want 5", len(calls))
	}
	for i, c := range calls {
		if c.Key != key || c.Limit != lim {
			t.Fatalf("call %d = {key %q, limit %+v}, want {key %q, limit %+v}", i, c.Key, c.Limit, key, lim)
		}
	}

	memDoc := stDoc(be.URL, "", stRoute("rl", smRate("", 5, 3)))
	gC := n.gateway(t, memDoc, withClock(rlFake()), opt)
	gD := n.gateway(t, memDoc, withClock(rlFake()), opt)
	for _, g := range []*Gateway{gC, gD} {
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "2")
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "1")
		rlExpectRemaining(t, stDo(t, g, "/rl", smClient), "0")
		rlExpectLimited(t, stDo(t, g, "/rl", smClient), "1")
	}
	if got := len(shared.Calls()); got != 5 {
		t.Fatalf("shared store calls after memory gateways = %d, want 5", got)
	}

	changed := stDoc(be.URL, stStore("shared", ""), stRoute("rl", smRate("shared", 5, 4)))
	if err := gA.ReloadYAML([]byte(changed)); err != nil {
		t.Fatal(err)
	}
	rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "3")
	lim4 := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 4}
	key4 := ratelimit.PolicyID("rl", lim4) + ":ip:192.0.2.10"
	calls = shared.Calls()
	if len(calls) != 6 {
		t.Fatalf("shared store calls after reload = %d, want 6", len(calls))
	}
	if last := calls[5]; last.Key != key4 || last.Limit != lim4 {
		t.Fatalf("call after reload = {key %q, limit %+v}, want {key %q, limit %+v}", last.Key, last.Limit, key4, lim4)
	}
	if key4 == key {
		t.Fatal("changed burst kept the key namespace")
	}
	rlExpectLimited(t, stDo(t, gB, "/rl", smClient), "1")
}

func TestStoreMultiInstanceIndependence(t *testing.T) {
	t.Run("breaker", func(t *testing.T) {
		n := newMemNetwork()
		be := newRSBackend(t, n, rsStatusSeq(http.StatusServiceUnavailable))
		shared := stShared(t, rlFake())
		var factoryCalls atomic.Int64
		opt := stOption(shared, &factoryCalls)
		doc := smDoc(stStore("shared", ""), miTimeouts+miRR+"    circuit_breaker:\n"+rsBreaker, []string{be.URL}, stRoute("rl", smRate("shared", 1000, 1000)))
		gA := n.gateway(t, doc, withClock(rlFake()), opt)
		gB := n.gateway(t, doc, withClock(rlFake()), opt)

		for range 3 {
			if rec := stDo(t, gA, "/rl", smClient); rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("A status = %d, want 503", rec.Code)
			}
		}
		if got := be.hitCount(); got != 3 {
			t.Fatalf("backend hits after A failures = %d, want 3", got)
		}
		bkCircuitOpen(t, stDo(t, gA, "/rl", smClient).Body.Bytes())
		if got := be.hitCount(); got != 3 {
			t.Fatalf("backend hits after short-circuit = %d, want 3", got)
		}
		if st := hostByURL(upstreamStatus(t, gA, "up"), be.URL); st.BreakerState != "open" {
			t.Fatalf("A breaker_state = %q, want open", st.BreakerState)
		}

		if st := hostByURL(upstreamStatus(t, gB, "up"), be.URL); st.BreakerState != "closed" {
			t.Fatalf("B breaker_state = %q, want closed", st.BreakerState)
		}
		if rec := stDo(t, gB, "/rl", smClient); rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("B status = %d, want 503 from backend", rec.Code)
		}
		if got := be.hitCount(); got != 4 {
			t.Fatalf("backend hits after B request = %d, want 4", got)
		}
	})

	t.Run("passive ejection", func(t *testing.T) {
		n := newMemNetwork()
		x := failingBackend(t, n, "x")
		x.Status.Store(http.StatusServiceUnavailable)
		y := failingBackend(t, n, "y")
		shared := stShared(t, rlFake())
		var factoryCalls atomic.Int64
		opt := stOption(shared, &factoryCalls)
		doc := smDoc(stStore("shared", ""), miTimeouts+miRR+miPassive, []string{x.URL, y.URL}, stRoute("rl", smRate("shared", 1000, 1000)))
		gA := n.gateway(t, doc, withClock(rlFake()), opt)
		gB := n.gateway(t, doc, withClock(rlFake()), opt)

		for range 4 {
			stDo(t, gA, "/rl", smClient)
		}
		if h := hostByURL(upstreamStatus(t, gA, "up"), x.URL); !h.Ejected {
			t.Fatal("host x not ejected on A")
		}
		if h := hostByURL(upstreamStatus(t, gB, "up"), x.URL); h.Ejected || h.EjectionCount != 0 {
			t.Fatalf("host x on B ejected=%v ejection_count=%d, want eligible", h.Ejected, h.EjectionCount)
		}
		before := x.Hits.Load()
		for range 4 {
			stDo(t, gB, "/rl", smClient)
		}
		if got := x.Hits.Load() - before; got != 2 {
			t.Fatalf("requests reaching x on B = %d, want 2 of 4", got)
		}
	})

	t.Run("reload", func(t *testing.T) {
		n := newMemNetwork()
		be, _ := rlBackend(t, n)
		shared := stShared(t, rlFake())
		var factoryCalls atomic.Int64
		opt := stOption(shared, &factoryCalls)
		doc := stDoc(be.URL, stStore("shared", ""), stRoute("rl", smRate("shared", 5, 3)))
		gA := n.gateway(t, doc, withClock(rlFake()), opt)
		gB := n.gateway(t, doc, withClock(rlFake()), opt)
		fpA := gA.Config().Fingerprint()
		fpB := gB.Config().Fingerprint()

		if err := gA.ReloadYAML([]byte(stDoc(be.URL, stStore("shared", ""), stRoute("rl", smRate("shared", 7, 3))))); err != nil {
			t.Fatal(err)
		}
		if got := gA.Config().Fingerprint(); got == fpA {
			t.Fatal("A fingerprint unchanged after reload")
		}
		if got := gB.Config().Fingerprint(); got != fpB {
			t.Fatalf("B fingerprint = %q after A reload, want %q", got, fpB)
		}
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "2")
	})

	t.Run("guard and local limiter", func(t *testing.T) {
		n := newMemNetwork()
		be, _ := rlBackend(t, n)
		shared := stShared(t, rlFake())
		var factoryCalls atomic.Int64
		opt := stOption(shared, &factoryCalls)
		stores := stStore("shared", "    guard:\n      consecutive_failures: 2\n      open_timeout: 30s\n")
		doc := stDoc(be.URL, stores, stRoute("rl", smRate("shared", 5, 3)))
		gA := n.gateway(t, doc, withClock(rlFake()), opt)
		gB := n.gateway(t, doc, withClock(rlFake()), opt)
		handles := shared.Handles()
		if len(handles) != 2 {
			t.Fatalf("store handles = %d, want 2", len(handles))
		}
		hA, hB := handles[0], handles[1]
		down := fmt.Errorf("shared store down: %w", store.ErrUnavailable)

		hA.SetMode(teststore.Fail, down)
		rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "2")
		rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "1")
		rlExpectRemaining(t, stDo(t, gA, "/rl", smClient), "0")
		rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")
		if got := hA.Takes(); got != 2 {
			t.Fatalf("A store calls = %d, want 2 before the guard short-circuits", got)
		}
		if v := stGauge(t, gA, "lychgate_store_available", "store", "shared"); v != 0 {
			t.Fatalf("A store_available = %v, want 0", v)
		}
		if v := stCounter(t, gA, "lychgate_store_errors_total", "store", "shared", "op", "take"); v != 2 {
			t.Fatalf("A store_errors_total = %v, want 2", v)
		}

		if v := stGauge(t, gB, "lychgate_store_available", "store", "shared"); v != 1 {
			t.Fatalf("B store_available = %v, want 1", v)
		}
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "2")
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "1")
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "0")
		rlExpectLimited(t, stDo(t, gB, "/rl", smClient), "1")
		if got := hB.Takes(); got != 4 {
			t.Fatalf("B store calls = %d, want 4", got)
		}
		if got := hA.Takes(); got != 2 {
			t.Fatalf("A store calls after B traffic = %d, want 2", got)
		}

		hB.SetMode(teststore.Fail, down)
		rlExpectRemaining(t, stDo(t, gB, "/rl", smClient), "2")
		if v := stGauge(t, gB, "lychgate_store_available", "store", "shared"); v != 1 {
			t.Fatalf("B store_available after one failure = %v, want 1", v)
		}
		if v := stCounter(t, gB, "lychgate_store_errors_total", "store", "shared", "op", "take"); v != 1 {
			t.Fatalf("B store_errors_total = %v, want 1", v)
		}
		if v := stGauge(t, gA, "lychgate_store_available", "store", "shared"); v != 0 {
			t.Fatalf("A store_available after B failure = %v, want 0", v)
		}
		rlExpectLimited(t, stDo(t, gA, "/rl", smClient), "1")
	})
}
