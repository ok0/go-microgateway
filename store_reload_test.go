package microgateway

import (
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/store"
)

const srlGlobalID = "global:\n  request_id_header: X-Trace-Id\n"

func srlType(shared *teststore.Shared, calls *atomic.Int64) Option {
	return WithStoreType("test", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
		calls.Add(1)
		if p.Name == "bad" {
			return nil, errors.New("cannot open bad store")
		}
		return shared.Handle(), nil
	})
}

func srlDoc(backend, global, stores, extraUpstreams, routes string) string {
	if stores != "" {
		stores = "stores:\n" + stores
	}
	return "version: 1\n" + global + listenersYAML + stores +
		fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\n", backend) +
		extraUpstreams + "routes:\n" + routes
}

func srlStore(name string, failures int, endpoint string) string {
	return stStore(name, fmt.Sprintf("    guard:\n      consecutive_failures: %d\n    params:\n      endpoint: %s\n", failures, endpoint))
}

func srlRateLimit(storeName string) string {
	body := "    rate_limit:\n      rate: 3\n      per: 1h\n      max_keys: 100\n      key:\n        source: ip\n"
	if storeName != "" {
		body += "      store: " + storeName + "\n"
	}
	return body
}

func srlSample(t *testing.T, g *Gateway, s string) (float64, bool) {
	t.Helper()
	for _, line := range strings.Split(metricsText(t, g), "\n") {
		if rest, ok := strings.CutPrefix(line, s+" "); ok {
			v, err := strconv.ParseFloat(rest, 64)
			if err != nil {
				t.Fatal(err)
			}
			return v, true
		}
	}
	return 0, false
}

func srlExpectSample(t *testing.T, g *Gateway, s string, want float64) {
	t.Helper()
	got, ok := srlSample(t, g, s)
	if !ok {
		t.Fatalf("%s is absent, want %v", s, want)
	}
	if got != want {
		t.Fatalf("%s = %v, want %v", s, got, want)
	}
}

func srlExpectAbsent(t *testing.T, g *Gateway, s string) {
	t.Helper()
	if v, ok := srlSample(t, g, s); ok {
		t.Fatalf("%s = %v is present, want absent", s, v)
	}
}

func TestStoreReloadRetention(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	shared := stShared(t, fake)
	shared.SetMode(teststore.Fail, fmt.Errorf("store down: %w", store.ErrUnavailable))
	var calls atomic.Int64
	const client = "192.0.2.1:1000"

	g := n.gateway(t, srlDoc(be.URL, "", srlStore("s1", 2, "v0"), "", stRoute("rl", srlRateLimit("s1"))), withClock(fake), srlType(shared, &calls))
	reload := func(doc string) {
		t.Helper()
		if err := g.ReloadYAML([]byte(doc)); err != nil {
			t.Fatal(err)
		}
	}
	available := func(name string) string {
		return fmt.Sprintf("microgateway_store_available{store=%q}", name)
	}
	routes := stRoute("rl", srlRateLimit("s1")) + stRoute("other", "")

	rlExhaust(t, g, "/rl", client, nil)
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls = %d, want 1", got)
	}
	if got := len(shared.Calls()); got != 2 {
		t.Fatalf("store calls = %d, want 2", got)
	}
	srlExpectSample(t, g, available("s1"), 0)
	srlExpectSample(t, g, `microgateway_store_errors_total{store="s1",op="take"}`, 2)

	reload(srlDoc(be.URL, "", srlStore("s1", 2, "v0"), "", routes))
	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1", 2, "v0"), "", routes))
	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1", 2, "v0"), "", routes))
	srlExpectSample(t, g, `microgateway_config_reloads_total{result="unchanged"}`, 1)
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls after retained reloads = %d, want 1", got)
	}
	if got := len(shared.Handles()); got != 1 {
		t.Fatalf("handles after retained reloads = %d, want 1", got)
	}
	rlExpectLimited(t, stDo(t, g, "/rl", client), "1200")
	if got := len(shared.Calls()); got != 2 {
		t.Fatalf("store calls with the guard open = %d, want 2", got)
	}
	srlExpectSample(t, g, available("s1"), 0)

	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1", 3, "v0"), "", routes))
	srlExpectSample(t, g, available("s1"), 1)
	rlExpectLimited(t, stDo(t, g, "/rl", client), "1200")
	if got := shared.Calls(); len(got) != 3 || got[2].Handle != 0 {
		t.Fatalf("store calls after guard-only change = %+v, want a third call on handle 0", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("factory calls after guard-only change = %d, want 1", got)
	}
	srlExpectSample(t, g, available("s1"), 1)
	srlExpectSample(t, g, `microgateway_store_errors_total{store="s1",op="take"}`, 3)

	h0 := shared.Handles()[0]
	h0.SetMode(teststore.Hang, nil)
	ch := stAsync(t, g, "/rl", client)
	h0.WaitBlocked(1)
	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1", 3, "v1"), "", routes))
	if got := calls.Load(); got != 2 {
		t.Fatalf("factory calls after params change = %d, want 2", got)
	}
	if got := len(shared.Handles()); got != 2 {
		t.Fatalf("handles after params change = %d, want 2", got)
	}
	if got := h0.Closes(); got != 0 {
		t.Fatalf("old handle closed %d times while its request is in flight, want 0", got)
	}
	h0.Release()
	if rec := <-ch; rec.Code != http.StatusOK {
		t.Fatalf("in-flight request status = %d, want 200", rec.Code)
	}
	if got := h0.Closes(); got != 1 {
		t.Fatalf("old handle closed %d times after drain, want 1", got)
	}

	renamed := stRoute("rl", srlRateLimit("s1b")) + stRoute("other", "")
	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1b", 3, "v1"), "", renamed))
	if got := calls.Load(); got != 3 {
		t.Fatalf("factory calls after rename = %d, want 3", got)
	}
	if got := len(shared.Handles()); got != 3 {
		t.Fatalf("handles after rename = %d, want 3", got)
	}
	if got := shared.Handles()[1].Closes(); got != 1 {
		t.Fatalf("renamed-away handle closed %d times, want 1", got)
	}
	srlExpectAbsent(t, g, available("s1"))
	srlExpectSample(t, g, available("s1b"), 1)

	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1b", 3, "v1")+srlStore("aux", 2, "v0"), "", renamed+stRoute("aux", srlRateLimit("aux"))))
	if got := calls.Load(); got != 4 {
		t.Fatalf("factory calls after adding aux = %d, want 4", got)
	}
	if got := shared.Handles()[3].Closes(); got != 0 {
		t.Fatalf("aux handle closed %d times while referenced, want 0", got)
	}
	srlExpectSample(t, g, available("aux"), 1)

	reload(srlDoc(be.URL, srlGlobalID, srlStore("s1b", 3, "v1"), "", renamed))
	if got := calls.Load(); got != 4 {
		t.Fatalf("factory calls after unreferencing aux = %d, want 4", got)
	}
	if got := shared.Handles()[3].Closes(); got != 1 {
		t.Fatalf("unreferenced aux handle closed %d times, want 1", got)
	}
	srlExpectAbsent(t, g, available("aux"))

	failing := srlDoc(be.URL, srlGlobalID,
		srlStore("s1b", 3, "v1")+srlStore("fresh", 2, "v0")+srlStore("bad", 2, "v0"), "",
		renamed+stRoute("fresh", srlRateLimit("fresh"))+stRoute("bad", srlRateLimit("bad")))
	if err := g.ReloadYAML([]byte(failing)); err == nil {
		t.Fatal("reload with a failing store factory succeeded")
	}
	if got := calls.Load(); got != 6 {
		t.Fatalf("factory calls after failing reload = %d, want 6", got)
	}
	if got := len(shared.Handles()); got != 5 {
		t.Fatalf("handles after failing reload = %d, want 5", got)
	}
	if got := shared.Handles()[4].Closes(); got != 1 {
		t.Fatalf("handle created by the failed build closed %d times, want 1", got)
	}
	if got := shared.Handles()[2].Closes(); got != 0 {
		t.Fatalf("active handle closed %d times after failing reload, want 0", got)
	}
	for _, r := range g.Config().Routes {
		if r.Name == "bad" || r.Name == "fresh" {
			t.Fatalf("route %q from the failed reload is active", r.Name)
		}
	}

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	for i, h := range shared.Handles() {
		if got := h.Closes(); got != 1 {
			t.Fatalf("handle %d closed %d times after Close, want 1", i, got)
		}
	}
}

func TestStoreReloadNoGoroutineLeak(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	shared := stShared(t, fake)
	var calls atomic.Int64
	doc := func(failures int) string {
		return srlDoc(be.URL, "", srlStore("s1", failures, "v0"), "", stRoute("rl", srlRateLimit("s1")))
	}
	g := n.gateway(t, doc(3), withClock(fake), srlType(shared, &calls))
	t.Cleanup(func() { _ = g.Close() })

	base := runtime.NumGoroutine()
	for i := range 100 {
		failures := 4
		if i%2 == 1 {
			failures = 3
		}
		if err := g.ReloadYAML([]byte(doc(failures))); err != nil {
			t.Fatal(err)
		}
	}
	lo, hi := base*9/10, base*11/10
	for i := 0; ; i++ {
		got := runtime.NumGoroutine()
		if got >= lo && got <= hi {
			break
		}
		if i == 10000 {
			t.Fatalf("goroutines = %d after 100 reloads, baseline %d, want within [%d, %d]", got, base, lo, hi)
		}
		runtime.Gosched()
	}
}

func TestStoreGaugePrunedAfterDrain(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	shared := stShared(t, fake)
	var calls atomic.Int64
	const client = "192.0.2.1:1000"

	extraUps := fmt.Sprintf("  - name: up2\n    hosts:\n      - url: %q\n", be.URL)
	side := "  - name: side\n    match:\n      path: /side\n    upstream: up2\n"
	initial := srlDoc(be.URL, "", srlStore("s1", 2, "v0"), extraUps,
		stRoute("rl", srlRateLimit("s1"))+stRoute("mem", srlRateLimit(""))+side+stRoute("keep", ""))
	g := n.gateway(t, initial, withClock(fake), srlType(shared, &calls))

	h0 := shared.Handles()[0]
	h0.SetMode(teststore.Hang, nil)
	ch := stAsync(t, g, "/rl", client)
	h0.WaitBlocked(1)

	if err := g.ReloadYAML([]byte(srlDoc(be.URL, "", "", "", stRoute("keep", "")))); err != nil {
		t.Fatal(err)
	}
	srlExpectSample(t, g, `microgateway_store_available{store="s1"}`, 1)
	srlExpectSample(t, g, `microgateway_store_errors_total{store="s1",op="take"}`, 0)
	srlExpectSample(t, g, `microgateway_ratelimit_keys{route="rl"}`, 0)
	srlExpectSample(t, g, `microgateway_ratelimit_keys{route="mem"}`, 0)
	srlExpectSample(t, g, `microgateway_upstream_panic_mode{upstream="up2"}`, 0)

	h0.Release()
	if rec := <-ch; rec.Code != http.StatusOK {
		t.Fatalf("in-flight request status = %d, want 200", rec.Code)
	}

	srlExpectAbsent(t, g, `microgateway_store_available{store="s1"}`)
	srlExpectSample(t, g, `microgateway_store_errors_total{store="s1",op="take"}`, 0)
	srlExpectAbsent(t, g, `microgateway_ratelimit_keys{route="rl"}`)
	srlExpectAbsent(t, g, `microgateway_ratelimit_keys{route="mem"}`)
	srlExpectAbsent(t, g, `microgateway_upstream_panic_mode{upstream="up2"}`)
	srlExpectSample(t, g, `microgateway_upstream_panic_mode{upstream="up"}`, 0)
}

func TestStoreClosedAfterInFlightDrain(t *testing.T) {
	const (
		client = "192.0.2.50:1000"
		fresh  = "192.0.2.51:1000"
	)
	routes := stRoute("rl", srlRateLimit("s1")) + stRoute("other", "")
	cases := []struct {
		name   string
		reload func(backend string) string
	}{
		{"rename", func(b string) string {
			return srlDoc(b, "", srlStore("s2", 2, "v0"), "", stRoute("rl", srlRateLimit("s2"))+stRoute("other", ""))
		}},
		{"unreference", func(b string) string {
			return srlDoc(b, "", srlStore("s1", 2, "v0"), "", stRoute("rl", srlRateLimit(""))+stRoute("other", ""))
		}},
		{"remove", func(b string) string {
			return srlDoc(b, "", "", "", stRoute("rl", srlRateLimit(""))+stRoute("other", ""))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := newMemNetwork()
			fake := rlFake()
			be, _ := rlBackend(t, n)
			shared := stShared(t, fake)
			var calls atomic.Int64
			g := n.gateway(t, srlDoc(be.URL, "", srlStore("s1", 2, "v0"), "", routes), withClock(fake), srlType(shared, &calls))

			h := shared.Handles()[0]
			h.SetMode(teststore.Hang, nil)
			ch := stAsync(t, g, "/rl", client)
			h.WaitBlocked(1)

			if err := g.ReloadYAML([]byte(tc.reload(be.URL))); err != nil {
				t.Fatal(err)
			}
			if got := h.Closes(); got != 0 {
				t.Fatalf("handle closed %d times while its request is in flight, want 0", got)
			}
			if _, ok := srlSample(t, g, `microgateway_store_available{store="s1"}`); !ok {
				t.Fatal(`microgateway_store_available{store="s1"} is absent while its request is in flight, want present`)
			}
			takes := h.Takes()
			if rec := stDo(t, g, "/rl", fresh); rec.Code != http.StatusOK {
				t.Fatalf("request on the new generation status = %d, want 200", rec.Code)
			}
			if got := h.Takes(); got != takes {
				t.Fatalf("old handle takes = %d after a request on the new generation, want %d", got, takes)
			}

			h.SetMode(teststore.Healthy, nil)
			h.Release()
			if rec := <-ch; rec.Code != http.StatusOK {
				t.Fatalf("in-flight request status = %d, want 200", rec.Code)
			}
			if got := h.Closes(); got != 1 {
				t.Fatalf("handle closed %d times after drain, want 1", got)
			}
			srlExpectAbsent(t, g, `microgateway_store_available{store="s1"}`)

			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if got := h.Closes(); got != 1 {
				t.Fatalf("handle closed %d times after Close, want 1", got)
			}
			for i, other := range shared.Handles() {
				if got := other.Closes(); got != 1 {
					t.Fatalf("handle %d closed %d times after Close, want 1", i, got)
				}
			}
		})
	}
}
