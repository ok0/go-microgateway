package lychgate

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func rlReloadDoc(backend, name, rl string, extra bool) string {
	doc := "version: 1\n" + listenersYAML +
		fmt.Sprintf("upstreams:\n  - name: up\n    hosts:\n      - url: %q\nroutes:\n", backend)
	if name != "" {
		doc += fmt.Sprintf("  - name: %s\n    match:\n      path: /rl\n    upstream: up\n", name) + rl
	}
	if extra {
		doc += "  - name: other\n    match:\n      path: /other\n    upstream: up\n"
	}
	return doc
}

func rlBlock(timeout string, rate, burst, maxKeys int, source string) string {
	var b strings.Builder
	if timeout != "" {
		fmt.Fprintf(&b, "    timeout: %s\n", timeout)
	}
	b.WriteString("    rate_limit:\n")
	fmt.Fprintf(&b, "      rate: %d\n      per: 1h\n", rate)
	if burst > 0 {
		fmt.Fprintf(&b, "      burst: %d\n", burst)
	}
	fmt.Fprintf(&b, "      max_keys: %d\n", maxKeys)
	if source == "header" {
		b.WriteString("      key:\n        source: header\n        name: X-Tenant\n")
	} else {
		b.WriteString("      key:\n        source: ip\n")
	}
	return b.String()
}

func TestRateLimitStateCarryOver(t *testing.T) {
	n := newMemNetwork()
	fake := rlFake()
	be, _ := rlBackend(t, n)
	const client = "192.0.2.1:1000"
	g := n.gateway(t, rlReloadDoc(be.URL, "rl", rlBlock("", 3, 0, 100, "ip"), false), withClock(fake))
	t.Cleanup(func() { _ = g.Close() })

	reload := func(name, rl string, extra bool) {
		t.Helper()
		if err := g.ReloadYAML([]byte(rlReloadDoc(be.URL, name, rl, extra))); err != nil {
			t.Fatal(err)
		}
	}

	rlExhaust(t, g, "/rl", client, nil)

	reload("rl", rlBlock("", 3, 0, 100, "ip"), true)
	rlExpectLimited(t, rlGet(t, g, "/rl", client, nil), "1200")

	reload("rl", rlBlock("5s", 3, 0, 100, "ip"), true)
	rlExpectLimited(t, rlGet(t, g, "/rl", client, nil), "1200")

	reload("rl", rlBlock("5s", 3, 0, 100, "ip"), true)
	rlExpectLimited(t, rlGet(t, g, "/rl", client, nil), "1200")

	changed := func(name, rl string, burst int) {
		t.Helper()
		before := g.state.Len()
		reload(name, rl, false)
		rlExpectRemaining(t, rlGet(t, g, "/rl", client, nil), strconv.Itoa(burst-1))
		if got := g.state.Len(); got != before {
			t.Fatalf("state entries = %d, want %d", got, before)
		}
		if got := rsMetric(t, g, "lychgate_ratelimit_keys", "route", name); got != 1 {
			t.Fatalf("ratelimit_keys{route=%q} = %v, want 1", name, got)
		}
	}

	changed("rl", rlBlock("", 4, 0, 100, "ip"), 4)
	changed("rl", rlBlock("", 4, 5, 100, "ip"), 5)
	changed("rl", rlBlock("", 4, 5, 50, "ip"), 5)
	changed("rl", rlBlock("", 4, 5, 50, "header"), 5)
	changed("rl2", rlBlock("", 4, 5, 50, "header"), 5)

	before := g.state.Len()
	reload("", "", true)
	if got := g.state.Len(); got != before-1 {
		t.Fatalf("state entries after route removal = %d, want %d", got, before-1)
	}

	reload("rl", rlBlock("", 3, 0, 100, "ip"), true)
	rlExhaust(t, g, "/rl", client, nil)
	before = g.state.Len()
	if err := g.ReloadYAML([]byte(rlReloadDoc(be.URL, "rl", rlBlock("", 0, 0, 100, "ip"), true))); err == nil {
		t.Fatal("reload with rate 0 succeeded")
	}
	rlExpectLimited(t, rlGet(t, g, "/rl", client, nil), "1200")
	if got := g.state.Len(); got != before {
		t.Fatalf("state entries after failed reload = %d, want %d", got, before)
	}
}

func TestRateLimitReloadNoGoroutineLeak(t *testing.T) {
	n := newMemNetwork()
	be, _ := rlBackend(t, n)
	g := n.gateway(t, rlReloadDoc(be.URL, "rl", rlBlock("", 5, 0, 100, "ip"), false), withClock(rlFake()))
	t.Cleanup(func() { _ = g.Close() })

	base := runtime.NumGoroutine()
	for i := range 100 {
		rate := 6
		if i%2 == 1 {
			rate = 5
		}
		if err := g.ReloadYAML([]byte(rlReloadDoc(be.URL, "rl", rlBlock("", rate, 0, 100, "ip"), false))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; runtime.NumGoroutine() > base*11/10; i++ {
		if i == 10000 {
			t.Fatalf("goroutines = %d after 100 reloads, baseline %d", runtime.NumGoroutine(), base)
		}
		runtime.Gosched()
	}
}
