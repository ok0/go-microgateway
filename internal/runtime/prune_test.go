package runtime

import (
	"log/slog"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/teststore"
	"github.com/ok0/go-microgateway/store"
)

const pruneListeners = "version: 1\nlisteners:\n  - name: public\n    address: \":8080\"\n"

const pruneOld = pruneListeners + `upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
  - name: extra
    hosts:
      - url: "http://127.0.0.1:9002"
stores:
  - name: s
    type: test
routes:
  - name: a
    match:
      path: /a
    upstream: users
    rate_limit:
      rate: 5
      per: 1s
      key:
        source: ip
  - name: b
    match:
      path: /b
    upstream: extra
    rate_limit:
      rate: 5
      per: 1s
      key:
        source: ip
  - name: c
    match:
      path: /c
    upstream: users
    rate_limit:
      rate: 5
      per: 1s
      key:
        source: ip
      store: s
`

const pruneNew = pruneListeners + `upstreams:
  - name: users
    hosts:
      - url: "http://127.0.0.1:9001"
routes:
  - name: a
    match:
      path: /a
    upstream: users
    rate_limit:
      rate: 5
      per: 1s
      key:
        source: ip
`

const pruneHostsBoth = pruneListeners + `upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:9001"
      - url: "http://127.0.0.1:9002"
    circuit_breaker: {}
routes:
  - name: r
    match:
      path: /r
    upstream: up
`

const pruneHostsA = pruneListeners + `upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:9001"
    circuit_breaker: {}
routes:
  - name: r
    match:
      path: /r
    upstream: up
`

func stExposition(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	var b strings.Builder
	if err := reg.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func TestPruneGauges(t *testing.T) {
	fake := clock.NewFake(stStart)
	shared := teststore.NewShared(fake)
	t.Cleanup(func() { _ = shared.Close() })
	set := NewStoreSet(map[string]StoreType{
		stTypeName: {Caps: store.CapRateLimit, Build: func(*config.Store, *slog.Logger) (store.Backend, error) {
			return shared.Handle(), nil
		}},
	})
	reg := metrics.NewRegistry()
	deps := Deps{Clock: fake, Metrics: reg, Stores: set}

	oldSnap, _, err := build(t, mustParse(t, pruneOld), deps)
	if err != nil {
		t.Fatal(err)
	}
	newSnap, _, err := build(t, mustParse(t, pruneNew), deps)
	if err != nil {
		t.Fatal(err)
	}

	gauges := []string{
		`microgateway_ratelimit_keys{route="b"}`,
		`microgateway_upstream_panic_mode{upstream="extra"}`,
		`microgateway_store_available{store="s"}`,
	}
	for _, want := range gauges {
		if text := stExposition(t, reg); !strings.Contains(text, want) {
			t.Fatalf("series %s missing before prune", want)
		}
	}

	PruneGauges([]*Snapshot{oldSnap, newSnap})
	for _, want := range gauges {
		if text := stExposition(t, reg); !strings.Contains(text, want) {
			t.Errorf("series %s removed while live snapshots still own it", want)
		}
	}

	PruneGauges([]*Snapshot{newSnap})
	text := stExposition(t, reg)
	for _, gone := range gauges {
		if strings.Contains(text, gone) {
			t.Errorf("series %s still exposed after prune", gone)
		}
	}
	kept := []string{
		`microgateway_ratelimit_keys{route="a"}`,
		`microgateway_rate_limited_total{route="b"}`,
		`microgateway_ratelimit_store_fallback_total{route="c",policy="local"}`,
		`microgateway_store_errors_total{store="s",op="take"}`,
	}
	for _, want := range kept {
		if !strings.Contains(text, want) {
			t.Errorf("series %s missing after prune", want)
		}
	}

	PruneGauges(nil)
}

func TestPruneGaugesHosts(t *testing.T) {
	fake := clock.NewFake(stStart)
	reg := metrics.NewRegistry()
	deps := Deps{Clock: fake, Metrics: reg}

	s1, _, err := build(t, mustParse(t, pruneHostsBoth), deps)
	if err != nil {
		t.Fatal(err)
	}
	s2, _, err := build(t, mustParse(t, pruneHostsA), deps)
	if err != nil {
		t.Fatal(err)
	}

	const (
		bHealthy = `microgateway_upstream_host_healthy{upstream="up",host="127.0.0.1:9002"}`
		bBreaker = `microgateway_circuit_breaker_state{upstream="up",host="127.0.0.1:9002"}`
		bAttempt = `microgateway_upstream_attempts_total{upstream="up",host="127.0.0.1:9002",outcome="success"}`
		aHealthy = `microgateway_upstream_host_healthy{upstream="up",host="127.0.0.1:9001"}`
		aBreaker = `microgateway_circuit_breaker_state{upstream="up",host="127.0.0.1:9001"}`
		aggr     = `microgateway_circuit_breaker_state{upstream="up",host=""}`
	)

	PruneGauges([]*Snapshot{s1, s2})
	text := stExposition(t, reg)
	for _, want := range []string{bHealthy, bBreaker, aHealthy, aBreaker, aggr} {
		if !strings.Contains(text, want) {
			t.Errorf("series %s removed while the generation holding it is live", want)
		}
	}

	PruneGauges([]*Snapshot{s2})
	text = stExposition(t, reg)
	for _, gone := range []string{bHealthy, bBreaker} {
		if strings.Contains(text, gone) {
			t.Errorf("series %s still exposed after its host left the live upstream", gone)
		}
	}
	for _, want := range []string{aHealthy, aBreaker, aggr, bAttempt} {
		if !strings.Contains(text, want) {
			t.Errorf("series %s missing after prune", want)
		}
	}
}

func TestRegisterSeriesSharesHostTable(t *testing.T) {
	reg := metrics.NewRegistry()
	a, err := RegisterSeries(reg)
	if err != nil {
		t.Fatal(err)
	}
	b, err := RegisterSeries(reg)
	if err != nil {
		t.Fatal(err)
	}
	if a.Upstream == b.Upstream || a.Upstream.Hosts != b.Upstream.Hosts || a.Upstream.Hosts == nil {
		t.Fatalf("same registry: distinct metrics=%v shared table=%v, want true and true with a non-nil table", a.Upstream != b.Upstream, a.Upstream.Hosts == b.Upstream.Hosts)
	}
	c, err := RegisterSeries(metrics.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	if c.Upstream.Hosts == a.Upstream.Hosts {
		t.Fatal("two registries share one host gauge table")
	}
}

func TestPruneGaugesHostsPerRegistry(t *testing.T) {
	const (
		bHealthy = `microgateway_upstream_host_healthy{upstream="up",host="127.0.0.1:9002"}`
		bBreaker = `microgateway_circuit_breaker_state{upstream="up",host="127.0.0.1:9002"}`
		aHealthy = `microgateway_upstream_host_healthy{upstream="up",host="127.0.0.1:9001"}`
		aBreaker = `microgateway_circuit_breaker_state{upstream="up",host="127.0.0.1:9001"}`
	)
	reg1 := metrics.NewRegistry()
	reg2 := metrics.NewRegistry()
	if _, _, err := build(t, mustParse(t, pruneHostsBoth), Deps{Clock: clock.NewFake(stStart), Metrics: reg1}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := build(t, mustParse(t, pruneHostsBoth), Deps{Clock: clock.NewFake(stStart), Metrics: reg2}); err != nil {
		t.Fatal(err)
	}
	q2, _, err := build(t, mustParse(t, pruneHostsA), Deps{Clock: clock.NewFake(stStart), Metrics: reg2})
	if err != nil {
		t.Fatal(err)
	}

	PruneGauges([]*Snapshot{q2})
	text2 := stExposition(t, reg2)
	for _, gone := range []string{bHealthy, bBreaker} {
		if strings.Contains(text2, gone) {
			t.Errorf("registry of the pruned snapshot still exposes %s", gone)
		}
	}
	text1 := stExposition(t, reg1)
	for _, want := range []string{bHealthy, bBreaker, aHealthy, aBreaker} {
		if !strings.Contains(text1, want) {
			t.Errorf("other registry lost %s", want)
		}
	}
}
