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
