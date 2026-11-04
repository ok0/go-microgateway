package upstream

import (
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/state"
)

func hgConfig(hs ...config.Host) config.Upstream {
	cfg := rrBreakerCfg(hs...)
	cfg.Name = "up"
	return cfg
}

func expositionLines(t *testing.T, r *metrics.Registry) []string {
	t.Helper()
	var b strings.Builder
	if err := r.WriteText(&b); err != nil {
		t.Fatal(err)
	}
	return strings.Split(b.String(), "\n")
}

func hostSeries(t *testing.T, r *metrics.Registry) []string {
	t.Helper()
	var out []string
	for _, line := range expositionLines(t, r) {
		if strings.HasPrefix(line, "microgateway_upstream_host_healthy{") || strings.HasPrefix(line, "microgateway_circuit_breaker_state{") {
			out = append(out, line)
		}
	}
	return out
}

func hasSeries(lines []string, series string) bool {
	for _, l := range lines {
		if strings.HasPrefix(l, series+" ") {
			return true
		}
	}
	return false
}

type hgFixture struct {
	reg  *state.Registry
	mreg *metrics.Registry
	m    *Metrics
	d    Deps
}

func newHGFixture(t *testing.T) hgFixture {
	t.Helper()
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	mreg := metrics.NewRegistry()
	m, err := newMetrics(mreg)
	if err != nil {
		t.Fatal(err)
	}
	return hgFixture{reg: reg, mreg: mreg, m: m, d: Deps{Clock: clock.NewFake(testStart), Metrics: m}}
}

const (
	hgHealthyB  = `microgateway_upstream_host_healthy{upstream="up",host="b.test:80"}`
	hgBreakerB  = `microgateway_circuit_breaker_state{upstream="up",host="b.test:80"}`
	hgHealthyA  = `microgateway_upstream_host_healthy{upstream="up",host="a.test:80"}`
	hgAggregate = `microgateway_circuit_breaker_state{upstream="up",host=""}`
)

func TestHostGaugesKeptWhileAnotherGenerationHolds(t *testing.T) {
	f := newHGFixture(t)
	u1, _ := buildDeps(t, f.reg, hgConfig(hosts("http://a.test:80", "http://b.test:80")...), f.d)
	if lines := hostSeries(t, f.mreg); !hasSeries(lines, hgHealthyB) || !hasSeries(lines, hgBreakerB) {
		t.Fatalf("b series missing after first build: %v", lines)
	}

	u2, _ := buildDeps(t, f.reg, hgConfig(hosts("http://a.test:80")...), f.d)
	if lines := hostSeries(t, f.mreg); !hasSeries(lines, hgHealthyB) || !hasSeries(lines, hgBreakerB) {
		t.Fatalf("b series removed while the first generation still holds b: %v", lines)
	}

	f.m.RetainHosts([]*Upstream{u2})
	lines := hostSeries(t, f.mreg)
	if hasSeries(lines, hgHealthyB) || hasSeries(lines, hgBreakerB) {
		t.Fatalf("b series kept after its generation drained: %v", lines)
	}
	if !hasSeries(lines, hgHealthyA) || !hasSeries(lines, `microgateway_circuit_breaker_state{upstream="up",host="a.test:80"}`) {
		t.Fatalf("a series removed: %v", lines)
	}
	if !hasSeries(lines, hgAggregate) {
		t.Fatalf("aggregate breaker series removed: %v", lines)
	}
	if !hasSeries(expositionLines(t, f.mreg), `microgateway_upstream_attempts_total{upstream="up",host="b.test:80",outcome="success"}`) {
		t.Fatal("attempts series of b removed")
	}

	for _, h := range u1.Hosts() {
		if h.URL.Host == "b.test:80" {
			u1.pool.setHealthGauge(h)
		}
	}
	f.m.setBreakerState("up", "b.test:80", 2)
	if lines := hostSeries(t, f.mreg); hasSeries(lines, hgHealthyB) || hasSeries(lines, hgBreakerB) {
		t.Fatalf("retired generation recreated b series: %v", lines)
	}

	u1.Activate()
	if lines := hostSeries(t, f.mreg); hasSeries(lines, hgHealthyB) || hasSeries(lines, hgBreakerB) {
		t.Fatalf("re-activated retired generation recreated b series: %v", lines)
	}
}

func TestHostGaugesReAddedFresh(t *testing.T) {
	f := newHGFixture(t)
	buildDeps(t, f.reg, hgConfig(hosts("http://a.test:80", "http://b.test:80")...), f.d)
	u2, _ := buildDeps(t, f.reg, hgConfig(hosts("http://a.test:80")...), f.d)
	f.m.RetainHosts([]*Upstream{u2})
	if lines := hostSeries(t, f.mreg); hasSeries(lines, hgHealthyB) {
		t.Fatalf("b series kept after drain: %v", lines)
	}

	buildDeps(t, f.reg, hgConfig(hosts("http://a.test:80", "http://b.test:80")...), f.d)
	lines := expositionLines(t, f.mreg)
	if !slices.Contains(lines, hgHealthyB+" 1") {
		t.Fatalf("re-added b host_healthy is not 1: %v", hostSeries(t, f.mreg))
	}
	if !slices.Contains(lines, hgBreakerB+" 0") {
		t.Fatalf("re-added b breaker_state is not 0: %v", hostSeries(t, f.mreg))
	}
}

func TestHostGaugesAggregateNeverGated(t *testing.T) {
	f := newHGFixture(t)
	f.m.setBreakerState("up", "", 2)
	if !slices.Contains(expositionLines(t, f.mreg), hgAggregate+" 2") {
		t.Fatalf("aggregate breaker series not written without a holder: %v", hostSeries(t, f.mreg))
	}
}

func TestHostGaugesConcurrentWrites(t *testing.T) {
	f := newHGFixture(t)
	u := &Upstream{Name: "up"}
	ha := &Host{URL: &url.URL{Scheme: "http", Host: "a.test:80"}}
	hb := &Host{URL: &url.URL{Scheme: "http", Host: "b.test:80"}}
	both := []*Host{ha, hb}
	only := []*Host{ha}

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				for _, host := range []string{"a.test:80", "b.test:80"} {
					f.m.setHostHealthy("up", host, float64(i%2))
					f.m.setBreakerState("up", host, float64(i%3))
				}
			}
		}()
	}
	for i := 0; i < 200; i++ {
		if i%2 == 0 {
			f.m.holdHosts(u, both)
		} else {
			f.m.holdHosts(u, only)
		}
	}
	wg.Wait()

	for _, line := range hostSeries(t, f.mreg) {
		if strings.Contains(line, `host="b.test:80"`) {
			t.Errorf("series of pruned host b still exposed: %s", line)
		}
	}
}

func TestHostGaugesSharedAcrossMetricsOfOneRegistry(t *testing.T) {
	f := newHGFixture(t)
	m1 := f.m
	m2, err := newMetrics(f.mreg)
	if err != nil {
		t.Fatal(err)
	}
	if m1 == m2 || m1.Hosts != m2.Hosts {
		t.Fatalf("metrics of one registry: distinct=%v shared table=%v, want true and true", m1 != m2, m1.Hosts == m2.Hosts)
	}
	buildDeps(t, f.reg, hgConfig(hosts("http://a.test:80", "http://b.test:80")...), Deps{Clock: clock.NewFake(testStart), Metrics: m1})
	u2, _ := buildDeps(t, f.reg, hgConfig(hosts("http://a.test:80")...), Deps{Clock: clock.NewFake(testStart), Metrics: m2})
	if lines := hostSeries(t, f.mreg); !hasSeries(lines, hgHealthyB) || !hasSeries(lines, hgBreakerB) {
		t.Fatalf("b series missing while the first generation holds b: %v", lines)
	}

	m2.RetainHosts([]*Upstream{u2})
	lines := hostSeries(t, f.mreg)
	if hasSeries(lines, hgHealthyB) || hasSeries(lines, hgBreakerB) {
		t.Fatalf("b series kept after its generation drained: %v", lines)
	}
	if !hasSeries(lines, hgHealthyA) {
		t.Fatalf("a host_healthy series removed: %v", lines)
	}
	if !hasSeries(lines, `microgateway_circuit_breaker_state{upstream="up",host="a.test:80"}`) {
		t.Fatalf("a breaker_state series removed: %v", lines)
	}
	if !hasSeries(lines, hgAggregate) {
		t.Fatalf("aggregate breaker series removed: %v", lines)
	}
}

func TestHostGaugesSeparatePerRegistry(t *testing.T) {
	f1 := newHGFixture(t)
	f2 := newHGFixture(t)
	if f1.m.Hosts == f2.m.Hosts {
		t.Fatal("two registries share one host gauge table")
	}
	buildDeps(t, f1.reg, hgConfig(hosts("http://a.test:80", "http://b.test:80")...), f1.d)
	buildDeps(t, f2.reg, hgConfig(hosts("http://a.test:80", "http://b.test:80")...), f2.d)

	f2.m.RetainHosts(nil)
	for _, line := range hostSeries(t, f2.mreg) {
		if strings.Contains(line, `host="a.test:80"`) || strings.Contains(line, `host="b.test:80"`) {
			t.Fatalf("registry without live generations still exposes host series: %s", line)
		}
	}

	lines := hostSeries(t, f1.mreg)
	for _, want := range []string{
		hgHealthyA,
		hgHealthyB,
		`microgateway_circuit_breaker_state{upstream="up",host="a.test:80"}`,
		hgBreakerB,
	} {
		if !hasSeries(lines, want) {
			t.Fatalf("registry with a live generation lost %s: %v", want, lines)
		}
	}
}

func TestBuildRejectsMetricsWithoutHosts(t *testing.T) {
	f := newHGFixture(t)
	mm := *f.m
	mm.Hosts = nil
	tx := f.reg.Begin()
	_, err := Build(hgConfig(hosts("http://a.test:80")...), Deps{State: tx, Clock: clock.NewFake(testStart), Metrics: &mm})
	if err == nil || !strings.Contains(err.Error(), "Metrics.Hosts is nil") {
		tx.Abort()
		t.Fatalf("Build error = %v, want one naming Metrics.Hosts is nil", err)
	}
	tx.Abort()
}
