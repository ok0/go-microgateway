package upstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/breaker"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/health"
	"github.com/ok0/go-microgateway/internal/state"
)

type fakeResolver struct {
	mu      sync.Mutex
	answers []string
	err     error
	calls   int
}

func (r *fakeResolver) LookupHost(_ context.Context, _ string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return nil, r.err
	}
	return append([]string(nil), r.answers...), nil
}

func (r *fakeResolver) set(answers []string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.answers = answers
	r.err = err
}

func dnsUpstream() config.Upstream {
	return config.Upstream{
		Name: "u",
		DNS: &config.DNS{
			Hostname:        "svc.test",
			Port:            8080,
			Scheme:          "http",
			RefreshInterval: dur(30 * time.Second),
		},
		PassiveHealth: &config.PassiveHealth{
			ConsecutiveFailures: 1,
			BaseEjectionTime:    dur(time.Hour),
			MaxEjectionTime:     dur(time.Hour),
			MaxEjectionPercent:  intp(100),
		},
	}
}

func TestDNSInitialSet(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	res := &fakeResolver{answers: []string{"10.0.0.9", "10.0.0.2", "10.0.0.5", "10.0.0.2"}}
	u, _ := buildDeps(t, reg, dnsUpstream(), Deps{Clock: clock.NewFake(testStart), Resolver: res})

	want := []string{"http://10.0.0.2:8080", "http://10.0.0.5:8080", "http://10.0.0.9:8080"}
	if got := hostURLs(u); !slices.Equal(got, want) {
		t.Fatalf("hosts = %v, want %v", got, want)
	}
	for _, h := range u.Hosts() {
		if h.Weight != 1 {
			t.Errorf("host %s weight = %d, want 1", h.URL, h.Weight)
		}
	}
}

func TestDNSRefresh(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	res := &fakeResolver{answers: []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}}
	u, _ := buildDeps(t, reg, dnsUpstream(), Deps{Clock: fake, Resolver: res})

	a := hostByURL(t, u, "http://10.0.0.1:8080")
	b := hostByURL(t, u, "http://10.0.0.2:8080")
	b.Begin()
	a.Begin()
	u.Record(b, breaker.Token{}, health.Failure)
	if !b.Ejected() {
		t.Fatal("setup: host b not ejected")
	}

	res.set([]string{"10.0.0.4", "10.0.0.3", "10.0.0.2"}, nil)
	blockUntil(t, fake, 1, "DNS refresh timer")
	fake.Advance(30 * time.Second)
	want := []string{"http://10.0.0.2:8080", "http://10.0.0.3:8080", "http://10.0.0.4:8080"}
	eventually(t, "refreshed host set", func() bool { return slices.Equal(hostURLs(u), want) })

	b2 := hostByURL(t, u, "http://10.0.0.2:8080")
	if got := b2.InFlight(); got != 1 {
		t.Errorf("in-flight of unchanged host = %d, want 1", got)
	}
	if !b2.Ejected() {
		t.Error("ejection of unchanged host was not kept")
	}

	a.Done()
	if got := a.InFlight(); got != 0 {
		t.Errorf("removed host in-flight after Done = %d, want 0", got)
	}
}

func TestDNSFailureKeepsLastGood(t *testing.T) {
	fake := clock.NewFake(testStart)
	m := newTestMetrics(t)
	var logs syncBuf
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	res := &fakeResolver{answers: []string{"10.0.0.1", "10.0.0.2"}}
	u, _ := buildDeps(t, reg, dnsUpstream(), Deps{Clock: fake, Resolver: res, Metrics: m, Logger: logger})
	want := []string{"http://10.0.0.1:8080", "http://10.0.0.2:8080"}

	res.set(nil, errors.New("servfail"))
	blockUntil(t, fake, 1, "DNS refresh timer")
	fake.Advance(30 * time.Second)
	eventually(t, "lookup failure logged", func() bool {
		return strings.Contains(logs.String(), "upstream dns lookup failed")
	})
	if got := m.DNSRefresh.With("u", "error").Value(); got != 1 {
		t.Errorf("error refreshes = %v, want 1", got)
	}
	if got := hostURLs(u); !slices.Equal(got, want) {
		t.Fatalf("hosts after failure = %v, want %v", got, want)
	}

	res.set([]string{}, nil)
	blockUntil(t, fake, 1, "DNS refresh timer")
	fake.Advance(30 * time.Second)
	eventually(t, "empty answer logged", func() bool {
		return strings.Contains(logs.String(), "upstream dns lookup returned no addresses")
	})
	if got := m.DNSRefresh.With("u", "empty").Value(); got != 1 {
		t.Errorf("empty refreshes = %v, want 1", got)
	}
	if got := hostURLs(u); !slices.Equal(got, want) {
		t.Fatalf("hosts after empty answer = %v, want %v", got, want)
	}
}

func TestDNSFirstLookupFails(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	res := &fakeResolver{err: errors.New("nxdomain")}
	u, _ := buildDeps(t, reg, dnsUpstream(), Deps{Clock: fake, Resolver: res})

	if _, _, err := u.Acquire(nil, nil, false); !errors.Is(err, ErrNoHosts) {
		t.Fatalf("Acquire before first success: err = %v, want ErrNoHosts", err)
	}

	res.set([]string{"10.0.0.7"}, nil)
	blockUntil(t, fake, 1, "DNS refresh timer")
	fake.Advance(30 * time.Second)
	eventually(t, "hosts after recovery", func() bool { return len(u.Hosts()) == 1 })
	if got := hostURLs(u); !slices.Equal(got, []string{"http://10.0.0.7:8080"}) {
		t.Fatalf("hosts = %v, want 10.0.0.7", got)
	}
	if _, _, err := u.Acquire(nil, nil, false); err != nil {
		t.Fatalf("Acquire after recovery: %v", err)
	}
}

func TestDNSCap(t *testing.T) {
	answers := make([]string, 0, 300)
	for i := 0; i < 300; i++ {
		answers = append(answers, fmt.Sprintf("10.2.%d.%d", i/250, i%250+1))
	}
	var logs syncBuf
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, dnsUpstream(), Deps{
		Clock:    clock.NewFake(testStart),
		Resolver: &fakeResolver{answers: answers},
		Logger:   slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if got := len(u.Hosts()); got != maxDNSHosts {
		t.Fatalf("hosts = %d, want %d", got, maxDNSHosts)
	}
	if last := u.Hosts()[len(u.Hosts())-1].URL.String(); last != "http://10.2.1.6:8080" {
		t.Errorf("last host = %s, want the 256th sorted address", last)
	}
	if !strings.Contains(logs.String(), "upstream dns answer truncated") {
		t.Error("missing truncation log")
	}
}

func TestDNSInvalidAnswersSkipped(t *testing.T) {
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	res := &fakeResolver{answers: []string{"not-an-ip", "10.3.0.1", "::zz", "2001:db8::1"}}
	u, _ := buildDeps(t, reg, dnsUpstream(), Deps{Clock: clock.NewFake(testStart), Resolver: res})
	want := []string{"http://10.3.0.1:8080", "http://[2001:db8::1]:8080"}
	if got := hostURLs(u); !slices.Equal(got, want) {
		t.Fatalf("hosts = %v, want %v", got, want)
	}
}

func TestDNSStaleWatcherIgnored(t *testing.T) {
	fake := clock.NewFake(testStart)
	m := newTestMetrics(t)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)

	res1 := &fakeResolver{answers: []string{"10.0.0.1"}}
	u1, l1 := buildDeps(t, reg, dnsUpstream(), Deps{Clock: fake, Resolver: res1, Metrics: m})
	old := u1.dns

	changed := dnsUpstream()
	changed.DNS.RefreshInterval = dur(60 * time.Second)
	res2 := &fakeResolver{answers: []string{"10.0.0.2"}}
	u2, _ := buildDeps(t, reg, changed, Deps{Clock: fake, Resolver: res2, Metrics: m})
	if u2.dns == old {
		t.Fatal("changed DNS block reused the watcher")
	}

	res1.set([]string{"10.0.0.9"}, nil)
	blockUntil(t, fake, 2, "DNS refresh timers")
	fake.Advance(30 * time.Second)
	eventually(t, "stale watcher refresh", func() bool {
		return m.DNSRefresh.With("u", "success").Value() == 3
	})
	if got := hostURLs(u2); !slices.Equal(got, []string{"http://10.0.0.2:8080"}) {
		t.Fatalf("new generation hosts = %v, want 10.0.0.2", got)
	}

	if err := l1.Release(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-old.done:
	default:
		t.Fatal("old watcher goroutine still running after its lease was released")
	}
}

func TestDNSPerInstance(t *testing.T) {
	fake := clock.NewFake(testStart)
	regA := state.NewRegistry(nil)
	t.Cleanup(regA.Close)
	regB := state.NewRegistry(nil)
	t.Cleanup(regB.Close)
	resA := &fakeResolver{answers: []string{"10.4.0.1"}}
	resB := &fakeResolver{answers: []string{"10.4.0.2"}}
	uA, _ := buildDeps(t, regA, dnsUpstream(), Deps{Clock: fake, Resolver: resA})
	uB, _ := buildDeps(t, regB, dnsUpstream(), Deps{Clock: fake, Resolver: resB})

	if got := hostURLs(uA); !slices.Equal(got, []string{"http://10.4.0.1:8080"}) {
		t.Fatalf("instance A hosts = %v", got)
	}
	if got := hostURLs(uB); !slices.Equal(got, []string{"http://10.4.0.2:8080"}) {
		t.Fatalf("instance B hosts = %v", got)
	}

	resA.set([]string{"10.4.0.9"}, nil)
	blockUntil(t, fake, 2, "DNS refresh timers")
	fake.Advance(30 * time.Second)
	eventually(t, "instance A refresh", func() bool {
		return slices.Equal(hostURLs(uA), []string{"http://10.4.0.9:8080"})
	})
	if got := hostURLs(uB); !slices.Equal(got, []string{"http://10.4.0.2:8080"}) {
		t.Fatalf("instance B hosts changed to %v", got)
	}
}

func TestDNSWatcherCloseStopsTimer(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	u, _ := buildDeps(t, reg, dnsUpstream(), Deps{Clock: fake, Resolver: &fakeResolver{answers: []string{"10.5.0.1"}}})
	blockUntil(t, fake, 1, "DNS refresh timer")
	if err := u.dns.Close(); err != nil {
		t.Fatal(err)
	}
	if n := fake.Waiters(); n != 0 {
		t.Fatalf("Waiters() = %d after Close, want 0", n)
	}
}

func TestDNSBreakerMembership(t *testing.T) {
	fake := clock.NewFake(testStart)
	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	cb := cbCfg()
	cb.OpenTimeout = dur(time.Hour)
	cfg := dnsUpstream()
	cfg.CircuitBreaker = &cb
	res := &fakeResolver{answers: []string{"10.0.0.1", "10.0.0.2"}}
	u, _ := buildDeps(t, reg, cfg, Deps{Clock: fake, Resolver: res})

	kept := hostByURL(t, u, "http://10.0.0.2:8080")
	trip(t, u, kept)
	keptBreaker := kept.Breaker

	res.set([]string{"10.0.0.2", "10.0.0.3"}, nil)
	blockUntil(t, fake, 1, "DNS refresh timer")
	fake.Advance(30 * time.Second)
	want := []string{"http://10.0.0.2:8080", "http://10.0.0.3:8080"}
	eventually(t, "refreshed host set", func() bool { return slices.Equal(hostURLs(u), want) })

	if got := hostByURL(t, u, "http://10.0.0.2:8080").Breaker; got != keptBreaker {
		t.Fatal("kept address got a new breaker")
	}
	if st := keptBreaker.State(); st != breaker.Open {
		t.Errorf("kept address breaker state = %v, want open", st)
	}
	added := hostByURL(t, u, "http://10.0.0.3:8080").Breaker
	if added == nil || added.State() != breaker.Closed {
		t.Errorf("new address breaker = %v, want a closed breaker", added)
	}
	u.brk.mu.Lock()
	_, stale := u.brk.byKey["http://10.0.0.1:8080"]
	u.brk.mu.Unlock()
	if stale {
		t.Error("removed address kept its breaker")
	}
}
