package microgateway

import (
	"math"
	"net/http"
	"runtime"
	"slices"
	"testing"
)

func stateDoc(hosts []*hostStub) upstreamDoc {
	return upstreamDoc{
		name:    "up",
		urls:    urlsOf(hosts),
		passive: passiveHourFlow,
		active:  activeFlow("/health", "200-399"),
	}
}

func reloadState(t *testing.T, n testNetwork) (*healthRig, <-chan getResult, []*hostStub) {
	t.Helper()
	gate := newHoldGate()
	hosts := make([]*hostStub, 5)
	for i := range hosts {
		hosts[i] = newHostStub(t, n, string(rune('a'+i)), "/health", gate)
	}
	r := newHealthRig(t, n, stateDoc(hosts), hosts, gate)

	hosts[0].setMode(stubHoldBody)
	held := startGet(r.client, r.url+"/api/held")
	awaitSignal(t, gate.arrived, "held request to reach the backend")
	hosts[0].setMode(stubAnswer)

	hosts[2].setStatus(http.StatusServiceUnavailable)
	r.driveUntilEjected(hosts[2])
	hosts[2].setStatus(http.StatusOK)

	hosts[1].setStatus(http.StatusServiceUnavailable)
	r.driveUntilConsecutive(hosts[1], 2)
	hosts[1].setStatus(http.StatusOK)

	hosts[3].setProbeStatus(http.StatusServiceUnavailable)
	for range 3 {
		r.tick(5, 0)
	}
	return r, held, hosts
}

func assertReloadState(t *testing.T, r *healthRig, hosts []*hostStub, remaining float64) {
	t.Helper()
	c := r.host(hosts[2])
	if !c.Ejected || c.EjectionCount != 1 || c.EjectionRemainingSeconds != remaining {
		t.Fatalf("ejected host = %+v, want ejected once with %v seconds remaining", c, remaining)
	}
	if b := r.host(hosts[1]); b.ConsecutiveFailures != 2 {
		t.Fatalf("passive consecutive failures = %d, want 2", b.ConsecutiveFailures)
	}
	if d := r.host(hosts[3]); d.ActiveCheck == nil || d.ActiveCheck.Status != "unhealthy" || d.Healthy {
		t.Fatalf("active host = %+v, want unhealthy", d)
	}
	if a := r.host(hosts[0]); a.InFlight != 1 {
		t.Fatalf("held host in_flight = %d, want 1", a.InFlight)
	}
}

func releaseHeld(t *testing.T, gate *holdGate, held <-chan getResult) {
	t.Helper()
	gate.open()
	if got := awaitGet(t, held).status; got != http.StatusOK {
		t.Fatalf("held status = %d, want 200", got)
	}
}

func TestReloadKeepsHealthState(t *testing.T) {
	n := newMemNetwork()

	t.Run("host weight change", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		doc := stateDoc(hosts)
		doc.weights = []int{0, 0, 0, 0, 2}
		r.reload(doc)
		assertReloadState(t, r, hosts, remaining)
		r.tick(5, 0)
		releaseHeld(t, r.gate, held)
	})

	t.Run("route added", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		doc := stateDoc(hosts)
		doc.addRoute = true
		r.reload(doc)
		assertReloadState(t, r, hosts, remaining)
		r.tick(5, 0)
		releaseHeld(t, r.gate, held)
	})

	t.Run("host added", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		added := newHostStub(t, n, "f", "/health", r.gate)
		doc := stateDoc(append(slices.Clone(hosts), added))
		r.reload(doc)
		assertReloadState(t, r, hosts, remaining)
		r.tick(6, 0)
		releaseHeld(t, r.gate, held)
	})

	t.Run("host removed", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		r.reload(stateDoc(hosts[:4]))
		assertReloadState(t, r, hosts, remaining)
		if got := len(r.status().Hosts); got != 4 {
			t.Fatalf("hosts after removal = %d, want 4", got)
		}
		r.tick(4, 0)
		releaseHeld(t, r.gate, held)
	})
}

func TestReloadHealthDefinitionChange(t *testing.T) {
	n := newMemNetwork()

	t.Run("passive change resets ejection only", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		doc := stateDoc(hosts)
		doc.passive = passiveTwoHourFlow
		r.reload(doc)
		if c := r.host(hosts[2]); c.Ejected || c.EjectionCount != 0 {
			t.Fatalf("host after passive change = %+v, want not ejected", c)
		}
		if d := r.host(hosts[3]); d.ActiveCheck == nil || d.ActiveCheck.Status != "unhealthy" || d.Healthy {
			t.Fatalf("active state after passive change = %+v, want unhealthy", d)
		}
		if a := r.host(hosts[0]); a.InFlight != 1 {
			t.Fatalf("held host in_flight = %d, want 1", a.InFlight)
		}
		releaseHeld(t, r.gate, held)
	})

	t.Run("active change resets active status only", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		remaining := r.host(hosts[2]).EjectionRemainingSeconds
		doc := stateDoc(hosts)
		doc.active = activeFlow("/healthz", "200-399")
		r.reload(doc)
		if c := r.host(hosts[2]); !c.Ejected || c.EjectionRemainingSeconds != remaining {
			t.Fatalf("ejection after active change = %+v, want unchanged", c)
		}
		if d := r.host(hosts[3]); d.ActiveCheck == nil || d.ActiveCheck.Status != "unknown" || !d.Healthy {
			t.Fatalf("active state after active change = %+v, want unknown and healthy", d)
		}
		if b := r.host(hosts[1]); b.ConsecutiveFailures != 2 {
			t.Fatalf("passive consecutive failures = %d, want 2", b.ConsecutiveFailures)
		}
		if a := r.host(hosts[0]); a.InFlight != 1 {
			t.Fatalf("held host in_flight = %d, want 1", a.InFlight)
		}
		releaseHeld(t, r.gate, held)
	})

	t.Run("rename resets all", func(t *testing.T) {
		r, held, hosts := reloadState(t, n)
		doc := stateDoc(hosts)
		doc.name = "up2"
		r.reload(doc)
		st := upstreamStatus(t, r.g, "up2")
		if c := hostByURL(st, hosts[2].URL); c.Ejected || c.EjectionCount != 0 {
			t.Fatalf("host after rename = %+v, want not ejected", c)
		}
		if d := hostByURL(st, hosts[3].URL); d.ActiveCheck == nil || d.ActiveCheck.Status != "unknown" || !d.Healthy {
			t.Fatalf("active state after rename = %+v, want unknown and healthy", d)
		}
		if b := hostByURL(st, hosts[1].URL); b.ConsecutiveFailures != 0 {
			t.Fatalf("passive consecutive failures after rename = %d, want 0", b.ConsecutiveFailures)
		}
		if a := hostByURL(st, hosts[0].URL); a.InFlight != 0 {
			t.Fatalf("in_flight after rename = %d, want 0", a.InFlight)
		}
		releaseHeld(t, r.gate, held)
	})
}

func TestFailedReloadKeepsHealth(t *testing.T) {
	n := newMemNetwork()
	r, held, hosts := reloadState(t, n)
	before := adminBody(t, r.g, "/upstreams")

	if err := r.g.ReloadYAML([]byte("version: 2\n")); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	doc := stateDoc(hosts)
	doc.badMiddleware = true
	if err := r.g.ReloadYAML([]byte(doc.yaml())); err == nil {
		t.Fatal("reload with an unknown middleware type succeeded")
	}

	if after := adminBody(t, r.g, "/upstreams"); after != before {
		t.Fatalf("admin state changed after failed reloads:\n%s\n%s", before, after)
	}
	r.tick(5, 0)
	releaseHeld(t, r.gate, held)
}

func TestHundredReloadsNoGoroutineLeak(t *testing.T) {
	n := newMemNetwork()
	gate := newHoldGate()
	hosts := []*hostStub{
		newHostStub(t, n, "a", "/health", gate),
		newHostStub(t, n, "b", "/health", gate),
	}
	doc := upstreamDoc{name: "up", urls: urlsOf(hosts), active: activeFlow("/health", "200-399")}
	r := newHealthRig(t, n, doc, hosts, gate)
	r.tick(2, 0)
	baseline := runtime.NumGoroutine()

	for i := range 100 {
		path := "/health"
		if i%2 == 0 {
			path = "/healthz"
		}
		doc.active = activeFlow(path, "200-399")
		r.reload(doc)
	}
	eventually(t, func() bool {
		return math.Abs(float64(runtime.NumGoroutine()-baseline)) <= 0.1*float64(baseline)
	})
	if got := r.fake.Waiters(); got != len(hosts) {
		t.Fatalf("pending timers after reloads = %d, want %d live probers", got, len(hosts))
	}
	r.tick(2, 0)
}
