package health

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

var healthStart = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

type ejection struct {
	key   string
	d     time.Duration
	count int
}

type eventLog struct {
	mu       sync.Mutex
	ejected  []ejection
	restored []string
	refused  []string
}

func (l *eventLog) events() Events {
	return Events{
		Ejected: func(key string, d time.Duration, count int) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.ejected = append(l.ejected, ejection{key: key, d: d, count: count})
		},
		Restored: func(key, reason string) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.restored = append(l.restored, key+":"+reason)
		},
		Refused: func(key string) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.refused = append(l.refused, key)
		},
	}
}

func (l *eventLog) snapshot() ([]ejection, []string, []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]ejection(nil), l.ejected...), append([]string(nil), l.restored...), append([]string(nil), l.refused...)
}

func intp(v int) *int {
	return &v
}

func passiveCfg(threshold int, base, maxEject time.Duration, percent *int) config.PassiveHealth {
	return config.PassiveHealth{
		ConsecutiveFailures: threshold,
		BaseEjectionTime:    config.Duration(base),
		MaxEjectionTime:     config.Duration(maxEject),
		MaxEjectionPercent:  percent,
	}
}

func members(keys ...string) []Member {
	ms := make([]Member, 0, len(keys))
	for _, k := range keys {
		ms = append(ms, Member{Key: k, Weight: 1})
	}
	return ms
}

func newTestEjector(cfg config.PassiveHealth, clk clock.Clock, ms []Member) (*Ejector, *eventLog) {
	log := &eventLog{}
	e := NewEjector(cfg, clk, log.events())
	e.SetMembers(ms)
	return e, log
}

func TestEjectAfterConsecutiveFailures(t *testing.T) {
	clk := clock.NewFake(healthStart)
	e, log := newTestEjector(passiveCfg(3, 30*time.Second, 300*time.Second, intp(100)), clk, members("a", "b"))

	a := e.Record("a")
	a.Observe(Failure)
	a.Observe(Failure)
	a.Observe(Ignored)
	if a.Ejected() {
		t.Fatal("ignored outcome must not complete an ejection")
	}
	if ej, _, _ := log.snapshot(); len(ej) != 0 {
		t.Fatalf("ejected events = %d before threshold, want 0", len(ej))
	}
	a.Observe(Failure)
	if !a.Ejected() {
		t.Fatal("third consecutive failure must eject")
	}
	ej, _, _ := log.snapshot()
	if len(ej) != 1 || ej[0].key != "a" {
		t.Fatalf("ejected events = %+v, want one event for a", ej)
	}
	a.Observe(Failure)
	if ej, _, _ := log.snapshot(); len(ej) != 1 {
		t.Fatalf("outcomes during ejection must not produce events, got %d", len(ej))
	}
	st := a.Status()
	if !st.Ejected || st.Remaining != 30*time.Second || st.EjectionCount != 1 {
		t.Fatalf("status = %+v", st)
	}

	b := e.Record("b")
	b.Observe(Failure)
	b.Observe(Failure)
	b.Observe(Success)
	b.Observe(Failure)
	b.Observe(Failure)
	if b.Ejected() {
		t.Fatal("success must reset the consecutive failure count")
	}
	b.Observe(Failure)
	if !b.Ejected() {
		t.Fatal("third consecutive failure after reset must eject")
	}
	if ej, _, _ := log.snapshot(); len(ej) != 2 {
		t.Fatalf("ejected events = %d, want 2", len(ej))
	}
}

func TestEjectionGrowthAndCap(t *testing.T) {
	clk := clock.NewFake(healthStart)
	e, log := newTestEjector(passiveCfg(1, 30*time.Second, 120*time.Second, intp(100)), clk, members("a"))
	a := e.Record("a")
	want := []time.Duration{30 * time.Second, 60 * time.Second, 90 * time.Second, 120 * time.Second, 120 * time.Second}
	for i, w := range want {
		a.Observe(Failure)
		ej, _, _ := log.snapshot()
		if len(ej) != i+1 || ej[i].d != w || ej[i].count != i+1 {
			t.Fatalf("cycle %d: ejections = %+v, want duration %v count %d", i, ej, w, i+1)
		}
		if st := a.Status(); !st.Ejected || st.Remaining != w {
			t.Fatalf("cycle %d: status = %+v", i, st)
		}
		clk.Advance(w)
		if a.Ejected() {
			t.Fatalf("cycle %d: ejection must expire after %v", i, w)
		}
	}
	_, restored, _ := log.snapshot()
	if len(restored) != len(want) {
		t.Fatalf("restored events = %v, want %d expired", restored, len(want))
	}
	for _, r := range restored {
		if r != "a:expired" {
			t.Fatalf("restored event = %q, want a:expired", r)
		}
	}
}

func TestEjectionDecay(t *testing.T) {
	clk := clock.NewFake(healthStart)
	e, log := newTestEjector(passiveCfg(1, 30*time.Second, 60*time.Second, intp(100)), clk, members("a"))
	a := e.Record("a")

	a.Observe(Failure)
	clk.Advance(30 * time.Second)
	a.Ejected()
	a.Observe(Failure)
	clk.Advance(60 * time.Second)
	a.Ejected()

	clk.Advance(60 * time.Second)
	a.Observe(Failure)
	ej, _, _ := log.snapshot()
	if last := ej[len(ej)-1]; last.d != 60*time.Second || last.count != 2 {
		t.Fatalf("after one max_ejection_time the next ejection = %+v, want 60s count 2", last)
	}
	clk.Advance(60 * time.Second)
	a.Ejected()

	clk.Advance(3 * 60 * time.Second)
	a.Observe(Failure)
	ej, _, _ = log.snapshot()
	if last := ej[len(ej)-1]; last.d != 30*time.Second || last.count != 1 {
		t.Fatalf("after three max_ejection_time the next ejection = %+v, want 30s count 1", last)
	}
}

func TestMaxEjectionPercent(t *testing.T) {
	t.Run("half of four", func(t *testing.T) {
		clk := clock.NewFake(healthStart)
		e, log := newTestEjector(passiveCfg(1, 30*time.Second, 300*time.Second, intp(50)), clk, members("a", "b", "c", "d"))
		e.Record("a").Observe(Failure)
		e.Record("b").Observe(Failure)
		e.Record("c").Observe(Failure)
		ej, _, refused := log.snapshot()
		if len(ej) != 2 {
			t.Fatalf("ejections = %d, want 2", len(ej))
		}
		if len(refused) != 1 || refused[0] != "c" {
			t.Fatalf("refused = %v, want [c]", refused)
		}
		c := e.Record("c")
		if c.Ejected() {
			t.Fatal("refused host must not be ejected")
		}
		if st := c.Status(); st.ConsecutiveFailures != 1 {
			t.Fatalf("refused host failure count = %d, want 1 (not reset)", st.ConsecutiveFailures)
		}
	})

	t.Run("zero disables ejection", func(t *testing.T) {
		clk := clock.NewFake(healthStart)
		e, log := newTestEjector(passiveCfg(1, 30*time.Second, 300*time.Second, intp(0)), clk, members("a", "b"))
		e.Record("a").Observe(Failure)
		e.Record("b").Observe(Failure)
		ej, _, refused := log.snapshot()
		if len(ej) != 0 || len(refused) != 0 {
			t.Fatalf("ejected = %v refused = %v, want none", ej, refused)
		}
	})

	t.Run("ten percent of four allows one", func(t *testing.T) {
		clk := clock.NewFake(healthStart)
		e, log := newTestEjector(passiveCfg(1, 30*time.Second, 300*time.Second, intp(10)), clk, members("a", "b", "c", "d"))
		e.Record("a").Observe(Failure)
		e.Record("b").Observe(Failure)
		ej, _, refused := log.snapshot()
		if len(ej) != 1 || ej[0].key != "a" || len(refused) != 1 || refused[0] != "b" {
			t.Fatalf("ejected = %+v refused = %v", ej, refused)
		}
	})

	t.Run("zero weight members are not counted", func(t *testing.T) {
		clk := clock.NewFake(healthStart)
		e, log := newTestEjector(passiveCfg(1, 30*time.Second, 300*time.Second, intp(50)), clk, nil)
		e.SetMembers([]Member{{Key: "a", Weight: 1}, {Key: "b", Weight: 1}, {Key: "c", Weight: 0}, {Key: "d", Weight: 0}})
		e.Record("a").Observe(Failure)
		e.Record("b").Observe(Failure)
		ej, _, refused := log.snapshot()
		if len(ej) != 1 || ej[0].key != "a" || len(refused) != 1 || refused[0] != "b" {
			t.Fatalf("ejected = %+v refused = %v", ej, refused)
		}
	})
}

func TestClear(t *testing.T) {
	clk := clock.NewFake(healthStart)
	e, log := newTestEjector(passiveCfg(1, 30*time.Second, 300*time.Second, intp(100)), clk, members("a"))
	a := e.Record("a")
	a.Observe(Failure)
	clk.Advance(10 * time.Second)

	a.Clear()
	if a.Ejected() {
		t.Fatal("Clear must end the ejection")
	}
	_, restored, _ := log.snapshot()
	if len(restored) != 1 || restored[0] != "a:active_check" {
		t.Fatalf("restored = %v, want [a:active_check]", restored)
	}
	if st := a.Status(); st.Ejected || st.EjectionCount != 1 {
		t.Fatalf("status after clear = %+v", st)
	}

	clk.Advance(300 * time.Second)
	if st := a.Status(); st.EjectionCount != 0 {
		t.Fatalf("ejection count = %d, want 0 after one max_ejection_time since clear", st.EjectionCount)
	}

	a.Clear()
	if _, restored, _ := log.snapshot(); len(restored) != 1 {
		t.Fatalf("Clear on a host that is not ejected must not fire, restored = %v", restored)
	}
}

func TestSetMembers(t *testing.T) {
	clk := clock.NewFake(healthStart)
	e, log := newTestEjector(passiveCfg(1, 30*time.Second, 300*time.Second, intp(100)), clk, members("a", "b"))
	ra := e.Record("a")
	ra.Observe(Failure)
	if !ra.Ejected() {
		t.Fatal("a must be ejected before membership change")
	}

	e.SetMembers(members("a", "c"))
	if e.Record("a") != ra {
		t.Fatal("retained key must keep the same record")
	}
	if !e.Record("a").Ejected() {
		t.Fatal("retained key must keep its ejection state")
	}
	if e.Record("b") != nil {
		t.Fatal("removed key must have no record")
	}
	if e.Record("c") == nil {
		t.Fatal("added key must have a record")
	}

	e.SetMembers(members("b"))
	if e.Record("a") != nil {
		t.Fatal("removed key must have no record")
	}
	ra.Observe(Failure)
	if ej, _, _ := log.snapshot(); len(ej) != 1 {
		t.Fatalf("stale record must not eject, events = %d", len(ej))
	}

	e.SetMembers(members("a"))
	fresh := e.Record("a")
	if fresh == ra {
		t.Fatal("re-added key must get a fresh record")
	}
	if fresh.Ejected() {
		t.Fatal("re-added key must start without an ejection")
	}
}

func TestConcurrentObserve(t *testing.T) {
	clk := clock.NewFake(healthStart)
	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7"}
	e, log := newTestEjector(passiveCfg(1, 30*time.Second, 300*time.Second, intp(50)), clk, members(keys...))

	var wg sync.WaitGroup
	for g := 0; g < 64; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			r := e.Record(keys[g%len(keys)])
			for i := 0; i < 50; i++ {
				r.Observe(Failure)
			}
		}(g)
	}
	wg.Wait()

	ej, _, _ := log.snapshot()
	if len(ej) != 4 {
		t.Fatalf("ejections = %d, want exactly the allowed 4", len(ej))
	}
	seen := map[string]bool{}
	for _, x := range ej {
		if seen[x.key] {
			t.Fatalf("host %s ejected twice", x.key)
		}
		seen[x.key] = true
	}
}

func TestNewEjectorDefaults(t *testing.T) {
	clk := clock.NewFake(healthStart)
	e := NewEjector(config.PassiveHealth{}, clk, Events{})
	if e.threshold != 5 || e.base != 30*time.Second || e.maxEject != 300*time.Second || e.maxPercent != 50 {
		t.Fatalf("defaults = threshold %d base %v max %v percent %d", e.threshold, e.base, e.maxEject, e.maxPercent)
	}

	e = NewEjector(config.PassiveHealth{BaseEjectionTime: config.Duration(60 * time.Second), MaxEjectionTime: config.Duration(30 * time.Second)}, clk, Events{})
	if e.maxEject != 60*time.Second {
		t.Fatalf("max ejection time below base = %v, want base", e.maxEject)
	}

	e, _ = newTestEjector(passiveCfg(1, 30*time.Second, 300*time.Second, intp(100)), clk, members("a"))
	a := e.Record("a")
	a.Observe(Failure)
	clk.Advance(30 * time.Second)
	if a.Ejected() {
		t.Fatal("ejection must expire without callbacks configured")
	}
	a.Observe(Failure)
	if !a.Ejected() {
		t.Fatal("ejection must still apply without callbacks configured")
	}
	a.Clear()
	e.Record("a").Observe(Failure)
	if !a.Ejected() {
		t.Fatal("ejection must apply again after clear")
	}
	e.SetMembers(nil)
	if e.Record("a") != nil {
		t.Fatal("empty membership must have no records")
	}
}

func TestConstructorsTakeNoSharedHandle(t *testing.T) {
	ejectorType := reflect.TypeOf(NewEjector)
	if ejectorType.NumIn() != 3 {
		t.Fatalf("NewEjector takes %d parameters, want 3", ejectorType.NumIn())
	}
	if ejectorType.In(0) != reflect.TypeOf(config.PassiveHealth{}) {
		t.Fatalf("NewEjector parameter 0 = %v", ejectorType.In(0))
	}
	if ejectorType.In(1) != reflect.TypeOf((*clock.Clock)(nil)).Elem() {
		t.Fatalf("NewEjector parameter 1 = %v", ejectorType.In(1))
	}
	if ejectorType.In(2) != reflect.TypeOf(Events{}) {
		t.Fatalf("NewEjector parameter 2 = %v", ejectorType.In(2))
	}

	checkerType := reflect.TypeOf(NewChecker)
	if checkerType.NumIn() != 2 {
		t.Fatalf("NewChecker takes %d parameters, want 2", checkerType.NumIn())
	}
	if checkerType.In(0) != reflect.TypeOf(config.ActiveHealth{}) {
		t.Fatalf("NewChecker parameter 0 = %v", checkerType.In(0))
	}
	if checkerType.In(1) != reflect.TypeOf(CheckerDeps{}) {
		t.Fatalf("NewChecker parameter 1 = %v", checkerType.In(1))
	}

	assertFields(t, reflect.TypeOf(CheckerDeps{}), "Clock", "Rand", "OnProbe", "OnChange")
	assertFields(t, reflect.TypeOf(Events{}), "Ejected", "Restored", "Refused")
}

func assertFields(t *testing.T, typ reflect.Type, names ...string) {
	t.Helper()
	if typ.NumField() != len(names) {
		t.Fatalf("%v has %d fields, want %d", typ, typ.NumField(), len(names))
	}
	for i, n := range names {
		if typ.Field(i).Name != n {
			t.Fatalf("%v field %d = %s, want %s", typ, i, typ.Field(i).Name, n)
		}
	}
}
