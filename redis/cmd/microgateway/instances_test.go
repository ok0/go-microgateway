package main

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

type redisTarget struct {
	addr, password string
	db             int
}

const reloadSeries = `microgateway_config_reloads_total{result="success"}`

func runTwoInstances(t *testing.T, target redisTarget) {
	t.Helper()
	t.Run("shared", func(t *testing.T) {
		twoInstances(t, target, true, 5)
	})
	t.Run("memory", func(t *testing.T) {
		twoInstances(t, target, false, 10)
	})
}

func twoInstances(t *testing.T, target redisTarget, useStore bool, wantAdmitted int) {
	t.Helper()
	prefix := uniquePrefix(t)
	t.Cleanup(func() { deletePrefix(t, target.addr, target.password, target.db, prefix) })

	opts := docOpts{
		Backend:   backend(t),
		RedisAddr: target.addr,
		Prefix:    prefix,
		DB:        target.db,
		Password:  target.password != "",
		UseStore:  useStore,
	}
	dir := t.TempDir()
	aPath := filepath.Join(dir, "a.yaml")
	bPath := filepath.Join(dir, "b.yaml")
	writeFile(t, aPath, instanceDoc(opts))
	writeFile(t, bPath, instanceDoc(opts))

	lookup := lookupWith(map[string]string{"MG_TEST_REDIS_PASSWORD": target.password})
	a := startInstance(t, t.Context(), aPath, lookup)
	b := startInstance(t, t.Context(), bPath, lookup)

	if a.addrs.Listeners["public"].String() == b.addrs.Listeners["public"].String() {
		t.Fatal("both instances bound the same listener address")
	}
	baseA := "http://" + a.addrs.Listeners["public"].String()
	baseB := "http://" + b.addrs.Listeners["public"].String()
	adminA := a.addrs.Admin.String()
	adminB := b.addrs.Admin.String()

	admitted := 0
	for i := range 20 {
		base := baseA
		if i%2 == 1 {
			base = baseB
		}
		admitted += tally(t, base, 1)
	}
	if admitted != wantAdmitted {
		t.Fatalf("admitted %d of 20 requests across two instances, want %d", admitted, wantAdmitted)
	}

	beforeA := metric(t, adminA, reloadSeries)
	beforeB := metric(t, adminB, reloadSeries)
	opts.ExtraRoute = true
	writeFile(t, aPath, instanceDoc(opts))
	a.sig <- syscall.SIGHUP
	eventually(t, func() bool {
		resp, _ := get(t, baseA+"/extra")
		return resp.StatusCode == http.StatusOK && metric(t, adminA, reloadSeries) > beforeA
	})
	if resp, _ := get(t, baseB+"/extra"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("instance B serves /extra with status %d after A reloaded, want 404", resp.StatusCode)
	}
	if got := metric(t, adminB, reloadSeries); got != beforeB {
		t.Fatalf("instance B reload success count %v, want unchanged %v", got, beforeB)
	}
	if strings.Contains(b.errb.String(), "reload requested") {
		t.Fatal("instance B logged a reload request after A was signalled")
	}
	if n := tally(t, baseA, 1); n != 0 {
		t.Fatal("instance A admitted a request after the shared limit was spent")
	}
	if n := tally(t, baseB, 1); n != 0 {
		t.Fatal("instance B admitted a request after the shared limit was spent")
	}

	if code := a.stop(t); code != 0 {
		t.Fatalf("instance A exited with code %d, want 0: %s", code, a.errb.String())
	}
	if code := b.stop(t); code != 0 {
		t.Fatalf("instance B exited with code %d, want 0: %s", code, b.errb.String())
	}
	for name, inst := range map[string]*instance{"A": a, "B": b} {
		if !strings.Contains(inst.errb.String(), `"stopped"`) {
			t.Fatalf("instance %s did not log stopped: %s", name, inst.errb.String())
		}
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestTwoInstancesShareRedisLimit(t *testing.T) {
	m := miniredis.RunT(t)
	m.SetTime(time.Unix(1_700_000_000, 0))
	pw := uniqueSecret(t, "pw-")
	m.RequireAuth(pw)
	runTwoInstances(t, redisTarget{addr: m.Addr(), password: pw, db: 0})
}
