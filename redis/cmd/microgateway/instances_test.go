package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/testwait"
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
	eventuallyAdvancing(t, "instance A to serve /extra after SIGHUP", a.clk, 250*time.Millisecond, func() bool {
		resp, _ := get(t, baseA+"/extra")
		return resp.StatusCode == http.StatusOK && metric(t, adminA, reloadSeries) > beforeA
	}, a.errb.String)
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

type recordingOrigin struct {
	url   string
	mu    sync.Mutex
	calls []http.Header
}

func recordingBackend(t *testing.T) *recordingOrigin {
	t.Helper()
	o := &recordingOrigin{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		o.calls = append(o.calls, r.Header.Clone())
		o.mu.Unlock()
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("ETag", `"v1"`)
		_, _ = fmt.Fprint(w, "ok")
	}))
	t.Cleanup(srv.Close)
	o.url = srv.URL
	return o
}

func (o *recordingOrigin) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.calls)
}

func (o *recordingOrigin) last() http.Header {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls[len(o.calls)-1]
}

func postAdminPurge(t *testing.T, addr, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/cache/purge", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	client := &http.Client{Timeout: testwait.Guard(t)}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

func cacheGet(t *testing.T, base string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/c", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "shared.test"
	client := &http.Client{Timeout: testwait.Guard(t)}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatal(err)
	}
	return resp
}

func cacheStatusOf(t *testing.T, resp *http.Response) httpcache.Status {
	t.Helper()
	sts, err := httpcache.ParseStatus(strings.Join(resp.Header.Values("Cache-Status"), ", "))
	if err != nil {
		t.Fatalf("Cache-Status %q: %v", resp.Header.Values("Cache-Status"), err)
	}
	if len(sts) == 0 {
		t.Fatal("response has no Cache-Status")
	}
	return sts[len(sts)-1]
}

func twoInstancesCache(t *testing.T, target redisTarget, soft bool) {
	t.Helper()
	prefix := uniquePrefix(t)
	t.Cleanup(func() { deletePrefix(t, target.addr, target.password, target.db, prefix) })
	token := uniqueSecret(t, "admin-")
	origin := recordingBackend(t)

	opts := docOpts{
		Backend:    origin.url,
		RedisAddr:  target.addr,
		Prefix:     prefix,
		DB:         target.db,
		Password:   target.password != "",
		Cache:      true,
		AdminToken: token,
	}
	dir := t.TempDir()
	aPath := filepath.Join(dir, "a.yaml")
	bPath := filepath.Join(dir, "b.yaml")
	writeFile(t, aPath, instanceDoc(opts))
	writeFile(t, bPath, instanceDoc(opts))

	lookup := lookupWith(map[string]string{"MG_TEST_REDIS_PASSWORD": target.password})
	a := startInstance(t, t.Context(), aPath, lookup)
	b := startInstance(t, t.Context(), bPath, lookup)
	baseA := "http://" + a.addrs.Listeners["public"].String()
	baseB := "http://" + b.addrs.Listeners["public"].String()
	adminA := a.addrs.Admin.String()

	respA := cacheGet(t, baseA)
	if st := cacheStatusOf(t, respA); st.Hit || !st.Stored {
		t.Fatalf("instance A first request %+v, want stored and not hit", st)
	}
	respB := cacheGet(t, baseB)
	if st := cacheStatusOf(t, respB); !st.Hit {
		t.Fatalf("instance B request %+v, want hit on the entry A stored", st)
	}
	if n := origin.count(); n != 1 {
		t.Fatalf("origin requests after the shared fill = %d, want 1", n)
	}

	if code, _ := postAdminPurge(t, b.addrs.Listeners["public"].String(), token, `{"all":true}`); code != http.StatusNotFound {
		t.Fatalf("POST /cache/purge on the proxy listener status %d, want 404", code)
	}

	body := `{"all":true}`
	if soft {
		body = `{"all":true,"soft":true}`
	}
	if code, out := postAdminPurge(t, adminA, token, body); code != http.StatusOK {
		t.Fatalf("admin purge on A status %d, want 200: %s", code, out)
	}

	respB = cacheGet(t, baseB)
	if st := cacheStatusOf(t, respB); st.Hit {
		t.Fatalf("instance B request after the purge %+v, want not hit", st)
	}
	if n := origin.count(); n != 2 {
		t.Fatalf("origin requests after the purge = %d, want 2", n)
	}
	last := origin.last()
	if soft {
		if last.Get("If-None-Match") == "" {
			t.Fatalf("origin request after the soft purge has no If-None-Match: %v", last)
		}
	} else if last.Get("If-None-Match") != "" || last.Get("If-Modified-Since") != "" {
		t.Fatalf("origin request after the hard purge is conditional: %v", last)
	}

	if code := a.stop(t); code != 0 {
		t.Fatalf("instance A exited with code %d, want 0: %s", code, a.errb.String())
	}
	if code := b.stop(t); code != 0 {
		t.Fatalf("instance B exited with code %d, want 0: %s", code, b.errb.String())
	}
}

func TestTwoInstancesShareRedisCache(t *testing.T) {
	m := miniredis.RunT(t)
	m.SetTime(time.Unix(1_700_000_000, 0))
	pw := uniqueSecret(t, "pw-")
	m.RequireAuth(pw)
	target := redisTarget{addr: m.Addr(), password: pw, db: 0}
	t.Run("hard", func(t *testing.T) {
		twoInstancesCache(t, target, false)
	})
	t.Run("soft", func(t *testing.T) {
		twoInstancesCache(t, target, true)
	})
}
