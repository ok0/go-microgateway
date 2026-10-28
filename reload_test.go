package lychgate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/upstream"
)

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuf) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func newJSONLogger(l *logBuf) *slog.Logger {
	return slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (l *logBuf) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(l.String(), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func writeConfig(t *testing.T, dir, doc string) string {
	t.Helper()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func counters(g *Gateway) (success, failure, unchanged float64) {
	return g.reloads.success.Value(), g.reloads.failure.Value(), g.reloads.unchanged.Value()
}

func metricsText(t *testing.T, g *Gateway) string {
	t.Helper()
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	return rec.Body.String()
}

func adminBody(t *testing.T, g *Gateway, path string) string {
	t.Helper()
	rec := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s status = %d, want 200", path, rec.Code)
	}
	return rec.Body.String()
}

func servedBy(g *Gateway, path string) string {
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	if rec.Code != http.StatusOK {
		return ""
	}
	var e echo
	if err := json.NewDecoder(rec.Body).Decode(&e); err != nil {
		return ""
	}
	return e.Backend
}

func statusOf(client *http.Client, url string) int {
	resp, err := client.Get(url)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func passDoc(backendURL string) string {
	return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
middlewares:
  - name: m
    type: pass
routes:
  - name: all
    upstream: up
    middlewares: [m]
`, backendURL)
}

func identityMiddleware(next http.Handler) http.Handler { return next }

func TestReloadEntryPointsEquivalent(t *testing.T) {
	n := newMemNetwork()
	beA := n.serve(t, echoHandler("A"))
	beB := n.serve(t, echoHandler("B"))
	var factoryCalls atomic.Int64
	pass := func(p MiddlewareParams) (Middleware, error) {
		factoryCalls.Add(1)
		return identityMiddleware, nil
	}
	docA := passDoc(beA.URL)
	docB := passDoc(beB.URL)
	g := n.gateway(t, docA, WithMiddleware("pass", pass))
	if got := factoryCalls.Load(); got != 1 {
		t.Fatalf("factory calls after New = %d, want 1", got)
	}

	if err := g.ReloadYAML([]byte(docB)); err != nil {
		t.Fatal(err)
	}
	if s, f, u := counters(g); s != 1 || f != 0 || u != 0 {
		t.Fatalf("counters after ReloadYAML = %v %v %v, want 1 0 0", s, f, u)
	}
	if got := servedBy(g, "/x"); got != "B" {
		t.Fatalf("backend after ReloadYAML = %q, want B", got)
	}
	cfgB, err := config.Parse([]byte(docB))
	if err != nil {
		t.Fatal(err)
	}
	fpB := cfgB.Fingerprint()
	if got := g.Config().Fingerprint(); got != fpB {
		t.Fatalf("fingerprint after ReloadYAML differs from parsed B")
	}
	calls := factoryCalls.Load()

	if err := g.Reload(cfgB); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := writeConfig(t, dir, docB)
	if err := g.ReloadFile(path); err != nil {
		t.Fatal(err)
	}
	if s, f, u := counters(g); s != 1 || f != 0 || u != 2 {
		t.Fatalf("counters after Reload and ReloadFile = %v %v %v, want 1 0 2", s, f, u)
	}
	if got := g.Config().Fingerprint(); got != fpB {
		t.Fatalf("fingerprint after ReloadFile differs from parsed B")
	}
	if got := factoryCalls.Load(); got != calls {
		t.Fatalf("factory calls changed on unchanged reloads: %d -> %d", calls, got)
	}

	before := g.cur.Load()
	edited := "# reformatted\n\n" + strings.ReplaceAll(docB, "\n", "  \n")
	writeConfig(t, dir, edited)
	if err := g.ReloadFile(path); err != nil {
		t.Fatal(err)
	}
	if _, _, u := counters(g); u != 3 {
		t.Fatalf("unchanged after comment edit = %v, want 3", u)
	}
	if g.cur.Load() != before {
		t.Fatal("comment-only edit replaced the active generation")
	}
	if got := factoryCalls.Load(); got != calls {
		t.Fatalf("factory calls after comment-only edit = %d, want %d", got, calls)
	}

	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	if err := g.ReloadFile(path); err != nil {
		t.Fatal(err)
	}
	if _, _, u := counters(g); u != 4 {
		t.Fatalf("unchanged after touch = %v, want 4", u)
	}
	if g.cur.Load() != before {
		t.Fatal("touch replaced the active generation")
	}
	if got := factoryCalls.Load(); got != calls {
		t.Fatalf("factory calls after touch = %d, want %d", got, calls)
	}
}

func TestReloadEnvironment(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("env"))
	var mu sync.Mutex
	env := map[string]string{"LYCHGATE_TEST_ENV": "one"}
	lookup := func(k string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		v, ok := env[k]
		return v, ok
	}
	setEnv := func(v string) {
		mu.Lock()
		defer mu.Unlock()
		env["LYCHGATE_TEST_ENV"] = v
	}
	doc := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: all
    upstream: up
    response_headers:
      set:
        X-Env: "${LYCHGATE_TEST_ENV}"
`, be.URL)
	g := n.gateway(t, doc, WithEnvLookup(lookup))
	resp, _ := serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got := resp.Header.Get("X-Env"); got != "one" {
		t.Fatalf("X-Env = %q, want one", got)
	}
	fp1 := g.Config().Fingerprint()

	setEnv("two")
	if err := g.ReloadYAML([]byte(doc)); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := counters(g); s != 1 {
		t.Fatalf("success = %v, want 1", s)
	}
	if g.Config().Fingerprint() == fp1 {
		t.Fatal("fingerprint did not change with the environment value")
	}
	resp, _ = serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got := resp.Header.Get("X-Env"); got != "two" {
		t.Fatalf("X-Env after reload = %q, want two", got)
	}
}

func TestReloadFailureKeepsRunning(t *testing.T) {
	n := newMemNetwork()
	base := func(backendURL string) string {
		return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
middlewares:
  - name: m
    type: pass
routes:
  - name: all
    upstream: up
    middlewares: [m]
`, backendURL)
	}
	failing := func(p MiddlewareParams) (Middleware, error) {
		var c struct {
			Fail bool `yaml:"fail"`
		}
		if err := p.Decode(&c); err != nil {
			return nil, err
		}
		if c.Fail {
			return nil, errors.New("factory refused")
		}
		return identityMiddleware, nil
	}

	type failCase struct {
		name     string
		yaml     func(doc string) string
		wantPath string
		mutate   func(c *config.Config)
	}
	cases := []failCase{
		{
			name: "syntax error",
			yaml: func(string) string { return "version: 1\nroutes: [\n" },
		},
		{
			name: "unknown key",
			yaml: func(doc string) string {
				return strings.Replace(doc, "    upstream: up\n", "    upstream: up\n    bogus: 1\n", 1)
			},
			wantPath: "routes[0].bogus",
		},
		{
			name: "dangling reference",
			yaml: func(doc string) string {
				return strings.Replace(doc, "    upstream: up\n", "    upstream: missing\n", 1)
			},
			wantPath: "routes[0].upstream",
			mutate:   func(c *config.Config) { c.Routes[0].Upstream = "missing" },
		},
		{
			name: "unknown middleware type",
			yaml: func(doc string) string {
				return strings.Replace(doc, "type: pass", "type: nope", 1)
			},
			wantPath: "middlewares[0].type",
			mutate:   func(c *config.Config) { c.Middlewares[0].Type = "nope" },
		},
		{
			name: "factory error",
			yaml: func(doc string) string {
				return strings.Replace(doc, "type: pass\n", "type: pass\n    params:\n      fail: true\n", 1)
			},
			wantPath: "middlewares[0]",
			mutate:   func(c *config.Config) { c.Middlewares[0].Params = config.NewSecretParams(map[string]any{"fail": true}) },
		},
	}

	for _, tc := range cases {
		methods := []string{"ReloadYAML", "ReloadFile"}
		if tc.mutate != nil {
			methods = append(methods, "Reload")
		}
		for _, method := range methods {
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				be := n.serve(t, echoHandler("ok"))
				doc := base(be.URL)
				buf := &logBuf{}
				g := n.gateway(t, doc, WithMiddleware("pass", failing), WithLogger(newJSONLogger(buf)))
				before := g.Config()
				stateLen := g.state.Len()
				s0, f0, u0 := counters(g)

				var err error
				switch method {
				case "ReloadYAML":
					err = g.ReloadYAML([]byte(tc.yaml(doc)))
				case "ReloadFile":
					err = g.ReloadFile(writeConfig(t, t.TempDir(), tc.yaml(doc)))
				case "Reload":
					cfg, perr := config.Parse([]byte(doc))
					if perr != nil {
						t.Fatal(perr)
					}
					tc.mutate(cfg)
					err = g.Reload(cfg)
				}

				var ve *config.ValidationError
				if !errors.As(err, &ve) {
					t.Fatalf("err = %v, want *config.ValidationError", err)
				}
				if method != "Reload" {
					for _, fe := range ve.Errors {
						if fe.Line <= 0 {
							t.Fatalf("YAML-sourced error %+v has no line", fe)
						}
					}
				}
				if tc.wantPath != "" {
					found := false
					for _, fe := range ve.Errors {
						if fe.Path == tc.wantPath {
							found = true
						}
					}
					if !found {
						t.Fatalf("errors = %+v, want a path %q", ve.Errors, tc.wantPath)
					}
				}

				if g.Config() != before {
					t.Fatal("Config() changed after a failed reload")
				}
				if got := servedBy(g, "/x"); got != "ok" {
					t.Fatalf("backend after failed reload = %q, want ok", got)
				}
				if s, f, u := counters(g); s != s0 || f != f0+1 || u != u0 {
					t.Fatalf("counters = %v %v %v, want %v %v %v", s, f, u, s0, f0+1, u0)
				}
				if got := g.state.Len(); got != stateLen {
					t.Fatalf("state entries = %d, want %d", got, stateLen)
				}
				if got := g.live.Load(); got != 1 {
					t.Fatalf("live generations = %d, want 1", got)
				}
				recs := buf.records(t, "configuration reload failed; previous configuration remains active")
				if len(recs) != 1 {
					t.Fatalf("failure records = %d, want 1", len(recs))
				}
				if recs[0]["level"] != "WARN" {
					t.Fatalf("failure level = %v, want WARN", recs[0]["level"])
				}
				errs, ok := recs[0]["errors"].([]any)
				if !ok || len(errs) == 0 {
					t.Fatalf("errors attribute = %v", recs[0]["errors"])
				}
				if tc.wantPath != "" {
					listed := false
					for _, e := range errs {
						if s, ok := e.(string); ok && strings.HasPrefix(s, tc.wantPath) {
							listed = true
						}
					}
					if !listed {
						t.Fatalf("errors attribute = %v, want an entry for %q", errs, tc.wantPath)
					}
				}
			})
		}
	}

	t.Run("missing file", func(t *testing.T) {
		be := n.serve(t, echoHandler("ok"))
		g := n.gateway(t, base(be.URL), WithMiddleware("pass", failing))
		err := g.ReloadFile(filepath.Join(t.TempDir(), "missing.yaml"))
		if err == nil {
			t.Fatal("ReloadFile of a missing file returned nil")
		}
		var ve *config.ValidationError
		if errors.As(err, &ve) {
			t.Fatalf("missing file error = %v, want a plain error", err)
		}
		if s, f, u := counters(g); s != 0 || f != 1 || u != 0 {
			t.Fatalf("counters = %v %v %v, want 0 1 0", s, f, u)
		}
		if got := servedBy(g, "/x"); got != "ok" {
			t.Fatalf("backend = %q, want ok", got)
		}
	})
}

func TestReloadNilAndClosed(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("x"))
	doc := passDoc(be.URL)
	g := n.gateway(t, doc, WithMiddleware("pass", func(MiddlewareParams) (Middleware, error) {
		return identityMiddleware, nil
	}))
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	path := writeConfig(t, t.TempDir(), doc)

	if err := g.Reload(nil); err == nil {
		t.Fatal("Reload(nil) returned nil")
	}
	if s, f, u := counters(g); s != 0 || f != 0 || u != 0 {
		t.Fatalf("counters after Reload(nil) = %v %v %v, want 0 0 0", s, f, u)
	}

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
	if err := g.Reload(cfg); !errors.Is(err, ErrClosed) {
		t.Fatalf("Reload after Close = %v, want ErrClosed", err)
	}
	if err := g.ReloadYAML([]byte(doc)); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReloadYAML after Close = %v, want ErrClosed", err)
	}
	if err := g.ReloadFile(path); !errors.Is(err, ErrClosed) {
		t.Fatalf("ReloadFile after Close = %v, want ErrClosed", err)
	}
	if err := g.WatchFile(context.Background(), path); !errors.Is(err, ErrClosed) {
		t.Fatalf("WatchFile after Close = %v, want ErrClosed", err)
	}

	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ServeHTTP after Close status = %d, want 503", rec.Code)
	}
	var body struct {
		Error apiError `json:"error"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "service_unavailable" {
		t.Fatalf("code = %q, want service_unavailable", body.Error.Code)
	}
}

func TestReloadCallerMutation(t *testing.T) {
	n := newMemNetwork()
	beOld := n.serve(t, echoHandler("old"))
	beNew := n.serve(t, echoHandler("new"))
	g := n.gateway(t, oneRouteDoc("", beOld.URL))

	doc := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: orig
    match:
      path: /orig
    upstream: up
`, beNew.URL)
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Reload(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Routes[0].Match.Path = "/mutated"
	cfg.Upstreams[0].Hosts[0].URL = config.NewSecretURL(beOld.URL)

	if got := servedBy(g, "/orig"); got != "new" {
		t.Fatalf("backend after caller mutation = %q, want new", got)
	}
	if got := g.Config(); got == cfg {
		t.Fatal("Config() returned the caller's configuration")
	}
	if got := g.Config().Routes[0].Match.Path; got != "/orig" {
		t.Fatalf("Config() route path = %q, want /orig", got)
	}
	if got := g.Config().Upstreams[0].Hosts[0].URL.Reveal(); got != beNew.URL {
		t.Fatalf("Config() host URL = %q, want %q", got, beNew.URL)
	}
}

func TestReloadReportsHostURLCredentialChange(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("cred"))
	buf := &logBuf{}
	doc := func(host string) string {
		return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: old
    match:
      path: /old
    upstream: up
`, host)
	}
	hostOld := strings.Replace(be.URL, "http://", "http://user:pw1@", 1)
	hostNew := strings.Replace(be.URL, "http://", "http://user:pw2@", 1)
	g := n.gateway(t, doc(hostOld), WithLogger(newJSONLogger(buf)))

	if err := g.ReloadYAML([]byte(doc(hostNew))); err != nil {
		t.Fatal(err)
	}
	recs := buf.records(t, "configuration reloaded")
	if len(recs) != 1 {
		t.Fatalf("reloaded records = %d, want 1", len(recs))
	}
	if recs[0]["upstreams_changed"] != float64(1) {
		t.Fatalf("upstreams_changed = %v, want 1", recs[0]["upstreams_changed"])
	}
}

func TestReloadConcurrent(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("conc"))
	var mu sync.Mutex
	var gens []string
	var inBuild atomic.Int64
	factory := func(p MiddlewareParams) (Middleware, error) {
		if inBuild.Add(1) != 1 {
			t.Error("concurrent middleware factory call")
		}
		defer inBuild.Add(-1)
		var c struct {
			Gen  string `yaml:"gen"`
			Fail bool   `yaml:"fail"`
		}
		if err := p.Decode(&c); err != nil {
			return nil, err
		}
		if c.Fail {
			return nil, errors.New("factory refused")
		}
		mu.Lock()
		gens = append(gens, c.Gen)
		mu.Unlock()
		return identityMiddleware, nil
	}
	doc := func(gen string, fail bool) string {
		return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
middlewares:
  - name: m
    type: gen
    params:
      gen: %q
      fail: %t
routes:
  - name: all
    upstream: up
    middlewares: [m]
    response_headers:
      set:
        X-Gen: %q
`, be.URL, gen, fail, gen)
	}
	docs := []string{doc("A", false), doc("B", false), doc("A", true)}
	invalid := "version: 1\nroutes: [\n"

	files := make([]string, 0, len(docs)+1)
	cfgs := make([]*config.Config, 0, len(docs))
	for _, d := range docs {
		files = append(files, writeConfig(t, t.TempDir(), d))
		c, err := config.Parse([]byte(d))
		if err != nil {
			t.Fatal(err)
		}
		cfgs = append(cfgs, c)
	}
	invalidFile := writeConfig(t, t.TempDir(), invalid)

	g := n.gateway(t, docs[0], WithMiddleware("gen", factory))

	var wg sync.WaitGroup
	for id := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 25 {
				idx := (id + i) % 4
				method := i % 3
				var err error
				wantErr := idx >= 2
				if idx == 3 {
					if method == 0 {
						err = g.ReloadYAML([]byte(invalid))
					} else {
						err = g.ReloadFile(invalidFile)
					}
				} else {
					switch method {
					case 0:
						err = g.Reload(cfgs[idx])
					case 1:
						err = g.ReloadYAML([]byte(docs[idx]))
					default:
						err = g.ReloadFile(files[idx])
					}
				}
				if (err != nil) != wantErr {
					t.Errorf("reload idx %d method %d: err = %v, wantErr %v", idx, method, err, wantErr)
				}
			}
		}()
	}
	wg.Wait()

	mu.Lock()
	last := gens[len(gens)-1]
	mu.Unlock()
	if got := g.Config().Routes[0].ResponseHeaders.Set["X-Gen"].Reveal(); got != last {
		t.Fatalf("route header = %q, want last built gen %q", got, last)
	}
	resp, _ := serve(t, g, httptest.NewRequest(http.MethodGet, "/x", nil))
	if got := resp.Header.Get("X-Gen"); got != last {
		t.Fatalf("response X-Gen = %q, want %q", got, last)
	}
}

func TestFactoriesFixedAtNew(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("f"))
	be2 := n.serve(t, echoHandler("f2"))
	var calls atomic.Int64
	tag := func(MiddlewareParams) (Middleware, error) {
		calls.Add(1)
		return identityMiddleware, nil
	}
	doc := func(url string, typ string) string {
		return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
middlewares:
  - name: m1
    type: %s
  - name: m2
    type: %s
routes:
  - name: all
    upstream: up
    middlewares: [m1, m2]
`, url, typ, typ)
	}
	g := n.gateway(t, doc(be.URL, "tag"), WithMiddleware("tag", tag))
	if got := calls.Load(); got != 2 {
		t.Fatalf("factory calls after New = %d, want 2", got)
	}
	if err := g.ReloadYAML([]byte(doc(be2.URL, "tag"))); err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("factory calls after changed reload = %d, want 4", got)
	}
	err := g.ReloadYAML([]byte(doc(be2.URL, "other")))
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("unregistered type err = %v, want *config.ValidationError", err)
	}
	if got := calls.Load(); got != 4 {
		t.Fatalf("factory calls after failed reload = %d, want 4", got)
	}
	if s, f, _ := counters(g); s != 1 || f != 1 {
		t.Fatalf("success, failure = %v, %v, want 1, 1", s, f)
	}
}

func TestReloadMetricsAndAdmin(t *testing.T) {
	n := newMemNetwork()
	t0 := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	fake := clock.NewFake(t0)
	be := n.serve(t, echoHandler("m"))
	be2 := n.serve(t, echoHandler("m2"))
	docOld := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: old
    match:
      path: /old
    upstream: up
`, be.URL)
	docNew := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
  - name: up2
    hosts:
      - url: %q
routes:
  - name: fresh
    match:
      path: /fresh
    upstream: up2
`, be.URL, be2.URL)
	invalid := "version: 1\nroutes: [\n"
	ts := func(tt time.Time) string {
		return strconv.FormatFloat(float64(tt.Unix()), 'g', -1, 64)
	}
	tsLine := func(tt time.Time) string {
		return "lychgate_config_last_reload_success_timestamp_seconds " + ts(tt)
	}

	g := n.gateway(t, docOld, withClock(fake))
	body := metricsText(t, g)
	for _, line := range []string{
		`lychgate_config_reloads_total{result="success"} 0`,
		`lychgate_config_reloads_total{result="failure"} 0`,
		`lychgate_config_reloads_total{result="unchanged"} 0`,
		tsLine(t0),
	} {
		if !metricsHasLine(body, line) {
			t.Fatalf("metrics lack %q after New\n%s", line, body)
		}
	}

	fake.Advance(time.Minute)
	if err := g.ReloadYAML([]byte(invalid)); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	body = metricsText(t, g)
	if !metricsHasLine(body, tsLine(t0)) {
		t.Fatalf("timestamp moved after failure\n%s", body)
	}
	if !strings.Contains(adminBody(t, g, "/config"), `"name":"old"`) {
		t.Fatal("/config lacks the old route after failure")
	}

	fake.Advance(time.Minute)
	if err := g.ReloadYAML([]byte(docOld)); err != nil {
		t.Fatal(err)
	}
	body = metricsText(t, g)
	if !metricsHasLine(body, tsLine(t0)) {
		t.Fatalf("timestamp moved after unchanged reload\n%s", body)
	}
	if !metricsHasLine(body, `lychgate_config_reloads_total{result="unchanged"} 1`) {
		t.Fatalf("unchanged counter lacks 1\n%s", body)
	}

	if got := servedBy(g, "/old"); got != "m" {
		t.Fatalf("/old backend = %q, want m", got)
	}
	fake.Advance(time.Minute)
	if err := g.ReloadYAML([]byte(docNew)); err != nil {
		t.Fatal(err)
	}
	t3 := t0.Add(3 * time.Minute)
	body = metricsText(t, g)
	if !metricsHasLine(body, tsLine(t3)) {
		t.Fatalf("timestamp not updated after success\n%s", body)
	}
	if !metricsHasLine(body, `lychgate_requests_total{route="old",method="GET",code="200"} 1`) {
		t.Fatalf("removed route series missing after reload\n%s", body)
	}

	var cfgBody struct {
		Routes []config.Route `json:"routes"`
	}
	if err := json.Unmarshal([]byte(adminBody(t, g, "/config")), &cfgBody); err != nil {
		t.Fatal(err)
	}
	if len(cfgBody.Routes) != 1 || cfgBody.Routes[0].Name != "fresh" {
		t.Fatalf("/config routes = %+v, want only fresh", cfgBody.Routes)
	}

	var ups []upstream.Status
	if err := json.Unmarshal([]byte(adminBody(t, g, "/upstreams")), &ups); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, u := range ups {
		names[u.Name] = true
	}
	if !names["up2"] {
		t.Fatalf("/upstreams = %+v, want up2", ups)
	}
}

func TestReloadLogRecords(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("log"))
	buf := &logBuf{}
	docOld := "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: old
    match:
      path: /old
    upstream: up
`, be.URL)
	docNew := docOld + "  - name: extra\n    match:\n      path: /extra\n    upstream: up\n"
	g := n.gateway(t, docOld, WithLogger(newJSONLogger(buf)))

	if err := g.ReloadYAML([]byte(docNew)); err != nil {
		t.Fatal(err)
	}
	recs := buf.records(t, "configuration reloaded")
	if len(recs) != 1 {
		t.Fatalf("reloaded records = %d, want 1", len(recs))
	}
	r := recs[0]
	if r["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", r["level"])
	}
	hex12 := regexp.MustCompile(`^[0-9a-f]{12}$`)
	oldFP, _ := r["old_fingerprint"].(string)
	newFP, _ := r["new_fingerprint"].(string)
	if !hex12.MatchString(oldFP) || !hex12.MatchString(newFP) {
		t.Fatalf("fingerprints = %v, %v, want 12 hex digits", r["old_fingerprint"], r["new_fingerprint"])
	}
	if oldFP == newFP {
		t.Fatal("fingerprints are equal after a change")
	}
	if r["routes_added"] != float64(1) {
		t.Fatalf("routes_added = %v, want 1", r["routes_added"])
	}
	if r["upstreams_changed"] != float64(0) {
		t.Fatalf("upstreams_changed = %v, want 0", r["upstreams_changed"])
	}

	if err := g.ReloadYAML([]byte(docNew)); err != nil {
		t.Fatal(err)
	}
	un := buf.records(t, "configuration unchanged")
	if len(un) != 1 {
		t.Fatalf("unchanged records = %d, want 1", len(un))
	}
	if un[0]["level"] != "DEBUG" {
		t.Fatalf("unchanged level = %v, want DEBUG", un[0]["level"])
	}

	if err := g.ReloadYAML([]byte("version: 1\nroutes: [\n")); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	fl := buf.records(t, "configuration reload failed; previous configuration remains active")
	if len(fl) != 1 {
		t.Fatalf("failure records = %d, want 1", len(fl))
	}
	if fl[0]["level"] != "WARN" {
		t.Fatalf("failure level = %v, want WARN", fl[0]["level"])
	}
	if _, ok := fl[0]["errors"]; !ok {
		t.Fatal("failure record lacks errors")
	}
}

func TestNewIsNotAReload(t *testing.T) {
	n := newMemNetwork()
	t0 := time.Date(2026, 6, 7, 8, 9, 10, 0, time.UTC)
	be := n.serve(t, echoHandler("new"))
	g := n.gateway(t, oneRouteDoc("", be.URL), withClock(clock.NewFake(t0)))
	if s, f, u := counters(g); s != 0 || f != 0 || u != 0 {
		t.Fatalf("counters after New = %v %v %v, want 0 0 0", s, f, u)
	}
	line := "lychgate_config_last_reload_success_timestamp_seconds " +
		strconv.FormatFloat(float64(t0.Unix()), 'g', -1, 64)
	if !metricsHasLine(metricsText(t, g), line) {
		t.Fatalf("timestamp line %q missing after New", line)
	}
}
