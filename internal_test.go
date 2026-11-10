package microgateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/hooks"
	"github.com/ok0/go-microgateway/internal/leakcheck"
	"github.com/ok0/go-microgateway/internal/middleware"
	"github.com/ok0/go-microgateway/internal/runtime"
	"github.com/ok0/go-microgateway/internal/testwait"
	"github.com/ok0/go-microgateway/store"
)

const listenersYAML = "listeners:\n  - name: public\n    address: \"127.0.0.1:0\"\n"

type echo struct {
	Backend string      `json:"backend"`
	Path    string      `json:"path"`
	RawPath string      `json:"raw_path"`
	Host    string      `json:"host"`
	Headers http.Header `json:"headers"`
}

func echoHandler(name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(echo{
			Backend: name,
			Path:    r.URL.Path,
			RawPath: r.URL.RawPath,
			Host:    r.Host,
			Headers: r.Header,
		})
	})
}

func backend(t *testing.T, name string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(echoHandler(name))
	t.Cleanup(s.Close)
	return s
}

func eventually(t *testing.T, what string, cond func() bool, state func() string) {
	t.Helper()
	testwait.Until(t, what, cond, state)
}

func gwWith(t *testing.T, doc string, opts ...Option) *Gateway {
	t.Helper()
	g, err := NewFromYAML([]byte(doc), append([]Option{withClock(clock.NewFake(time.Now()))}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

func routeDoc(backendURL, routeExtra string) string {
	return "version: 1\n" + listenersYAML + fmt.Sprintf(`upstreams:
  - name: up
    hosts:
      - url: %q
routes:
  - name: all
    upstream: up
`, backendURL) + routeExtra
}

func assertNoSecretForms(t *testing.T, text, secret string) {
	t.Helper()
	for _, f := range leakcheck.Forms(secret) {
		if strings.Contains(text, f) {
			t.Fatalf("output reveals a secret in form %q", f)
		}
	}
}

type fixedRand struct{}

func (*fixedRand) Int64N(int64) int64 { return 0 }

func (*fixedRand) Float64() float64 { return 0 }

type stubResolver struct{}

func (*stubResolver) LookupHost(context.Context, string) ([]string, error) {
	return []string{"192.0.2.1"}, nil
}

func TestHooksWithRand(t *testing.T) {
	r := &fixedRand{}
	opt, ok := hooks.WithRand(r).(Option)
	if !ok {
		t.Fatal("hooks.WithRand did not return a microgateway.Option")
	}
	var o options
	opt(&o)
	if o.rnd != clock.Rand(r) {
		t.Fatalf("option rand = %v, want the test rand", o.rnd)
	}
}

func TestHooksWithDialer(t *testing.T) {
	errDial := errors.New("dial refused")
	var gotNetwork, gotAddr string
	d := func(ctx context.Context, network, addr string) (net.Conn, error) {
		gotNetwork, gotAddr = network, addr
		return nil, errDial
	}
	opt, ok := hooks.WithDialer(d).(Option)
	if !ok {
		t.Fatal("hooks.WithDialer did not return a microgateway.Option")
	}
	var o options
	opt(&o)
	if o.dial == nil {
		t.Fatal("option did not set the dialer")
	}
	conn, err := o.dial(context.Background(), "tcp", "up.example:80")
	if conn != nil || !errors.Is(err, errDial) {
		t.Fatalf("dial = (%v, %v), want (nil, %v)", conn, err, errDial)
	}
	if gotNetwork != "tcp" || gotAddr != "up.example:80" {
		t.Fatalf("dialer got (%q, %q), want (%q, %q)", gotNetwork, gotAddr, "tcp", "up.example:80")
	}
}

func TestHooksWithResolver(t *testing.T) {
	r := &stubResolver{}
	opt, ok := hooks.WithResolver(r).(Option)
	if !ok {
		t.Fatal("hooks.WithResolver did not return a microgateway.Option")
	}
	var o options
	opt(&o)
	if o.resolver != r {
		t.Fatalf("option resolver = %v, want the test resolver", o.resolver)
	}
}

func TestHooksWithFlowTrace(t *testing.T) {
	var got []string
	opt, ok := hooks.WithFlowTrace(func(s string) { got = append(got, s) }).(Option)
	if !ok {
		t.Fatal("hooks.WithFlowTrace did not return a microgateway.Option")
	}
	var o options
	opt(&o)
	if o.flowTrace == nil {
		t.Fatal("option did not set the flow trace")
	}
	o.flowTrace("x")
	if !slices.Equal(got, []string{"x"}) {
		t.Fatalf("flow trace recorded %v, want [x]", got)
	}
}

func TestHooksInspect(t *testing.T) {
	t.Run("reports state, generations and reloads", func(t *testing.T) {
		be := backend(t, "a")
		doc := routeDoc(be.URL, "")
		g := gwWith(t, doc)
		check := func(t *testing.T) hooks.Stats {
			t.Helper()
			eventually(t, "one live generation", func() bool { return g.live.Load() == 1 }, func() string { return fmt.Sprintf("live generations: %d", g.live.Load()) })
			s := hooks.Inspect(g)
			if s.StateEntries != g.state.Len() {
				t.Fatalf("StateEntries = %d, want %d", s.StateEntries, g.state.Len())
			}
			if s.LiveGenerations != g.live.Load() {
				t.Fatalf("LiveGenerations = %d, want %d", s.LiveGenerations, g.live.Load())
			}
			if s.Generation != any(g.cur.Load()) {
				t.Fatalf("Generation = %v, want the active generation", s.Generation)
			}
			if s.ReloadSuccess != g.reloads.success.Value() {
				t.Fatalf("ReloadSuccess = %v, want %v", s.ReloadSuccess, g.reloads.success.Value())
			}
			if s.ReloadFailure != g.reloads.failure.Value() {
				t.Fatalf("ReloadFailure = %v, want %v", s.ReloadFailure, g.reloads.failure.Value())
			}
			if s.ReloadUnchanged != g.reloads.unchanged.Value() {
				t.Fatalf("ReloadUnchanged = %v, want %v", s.ReloadUnchanged, g.reloads.unchanged.Value())
			}
			return s
		}

		s0 := check(t)
		if s0.StateEntries <= 0 {
			t.Fatalf("StateEntries = %d, want > 0", s0.StateEntries)
		}
		if s0.LiveGenerations != 1 {
			t.Fatalf("LiveGenerations = %d, want 1", s0.LiveGenerations)
		}
		if s0.Generation == any((*generation)(nil)) {
			t.Fatal("Generation is nil, want the active generation")
		}
		if s0.ReloadSuccess != 0 || s0.ReloadFailure != 0 || s0.ReloadUnchanged != 0 {
			t.Fatalf("reload counters = (%v, %v, %v), want all 0", s0.ReloadSuccess, s0.ReloadFailure, s0.ReloadUnchanged)
		}

		if err := g.ReloadYAML([]byte(doc)); err != nil {
			t.Fatal(err)
		}
		s1 := check(t)
		if s1.Generation != s0.Generation {
			t.Fatal("unchanged reload replaced the generation")
		}
		if s1.ReloadUnchanged != 1 || s1.ReloadSuccess != 0 {
			t.Fatalf("after unchanged reload: ReloadUnchanged = %v, ReloadSuccess = %v, want 1 and 0", s1.ReloadUnchanged, s1.ReloadSuccess)
		}

		be2 := backend(t, "b")
		if err := g.ReloadYAML([]byte(routeDoc(be2.URL, ""))); err != nil {
			t.Fatal(err)
		}
		s2 := check(t)
		if s2.Generation == s1.Generation {
			t.Fatal("changed reload kept the generation")
		}
		if s2.ReloadSuccess != 1 {
			t.Fatalf("ReloadSuccess = %v, want 1", s2.ReloadSuccess)
		}
		if s2.LiveGenerations != 1 {
			t.Fatalf("LiveGenerations = %d, want 1", s2.LiveGenerations)
		}

		if err := g.ReloadYAML([]byte("version: [\n")); err == nil {
			t.Fatal("invalid reload succeeded")
		}
		s3 := check(t)
		if s3.ReloadFailure != 1 {
			t.Fatalf("ReloadFailure = %v, want 1", s3.ReloadFailure)
		}
		if s3.Generation != s2.Generation {
			t.Fatal("failed reload replaced the generation")
		}
	})

	t.Run("panics on other types", func(t *testing.T) {
		for _, v := range []any{nil, "gateway", 42} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("Inspect(%#v) did not panic", v)
					}
				}()
				hooks.Inspect(v)
			}()
		}
	})
}

func TestHooksWithClock(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_700_000_000, 0))
	opt, ok := hooks.WithClock(fake).(Option)
	if !ok {
		t.Fatal("hooks.WithClock did not return a microgateway.Option")
	}
	var o options
	opt(&o)
	if o.clk != clock.Clock(fake) {
		t.Fatalf("option clock = %v, want the fake clock", o.clk)
	}
}

func TestHooksStoreClock(t *testing.T) {
	doc := "version: 1\n" + listenersYAML + `upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:1"
stores:
  - name: s
    type: probe
routes:
  - name: r
    upstream: up
    rate_limit: {rate: 5, per: 1s, store: s}
`
	fake := clock.NewFake(time.Now())
	cases := []struct {
		name string
		opts []Option
		want func(clock.Clock) bool
	}{
		{"gateway clock", []Option{withClock(fake)}, func(c clock.Clock) bool { return c == clock.Clock(fake) }},
		// Real clock: a gateway without a clock option runs on the real clock, and so must its stores.
		{"default", nil, clock.IsReal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got clock.Clock
			called := false
			probe := func(p StoreParams) (store.Backend, error) {
				called = true
				got = hooks.StoreClock(p)
				return nil, errors.New("probe: no backend")
			}
			opts := append([]Option{WithStoreType("probe", store.CapRateLimit, probe)}, tc.opts...)
			if _, err := NewFromYAML([]byte(doc), opts...); err == nil {
				t.Fatal("NewFromYAML succeeded, want the error of the probe factory")
			}
			if !called {
				t.Fatal("the store factory was not called")
			}
			if got == nil || !tc.want(got) {
				t.Fatalf("hooks.StoreClock = %v, want the clock of the gateway", got)
			}
		})
	}
}

func TestPublicTypesNeverRevealSecrets(t *testing.T) {
	const (
		pw   = "pub-pw-LEAK-4e1"
		par  = "pub-par-LEAK-8a2"
		deep = "pub-deep-LEAK-1c7"
		lst  = "pub-lst-LEAK-9d3"
		upw  = "pub-urlpw-LEAK-2b6"
		uq   = "pub-urlq-LEAK-5f0"
	)
	secrets := []string{pw, par, deep, lst, upw, uq}
	params := func() config.SecretParams {
		return config.NewSecretParams(map[string]any{"tok": par, "n": map[string]any{"deep": deep, "list": []any{lst}}})
	}
	leaf := func(typ reflect.Type) (reflect.Value, bool) {
		switch typ {
		case reflect.TypeFor[config.Secret]():
			return reflect.ValueOf(config.NewSecret(pw)), true
		case reflect.TypeFor[config.SecretParams]():
			return reflect.ValueOf(params()), true
		case reflect.TypeFor[config.SecretURL]():
			return reflect.ValueOf(config.NewSecretURL("https://u:" + upw + "@h.example:8443/p?tok=" + uq)), true
		}
		return reflect.Value{}, false
	}

	registry := map[string]reflect.Type{
		"Gateway": reflect.TypeFor[Gateway](),
		"Option":  reflect.TypeFor[Option](),
		// Caller-owned purge values: they reach no config secret type and the purge service never renders them.
		"PurgeRequest":      reflect.TypeFor[PurgeRequest](),
		"PurgeResult":       reflect.TypeFor[PurgeResult](),
		"KeySetFetcher":     reflect.TypeFor[KeySetFetcher](),
		"Identity":          reflect.TypeFor[Identity](),
		"Middleware":        reflect.TypeFor[Middleware](),
		"MiddlewareFactory": reflect.TypeFor[MiddlewareFactory](),
		"MiddlewareParams":  reflect.TypeFor[MiddlewareParams](),
		"StoreFactory":      reflect.TypeFor[StoreFactory](),
		"StoreParams":       reflect.TypeFor[StoreParams](),
	}

	names, err := leakcheck.ExportedTypes(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range names {
		if _, ok := registry[n]; !ok {
			t.Errorf("exported type %s is missing from the leak registry; add it, and a sample if it can hold a secret", n)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(registry)) {
		if !slices.Contains(names, k) {
			t.Errorf("leak registry lists %s, which is not an exported type", k)
		}
	}

	targets := []reflect.Type{reflect.TypeFor[config.Secret](), reflect.TypeFor[config.SecretParams](), reflect.TypeFor[config.SecretURL]()}
	var containing []string
	for _, k := range slices.Sorted(maps.Keys(registry)) {
		if leakcheck.Reaches(registry[k], targets...) {
			containing = append(containing, k)
		}
	}
	for _, n := range []string{"Gateway", "MiddlewareParams", "StoreParams"} {
		if !slices.Contains(containing, n) {
			t.Fatalf("containing types lack %s; the leak matrix would be vacuous", n)
		}
	}

	samples := map[string]func(t *testing.T) (any, bool){
		"MiddlewareParams": func(t *testing.T) (any, bool) {
			p := MiddlewareParams{Name: "m", Type: "t", params: params()}
			return p, leakcheck.Holds(p, secrets...)
		},
		"StoreParams": func(t *testing.T) (any, bool) {
			redis := leakcheck.Fill(reflect.TypeFor[*config.RedisStore](), leaf).Interface().(*config.RedisStore)
			p := StoreParams{Name: "s", Type: "redis", Redis: redis, params: params()}
			return p, leakcheck.Holds(p, secrets...)
		},
		"Gateway": func(t *testing.T) (any, bool) {
			be := backend(t, "b")
			host := strings.Replace(be.URL, "http://", "http://u:"+upw+"@", 1)
			env := WithEnvLookup(func(k string) (string, bool) {
				switch k {
				case "UP_TOKEN":
					return pw, true
				case "MW_TOKEN":
					return par, true
				}
				return "", false
			})
			doc := routeDoc(host, "    middlewares: [m]\n    request_headers:\n      set:\n        Authorization: \"Bearer ${UP_TOKEN}\"\n") + "middlewares:\n  - name: m\n    type: tok\n    params:\n      token: \"${MW_TOKEN}\"\n"
			g := gwWith(t, doc, WithMiddleware("tok", func(MiddlewareParams) (Middleware, error) {
				return func(h http.Handler) http.Handler { return h }, nil
			}), env)
			c := g.Config()
			var tok struct {
				Token string `yaml:"token"`
			}
			holds := strings.Contains(c.Upstreams[0].Hosts[0].URL.Reveal(), upw) &&
				c.Routes[0].RequestHeaders.Set["Authorization"].Reveal() == "Bearer "+pw &&
				config.DecodeSecretParams(c.Middlewares[0].Params, &tok) == nil && tok.Token == par
			return g, holds
		},
	}

	for _, name := range containing {
		sample, ok := samples[name]
		if !ok {
			t.Errorf("%s can hold a secret but has no sample in the leak matrix", name)
			continue
		}
		t.Run(name, func(tt *testing.T) {
			v, holds := sample(tt)
			if !holds {
				tt.Fatalf("sample of %s holds no secret; the check would be vacuous", name)
			}
			leakcheck.Check(tt, name, v, secrets...)
		})
	}
	for _, k := range slices.Sorted(maps.Keys(samples)) {
		if !slices.Contains(containing, k) {
			t.Errorf("leak matrix sample %s is stale", k)
		}
	}
}

func TestPurgeErrorsNeverEchoRequest(t *testing.T) {
	const (
		pu = "pur-ui-LEAK-6b1"
		pq = "pur-q-LEAK-0e4"
		pp = "pur-p-LEAK-2c8"
		pt = "pur-t-LEAK-7f5"
	)
	be := backend(t, "b")
	g := gwWith(t, routeDoc(be.URL, "    cache:\n      enabled: true\n"))

	cases := []struct {
		name string
		req  PurgeRequest
	}{
		{"userinfo", PurgeRequest{URLs: []string{"http://" + pu + ":x@h/" + pp + "?k=" + pq}}},
		{"scheme", PurgeRequest{URLs: []string{"ftp://h/" + pp + "?k=" + pq}}},
		{"key too long", PurgeRequest{URLs: []string{"http://h/" + pp + strings.Repeat("a", 8<<10) + "?k=" + pq}}},
		{"two selectors", PurgeRequest{URLs: []string{"http://h/" + pp + "?k=" + pq}, Tags: []string{pt}}},
		{"control tag", PurgeRequest{Tags: []string{pt + "\x01"}}},
		{"long tag", PurgeRequest{Tags: []string{pt + strings.Repeat("x", 1024)}}},
	}
	for _, c := range cases {
		res, err := g.Purge(context.Background(), c.req)
		if !errors.Is(err, ErrInvalidPurgeRequest) {
			t.Fatalf("%s: err = %v, want ErrInvalidPurgeRequest", c.name, err)
		}
		leakcheck.Check(t, c.name, err, pu, pq, pp, pt)
		leakcheck.Check(t, c.name+" result", res, pu, pq, pp, pt)
	}

	g2 := gwWith(t, routeDoc(be.URL, ""))
	res, err := g2.Purge(context.Background(), PurgeRequest{URLs: []string{"http://h/" + pp + "?k=" + pq}})
	if !errors.Is(err, ErrCacheDisabled) {
		t.Fatalf("cache disabled: err = %v, want ErrCacheDisabled", err)
	}
	leakcheck.Check(t, "cache disabled", err, pu, pq, pp, pt)
	leakcheck.Check(t, "cache disabled result", res, pu, pq, pp, pt)

	res, err = g.Purge(context.Background(), PurgeRequest{URLs: []string{"http://h/" + pp + "?k=" + pq}, Soft: true})
	if err != nil {
		t.Fatalf("soft purge: unexpected error %v", err)
	}
	leakcheck.Check(t, "result", res, pu, pq, pp, pt)
}

func TestUserMiddlewareParamsSecret(t *testing.T) {
	const secret = "mt-LEAK-3c9"
	env := WithEnvLookup(func(k string) (string, bool) {
		if k == "MW_TOKEN" {
			return secret, true
		}
		return "", false
	})
	be := backend(t, "b")
	passthrough := func(next http.Handler) http.Handler { return next }

	t.Run("factory decodes secret", func(t *testing.T) {
		var revealed bool
		factory := func(p MiddlewareParams) (Middleware, error) {
			var c struct {
				Token config.Secret `yaml:"token"`
				N     int           `yaml:"n"`
			}
			if err := p.Decode(&c); err != nil {
				return nil, err
			}
			revealed = c.Token.Reveal() == secret && c.N == 2
			return passthrough, nil
		}
		doc := routeDoc(be.URL, "    middlewares: [m]\n") + "middlewares:\n  - name: m\n    type: tok\n    params:\n      token: \"${MW_TOKEN}\"\n      n: 2\n"
		g := gwWith(t, doc, WithMiddleware("tok", factory), env)
		if !revealed {
			t.Fatal("factory did not receive the revealed token and integer")
		}
		out, err := json.Marshal(g.Config().Redacted())
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(out), "token") {
			t.Fatalf("redacted config %s does not contain the param key", out)
		}
		assertNoSecretForms(t, string(out), secret)
	})

	t.Run("type mismatch error is value-free", func(t *testing.T) {
		factory := func(p MiddlewareParams) (Middleware, error) {
			var c struct {
				N int `yaml:"n"`
			}
			if err := p.Decode(&c); err != nil {
				return nil, err
			}
			return passthrough, nil
		}
		doc := routeDoc(be.URL, "    middlewares: [m]\n") + "middlewares:\n  - name: m\n    type: tok\n    params:\n      n: \"${MW_TOKEN}\"\n"
		_, err := NewFromYAML([]byte(doc), withClock(clock.NewFake(time.Now())), WithMiddleware("tok", factory), env)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "middlewares[0]") {
			t.Fatalf("error %q does not contain %q", err, "middlewares[0]")
		}
		assertNoSecretForms(t, err.Error(), secret)
	})

	t.Run("factory error is scrubbed", func(t *testing.T) {
		factory := func(p MiddlewareParams) (Middleware, error) {
			var c struct {
				Token config.Secret `yaml:"token"`
			}
			if err := p.Decode(&c); err != nil {
				return nil, err
			}
			tok := c.Token.Reveal()
			return nil, fmt.Errorf("bad token %s / %x", tok, tok)
		}
		doc := routeDoc(be.URL, "    middlewares: [m]\n") + "middlewares:\n  - name: m\n    type: tok\n    params:\n      token: \"${MW_TOKEN}\"\n"
		_, err := NewFromYAML([]byte(doc), withClock(clock.NewFake(time.Now())), WithMiddleware("tok", factory), env)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "REDACTED") {
			t.Fatalf("error %q does not contain REDACTED", err)
		}
		assertNoSecretForms(t, err.Error(), secret)
	})

	t.Run("formatted params hide secret", func(t *testing.T) {
		leakcheck.Check(t, "MiddlewareParams", MiddlewareParams{Name: "m", Type: "t", params: config.NewSecretParams(map[string]any{"token": secret})}, secret)
	})
}

func TestStoreParamsNeverFormat(t *testing.T) {
	const (
		pw   = "pw-LEAK-7f3a9"
		par  = "par-LEAK-91c2"
		deep = "deep-LEAK-55e1"
		lst  = "lst-LEAK-0b7d"
		num  = 918273645
		upw  = "urlpw-LEAK-c41e"
		uq   = "urlq-LEAK-77ad"
	)
	leaf := func(typ reflect.Type) (reflect.Value, bool) {
		switch typ {
		case reflect.TypeFor[config.Secret]():
			return reflect.ValueOf(config.NewSecret(pw)), true
		case reflect.TypeFor[config.SecretParams]():
			return reflect.ValueOf(config.NewSecretParams(map[string]any{
				"tok": par,
				"n":   map[string]any{"deep": deep, "list": []any{lst}},
				"num": num,
			})), true
		case reflect.TypeFor[config.SecretURL]():
			return reflect.ValueOf(config.NewSecretURL("https://u:" + upw + "@h.example:8443/p?tok=" + uq)), true
		}
		return reflect.Value{}, false
	}
	redis := leakcheck.Fill(reflect.TypeFor[*config.RedisStore](), leaf).Interface().(*config.RedisStore)
	p := StoreParams{
		Name:  "s",
		Type:  "redis",
		Redis: redis,
		params: config.NewSecretParams(map[string]any{
			"tok": par,
			"n":   map[string]any{"deep": deep, "list": []any{lst}},
			"num": num,
		}),
	}
	secrets := []string{pw, par, deep, lst, "918273645", "36bc9a4d", upw, uq}
	leakcheck.Check(t, "StoreParams", p, secrets...)
	leakcheck.Check(t, "*StoreParams", &p, secrets...)
}

func TestLoadOptionsReportsCapabilityMismatch(t *testing.T) {
	set := runtime.NewStoreSet(map[string]runtime.StoreType{"plain": {}})
	reg := middleware.NewRegistry()
	doc := "version: 1\n" + listenersYAML + `upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:1"
global:
  drain_timeout: soon
stores:
  - name: s
    type: plain
routes:
  - name: r
    upstream: up
    rate_limit: {rate: 5, per: 1s, store: s}
`
	_, err := config.Parse([]byte(doc), loadOptions(nil, set, reg)...)
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error = %v, want a validation error", err)
	}
	if len(ve.Errors) != 2 {
		t.Fatalf("got %d errors, want 2: %v", len(ve.Errors), ve.Errors)
	}
	drain, rl := ve.Errors[0], ve.Errors[1]
	if drain.Path != "global.drain_timeout" || drain.Line != 10 || !strings.Contains(drain.Msg, "soon") {
		t.Fatalf("first error = %+v, want global.drain_timeout on line 10 naming %q", drain, "soon")
	}
	if rl.Path != "routes[0].rate_limit.store" || rl.Line != 17 || rl.Msg != `store "s" (type "plain") does not support rate limiting` {
		t.Fatalf("second error = %+v, want routes[0].rate_limit.store on line 17 reporting the capability mismatch", rl)
	}
}

func TestResolveDoesNotModifyConfig(t *testing.T) {
	doc := "version: 1\n" + listenersYAML + `upstreams:
  - name: up
    hosts:
      - url: "http://127.0.0.1:1"
stores:
  - name: s
    type: nosuch
middlewares:
  - name: m
    type: mystery
routes:
  - name: r
    upstream: up
`
	cfg, err := config.Parse([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	before := cfg.Clone()
	fp := cfg.Fingerprint()
	errs := resolve(cfg, runtime.NewStoreSet(nil), middleware.NewRegistry())
	if len(errs) != 2 {
		t.Fatalf("got %d errors, want 2: %v", len(errs), errs)
	}
	if cfg.Fingerprint() != fp {
		t.Fatal("resolve changed the fingerprint of the config")
	}
	if !reflect.DeepEqual(cfg, before) {
		t.Fatal("resolve modified the config")
	}
}
