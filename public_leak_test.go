package lychgate

import (
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/leakcheck"
)

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
		"Gateway":           reflect.TypeFor[Gateway](),
		"Option":            reflect.TypeFor[Option](),
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
