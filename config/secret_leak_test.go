package config

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/ok0/lychgate/internal/leakcheck"
)

func TestNoFormattingPathRevealsSecrets(t *testing.T) {
	const (
		pw   = "pw-LEAK-7f3a9"
		par  = "par-LEAK-91c2"
		deep = "deep-LEAK-55e1"
		lst  = "lst-LEAK-0b7d"
		num  = 918273645
		upw  = "urlpw-LEAK-c41e"
		uq   = "urlq-LEAK-77ad"
		hexN = "36bbbe6d"
	)

	leaf := func(typ reflect.Type) (reflect.Value, bool) {
		switch typ {
		case reflect.TypeFor[Secret]():
			return reflect.ValueOf(NewSecret(pw)), true
		case reflect.TypeFor[SecretParams]():
			return reflect.ValueOf(NewSecretParams(map[string]any{
				"tok": par,
				"n":   map[string]any{"deep": deep, "list": []any{lst}},
				"num": num,
			})), true
		case reflect.TypeFor[SecretURL]():
			return reflect.ValueOf(NewSecretURL("https://u:" + upw + "@h.example:8443/p?tok=" + uq)), true
		}
		return reflect.Value{}, false
	}

	types := leakcheck.Types(reflect.TypeFor[Config]())
	for _, root := range []reflect.Type{reflect.TypeFor[APIKeyFile](), reflect.TypeFor[HeadersParams](), reflect.TypeFor[CORSParams](), reflect.TypeFor[RequestSizeLimitParams](), reflect.TypeFor[ValidationError](), reflect.TypeFor[ParamsError]()} {
		for _, typ := range leakcheck.Types(root) {
			if !slices.Contains(types, typ) {
				types = append(types, typ)
			}
		}
	}
	targets := []reflect.Type{reflect.TypeFor[Secret](), reflect.TypeFor[SecretParams](), reflect.TypeFor[SecretURL]()}

	known := map[string]bool{"Duration": true, "ParseOption": true}
	for _, typ := range types {
		if typ.PkgPath() == reflect.TypeFor[Config]().PkgPath() {
			known[typ.Name()] = true
		}
	}
	exported, err := leakcheck.ExportedTypes(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range exported {
		if !known[n] {
			t.Errorf("exported type config.%s is not reached by the leak matrix; add it to the roots", n)
		}
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[Duration](), reflect.TypeFor[ParseOption]()} {
		if leakcheck.Reaches(typ, targets...) {
			t.Errorf("%s reaches a config secret; it needs a sample in the leak matrix", typ)
		}
	}

	var containing []reflect.Type
	for _, typ := range types {
		if leakcheck.Reaches(typ, targets...) {
			containing = append(containing, typ)
		}
	}
	for _, target := range targets {
		if !slices.Contains(containing, target) {
			containing = append(containing, target)
		}
	}

	required := []string{
		"config.Config", "config.Store", "config.RedisStore", "config.Auth", "config.JWTProvider",
		"config.JWTKey", "config.JWKS", "config.APIKeyProvider", "config.APIKey", "config.APIKeyFile",
		"config.Upstream", "config.Host", "config.Secret", "config.SecretParams", "config.SecretURL",
		"config.Route", "config.Match", "config.HeaderOps", "config.Middleware", "config.HeadersParams",
	}
	var names []string
	for _, typ := range containing {
		names = append(names, typ.String())
	}
	for _, name := range required {
		if !slices.Contains(names, name) {
			t.Fatalf("containing types lack %s; the leak matrix would be vacuous", name)
		}
	}

	for _, typ := range containing {
		t.Run(typ.String(), func(tt *testing.T) {
			v := leakcheck.Fill(typ, leaf)
			if !leakcheck.Holds(v.Interface(), pw, par, deep, lst, upw, uq) {
				tt.Fatalf("filled %s holds no secret; the check would be vacuous", typ)
			}
			leakcheck.Check(tt, typ.String(), v.Interface(), pw, par, deep, lst, "918273645", hexN, upw, uq)
		})
	}

	t.Run("regression", func(tt *testing.T) {
		render := func(format string, v any) string {
			return fmt.Sprintf(format, v)
		}
		assertNoForm := func(name, out string, secrets ...string) {
			tt.Helper()
			for _, s := range secrets {
				for _, f := range leakcheck.Forms(s) {
					if strings.Contains(out, f) {
						tt.Errorf("%s reveals a secret (form %q)", name, f)
					}
				}
			}
		}
		store := Store{Redis: &RedisStore{Password: NewSecret(pw)}}
		assertNoForm("%s of Store", render("%s", store), pw)
		cfg := Config{Stores: []Store{store}}
		assertNoForm("%s of Config", render("%s", cfg), pw)
		assertNoForm("%q of Config", render("%q", cfg), pw)
		assertNoForm("%10s of Config", render("%10s", cfg), pw)
		route := Route{RequestHeaders: HeaderOps{Set: map[string]Secret{"Authorization": NewSecret("Bearer " + pw)}}, Match: Match{Headers: map[string]Secret{"X": NewSecret(pw)}}}
		assertNoForm("%s of Route", render("%s", route), pw)
		assertNoForm("%q of Route", render("%q", route), pw)
		mw := struct{ P *Middleware }{&Middleware{Params: NewSecretParams(map[string]any{"tok": par})}}
		assertNoForm("%s of Middleware", render("%s", mw), par)
		assertNoForm("%q of Middleware", render("%q", mw), par)
		cfgMW := Config{Middlewares: []Middleware{{Params: NewSecretParams(map[string]any{"tok": par})}}}
		assertNoForm("%s of Config with middleware", render("%s", cfgMW), par)
		auth := Auth{JWT: []JWTProvider{{Keys: []JWTKey{{Secret: NewSecret(pw)}}, JWKS: &JWKS{URL: NewSecretURL("https://u:" + upw + "@h.example:8443/p?tok=" + uq)}}}}
		assertNoForm("%s of Auth", render("%s", auth), pw, upw, uq)
		upstream := struct{ P *Upstream }{&Upstream{Hosts: []Host{{URL: NewSecretURL("https://u:" + upw + "@h.example:8443/p?tok=" + uq)}}}}
		assertNoForm("%s of pointer-holding Upstream", render("%s", upstream), pw, upw, uq)
	})
}

func TestSecretFieldsTyped(t *testing.T) {
	fieldType := func(v any, name string) reflect.Type {
		f, ok := reflect.TypeOf(v).FieldByName(name)
		if !ok {
			t.Fatalf("%s has no field %s", reflect.TypeOf(v), name)
		}
		return f.Type
	}
	checks := []struct {
		name string
		got  reflect.Type
		want reflect.Type
	}{
		{"JWTKey.Secret", fieldType(JWTKey{}, "Secret"), reflect.TypeFor[Secret]()},
		{"APIKey.Hash", fieldType(APIKey{}, "Hash"), reflect.TypeFor[Secret]()},
		{"RedisStore.Password", fieldType(RedisStore{}, "Password"), reflect.TypeFor[Secret]()},
		{"RedisStore.SentinelPassword", fieldType(RedisStore{}, "SentinelPassword"), reflect.TypeFor[Secret]()},
		{"Store.Params", fieldType(Store{}, "Params"), reflect.TypeFor[SecretParams]()},
		{"Host.URL", fieldType(Host{}, "URL"), reflect.TypeFor[SecretURL]()},
		{"JWKS.URL", fieldType(JWKS{}, "URL"), reflect.TypeFor[SecretURL]()},
		{"HeaderOps.Add", fieldType(HeaderOps{}, "Add"), reflect.TypeFor[map[string]Secret]()},
		{"HeaderOps.Set", fieldType(HeaderOps{}, "Set"), reflect.TypeFor[map[string]Secret]()},
		{"Match.Headers", fieldType(Match{}, "Headers"), reflect.TypeFor[map[string]Secret]()},
		{"Middleware.Params", fieldType(Middleware{}, "Params"), reflect.TypeFor[SecretParams]()},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s is %s, want %s", c.name, c.got, c.want)
		}
	}
}
