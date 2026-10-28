package lychgate

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/leakcheck"
	"github.com/ok0/lychgate/internal/teststore"
	"github.com/ok0/lychgate/store"
)

type srdBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *srdBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *srdBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type srdTee struct{ hs []slog.Handler }

func (t srdTee) Enabled(ctx context.Context, l slog.Level) bool {
	for _, h := range t.hs {
		if h.Enabled(ctx, l) {
			return true
		}
	}
	return false
}

func (t srdTee) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, h := range t.hs {
		if h.Enabled(ctx, r.Level) {
			errs = append(errs, h.Handle(ctx, r.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (t srdTee) WithAttrs(attrs []slog.Attr) slog.Handler {
	hs := make([]slog.Handler, len(t.hs))
	for i, h := range t.hs {
		hs[i] = h.WithAttrs(attrs)
	}
	return srdTee{hs: hs}
}

func (t srdTee) WithGroup(name string) slog.Handler {
	hs := make([]slog.Handler, len(t.hs))
	for i, h := range t.hs {
		hs[i] = h.WithGroup(name)
	}
	return srdTee{hs: hs}
}

func srdMessages(h *captureHandler) []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, r := range h.records {
		out = append(out, r.Message)
	}
	return out
}

type srdForm struct{ label, text string }

func srdForms(v string) []srdForm {
	return []srdForm{
		{"raw", v},
		{"query-escaped", url.QueryEscape(v)},
		{"path-escaped", url.PathEscape(v)},
		{"base64", base64.StdEncoding.EncodeToString([]byte(v))},
		{"raw base64", base64.RawStdEncoding.EncodeToString([]byte(v))},
		{"url base64", base64.URLEncoding.EncodeToString([]byte(v))},
		{"raw url base64", base64.RawURLEncoding.EncodeToString([]byte(v))},
		{"hex", hex.EncodeToString([]byte(v))},
	}
}

func srdLeakCheck(t *testing.T, outputs map[string]string) {
	t.Helper()
	secrets := []struct{ kind, value string }{
		{"redis password", stSecret},
		{"sentinel password", stSentinelSecret},
		{"param token", stParamSecret},
	}
	for _, name := range slices.Sorted(maps.Keys(outputs)) {
		for _, s := range secrets {
			for _, f := range srdForms(s.value) {
				if strings.Contains(outputs[name], f.text) {
					t.Errorf("output %s contains the %s in %s form", name, s.kind, f.label)
				}
			}
		}
	}
}

func srdRender(t *testing.T, v any) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "%v|%+v|%#v|%s|%q|%x|%X|%10s\n", v, v, v, v, v, v, v, v)
	fmt.Fprintln(&b, fmt.Sprint(v))
	js, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	b.Write(js)
	b.WriteByte('\n')
	ys, err := yaml.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	b.Write(ys)
	slog.New(slog.NewTextHandler(&b, nil)).Info("render", "value", v)
	slog.New(slog.NewJSONHandler(&b, nil)).Info("render", "value", v)
	return b.String()
}

func srdStore(t *testing.T, cfg *config.Config, name string) *config.Store {
	t.Helper()
	for i := range cfg.Stores {
		if cfg.Stores[i].Name == name {
			return &cfg.Stores[i]
		}
	}
	t.Fatalf("store %q not found in config", name)
	return nil
}

func srdStores() string {
	return fmt.Sprintf(`  - name: cache
    type: redis
    redis:
      mode: sentinel
      master_name: mymaster
      addresses:
        - "sentinel.internal:26379"
      username: app
      password: %q
      sentinel_username: watcher
      sentinel_password: %q
  - name: open
    type: redis
    redis:
      addresses:
        - "open.internal:6379"
      password: ""
  - name: vault
    type: vault
    params:
      token: %q
      empty: ""
      retries: 3
      nested:
        key: %q
`, stSecret, stSentinelSecret, stParamSecret, stParamSecret)
}

func srdRoutes() string {
	return stRoute("cached", "    rate_limit:\n      rate: 2\n      per: 1h\n      burst: 2\n      store: cache\n      on_store_error: local\n") +
		stRoute("vaulted", "    rate_limit:\n      rate: 2\n      per: 1h\n      burst: 2\n      store: vault\n      on_store_error: deny\n")
}

func srdDoc(backend string) string {
	return stDoc(backend, srdStores(), srdRoutes())
}

func srdBadDoc(backend string) string {
	stores := fmt.Sprintf(`  - name: cache
    type: redis
    redis:
      mode: standalone
      addresses:
        - "redis://:%s@cache.internal:6379"
      password: %q
      sentinel_password: %q
      key_prefix: "a{b"
  - name: vault
    type: vault
    params:
      token: %q
`, stSecret, stSecret, stSentinelSecret, stParamSecret)
	return stDoc(backend, stores, stRoute("cached", "    rate_limit:\n      rate: 2\n      per: 1h\n      store: cache\n"))
}

func srdMismatchDoc(backend string) string {
	stores := fmt.Sprintf("  - name: cache\n    type: redis\n    redis:\n      addresses:\n        - \"cache.internal:6379\"\n      db: %q\n", stSecret)
	return stDoc(backend, stores, stRoute("cached", "    rate_limit:\n      rate: 2\n      per: 1h\n      store: cache\n"))
}

func srdEnvDoc(backend, password, onErr string) string {
	stores := fmt.Sprintf("  - name: cache\n    type: redis\n    redis:\n      addresses:\n        - \"cache.internal:6379\"\n      password: %s\n", password)
	route := stRoute("envd", "    rate_limit:\n      rate: 2\n      per: 1h\n      store: cache\n      on_store_error: "+onErr+"\n")
	return stDoc(backend, stores, route)
}

func srdFailed(t *testing.T, err error, what string) string {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected an error", what)
	}
	return err.Error()
}

func TestStoreSecretsNeverSurface(t *testing.T) {
	fake := clock.NewFake(time.Unix(1_700_000_000, 0))
	shared := stShared(t, fake)
	n := newMemNetwork()
	be, _ := rlBackend(t, n)

	jsonBuf, textBuf := &srdBuf{}, &srdBuf{}
	capture := &captureHandler{}
	logger := slog.New(srdTee{hs: []slog.Handler{
		slog.NewJSONHandler(jsonBuf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		slog.NewTextHandler(textBuf, &slog.HandlerOptions{Level: slog.LevelDebug}),
		capture,
	}})

	var rendered []string
	opts := []Option{
		withClock(fake),
		WithLogger(logger),
		WithStoreType("redis", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
			p.Logger.Info("redis store entry built", "params", p, "redis", p.Redis)
			rendered = append(rendered, srdRender(t, p), srdRender(t, &p), srdRender(t, p.Redis))
			h := shared.Handle()
			h.SetMode(teststore.Fail, fmt.Errorf("auth %s sentinel %s rejected: %w", stSecret, stSentinelSecret, store.ErrUnavailable))
			return h, nil
		}),
		WithStoreType("vault", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
			p.Logger.Info("vault store entry built", "params", p)
			rendered = append(rendered, srdRender(t, p), srdRender(t, &p))
			h := shared.Handle()
			h.SetMode(teststore.Fail, fmt.Errorf("token %s rejected: %w", stParamSecret, store.ErrUnavailable))
			return h, nil
		}),
	}

	g := n.gateway(t, srdDoc(be.URL), opts...)

	const client = "192.0.2.10:4000"
	var statuses []int
	var limited string
	for range 5 {
		rec := stDo(t, g, "/cached", client)
		statuses = append(statuses, rec.Code)
		if rec.Code == http.StatusTooManyRequests {
			limited = rec.Body.String()
		}
	}
	if want := []int{200, 200, 429, 429, 429}; !slices.Equal(statuses, want) {
		t.Fatalf("cached statuses = %v, want %v", statuses, want)
	}

	var unavailable string
	for range 4 {
		rec := stDo(t, g, "/vaulted", client)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("vaulted status = %d, want 503", rec.Code)
		}
		unavailable = rec.Body.String()
	}
	if limited == "" || unavailable == "" {
		t.Fatal("expected a 429 body and a 503 body")
	}

	msgs := srdMessages(capture)
	for _, want := range []string{
		"rate limit store unavailable; failure policies active",
		"rate limit store call failed; failure policy applied",
	} {
		if !slices.Contains(msgs, want) {
			t.Errorf("missing log record %q", want)
		}
	}
	if got := stGauge(t, g, "lychgate_store_available", "store", "cache"); got != 0 {
		t.Errorf("cache store_available = %v, want 0", got)
	}
	if got := stCounter(t, g, "lychgate_store_errors_total", "store", "cache", "op", "take"); got != 3 {
		t.Errorf("cache store_errors_total = %v, want 3", got)
	}
	if got := stCounter(t, g, "lychgate_store_errors_total", "store", "vault", "op", "take"); got != 3 {
		t.Errorf("vault store_errors_total = %v, want 3", got)
	}

	metricsBody := metricsText(t, g)
	configBody := adminBody(t, g, "/config")

	cfg := g.Config()
	cache := srdStore(t, cfg, "cache")
	vault := srdStore(t, cfg, "vault")
	stored := srdRender(t, cache) + srdRender(t, cache.Redis) + srdRender(t, cache.Redis.Password) + srdRender(t, cache.Redis.SentinelPassword)
	stored += srdRender(t, vault) + srdRender(t, vault.Params) + srdRender(t, &vault.Params)

	var cfgJSON struct {
		Stores []struct {
			Name  string `json:"name"`
			Redis *struct {
				Password         string `json:"password"`
				SentinelPassword string `json:"sentinel_password"`
			} `json:"redis"`
			Params map[string]any `json:"params"`
		} `json:"stores"`
	}
	if err := json.Unmarshal([]byte(configBody), &cfgJSON); err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]int)
	for i, s := range cfgJSON.Stores {
		byName[s.Name] = i
	}
	redisOf := func(name string) (pw, spw string) {
		i, ok := byName[name]
		if !ok || cfgJSON.Stores[i].Redis == nil {
			t.Fatalf("store %q with redis block missing from /config", name)
		}
		return cfgJSON.Stores[i].Redis.Password, cfgJSON.Stores[i].Redis.SentinelPassword
	}
	if pw, spw := redisOf("cache"); pw != config.Redacted || spw != config.Redacted {
		t.Errorf("/config cache passwords = (%q, %q), want REDACTED for both", pw, spw)
	}
	if pw, spw := redisOf("open"); pw != "" || spw != "" {
		t.Errorf("/config open passwords = (%q, %q), want empty for both", pw, spw)
	}
	if i, ok := byName["vault"]; !ok {
		t.Fatal("store vault missing from /config")
	} else {
		params := cfgJSON.Stores[i].Params
		if params["token"] != config.Redacted || params["retries"] != config.Redacted {
			t.Errorf("/config vault params = %v, want token and retries REDACTED", params)
		}
		if params["empty"] != "" {
			t.Errorf("/config vault empty param = %v, want empty", params["empty"])
		}
		nested, _ := params["nested"].(map[string]any)
		if nested["key"] != config.Redacted {
			t.Errorf("/config vault nested key = %v, want REDACTED", nested["key"])
		}
	}

	if got := cache.Redis.Password.Reveal(); got != stSecret {
		t.Error("source password no longer reveals after /config")
	}
	if got := cache.Redis.SentinelPassword.Reveal(); got != stSentinelSecret {
		t.Error("source sentinel password no longer reveals after /config")
	}
	if !slices.Contains(vault.Params.StringLeaves(), stParamSecret) {
		t.Error("source params no longer reveal their token after /config")
	}

	_, errBad := NewFromYAML([]byte(srdBadDoc(be.URL)), opts...)
	badText := srdFailed(t, errBad, "New with invalid store fields")
	_, errBare := NewFromYAML([]byte(srdBadDoc(be.URL)))
	bareText := srdFailed(t, errBare, "New without registered types")
	_, errMismatch := NewFromYAML([]byte(srdMismatchDoc(be.URL)), opts...)
	mismatchText := srdFailed(t, errMismatch, "New with a type mismatch")

	_, errFactory := NewFromYAML([]byte(srdDoc(be.URL)),
		withClock(fake),
		WithStoreType("redis", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
			return nil, fmt.Errorf("dial %s with password %s refused", p.Redis.Addresses[0], p.Redis.Password.Reveal())
		}),
		WithStoreType("vault", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
			return shared.Handle(), nil
		}),
	)
	factoryText := srdFailed(t, errFactory, "New with a failing factory")

	valid, err := config.Parse([]byte(srdDoc(be.URL)))
	if err != nil {
		t.Fatal(err)
	}
	valid.Stores[0].Redis.Addresses = []string{"redis://:" + stSecret + "@cache.internal:6379"}
	errValidateBare := Validate(valid)
	validateBareText := srdFailed(t, errValidateBare, "Validate without registered types")
	errValidate := Validate(valid, opts...)
	validateText := srdFailed(t, errValidate, "Validate with registered types")

	unsetLookup := func(string) (string, bool) { return "", false }
	_, errUnset := NewFromYAML([]byte(srdEnvDoc(be.URL, "${LG_UNSET_PW}", "local")), WithEnvLookup(unsetLookup))
	unsetText := srdFailed(t, errUnset, "New with an unset env reference")
	if !strings.Contains(unsetText, "LG_UNSET_PW") {
		t.Errorf("unset env error does not name LG_UNSET_PW")
	}
	setLookup := func(k string) (string, bool) {
		if k == "LG_SET_PW" {
			return stSecret, true
		}
		return "", false
	}
	_, errSet := NewFromYAML([]byte(srdEnvDoc(be.URL, "${LG_SET_PW}", "maybe")), withClock(fake), WithEnvLookup(setLookup),
		WithStoreType("redis", store.CapRateLimit, func(p StoreParams) (store.Backend, error) {
			return shared.Handle(), nil
		}))
	setText := srdFailed(t, errSet, "New with a set env reference and an invalid field")

	outputs := map[string]string{
		"log.json":        jsonBuf.String(),
		"log.text":        textBuf.String(),
		"metrics":         metricsBody,
		"config":          configBody,
		"response.429":    limited,
		"response.503":    unavailable,
		"factory":         strings.Join(rendered, "\n"),
		"format.store":    stored,
		"new.invalid":     badText,
		"new.bare":        bareText,
		"new.mismatch":    mismatchText,
		"new.factory":     factoryText,
		"validate.bare":   validateBareText,
		"validate":        validateText,
		"env.unset":       unsetText,
		"env.set.invalid": setText,
	}
	srdLeakCheck(t, outputs)

	if !strings.Contains(outputs["log.json"], config.Redacted) {
		t.Error("log output does not carry the scrubbed backend error")
	}
	if !strings.Contains(outputs["factory"], config.Redacted) {
		t.Error("factory renderings do not show the redacted secrets")
	}
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
