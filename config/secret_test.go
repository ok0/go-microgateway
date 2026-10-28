package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"go.yaml.in/yaml/v3"

	"github.com/ok0/lychgate/internal/leakcheck"
)

type secretHolder struct {
	P Secret `json:"p" yaml:"p"`
}

type paramsHolder struct {
	P SecretParams `json:"p" yaml:"p"`
}

func secretRenderings(t *testing.T, value any) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%10s"} {
		out[verb] = fmt.Sprintf(verb, value)
	}
	out["Sprint"] = fmt.Sprint(value)
	out["Sprintln"] = fmt.Sprintln(value)

	j, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	out["json"] = string(j)

	y, err := yaml.Marshal(value)
	if err != nil {
		t.Fatalf("yaml.Marshal: %v", err)
	}
	out["yaml"] = string(y)

	var jsonBuf, textBuf bytes.Buffer
	jl := slog.New(slog.NewJSONHandler(&jsonBuf, nil))
	tl := slog.New(slog.NewTextHandler(&textBuf, nil))
	for _, l := range []*slog.Logger{jl, tl} {
		l.Info("event", "attr", value)
		l.Info("event", "struct", struct{ V any }{value})
		l.Info("event", "any", slog.AnyValue(value))
	}
	out["slog-json"] = jsonBuf.String()
	out["slog-text"] = textBuf.String()
	return out
}

func TestSecretNeverFormats(t *testing.T) {
	const raw = "hunter2-secret"
	s := NewSecret(raw)
	for name, got := range secretRenderings(t, s) {
		if strings.Contains(got, raw) {
			t.Errorf("%s leaks the secret: %q", name, got)
		}
	}
	for name, got := range secretRenderings(t, secretHolder{P: s}) {
		if strings.Contains(got, raw) {
			t.Errorf("struct field %s leaks the secret: %q", name, got)
		}
	}

	if s.Reveal() != raw {
		t.Errorf("Reveal() = %q, want the secret", s.Reveal())
	}
	if s.String() != Redacted || NewSecret("").String() != "" {
		t.Errorf("String() = %q / %q", s.String(), NewSecret("").String())
	}
	if got := fmt.Sprint(Secret{}); got != "" {
		t.Errorf("zero Secret formats as %q", got)
	}

	store := RedisStore{Password: s, SentinelPassword: s, Addresses: []string{"127.0.0.1:6379"}}
	for name, got := range secretRenderings(t, store) {
		if strings.Contains(got, raw) {
			t.Errorf("RedisStore %s leaks the secret: %q", name, got)
		}
	}
	for _, v := range []any{&store, store} {
		if got := fmt.Sprintf("%+v", v); strings.Contains(got, raw) {
			t.Errorf("RedisStore %T with %%+v leaks the secret: %q", v, got)
		}
	}
	b, err := json.Marshal(store)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), raw) {
		t.Errorf("RedisStore JSON leaks the secret: %s", b)
	}
	if !strings.Contains(string(b), Redacted) {
		t.Errorf("RedisStore JSON has no REDACTED marker: %s", b)
	}

	tb, err := s.MarshalText()
	if err != nil || strings.Contains(string(tb), raw) {
		t.Errorf("MarshalText = %q, %v", tb, err)
	}
	lv := s.LogValue().String()
	if strings.Contains(lv, raw) {
		t.Errorf("LogValue leaks the secret: %q", lv)
	}
}

func TestSecretParamsNeverFormat(t *testing.T) {
	params := NewSecretParams(map[string]any{
		"token": "tok-123456",
		"n":     map[string]any{"deep": "deep-secret"},
		"num":   42,
	})
	secrets := []string{"tok-123456", "deep-secret"}
	check := func(label string, v any) {
		t.Helper()
		for name, got := range secretRenderings(t, v) {
			for _, sec := range secrets {
				if strings.Contains(got, sec) {
					t.Errorf("%s %s leaks %q: %q", label, name, sec, got)
				}
			}
		}
	}
	check("SecretParams", params)
	check("nested SecretParams", NewSecretParams(map[string]any{"outer": NewSecretParams(map[string]any{"deep": "deep-secret"}), "token": "tok-123456"}))
	check("struct field", paramsHolder{P: params})
	check("pointer", &params)

	if got := params.StringLeaves(); !slices.Equal(got, []string{"deep-secret", "tok-123456"}) {
		t.Errorf("StringLeaves() = %v", got)
	}
	if params.Reveal()["token"] != "tok-123456" {
		t.Errorf("source params changed: %v", params.Reveal()["token"])
	}
	if got := params.Redacted().Reveal()["num"]; got != Redacted {
		t.Errorf("number leaf = %v, want REDACTED", got)
	}
}

func TestSecretURLFormats(t *testing.T) {
	const raw = "https://u:urlpw-LEAK-c41e@h.example:8443/p?tok=urlq-LEAK-77ad"
	const host = "h.example:8443/p"
	leaks := []string{"urlpw-LEAK-c41e", "urlq-LEAK-77ad"}
	u := NewSecretURL(raw)

	if u.Reveal() != raw {
		t.Errorf("Reveal() = %q, want the input", u.Reveal())
	}

	renderings := map[string]string{}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		renderings[verb] = fmt.Sprintf(verb, u)
	}
	j, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	renderings["json"] = string(j)
	y, err := yaml.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	renderings["yaml"] = string(y)
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("event", "url", u)
	renderings["slog-json"] = buf.String()

	for name, got := range renderings {
		if !strings.Contains(got, host) || !strings.Contains(got, Redacted) {
			t.Errorf("%s = %q, want the host, path and REDACTED", name, got)
		}
		for _, leak := range leaks {
			if strings.Contains(got, leak) {
				t.Errorf("%s leaks a URL credential or query value", name)
			}
		}
	}

	if got := fmt.Sprint(SecretURL{}); got != "" {
		t.Errorf("zero SecretURL formats as %q", got)
	}
	leakcheck.Check(t, "SecretURL", u, leaks...)
}

func TestDecodeSecretParams(t *testing.T) {
	type target struct {
		Port int `yaml:"port"`
		N    struct {
			Known string `yaml:"known"`
		} `yaml:"n"`
	}

	var unknown target
	err := DecodeSecretParams(NewSecretParams(map[string]any{"x": "x-secret-value"}), &unknown)
	var pe *ParamsError
	if !errors.As(err, &pe) || len(pe.Errors) != 1 {
		t.Fatalf("unknown key: err = %v", err)
	}
	if pe.Errors[0].Path != "x" || pe.Errors[0].Msg != `unknown key "x"` {
		t.Errorf("unknown key error = %+v", pe.Errors[0])
	}
	if strings.Contains(err.Error(), "x-secret-value") {
		t.Errorf("error leaks the value: %v", err)
	}

	var wrong target
	err = DecodeSecretParams(NewSecretParams(map[string]any{"port": "p0rt-secret"}), &wrong)
	if !errors.As(err, &pe) || len(pe.Errors) != 1 {
		t.Fatalf("wrong type: err = %v", err)
	}
	if pe.Errors[0].Path != "port" || pe.Errors[0].Msg != "cannot decode a !!str value into int" {
		t.Errorf("wrong type error = %+v", pe.Errors[0])
	}
	if strings.Contains(err.Error(), "p0rt-secret") {
		t.Errorf("error leaks the value: %v", err)
	}

	var nested target
	err = DecodeSecretParams(NewSecretParams(map[string]any{"n": map[string]any{"deep": "deep-secret"}}), &nested)
	if !errors.As(err, &pe) || len(pe.Errors) != 1 {
		t.Fatalf("nested unknown: err = %v", err)
	}
	if pe.Errors[0].Path != "n.deep" || pe.Errors[0].Msg != `unknown key "deep"` {
		t.Errorf("nested error = %+v, want path n.deep", pe.Errors[0])
	}
	if strings.Contains(err.Error(), "deep-secret") {
		t.Errorf("error leaks the value: %v", err)
	}

	var ok target
	if err := DecodeSecretParams(NewSecretParams(map[string]any{"port": 8080, "n": map[string]any{"known": "ok"}}), &ok); err != nil {
		t.Fatalf("valid decode: %v", err)
	}
	if ok.Port != 8080 || ok.N.Known != "ok" {
		t.Errorf("decoded = %+v", ok)
	}

	var none target
	if err := DecodeSecretParams(SecretParams{}, &none); err != nil {
		t.Errorf("nil params: %v", err)
	}
}
