package e2e

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
)

const opSecret = "0nepass-UNIQUE-4"

func opLine(t *testing.T, doc, substr string) int {
	t.Helper()
	for i, line := range strings.Split(doc, "\n") {
		if strings.Contains(line, substr) {
			return i + 1
		}
	}
	t.Fatalf("%q not found in document", substr)
	return 0
}

func opErrors(t *testing.T, err error) []config.FieldError {
	t.Helper()
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error = %T (%v), want *config.ValidationError", err, err)
	}
	return ve.Errors
}

func opSorted(errs []config.FieldError) []config.FieldError {
	out := slices.Clone(errs)
	slices.SortFunc(out, func(a, b config.FieldError) int {
		if c := cmp.Compare(a.Line, b.Line); c != 0 {
			return c
		}
		if c := strings.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		return strings.Compare(a.Msg, b.Msg)
	})
	return out
}

func opWant(t *testing.T, err error, want []config.FieldError) {
	t.Helper()
	got := opErrors(t, err)
	paths := map[string]bool{}
	for _, fe := range got {
		if paths[fe.Path] {
			t.Fatalf("path %q reported more than once: %v", fe.Path, got)
		}
		paths[fe.Path] = true
	}
	g, w := opSorted(got), opSorted(want)
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("errors mismatch\n got: %v\nwant: %v", g, w)
	}
}

func opBadDoc(backendURL string) string {
	return fmt.Sprintf(`version: 1
global:
  drain_timeout: soon
%supstreams:
  - name: up
    hosts:
      - url: %q
stores:
  - name: shared
    type: nosuch
middlewares:
  - name: tag
    type: mystery
routes:
  - name: limited
    match:
      path: /limited
    upstream: up
    rate_limit:
      rate: 5
      per: 1s
      store: missing
`, listenersYAML, backendURL)
}

func opBadWant(t *testing.T, doc string) []config.FieldError {
	t.Helper()
	return []config.FieldError{
		{Path: "global.drain_timeout", Line: opLine(t, doc, "drain_timeout: soon"), Msg: `invalid duration "soon" (use a value such as "250ms" or "5s")`},
		{Path: "stores[0].type", Line: opLine(t, doc, "type: nosuch"), Msg: `store type "nosuch" is not registered`},
		{Path: "middlewares[0].type", Line: opLine(t, doc, "type: mystery"), Msg: `unknown middleware type "mystery"`},
		{Path: "routes[0].rate_limit.store", Line: opLine(t, doc, "store: missing"), Msg: `store "missing" is not defined in stores`},
	}
}

func opTagger() microgateway.Option {
	return microgateway.WithMiddleware("tagger", func(microgateway.MiddlewareParams) (microgateway.Middleware, error) {
		return func(h http.Handler) http.Handler { return h }, nil
	})
}

func TestOnePassLoadReportsEverything(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("A"))
	doc := opBadDoc(be.URL)

	_, err := microgateway.NewFromYAML([]byte(doc))
	opWant(t, err, opBadWant(t, doc))

	g := n.gateway(t, watchDoc(be.URL, "/x"))
	fp := g.Config().Fingerprint()
	if got := servedBy(g, "/x"); got != "A" {
		t.Fatalf("served by %q before reload, want A", got)
	}
	_, f0, _ := counters(g)

	err = g.ReloadYAML([]byte(doc))
	opWant(t, err, opBadWant(t, doc))
	if _, f, _ := counters(g); f != f0+1 {
		t.Fatalf("failure counter after ReloadYAML = %v, want %v", f, f0+1)
	}
	if g.Config().Fingerprint() != fp {
		t.Fatal("ReloadYAML changed the active configuration")
	}
	if got := servedBy(g, "/x"); got != "A" {
		t.Fatalf("served by %q after ReloadYAML, want A", got)
	}

	path := writeConfig(t, t.TempDir(), doc)
	err = g.ReloadFile(path)
	opWant(t, err, opBadWant(t, doc))
	if _, f, _ := counters(g); f != f0+2 {
		t.Fatalf("failure counter after ReloadFile = %v, want %v", f, f0+2)
	}
	if g.Config().Fingerprint() != fp {
		t.Fatal("ReloadFile changed the active configuration")
	}
	if got := servedBy(g, "/x"); got != "A" {
		t.Fatalf("served by %q after ReloadFile, want A", got)
	}
}

func TestOnePassMixedDocuments(t *testing.T) {
	head := "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n    hosts:\n      - url: http://127.0.0.1:9001\n"
	route := "routes:\n  - name: r\n    match:\n      path: /r\n    upstream: up\n"
	type opCase struct {
		name  string
		doc   string
		opts  []microgateway.Option
		want  []config.FieldError
		count bool
	}
	var cases []opCase

	doc := head + "stores:\n  - name: s\n    type: redis\n" + route
	cases = append(cases, opCase{name: "redis without block", doc: doc, want: []config.FieldError{
		{Path: "stores[0].redis", Line: opLine(t, doc, "- name: s"), Msg: "is required for type redis"},
		{Path: "stores[0].type", Line: opLine(t, doc, "type: redis"), Msg: `store type "redis" is not registered`},
	}})

	doc = head + "stores:\n  - name: -bad\n    type: nosuch\n" + route
	cases = append(cases, opCase{name: "schema plus type", doc: doc, want: []config.FieldError{
		{Path: "stores[0].name", Line: opLine(t, doc, "name: -bad"), Msg: "must match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"},
		{Path: "stores[0].type", Line: opLine(t, doc, "type: nosuch"), Msg: `store type "nosuch" is not registered`},
	}})

	doc = head + "middlewares:\n  - name: m\n" + route
	cases = append(cases, opCase{name: "same path once", doc: doc, want: []config.FieldError{
		{Path: "middlewares[0].type", Line: opLine(t, doc, "- name: m"), Msg: "is required"},
	}})

	doc = head + "middlewares:\n  - name: a\n    type: mystery\n  - 7\n" + route
	cases = append(cases, opCase{name: "nested under failing path", doc: doc, want: []config.FieldError{
		{Path: "middlewares[0].type", Line: opLine(t, doc, "type: mystery"), Msg: `unknown middleware type "mystery"`},
		{Path: "middlewares[1]", Line: opLine(t, doc, "  - 7"), Msg: "cannot unmarshal !!int `7` into config.Middleware"},
	}})

	doc = "version: [\n"
	cases = append(cases, opCase{name: "syntax error", doc: doc, count: true, want: []config.FieldError{
		{Path: "", Line: 1, Msg: "did not find expected node content"},
	}})

	doc = "- a\n- b\n"
	cases = append(cases, opCase{name: "undecodable root", doc: doc, count: true, want: []config.FieldError{
		{Path: "", Line: 1, Msg: "cannot unmarshal !!seq into config.Config"},
	}})

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := microgateway.NewFromYAML([]byte(tc.doc), tc.opts...)
			opWant(t, err, tc.want)

			base := gwWith(t, watchDoc("http://127.0.0.1:9001", "/x"), tc.opts...)
			_, f0, _ := counters(base)
			err = base.ReloadYAML([]byte(tc.doc))
			opWant(t, err, tc.want)
			if tc.count {
				if _, f, _ := counters(base); f != f0+1 {
					t.Fatalf("failure counter = %v, want %v", f, f0+1)
				}
			}
		})
	}
}

func TestOnePassRegisteredTypes(t *testing.T) {
	fake := sapiFake()
	shared := stShared(t, fake)
	var calls atomic.Int64
	opts := []microgateway.Option{withClock(fake), stOption(shared, &calls), opTagger()}
	head := "version: 1\n" + listenersYAML + "upstreams:\n  - name: up\n    hosts:\n      - url: http://127.0.0.1:9001\n"
	route := "routes:\n  - name: r\n    match:\n      path: /r\n    upstream: up\n"
	doc := head + "stores:\n  - name: s\n    type: test\nmiddlewares:\n  - name: t\n    type: tagger\n" + route
	backend := "http://127.0.0.1:9001"

	g, err := microgateway.NewFromYAML([]byte(doc), opts...)
	if err != nil {
		t.Fatalf("NewFromYAML with registered types = %v, want nil", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	base := gwWith(t, watchDoc(backend, "/x"), opts...)
	if err := base.ReloadYAML([]byte(doc)); err != nil {
		t.Fatalf("ReloadYAML with registered types = %v, want nil", err)
	}
	if err := base.ReloadFile(writeConfig(t, t.TempDir(), doc)); err != nil {
		t.Fatalf("ReloadFile with registered types = %v, want nil", err)
	}
	if s, _, _ := counters(base); s != 1 {
		t.Fatalf("success counter = %v, want 1", s)
	}

	want := []config.FieldError{
		{Path: "stores[0].type", Line: opLine(t, doc, "type: test"), Msg: `store type "test" is not registered`},
		{Path: "middlewares[0].type", Line: opLine(t, doc, "type: tagger"), Msg: `unknown middleware type "tagger"`},
	}
	_, err = microgateway.NewFromYAML([]byte(doc))
	opWant(t, err, want)

	bare := gwWith(t, watchDoc(backend, "/x"), withClock(fake))
	err = bare.ReloadYAML([]byte(doc))
	opWant(t, err, want)
}

func TestOnePassWatchFile(t *testing.T) {
	n := newMemNetwork()
	beA := n.serve(t, echoHandler("A"))
	beB := n.serve(t, echoHandler("B"))
	var buf logBuf
	g := n.gateway(t, watchDoc(beA.URL, "/x"), microgateway.WithLogger(newJSONLogger(&buf)))
	path := writeConfig(t, t.TempDir(), watchDoc(beA.URL, "/x"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.WatchFile(ctx, path) }()

	bad := opBadDoc(beA.URL)
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	const msg = "configuration reload failed; previous configuration remains active"
	eventually(t, func() bool {
		_, f, _ := counters(g)
		return f >= 1 && len(buf.records(t, msg)) > 0
	})
	if got := servedBy(g, "/x"); got != "A" {
		t.Fatalf("served by %q after bad document, want A", got)
	}

	var got []string
	for _, rec := range buf.records(t, msg) {
		errs, ok := rec["errors"].([]any)
		if !ok {
			t.Fatalf("record errors = %T, want a list", rec["errors"])
		}
		for _, e := range errs {
			got = append(got, fmt.Sprint(e))
		}
	}
	var want []string
	for _, fe := range opBadWant(t, bad) {
		want = append(want, fe.String())
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("logged errors = %q, want %q", got, want)
	}

	if err := os.WriteFile(path, []byte(watchDoc(beB.URL, "/x")), 0o600); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return servedBy(g, "/x") == "B" })
	cancel()
	if err := awaitWatch(t, done); err != nil {
		t.Fatalf("WatchFile = %v, want nil", err)
	}
}

func opSecretDoc() string {
	return fmt.Sprintf(`version: 1
global:
  drain_timeout: soon
%supstreams:
  - name: up
    hosts:
      - url: "http://op:%s@127.0.0.1:9001"
stores:
  - name: r
    type: redis
    redis:
      mode: ring
      password: %q
      addresses: ["10.0.0.2:6379"]
  - name: c
    type: custom
    params:
      token: %q
middlewares:
  - name: m
    type: mystery
routes:
  - name: r
    match:
      path: /r
    upstream: up
    request_headers:
      set:
        X-Token: %s
`, listenersYAML, opSecret, stSecret, stParamSecret, opSecret)
}

func opSecretCheck(t *testing.T, name string, err error) {
	t.Helper()
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("%s error = %T (%v), want *config.ValidationError", name, err, err)
	}
	paths := map[string]bool{}
	for _, fe := range ve.Errors {
		paths[fe.Path] = true
	}
	for _, p := range []string{"global.drain_timeout", "stores[0].type", "stores[0].redis.mode", "stores[1].type", "middlewares[0].type"} {
		if !paths[p] {
			t.Errorf("%s: no error at %s", name, p)
		}
	}
	out := srdRender(t, err) + err.Error() + fmt.Sprintf("%v|%+v|%#v", ve, ve, ve.Errors)
	srdLeakCheck(t, map[string]string{name: out})
	for _, f := range srdForms(opSecret) {
		if strings.Contains(out, f.text) {
			t.Errorf("%s output contains the request header secret in %s form", name, f.label)
		}
	}
}

func TestOnePassSecretsNeverSurface(t *testing.T) {
	doc := opSecretDoc()
	_, err := microgateway.NewFromYAML([]byte(doc))
	opSecretCheck(t, "NewFromYAML", err)

	base := gwWith(t, watchDoc("http://127.0.0.1:9001", "/x"))
	err = base.ReloadYAML([]byte(doc))
	opSecretCheck(t, "ReloadYAML", err)
}

func TestOnePassMultiInstance(t *testing.T) {
	n := newMemNetwork()
	be := n.serve(t, echoHandler("A"))
	fake := sapiFake()
	shared := stShared(t, fake)
	var calls atomic.Int64
	doc := stDoc(be.URL, stStore("shared", ""), stRoute("lim", "    rate_limit:\n      rate: 5\n      per: 1s\n      store: shared\n"))
	a := n.gateway(t, doc, withClock(fake), stOption(shared, &calls))
	b := n.gateway(t, doc, withClock(fake), stOption(shared, &calls), opTagger())

	bad := opBadDoc(be.URL)
	opWant(t, a.ReloadYAML([]byte(bad)), opBadWant(t, bad))
	if s, f, _ := counters(a); s != 0 || f != 1 {
		t.Fatalf("A counters = success %v failure %v, want 0 and 1", s, f)
	}
	if s, f, _ := counters(b); s != 0 || f != 0 {
		t.Fatalf("B counters = success %v failure %v, want 0 and 0", s, f)
	}

	for _, g := range []*microgateway.Gateway{b, a} {
		if rec := stDo(t, g, "/lim", smClient); rec.Code != http.StatusOK {
			t.Fatalf("GET /lim = %d, want 200", rec.Code)
		}
	}

	withTag := doc + "middlewares:\n  - name: t\n    type: tagger\n"
	opWant(t, a.ReloadYAML([]byte(withTag)), []config.FieldError{
		{Path: "middlewares[0].type", Line: opLine(t, withTag, "type: tagger"), Msg: `unknown middleware type "tagger"`},
	})
	if s, f, _ := counters(a); s != 0 || f != 2 {
		t.Fatalf("A counters after tagger reload = success %v failure %v, want 0 and 2", s, f)
	}
	if err := b.ReloadYAML([]byte(withTag)); err != nil {
		t.Fatalf("B ReloadYAML = %v, want nil", err)
	}
	if s, f, _ := counters(b); s != 1 || f != 0 {
		t.Fatalf("B counters after reload = success %v failure %v, want 1 and 0", s, f)
	}
}
