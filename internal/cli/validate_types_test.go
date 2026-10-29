package cli

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/store"
)

func TestValidateUnknownMiddlewareType(t *testing.T) {
	src := minimalConfig + `middlewares:
  - name: x
    type: my_custom
    params:
      anything: 1
`
	path := writeTemp(t, src)
	want := fmt.Sprintf("middlewares[0].type (line %d): unknown middleware type \"my_custom\"", lineOf(t, src, "type: my_custom"))

	code, _, errs := runCmd(t, "validate", "-config", path)
	if code != 1 {
		t.Fatalf("validate exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, want) {
		t.Errorf("validate stderr lacks %q\nstderr:\n%s", want, errs)
	}
	if !strings.Contains(errs, ": 1 error(s)") {
		t.Errorf("validate stderr lacks the error count\nstderr:\n%s", errs)
	}

	code, _, errs = runCmd(t, "run", "-config", path)
	if code != 1 {
		t.Fatalf("run exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, want) {
		t.Errorf("run stderr lacks %q\nstderr:\n%s", want, errs)
	}
}

func TestValidateUnknownStoreType(t *testing.T) {
	src := minimalConfig + `stores:
  - name: shared
    type: redis
    redis:
      addresses: ["127.0.0.1:1"]
`
	path := writeTemp(t, src)
	want := fmt.Sprintf("stores[0].type (line %d): store type \"redis\" is not registered", lineOf(t, src, "type: redis"))

	code, _, errs := runCmd(t, "validate", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	if !strings.Contains(errs, want) {
		t.Errorf("stderr lacks %q\nstderr:\n%s", want, errs)
	}

	var built atomic.Int32
	f := func(microgateway.StoreParams) (store.Backend, error) {
		built.Add(1)
		return nil, errors.New("not built")
	}
	code, out, errs := runWith(t, []microgateway.Option{microgateway.WithStoreType("redis", store.CapRateLimit, f)}, "validate", "-config", path)
	if code != 0 {
		t.Fatalf("registered exit code = %d, want 0; stderr:\n%s", code, errs)
	}
	if !strings.Contains(out, "configuration is valid") {
		t.Errorf("stdout = %q, want it to contain %q", out, "configuration is valid")
	}
	if n := built.Load(); n != 0 {
		t.Errorf("store factory called %d times during validate, want 0", n)
	}
}

func TestValidateOnePass(t *testing.T) {
	src := strings.Replace(minimalConfig, "upstream: users", "upstream: nowhere", 1) + `stores:
  - name: a
    type: memcache
  - name: b
    type: redis
middlewares:
  - name: x
    type: my_custom
`
	path := writeTemp(t, src)
	var built atomic.Int32
	f := func(microgateway.StoreParams) (store.Backend, error) {
		built.Add(1)
		return nil, errors.New("not built")
	}
	code, _, errs := runWith(t, []microgateway.Option{microgateway.WithStoreType("redis", store.CapRateLimit, f)}, "validate", "-config", path)
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	wants := []string{
		fmt.Sprintf("routes[0].upstream (line %d): ", lineOf(t, src, "upstream: nowhere")),
		fmt.Sprintf("stores[0].type (line %d): store type \"memcache\" is not registered", lineOf(t, src, "type: memcache")),
		"stores[1].redis (line ",
		"is required for type redis",
		fmt.Sprintf("middlewares[0].type (line %d): unknown middleware type \"my_custom\"", lineOf(t, src, "type: my_custom")),
	}
	for _, w := range wants {
		if !strings.Contains(errs, w) {
			t.Errorf("stderr lacks %q\nstderr:\n%s", w, errs)
		}
	}
	if !strings.HasSuffix(errs, ": 4 error(s)\n") {
		t.Errorf("stderr does not end with the 4-error count\nstderr:\n%s", errs)
	}
	if n := built.Load(); n != 0 {
		t.Errorf("store factory called %d times during validate, want 0", n)
	}
}
