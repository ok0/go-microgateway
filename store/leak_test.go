package store_test

import (
	"maps"
	"reflect"
	"slices"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/leakcheck"
	"github.com/ok0/go-microgateway/store"
	"github.com/ok0/go-microgateway/store/storetest"
)

func TestPublicStoreTypesHoldNoSecrets(t *testing.T) {
	targets := []reflect.Type{reflect.TypeFor[config.Secret](), reflect.TypeFor[config.SecretParams](), reflect.TypeFor[config.SecretURL]()}
	if !leakcheck.Reaches(reflect.TypeFor[struct{ S *config.Secret }](), targets...) {
		t.Fatal("config secret targets are not reachable; the check would be vacuous")
	}
	registries := map[string]map[string]reflect.Type{
		".": {
			"Backend":      reflect.TypeFor[store.Backend](),
			"Capabilities": reflect.TypeFor[store.Capabilities](),
			"Algorithm":    reflect.TypeFor[store.Algorithm](),
			"Limit":        reflect.TypeFor[store.Limit](),
			"Decision":     reflect.TypeFor[store.Decision](),
			"RateLimiter":  reflect.TypeFor[store.RateLimiter](),
		},
		"storetest": {
			"Harness": reflect.TypeFor[storetest.Harness[store.RateLimiter]](),
		},
	}
	for _, dir := range []string{".", "storetest"} {
		registry := registries[dir]
		names, err := leakcheck.ExportedTypes(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range names {
			if _, ok := registry[n]; !ok {
				t.Errorf("exported type %s in %s is missing from the leak registry; add it, and a sample if it can hold a secret", n, dir)
			}
		}
		for _, k := range slices.Sorted(maps.Keys(registry)) {
			if !slices.Contains(names, k) {
				t.Errorf("leak registry lists %s, which is not an exported type of %s", k, dir)
			}
			if leakcheck.Reaches(registry[k], targets...) {
				t.Errorf("%s can hold a config secret; give it value-free formatting and a sample in the root leak matrix", k)
			}
		}
	}
}
