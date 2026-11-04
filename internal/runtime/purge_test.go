package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/state"
	"github.com/ok0/go-microgateway/store"
)

func purgeRouteDoc(cacheFields string) string {
	return cacheHead + "routes:\n  - name: api\n    match:\n      path: /api\n    upstream: users\n    cache:\n      enabled: true\n" + cacheFields
}

func TestRetentionHighWater(t *testing.T) {
	retention := new(cache.Retention)
	d := Deps{Retention: retention}

	_, _, err := build(t, mustParse(t, purgeRouteDoc("      max_ttl: 48h\n      stale_if_error: 1h\n      keep: 1h\n")), d)
	if err != nil {
		t.Fatal(err)
	}
	if got := retention.At(0); got != 50*time.Hour {
		t.Fatalf("retention after config A = %v, want 50h", got)
	}

	_, _, err = build(t, mustParse(t, purgeRouteDoc("      max_ttl: 1h\n")), d)
	if err != nil {
		t.Fatal(err)
	}
	if got := retention.At(0); got != 50*time.Hour {
		t.Errorf("retention after config B = %v, want 50h kept", got)
	}

	reg := state.NewRegistry(nil)
	t.Cleanup(reg.Close)
	tx := reg.Begin()
	if _, err := Build(mustParse(t, purgeRouteDoc("      max_ttl: 100h\n")), Deps{Retention: retention, Clock: clock.NewFake(buildStart)}, tx); err != nil {
		tx.Abort()
		t.Fatal(err)
	}
	tx.Abort()
	if got := retention.At(0); got != 50*time.Hour {
		t.Errorf("retention after an aborted build = %v, want 50h", got)
	}

	_, _, err = build(t, mustParse(t, purgeRouteDoc("      max_ttl: 8760h\n      stale_while_revalidate: 8760h\n      stale_if_error: 8760h\n      keep: 8760h\n")), d)
	if err != nil {
		t.Fatal(err)
	}
	if got := retention.At(0); got != store.MaxTTL {
		t.Errorf("retention after 365-day components = %v, want store.MaxTTL %v", got, store.MaxTTL)
	}
}

func TestSnapshotPurger(t *testing.T) {
	plain := mustParse(t, cacheHead+"routes:\n"+plainRouteBlock)
	s, _, err := build(t, plain, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Purger() != nil {
		t.Error("Purger() without a cache route != nil")
	}

	cached := mustParse(t, cacheHead+cacheRouteBlock)
	s, _, err = build(t, cached, Deps{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Purger() == nil {
		t.Fatal("Purger() with a memory cache route = nil")
	}
	if got := s.Purger().Scope(); got != "instance" {
		t.Errorf("Scope() = %q, want instance", got)
	}
}

func TestSnapshotAdminKeys(t *testing.T) {
	tokens := []string{"tok-ops-4Xq", "tok-ci-9Rz"}
	ids := []string{"ops", "ci"}
	var keys strings.Builder
	var want [][32]byte
	for i, tok := range tokens {
		sum := sha256.Sum256([]byte(tok))
		want = append(want, sum)
		keys.WriteString("      - id: " + ids[i] + "\n        hash: \"sha256:" + hex.EncodeToString(sum[:]) + "\"\n")
	}
	doc := cacheHead + "admin:\n  auth:\n    keys:\n" + keys.String() + "routes:\n" + plainRouteBlock
	s, _, err := build(t, mustParse(t, doc), Deps{})
	if err != nil {
		t.Fatal(err)
	}
	gotIDs, gotHashes := s.AdminKeys()
	if len(gotIDs) != 2 || gotIDs[0] != "ops" || gotIDs[1] != "ci" {
		t.Fatalf("AdminKeys ids = %v, want [ops ci]", gotIDs)
	}
	if len(gotHashes) != 2 || gotHashes[0] != want[0] || gotHashes[1] != want[1] {
		t.Errorf("AdminKeys hashes do not match the sha256 digests of the tokens")
	}

	none, _, err := build(t, mustParse(t, cacheHead+"routes:\n"+plainRouteBlock), Deps{})
	if err != nil {
		t.Fatal(err)
	}
	noIDs, noHashes := none.AdminKeys()
	if len(noIDs) != 0 || len(noHashes) != 0 {
		t.Errorf("AdminKeys without keys = %v, %d hashes, want empty", noIDs, len(noHashes))
	}
}

func TestPurgeSeriesPrecreated(t *testing.T) {
	text := func(doc string) string {
		t.Helper()
		reg := metrics.NewRegistry()
		if _, _, err := build(t, mustParse(t, doc), Deps{Metrics: reg}); err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		if err := reg.WriteText(&b); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}

	cached := text(cacheHead + cacheRouteBlock)
	for _, typ := range []string{"url", "tag", "all"} {
		for _, mode := range []string{"hard", "soft"} {
			line := `microgateway_cache_purges_total{type="` + typ + `",mode="` + mode + `"} 0`
			if !strings.Contains(cached, line) {
				t.Errorf("cache route text lacks %q", line)
			}
		}
	}

	plain := text(cacheHead + "routes:\n" + plainRouteBlock)
	if strings.Contains(plain, "microgateway_cache_purges_total") {
		t.Error("purge family present without cache routes")
	}
}

func TestCacheWiringUnsetDefaults(t *testing.T) {
	tests := []struct {
		name  string
		unset func(*config.Cache)
		want  string
	}{
		{"coalescing.enabled", func(c *config.Cache) { c.Coalescing.Enabled = nil }, "cache: coalescing.enabled is not set"},
		{"coalescing.max_keys", func(c *config.Cache) { c.Coalescing.MaxKeys = nil }, "cache: coalescing.max_keys is not set"},
		{"coalescing.max_waiters", func(c *config.Cache) { c.Coalescing.MaxWaiters = nil }, "cache: coalescing.max_waiters is not set"},
		{"max_background", func(c *config.Cache) { c.MaxBackground = nil }, "cache: max_background is not set"},
		{"store_timeout", func(c *config.Cache) { c.StoreTimeout = nil }, "cache: store_timeout is not set"},
		{"memory.max_bytes", func(c *config.Cache) { c.Memory.MaxBytes = nil }, "cache: memory.max_bytes is not set"},
		{"memory.shards", func(c *config.Cache) { c.Memory.Shards = nil }, "cache: memory.shards is not set"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := mustParse(t, purgeRouteDoc(""))
			tc.unset(&cfg.Cache)
			_, _, err := build(t, cfg, Deps{})
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}

	t.Run("nothing unset", func(t *testing.T) {
		_, _, err := build(t, mustParse(t, purgeRouteDoc("")), Deps{})
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
	})

	t.Run("coalescing disabled", func(t *testing.T) {
		cfg := mustParse(t, purgeRouteDoc(""))
		*cfg.Cache.Coalescing.Enabled = false
		cfg.Cache.Coalescing.MaxKeys = nil
		cfg.Cache.Coalescing.MaxWaiters = nil
		_, _, err := build(t, cfg, Deps{})
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
	})
}
