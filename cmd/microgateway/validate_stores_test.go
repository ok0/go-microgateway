package main

import (
	"strings"
	"testing"
)

func TestValidateStoresSeeded(t *testing.T) {
	code, _, errs := runCmd(t, "validate", "-config", "../../config/testdata/stores_invalid.yaml")
	if code != 1 {
		t.Fatalf("exit code = %d, want 1; stderr:\n%s", code, errs)
	}
	paths := []string{
		"stores[1].name",
		"stores[2].name",
		"stores[3].type",
		"stores[4].redis",
		"stores[5].redis",
		"stores[6].params",
		"stores[7].redis.mode",
		"stores[8].redis.addresses",
		"stores[9].redis.addresses[1]",
		"stores[10].redis.addresses[0]",
		"stores[11].redis.addresses[0]",
		"stores[12].redis.addresses[0]",
		"stores[13].redis.master_name",
		"stores[14].redis.master_name",
		"stores[15].redis.db",
		"stores[16].redis.tls",
		"stores[17].redis.tls.ca_file",
		"stores[18].redis.dial_timeout",
		"stores[19].redis.pool_size",
		"stores[20].redis.min_idle_conns",
		"stores[21].redis.key_prefix",
		"stores[22].guard.consecutive_failures",
		"stores[23].guard.open_timeout",
		"stores[24].guard.max_inflight",
		"stores[25].guard.max_inflight",
		"stores[26].redis.db",
		"stores[27].redis.pool_size",
		"stores[28].redis.db",
		"routes[0].rate_limit.store_timeout",
		"routes[0].rate_limit.on_store_error",
		"routes[1].rate_limit.on_store_error",
		"routes[1].rate_limit.store_timeout",
		"routes[2].rate_limit.store",
	}
	for _, p := range paths {
		if !strings.Contains(errs, p) {
			t.Errorf("stderr lacks path %q; stderr:\n%s", p, errs)
		}
	}
	if strings.Contains(errs, "p4ss-UNIQUE-1") {
		t.Errorf("stderr leaks a secret value; stderr:\n%s", errs)
	}
}
