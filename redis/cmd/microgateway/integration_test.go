//go:build integration

package main

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

func TestIntegrationTwoInstances(t *testing.T) {
	addr := os.Getenv("MICROGATEWAY_REDIS_ADDR")
	if addr == "" {
		t.Fatal("MICROGATEWAY_REDIS_ADDR is not set: the integration build tag runs this test against a Redis 6.2+ server at that host:port, e.g. docker run -d --rm -p 6379:6379 redis:7.4-alpine")
	}
	c := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { c.Close() })
	if !serverAtLeast(t, c, 6, 2) {
		t.Fatal("Redis 6.2 or later is required")
	}
	runTwoInstances(t, redisTarget{addr: addr, db: 14})
}

func serverAtLeast(t *testing.T, c goredis.UniversalClient, major, minor int) bool {
	t.Helper()
	info, err := c.Info(context.Background(), "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		version, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:")
		if !ok {
			continue
		}
		parts := strings.Split(version, ".")
		if len(parts) < 2 {
			t.Fatalf("redis_version %q: cannot parse", version)
		}
		gotMajor, err := strconv.Atoi(parts[0])
		if err != nil {
			t.Fatalf("redis_version %q: cannot parse: %v", version, err)
		}
		gotMinor, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Fatalf("redis_version %q: cannot parse: %v", version, err)
		}
		return gotMajor > major || (gotMajor == major && gotMinor >= minor)
	}
	t.Fatal("INFO server has no redis_version line")
	return false
}
