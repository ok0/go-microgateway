//go:build !integration

package redis

import "testing"

func TestIntegrationSuite(t *testing.T) {
	t.Skip("integration suite not built: run go test -tags integration with MICROGATEWAY_REDIS_ADDR set to a Redis 6.2+ host:port")
}
