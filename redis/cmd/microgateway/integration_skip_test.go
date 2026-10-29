//go:build !integration

package main

import "testing"

func TestIntegrationTwoInstances(t *testing.T) {
	t.Skip("integration test not built: run go test -tags integration with MICROGATEWAY_REDIS_ADDR set to a Redis 6.2+ host:port")
}
