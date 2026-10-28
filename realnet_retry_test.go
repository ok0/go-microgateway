//go:build realnet

package microgateway

import "testing"

func TestRetryBasicsTCP(t *testing.T) {
	runRetryBasics(t, tcpNetwork{})
}

func TestRetryIdempotencyTCP(t *testing.T) {
	runRetryIdempotency(t, tcpNetwork{})
}

func TestRetryBodyReplayTCP(t *testing.T) {
	runRetryBodyReplay(t, tcpNetwork{})
}

func TestRetryStreamingTCP(t *testing.T) {
	runRetryStreaming(t, tcpNetwork{})
}
