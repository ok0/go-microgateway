//go:build realnet

package lychgate

import "testing"

func TestBreakerOpenCloseTCP(t *testing.T) {
	runBreakerOpenClose(t, tcpNetwork{})
}

func TestBreakerSelectionTCP(t *testing.T) {
	runBreakerSelection(t, tcpNetwork{})
}
