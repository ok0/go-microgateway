//go:build realnet

// The tests in this file repeat the reload scenarios over loopback TCP. They open hundreds of loopback connections and are run explicitly with -tags realnet.
package microgateway

import "testing"

func TestFileEditUnderLoadTCP(t *testing.T) {
	runFileEditUnderLoad(t, tcpNetwork{})
}

func TestReloadStressConsistentTCP(t *testing.T) {
	runReloadStressConsistent(t, tcpNetwork{})
}

func TestInFlightSurvivesRemovalTCP(t *testing.T) {
	runInFlightSurvivesRemoval(t, tcpNetwork{})
}

func TestNoLeakAfterManyReloadsTCP(t *testing.T) {
	runNoLeakAfterManyReloads(t, tcpNetwork{})
}

func TestKeepAliveAcrossReloadTCP(t *testing.T) {
	runKeepAliveAcrossReload(t, tcpNetwork{})
}

func TestCarryOverPooledConnectionTCP(t *testing.T) {
	runCarryOverPooledConnection(t, tcpNetwork{})
}

func TestCarryOverInFlightTCP(t *testing.T) {
	runCarryOverInFlight(t, tcpNetwork{})
}

func TestTransportRecreatedOnTimeoutChangeTCP(t *testing.T) {
	runTransportRecreatedOnTimeoutChange(t, tcpNetwork{})
}

func TestRenameResetsStateTCP(t *testing.T) {
	runRenameResetsState(t, tcpNetwork{})
}

func TestPassiveEjectionTCP(t *testing.T) {
	runPassiveEjection(t, tcpNetwork{})
}

func TestActiveHealthTCP(t *testing.T) {
	runActiveHealth(t, tcpNetwork{})
}

func TestTransportLimitsTCP(t *testing.T) {
	runTransportLimits(t, tcpNetwork{})
}
