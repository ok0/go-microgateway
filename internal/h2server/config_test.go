// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package h2server_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/ok0/go-microgateway/internal/h2server"
)

func TestConfigServerSettings(t *testing.T) { synctest.Test(t, testConfigServerSettings) }
func testConfigServerSettings(t *testing.T) {
	config := &http.HTTP2Config{
		MaxConcurrentStreams:          1,
		MaxDecoderHeaderTableSize:     1<<20 + 2,
		MaxEncoderHeaderTableSize:     1<<20 + 3,
		MaxReadFrameSize:              1<<20 + 4,
		MaxReceiveBufferPerConnection: 64<<10 + 5,
		MaxReceiveBufferPerStream:     64<<10 + 6,
	}
	const maxHeaderBytes = 4096 + 7
	st := newServerTester(t, nil, func(s *http.Server) {
		s.MaxHeaderBytes = maxHeaderBytes
		s.HTTP2 = config
	})
	st.writePreface()
	st.writeSettings()
	st.wantSettings(map[SettingID]uint32{
		SettingMaxConcurrentStreams: uint32(config.MaxConcurrentStreams),
		SettingHeaderTableSize:      uint32(config.MaxDecoderHeaderTableSize),
		SettingInitialWindowSize:    uint32(config.MaxReceiveBufferPerStream),
		SettingMaxFrameSize:         uint32(config.MaxReadFrameSize),
		SettingMaxHeaderListSize:    maxHeaderBytes + (32 * 10),
	})
}

func TestConfigPingTimeoutServer(t *testing.T) { synctest.Test(t, testConfigPingTimeoutServer) }
func testConfigPingTimeoutServer(t *testing.T) {
	st := newServerTester(t, func(w http.ResponseWriter, r *http.Request) {
	}, func(h2 *http.HTTP2Config) {
		h2.SendPingTimeout = 2 * time.Second
		h2.PingTimeout = 3 * time.Second
	})
	st.greet()

	time.Sleep(2 * time.Second)
	_ = readFrame[*PingFrame](t, st)
	time.Sleep(3 * time.Second)
	st.wantClosed()
}
