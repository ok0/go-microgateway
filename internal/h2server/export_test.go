// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package h2server

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/h2server/hpack"
)

func NewForTest(clk clock.Clock, o Options) *Server {
	s := New(clk, o)
	s.inTests = true
	s.debugGoroutines = true
	s.verboseLogs = s.verboseLogs || *verboseH2
	s.goAwayTimeout = GoAwayTimeout
	s.testHookOnPanicMu = new(sync.Mutex)
	return s
}

const (
	DefaultMaxReadFrameSize = defaultMaxReadFrameSize
	DefaultMaxStreams       = defaultMaxStreams
	InflowMinRefresh        = inflowMinRefresh
	InitialHeaderTableSize  = initialHeaderTableSize
	InitialWindowSize       = initialWindowSize
	MaxFrameSize            = maxFrameSize
	MaxQueuedControlFrames  = maxQueuedControlFrames
	MinMaxFrameSize         = minMaxFrameSize
)

type (
	ServerConn  = serverConn
	Stream      = stream
	StreamState = streamState

	PseudoHeaderError     = pseudoHeaderError
	HeaderFieldNameError  = headerFieldNameError
	HeaderFieldValueError = headerFieldValueError
)

const (
	StateIdle             = stateIdle
	StateOpen             = stateOpen
	StateHalfClosedLocal  = stateHalfClosedLocal
	StateHalfClosedRemote = stateHalfClosedRemote
	StateClosed           = stateClosed
)

func (s *Server) TestServeConn(c net.Conn, opts *ServeConnOpts, newf func(*serverConn)) {
	s.serveConn(c, opts, newf)
}

func (sc *serverConn) TestFlowControlConsumed() (consumed int32) {
	conf := configFromServer(sc.hs)
	donec := make(chan struct{})
	sc.sendServeMsg(func(sc *serverConn) {
		defer close(donec)
		initial := int32(conf.MaxReceiveBufferPerConnection)
		avail := sc.inflow.avail + sc.inflow.unsent
		consumed = initial - avail
	})
	<-donec
	return consumed
}

func (sc *serverConn) TestStreamExists(id uint32) bool {
	ch := make(chan bool, 1)
	sc.serveMsgCh <- func(int) {
		ch <- (sc.streams[id] != nil)
	}
	return <-ch
}

func (sc *serverConn) TestStreamState(id uint32) streamState {
	ch := make(chan streamState, 1)
	sc.serveMsgCh <- func(int) {
		state, _ := sc.state(id)
		ch <- state
	}
	return <-ch
}

func (sc *serverConn) StartGracefulShutdown() { sc.startGracefulShutdown() }

func (sc *serverConn) TestHPACKEncoder() *hpack.Encoder {
	return sc.hpackEncoder
}

func (sc *serverConn) TestFramerMaxHeaderStringLen() int {
	return sc.framer.maxHeaderStringLen()
}

func (fr *Framer) TestSetDebugReadLoggerf(f func(string, ...any)) {
	fr.logReads = true
	fr.debugReadLoggerf = f
}

func (fr *Framer) TestSetDebugWriteLoggerf(f func(string, ...any)) {
	fr.logWrites = true
	fr.debugWriteLoggerf = f
}

func SummarizeFrame(f Frame) string {
	return summarizeFrame(f)
}

func (s *Server) SetTestHookOnPanic(t testing.TB, f func(sc *serverConn, panicVal any) (rePanic bool)) {
	s.testHookOnPanicMu.Lock()
	defer s.testHookOnPanicMu.Unlock()
	old := s.testHookOnPanic
	s.testHookOnPanic = f
	t.Cleanup(func() {
		s.testHookOnPanicMu.Lock()
		defer s.testHookOnPanicMu.Unlock()
		s.testHookOnPanic = old
	})
}

func (s *Server) SetDisableExtendedConnectProtocol(t testing.TB, v bool) {
	SetForTest(t, &s.disableExtendedConnectProtocol, v)
}

func (s *Server) LogFrameReads() bool  { return s.logFrameReads }
func (s *Server) LogFrameWrites() bool { return s.logFrameWrites }

const GoAwayTimeout = 25 * time.Millisecond

func EncodeHeaderRaw(t testing.TB, headers ...string) []byte {
	return encodeHeaderRaw(t, headers...)
}

func NewPriorityWriteSchedulerRFC9218() WriteScheduler {
	return newPriorityWriteSchedulerRFC9218()
}

func NewRoundRobinWriteScheduler() WriteScheduler {
	return newRoundRobinWriteScheduler()
}

func (s *Server) DisableGoroutineTracking(t testing.TB) {
	s.disableDebugGoroutines.Store(true)
	t.Cleanup(func() {
		s.disableDebugGoroutines.Store(false)
	})
}

func InvalidHTTP1LookingFrameHeader() FrameHeader {
	return invalidHTTP1LookingFrameHeader()
}
