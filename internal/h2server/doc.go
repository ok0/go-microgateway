// Package h2server is the HTTP/2 server half of the Go standard library, copied so that every HTTP/2 timer runs on an injected clock.
//
// Base: go1.27.2.
//
// Sources (paths relative to the Go source tree, GOROOT/src, unless noted):
//
//   - net/http/internal/http2: api.go, ascii.go, ciphers.go, config.go, databuffer.go, errors.go, flow.go, frame.go, gotrack.go, http2.go, pipe.go, server.go, write.go, writesched.go, writesched_priority_rfc9218.go, writesched_roundrobin.go
//   - net/http/http2.go: server half only, in nethttp.go
//   - vendor/golang.org/x/net/http2/hpack: encode.go, hpack.go, huffman.go, static_table.go, tables.go (in hpack/)
//   - vendor/golang.org/x/net/http/httpguts: guts.go, httplex.go (in httpguts/)
//   - net/http/internal/httpcommon/httpcommon.go (in httpcommon/)
//   - net/http/internal/httpsfv/httpsfv.go (in httpsfv/)
//   - LICENSE from the Go distribution root, PATENTS from src/PATENTS
//
// Local changes:
//
//   - L1 import paths and package clauses: every copied file declares package h2server or its subpackage name; the hpack, httpguts, httpcommon and httpsfv imports point at the subpackages of this directory; nethttp.go declares package h2server and qualifies net/http identifiers with http.
//   - L2 standard library shim types to net/http types: api.go drops the NoBody and LocalAddrContextKey variables; nethttp.go translates requests with (&http.Request{...}).WithContext.
//   - L3 internal sentinels to exported equivalents: api.go sets ErrAbortHandler, ErrBodyNotAllowed and ErrSkipAltProtocol to the net/http values; server.go calls http.DetectContentType; the net/http server settings read by the glue are supplied by OptionsFor and Attach in nethttp.go.
//   - L4 tunables and test hooks to per-Server fields: server.go adds the fields verboseLogs, logFrameReads, logFrameWrites, disableExtendedConnectProtocol, inTests, debugGoroutines, disableDebugGoroutines, testHookOnConn, testHookOnPanicMu, testHookOnPanic, goAwayTimeout, clk, idleClk, opts and keepAlivesOff to Server; frame.go adds Framer.verboseLogs, which serveConn sets from the Server; write.go passes the verbose flag to encKV and encodeHeaders; gotrack.go makes goroutineLock carry its Server; http2.go replaces the init function with godebugFlags.
//   - L5 clock injection: the timers listed under Clock run on the Server clock.
//   - L6 scope: declarations that only the transport or client needs are omitted, listed below.
//
// Clock:
//
//   - server.go serveConn: idleTimer on the housekeeping clock (idleClk), readIdleTimer and the first-SETTINGS timer on clk; lastFrameTime uses clk.Now.
//   - server.go handlePingTimer: now uses clk.Now.
//   - server.go readPreface: the preface timeout is an AfterFunc on clk that closes a channel.
//   - server.go shutDownIn: shutdownTimer on clk.
//   - server.go closeAllStreamsOnConnClose: stops idleTimer after closing the remaining streams, because closeStream re-arms it while the connection is torn down.
//   - server.go processHeaders and newStream: stream readDeadline and writeDeadline on clk.
//   - server.go responseWriter SetReadDeadline and SetWriteDeadline: the connection clock supplies Now and AfterFunc.
//   - http2.go writeWithByteTimeout: the per-write byte timer on clk, which moves the write deadline to a past time when it fires.
//   - The response Date header stays on time.Now (see Wall time below).
//
// Omitted declarations:
//
//   - api.go: ClientRequest and its Clone method, ClientResponse, TransportConfig, NoBody and LocalAddrContextKey.
//   - config.go: configFromTransport; setConfigDefaults takes no server flag and no transport defaults.
//   - gotrack.go: DebugGoroutines and disableDebugGoroutines as package variables (now Server fields).
//   - http2.go: the init function and the tunable variable block (now Server fields and godebugFlags).
//   - server.go: the test hook variables and goAwayTimeout as package variables (now Server fields).
//   - httpcommon/httpcommon.go: Request, EncodeHeadersParam, EncodeHeadersResult, EncodeHeaders, and the helpers only they use (asciiEqualFold, checkConnHeaders, commaSeparatedTrailers, lower, shouldSendReqContentLength, validateHeaders, validPseudoPath).
//   - httpguts/httplex.go: PunycodeHostPort, because it needs golang.org/x/net/idna, which is not copied.
//   - nethttp.go: the transport half of net/http/http2.go, its init function, http2ServerRequestFromRequest, setHTTP2Config and http2ExternalServerConfig.
//
// Resync: copy the files listed under Sources from the new toolchain, reapply the local changes, update the hashes in upstream_test.go, run this package's tests and internal/server's comparison tests.
//
// Wall time: the response Date header uses time.Now, as net/http does.
package h2server
