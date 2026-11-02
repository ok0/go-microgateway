// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package h2server

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"time"
)

// Since net/http imports the http2 package, http2 cannot use any net/http types.
// This file contains definitions which exist to to avoid introducing a dependency cycle.

var (
	ErrAbortHandler    = http.ErrAbortHandler
	ErrBodyNotAllowed  = http.ErrBodyNotAllowed
	ErrNotSupported    = errors.ErrUnsupported
	ErrSkipAltProtocol = http.ErrSkipAltProtocol
)

type Header = textproto.MIMEHeader

// ServerConfig is configuration from an http.Server.
type ServerConfig interface {
	MaxHeaderBytes() int
	MaxHeaderValueCount() int
	ConnState(net.Conn, ConnState)
	DoKeepAlives() bool
	WriteTimeout() time.Duration
	SendPingTimeout() time.Duration
	ErrorLog() *log.Logger
	ReadTimeout() time.Duration
	HTTP2Config() Config
	DisableClientPriority() bool
	IdleTimeout() time.Duration
}

type Handler interface {
	ServeHTTP(*ResponseWriter, *ServerRequest)
}

type ResponseWriter = responseWriter

type PushOptions struct {
	Method string
	Header Header
}

// A ServerRequest is a Request used by the HTTP/2 server.
type ServerRequest struct {
	Context       context.Context
	Proto         string // e.g. "HTTP/1.0"
	ProtoMajor    int    // e.g. 1
	ProtoMinor    int    // e.g. 0
	Method        string
	URL           *url.URL
	Header        Header
	Trailer       Header
	Body          io.ReadCloser
	Host          string
	ContentLength int64
	RemoteAddr    string
	RequestURI    string
	TLS           *tls.ConnectionState
	MultipartForm *multipart.Form
}

// ConnState is identical to net/http.ConnState.
type ConnState int

const (
	ConnStateNew ConnState = iota
	ConnStateActive
	ConnStateIdle
	ConnStateHijacked
	ConnStateClosed
)
