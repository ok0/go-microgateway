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
	"net"
	"net/http"
	"time"
)

// net/http supports HTTP/2 by default, but this support is removed when
// the nethttpomithttp2 build tag is set.
//
// HTTP/2 support is provided by the net/http/internal/http2 package.
//
// This file (http2.go) connects net/http to the http2 package.
// Since http imports http2, to avoid an import cycle we need to
// translate http package types (e.g., Request) into the equivalent
// http2 package types (e.g., http2.ClientRequest).
//
// The golang.org/x/net/http2 package is the original source of truth for
// the HTTP/2 implementation. At this time, users may still import that
// package and register its implementation on a net/http Transport or Server.
// However, the x/net package is no longer synchronized with std.

// Options is what net/http's bundled HTTP/2 server reads from an *http.Server (net/http/http2.go http2ServerConfig).
type Options struct {
	MaxHeaderBytes        int
	MaxHeaderValueCount   int
	ReadTimeout           time.Duration // stream read limit
	WriteTimeout          time.Duration // stream write limit
	IdleTimeout           time.Duration // connection idle limit
	SendPingTimeout       time.Duration // idle read interval before a PING
	DisableClientPriority bool
	HTTP2                 *http.HTTP2Config
	ConnState             func(net.Conn, http.ConnState)
	ErrorLog              *log.Logger
}

// OptionsFor returns the Options that net/http derives from hs: the idle rule of
// net/http's configureHTTP2 and the accessors of net/http's http2ServerConfig.
func OptionsFor(hs *http.Server) Options {
	o := Options{
		MaxHeaderBytes:        hs.MaxHeaderBytes,
		MaxHeaderValueCount:   hs.MaxHeaderValueCount,
		ReadTimeout:           hs.ReadTimeout,
		WriteTimeout:          hs.WriteTimeout,
		IdleTimeout:           hs.IdleTimeout,
		SendPingTimeout:       hs.ReadTimeout,
		DisableClientPriority: hs.DisableClientPriority,
		HTTP2:                 hs.HTTP2,
		ConnState:             hs.ConnState,
		ErrorLog:              hs.ErrorLog,
	}
	if o.MaxHeaderValueCount <= 0 {
		o.MaxHeaderValueCount = http.DefaultMaxHeaderValueCount
	}
	if o.IdleTimeout == 0 {
		o.IdleTimeout = hs.ReadTimeout
	}
	return o
}

// Attach registers the HTTP/2 server on hs. net/http then never configures its
// bundled server, because onceSetNextProtoDefaults returns when TLSNextProto["h2"] exists.
func (s *Server) Attach(hs *http.Server) error {
	_, h2 := hs.TLSNextProto["h2"]
	_, unencrypted := hs.TLSNextProto["unencrypted_http2"]
	if h2 || unencrypted {
		return errors.New("h2server: HTTP/2 already registered on this server")
	}
	tcfg := hs.TLSConfig
	if tcfg == nil {
		tcfg = &tls.Config{}
	}
	if err := s.Configure(optionsConfig{s}, tcfg); err != nil {
		return err
	}
	hs.RegisterOnShutdown(func() { s.keepAlivesOff.Store(true) })
	hs.RegisterOnShutdown(s.GracefulShutdown)
	if hs.TLSNextProto == nil {
		hs.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler))
	}
	hs.TLSNextProto["h2"] = s.ServeTLSNextProto
	hs.TLSNextProto["unencrypted_http2"] = s.ServeTLSNextProto
	return nil
}

// ServeTLSNextProto serves an HTTP/2 connection that net/http negotiated on hs.
func (s *Server) ServeTLSNextProto(hs *http.Server, c *tls.Conn, h http.Handler) {
	if f := s.testHookOnConn; f != nil {
		f()
	}
	var nc net.Conn = c
	saw := false
	if u, ok := c.NetConn().(interface{ UnencryptedNetConn() net.Conn }); ok {
		nc, saw = u.UnencryptedNetConn(), true
	}
	ctx := context.Background()
	if bc, ok := h.(interface{ BaseContext() context.Context }); ok {
		ctx = bc.BaseContext()
	}
	nc.SetReadDeadline(time.Time{})
	nc.SetWriteDeadline(time.Time{})
	s.ServeConn(nc, &ServeConnOpts{
		Context:          ctx,
		Handler:          http2Handler{h},
		BaseConfig:       optionsConfig{s},
		SawClientPreface: saw,
	})
}

type http2Handler struct {
	h http.Handler
}

func (h http2Handler) ServeHTTP(w *ResponseWriter, req *ServerRequest) {
	h.h.ServeHTTP(http2ResponseWriter{w}, (&http.Request{
		Proto:         "HTTP/2.0",
		ProtoMajor:    2,
		ProtoMinor:    0,
		Method:        req.Method,
		URL:           req.URL,
		Header:        http.Header(req.Header),
		RequestURI:    req.RequestURI,
		Trailer:       http.Header(req.Trailer),
		Body:          req.Body,
		Host:          req.Host,
		ContentLength: req.ContentLength,
		RemoteAddr:    req.RemoteAddr,
		TLS:           req.TLS,
		MultipartForm: req.MultipartForm,
	}).WithContext(req.Context))
}

type http2ResponseWriter struct {
	*ResponseWriter
}

// Optional http.ResponseWriter interfaces implemented.
var (
	_ http.CloseNotifier = http2ResponseWriter{}
	_ http.Flusher       = http2ResponseWriter{}
	_ io.StringWriter    = http2ResponseWriter{}
)

func (w http2ResponseWriter) Flush()            { w.ResponseWriter.FlushError() }
func (w http2ResponseWriter) FlushError() error { return w.ResponseWriter.FlushError() }

func (w http2ResponseWriter) Header() http.Header { return http.Header(w.ResponseWriter.Header()) }

func (w http2ResponseWriter) Push(target string, opts *http.PushOptions) error {
	var (
		method string
		header Header
	)
	if opts != nil {
		method = opts.Method
		header = Header(opts.Header)
	}
	err := w.ResponseWriter.Push(target, method, header)
	if err == ErrNotSupported {
		err = http.ErrNotSupported
	}
	return err
}

// optionsConfig adapts Options to the ServerConfig that the server reads.
type optionsConfig struct {
	s *Server
}

func (c optionsConfig) MaxHeaderBytes() int      { return c.s.opts.MaxHeaderBytes }
func (c optionsConfig) MaxHeaderValueCount() int { return c.s.opts.MaxHeaderValueCount }
func (c optionsConfig) ConnState(nc net.Conn, st ConnState) {
	if c.s.opts.ConnState != nil {
		c.s.opts.ConnState(nc, http.ConnState(st))
	}
}
func (c optionsConfig) DoKeepAlives() bool             { return !c.s.keepAlivesOff.Load() }
func (c optionsConfig) WriteTimeout() time.Duration    { return c.s.opts.WriteTimeout }
func (c optionsConfig) SendPingTimeout() time.Duration { return c.s.opts.SendPingTimeout }
func (c optionsConfig) ErrorLog() *log.Logger          { return c.s.opts.ErrorLog }
func (c optionsConfig) ReadTimeout() time.Duration     { return c.s.opts.ReadTimeout }
func (c optionsConfig) DisableClientPriority() bool    { return c.s.opts.DisableClientPriority }
func (c optionsConfig) IdleTimeout() time.Duration     { return c.s.opts.IdleTimeout }

func (c optionsConfig) HTTP2Config() Config {
	return mergeHTTP2Config(c.s.opts.HTTP2)
}

func mergeHTTP2Config(c *http.HTTP2Config) Config {
	if c == nil {
		return Config{}
	}
	return Config(*c)
}
