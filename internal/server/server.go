// Package server binds the configured listeners and serves a handler on them.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/ok0/lychgate/config"
)

// Server serves one handler on every configured listener.
type Server struct {
	cfg     *config.Config
	h       http.Handler
	logger  *slog.Logger
	servers []*http.Server
	names   []string
	lns     []net.Listener
	base    context.Context
	cancel  context.CancelFunc
	errc    chan error
	mu      sync.Mutex
	addrs   map[string]net.Addr
}

// New prepares a server for every listener in cfg. Listeners with TLS configured are rejected.
func New(cfg *config.Config, h http.Handler, logger *slog.Logger) (*Server, error) {
	for _, l := range cfg.Listeners {
		if l.TLS != nil {
			return nil, fmt.Errorf("listener %q: tls is not yet supported", l.Name)
		}
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	s := &Server{
		cfg:    cfg,
		h:      h,
		logger: logger,
		errc:   make(chan error, len(cfg.Listeners)),
		addrs:  make(map[string]net.Addr),
	}
	s.base, s.cancel = context.WithCancel(context.Background())
	for _, l := range cfg.Listeners {
		p := new(http.Protocols)
		p.SetHTTP1(true)
		srv := &http.Server{
			Handler:           h,
			ReadHeaderTimeout: l.ReadHeaderTimeout.Std(),
			ReadTimeout:       l.ReadTimeout.Std(),
			WriteTimeout:      l.WriteTimeout.Std(),
			IdleTimeout:       l.IdleTimeout.Std(),
			MaxHeaderBytes:    l.MaxHeaderBytes,
			BaseContext:       func(net.Listener) context.Context { return s.base },
			ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
			Protocols:         p,
		}
		s.servers = append(s.servers, srv)
		s.names = append(s.names, l.Name)
	}
	return s, nil
}

// Start binds every listener and begins serving. If any listener cannot be bound, the ones already
// bound are closed and an error is returned.
func (s *Server) Start() error {
	lns := make([]net.Listener, 0, len(s.servers))
	for i := range s.servers {
		name := s.names[i]
		ln, err := net.Listen("tcp", s.cfg.Listeners[i].Address)
		if err != nil {
			for _, open := range lns {
				open.Close()
			}
			return fmt.Errorf("listener %q: %w", name, err)
		}
		lns = append(lns, ln)
	}
	s.mu.Lock()
	s.lns = lns
	for i, ln := range lns {
		s.addrs[s.names[i]] = ln.Addr()
	}
	s.mu.Unlock()
	for i, srv := range s.servers {
		name := s.names[i]
		ln := lns[i]
		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.errc <- fmt.Errorf("listener %q: %w", name, err)
			}
		}()
	}
	return nil
}

// Addrs returns the bound address of each listener keyed by listener name.
func (s *Server) Addrs() map[string]net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]net.Addr, len(s.addrs))
	for k, v := range s.addrs {
		out[k] = v
	}
	return out
}

// Errors returns a channel that receives an error when a listener stops serving unexpectedly.
func (s *Server) Errors() <-chan error {
	return s.errc
}

// Shutdown gracefully stops every listener and waits for in-flight requests until ctx is done.
// Connections that remain open after that, including hijacked ones, are closed.
func (s *Server) Shutdown(ctx context.Context) error {
	var errs []error
	failed := make([]bool, len(s.servers))
	for i, srv := range s.servers {
		if err := srv.Shutdown(ctx); err != nil {
			errs = append(errs, err)
			failed[i] = true
		}
	}
	s.cancel()
	for i, srv := range s.servers {
		if failed[i] {
			srv.Close()
		}
	}
	return errors.Join(errs...)
}
