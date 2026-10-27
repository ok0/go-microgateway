// Package server binds the gateway and admin listeners, terminates TLS, and drains connections on shutdown.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ok0/lychgate/config"
)

// ErrForcedClose reports that connections were closed because they were still open after the drain timeout.
var ErrForcedClose = errors.New("server: connections were force-closed after the drain timeout")

type listener struct {
	name    string
	address string
	srv     *http.Server
	tls     bool
	ln      net.Listener
}

type certificate struct {
	cur atomic.Pointer[tls.Certificate]
}

func (c *certificate) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return c.cur.Load(), nil
}

type inflight struct {
	mu   sync.Mutex
	n    int
	zero chan struct{}
}

func (f *inflight) begin() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.n == 0 {
		f.zero = make(chan struct{})
	}
	f.n++
}

func (f *inflight) end() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.n--
	if f.n == 0 {
		close(f.zero)
	}
}

func (f *inflight) wait(ctx context.Context) bool {
	f.mu.Lock()
	if f.n == 0 {
		f.mu.Unlock()
		return true
	}
	zero := f.zero
	f.mu.Unlock()
	select {
	case <-zero:
		return true
	case <-ctx.Done():
		return false
	}
}

// Server serves a data handler on every configured listener and, when enabled, an admin handler on the admin listener.
type Server struct {
	cfg       *config.Config
	logger    *slog.Logger
	ready     func(bool)
	data      []*listener
	admin     *listener
	base      context.Context
	cancel    context.CancelFunc
	active    inflight
	errc      chan error
	mu        sync.Mutex
	addrs     map[string]net.Addr
	adminAddr net.Addr
}

// New prepares a server for every listener in cfg. The data handler h serves the listeners and the admin handler
// serves the admin listener when it is enabled in cfg. ready is called with false when shutdown begins. A nil
// logger discards log output and a nil ready callback does nothing.
func New(cfg *config.Config, h http.Handler, admin http.Handler, ready func(bool), logger *slog.Logger) (*Server, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if ready == nil {
		ready = func(bool) {}
	}
	s := &Server{
		cfg:    cfg,
		logger: logger,
		ready:  ready,
		addrs:  make(map[string]net.Addr),
	}
	s.base, s.cancel = context.WithCancel(context.Background())
	errLog := slog.NewLogLogger(logger.Handler(), slog.LevelWarn)
	for _, l := range cfg.Listeners {
		p := new(http.Protocols)
		p.SetHTTP1(true)
		srv := &http.Server{
			Handler:           s.track(h),
			ReadHeaderTimeout: l.ReadHeaderTimeout.Std(),
			ReadTimeout:       l.ReadTimeout.Std(),
			WriteTimeout:      l.WriteTimeout.Std(),
			IdleTimeout:       l.IdleTimeout.Std(),
			MaxHeaderBytes:    l.MaxHeaderBytes,
			BaseContext:       func(net.Listener) context.Context { return s.base },
			ErrorLog:          errLog,
		}
		ll := &listener{name: l.Name, address: l.Address, srv: srv}
		if l.TLS != nil {
			kp, err := tls.LoadX509KeyPair(l.TLS.CertFile, l.TLS.KeyFile)
			if err != nil {
				return nil, fmt.Errorf("listener %q: load certificate: %w", l.Name, err)
			}
			c := &certificate{}
			c.cur.Store(&kp)
			minVersion := uint16(tls.VersionTLS12)
			if l.TLS.MinVersion == "1.3" {
				minVersion = tls.VersionTLS13
			}
			srv.TLSConfig = &tls.Config{MinVersion: minVersion, GetCertificate: c.get}
			p.SetHTTP2(l.HTTP2 == nil || *l.HTTP2)
			ll.tls = true
		} else {
			p.SetUnencryptedHTTP2(l.H2C)
		}
		srv.Protocols = p
		s.data = append(s.data, ll)
	}
	if admin != nil && (cfg.Admin.Enabled == nil || *cfg.Admin.Enabled) {
		p := new(http.Protocols)
		p.SetHTTP1(true)
		s.admin = &listener{
			name:    "admin",
			address: cfg.Admin.Address,
			srv: &http.Server{
				Handler:           admin,
				ReadHeaderTimeout: 10 * time.Second,
				IdleTimeout:       120 * time.Second,
				BaseContext:       func(net.Listener) context.Context { return context.Background() },
				ErrorLog:          errLog,
				Protocols:         p,
			},
		}
	}
	n := len(s.data)
	if s.admin != nil {
		n++
	}
	s.errc = make(chan error, n)
	return s, nil
}

func (s *Server) track(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.active.begin()
		defer s.active.end()
		h.ServeHTTP(w, r)
	})
}

func (s *Server) listeners() []*listener {
	all := make([]*listener, 0, len(s.data)+1)
	all = append(all, s.data...)
	if s.admin != nil {
		all = append(all, s.admin)
	}
	return all
}

// Start binds every listener and begins serving. If any listener cannot be bound, the ones already
// bound are closed and an error is returned.
func (s *Server) Start() error {
	all := s.listeners()
	lns := make([]net.Listener, 0, len(all))
	for _, l := range all {
		ln, err := net.Listen("tcp", l.address)
		if err != nil {
			for _, open := range lns {
				open.Close()
			}
			return fmt.Errorf("listener %q: %w", l.name, err)
		}
		lns = append(lns, ln)
	}
	s.mu.Lock()
	for i, l := range all {
		l.ln = lns[i]
	}
	for _, l := range s.data {
		s.addrs[l.name] = l.ln.Addr()
	}
	if s.admin != nil {
		s.adminAddr = s.admin.ln.Addr()
	}
	s.mu.Unlock()
	for _, l := range all {
		go func(l *listener) {
			var err error
			if l.tls {
				err = l.srv.ServeTLS(l.ln, "", "")
			} else {
				err = l.srv.Serve(l.ln)
			}
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				s.errc <- fmt.Errorf("listener %q: %w", l.name, err)
			}
		}(l)
	}
	return nil
}

// Addrs returns the bound address of each data listener keyed by listener name.
func (s *Server) Addrs() map[string]net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]net.Addr, len(s.addrs))
	for k, v := range s.addrs {
		out[k] = v
	}
	return out
}

// AdminAddr returns the bound address of the admin listener, or nil when there is no admin listener.
func (s *Server) AdminAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.adminAddr
}

// Errors returns a channel that receives an error when a listener stops serving unexpectedly.
func (s *Server) Errors() <-chan error {
	return s.errc
}

// Shutdown stops accepting new connections, waits for in-flight requests until the drain timeout elapses or ctx is
// done, and then closes any connections that remain open, including hijacked and upgraded ones. It returns
// ErrForcedClose when connections had to be closed forcibly.
func (s *Server) Shutdown(ctx context.Context) error {
	s.ready(false)
	if d := s.cfg.Global.ShutdownDelay.Std(); d > 0 {
		t := time.NewTimer(d)
		select {
		case <-t.C:
		case <-ctx.Done():
		}
		t.Stop()
	}
	drainCtx, cancel := context.WithTimeout(ctx, s.cfg.Global.DrainTimeout.Std())
	errs := make([]error, len(s.data))
	var wg sync.WaitGroup
	for i, l := range s.data {
		wg.Add(1)
		go func(i int, l *listener) {
			defer wg.Done()
			if err := l.srv.Shutdown(drainCtx); err != nil && !isContextErr(err) {
				errs[i] = err
			}
		}(i, l)
	}
	wg.Wait()
	drained := s.active.wait(drainCtx)
	forced := !drained || drainCtx.Err() != nil
	s.cancel()
	for _, l := range s.data {
		l.srv.Close()
	}
	if s.admin != nil {
		s.admin.srv.Close()
	}
	cancel()
	if forced {
		s.logger.Warn("drain timeout; connections force-closed")
		return errors.Join(append([]error{ErrForcedClose}, errs...)...)
	}
	s.logger.Info("drain complete")
	return errors.Join(errs...)
}

func isContextErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
