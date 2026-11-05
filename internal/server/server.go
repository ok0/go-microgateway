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
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/h2server"
)

// ErrForcedClose reports that connections were closed because they were still open after the drain timeout.
var ErrForcedClose = errors.New("server: connections were force-closed after the drain timeout")

const (
	adminHeaderLimit = 10 * time.Second
	adminIdleLimit   = 120 * time.Second
)

type listener struct {
	name    string
	address string
	srv     *http.Server
	lim     connLimits
	ln      net.Listener
	h2      *h2server.Server
	h2opts  h2server.Options
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
	clk       clock.Clock
	// listen binds a listener; tests replace it to serve on in-memory listeners.
	listen func(network, address string) (net.Listener, error)
	// shutdownDelay and drainTimeout are guarded by mu.
	shutdownDelay time.Duration
	drainTimeout  time.Duration
}

// Option configures a Server created by New.
type Option func(*options)

type options struct {
	certs *Certificates
	clk   clock.Clock
}

// WithCertificates makes TLS listeners present the certificates held by c, which must already hold one for every TLS listener.
func WithCertificates(c *Certificates) Option {
	return func(o *options) { o.certs = c }
}

// WithClock makes the server time its shutdown delay, drain timeout, and the listeners' read-header, read, write,
// and idle limits with c. Without it the server uses the real clock.
func WithClock(c clock.Clock) Option {
	return func(o *options) { o.clk = c }
}

// New prepares a server for every listener in cfg. The data handler h serves the listeners and the admin handler
// serves the admin listener when it is enabled in cfg. ready is called with false when shutdown begins. A nil
// logger discards log output and a nil ready callback does nothing.
func New(cfg *config.Config, h http.Handler, admin http.Handler, ready func(bool), logger *slog.Logger, opts ...Option) (*Server, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if ready == nil {
		ready = func(bool) {}
	}
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	clk := o.clk
	if clk == nil {
		clk = clock.Real()
	}
	certs := o.certs
	if certs == nil {
		certs = NewCertificates(logger, clk)
		_, next, errs := certs.load(cfg)
		if len(errs) > 0 {
			return nil, fmt.Errorf("listener %q: load certificate: %s", cfg.Listeners[errs[0].listener].Name, errs[0].fe.Msg)
		}
		certs.install(next)
	} else {
		for _, l := range cfg.Listeners {
			if l.TLS != nil && !certs.loaded(l.Name) {
				return nil, fmt.Errorf("listener %q: no certificate loaded", l.Name)
			}
		}
	}
	s := &Server{
		cfg:           cfg,
		logger:        logger,
		ready:         ready,
		addrs:         make(map[string]net.Addr),
		shutdownDelay: cfg.Global.ShutdownDelay.Std(),
		drainTimeout:  cfg.Global.DrainTimeout.Std(),
		clk:           clk,
		listen:        net.Listen,
	}
	s.base, s.cancel = context.WithCancel(context.Background())
	errLog := slog.NewLogLogger(logger.Handler(), slog.LevelWarn)
	for _, l := range cfg.Listeners {
		p := new(http.Protocols)
		p.SetHTTP1(true)
		srv := &http.Server{
			Handler:        s.track(h),
			MaxHeaderBytes: l.MaxHeaderBytes,
			BaseContext:    func(net.Listener) context.Context { return s.base },
			ConnState:      trackConnState,
			ErrorLog:       errLog,
		}
		ll := &listener{name: l.Name, address: l.Address, srv: srv}
		ll.lim = connLimits{
			header: l.ReadHeaderTimeout.Std(),
			read:   l.ReadTimeout.Std(),
			write:  l.WriteTimeout.Std(),
			idle:   l.IdleTimeout.Std(),
		}
		if l.TLS != nil {
			minVersion := uint16(tls.VersionTLS12)
			if l.TLS.MinVersion == "1.3" {
				minVersion = tls.VersionTLS13
			}
			h2 := l.HTTP2 == nil || *l.HTTP2
			protos := []string{"http/1.1"}
			if h2 {
				protos = []string{"h2", "http/1.1"}
			}
			srv.TLSConfig = &tls.Config{MinVersion: minVersion, GetCertificate: certs.getter(l.Name), NextProtos: protos}
			p.SetHTTP2(h2)
			ll.lim.tls, ll.lim.h2 = true, h2
		} else {
			p.SetUnencryptedHTTP2(l.H2C)
			ll.lim.h2c = l.H2C
		}
		srv.Protocols = p
		if ll.lim.h2 || ll.lim.h2c {
			// The http.Server below only describes the configured limits for OptionsFor; it is never served.
			ll.h2opts = h2server.OptionsFor(&http.Server{MaxHeaderBytes: l.MaxHeaderBytes, ReadTimeout: ll.lim.read, WriteTimeout: ll.lim.write, IdleTimeout: ll.lim.idle, ConnState: trackConnState, ErrorLog: errLog})
			ll.h2 = h2server.New(clk, ll.h2opts)
			if err := ll.h2.Attach(srv); err != nil {
				return nil, fmt.Errorf("listener %q: %w", l.Name, err)
			}
		}
		s.data = append(s.data, ll)
	}
	if admin != nil && (cfg.Admin.Enabled == nil || *cfg.Admin.Enabled) {
		p := new(http.Protocols)
		p.SetHTTP1(true)
		s.admin = &listener{
			name:    "admin",
			address: cfg.Admin.Address,
			srv: &http.Server{
				Handler:     admin,
				BaseContext: func(net.Listener) context.Context { return context.Background() },
				ConnState:   trackConnState,
				ErrorLog:    errLog,
				Protocols:   p,
			},
			lim: connLimits{header: adminHeaderLimit, idle: adminIdleLimit},
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
		ln, err := s.listen("tcp", l.address)
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
		l.ln = &limitListener{
			Listener:  lns[i],
			lim:       l.lim,
			clk:       s.clk,
			idleClk:   clock.Housekeeping(s.clk),
			tlsConfig: l.srv.TLSConfig,
		}
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
			err := l.srv.Serve(l.ln)
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
	s.mu.Lock()
	delay, drain := s.shutdownDelay, s.drainTimeout
	s.mu.Unlock()
	if delay > 0 {
		select {
		case <-s.clk.After(delay):
		case <-ctx.Done():
		}
	}
	drainCtx, cancel := clock.WithTimeout(ctx, s.clk, drain)
	errs := make([]error, len(s.data))
	var wg sync.WaitGroup
	for i, l := range s.data {
		wg.Add(1)
		go func(i int, l *listener) {
			defer wg.Done()
			if err := l.srv.Shutdown(clock.WithoutDeadline(drainCtx)); err != nil && !isContextErr(err) {
				errs[i] = err
			}
		}(i, l)
	}
	wg.Wait()
	drained := s.active.wait(drainCtx)
	forced := !drained || drainCtx.Err() != nil
	// Close the servers before canceling the base context so that handlers whose requests are canceled
	// cannot return with an implicit 200 to a connection that is still open.
	for _, l := range s.data {
		l.srv.Close()
	}
	if s.admin != nil {
		s.admin.srv.Close()
	}
	s.cancel()
	cancel()
	if forced {
		s.logger.Warn("drain timeout; connections force-closed")
		return errors.Join(append([]error{ErrForcedClose}, errs...)...)
	}
	s.logger.Info("drain complete")
	return errors.Join(errs...)
}

// SetShutdownTimings changes the shutdown delay and drain timeout used by the next Shutdown.
func (s *Server) SetShutdownTimings(delay, drain time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdownDelay = delay
	s.drainTimeout = drain
}

func isContextErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}
