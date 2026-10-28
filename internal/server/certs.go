package server

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

type certificate struct {
	cur atomic.Pointer[tls.Certificate]
}

func (c *certificate) get(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return c.cur.Load(), nil
}

// Certificates holds the current certificate of every TLS listener and replaces them atomically when a new
// configuration is applied. Handshakes read the current certificate; established connections are unaffected.
type Certificates struct {
	logger  *slog.Logger
	clk     clock.Clock
	mu      sync.Mutex
	holders map[string]*certificate     // by listener name; never removed
	current map[string]*tls.Certificate // by pair digest, for the pairs currently applied
	parses  atomic.Int64                // key pairs parsed; observed by tests
}

// NewCertificates returns an empty Certificates. A nil logger discards log output and a nil clk uses the real clock.
func NewCertificates(logger *slog.Logger, clk clock.Clock) *Certificates {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	if clk == nil {
		clk = clock.Real()
	}
	return &Certificates{
		logger:  logger,
		clk:     clk,
		holders: map[string]*certificate{},
		current: map[string]*tls.Certificate{},
	}
}

// Prepare reads the certificate and key files of every TLS listener in cfg and returns a digest of them. The
// returned apply function makes them current. An unreadable or invalid pair fails Prepare with a
// *config.ValidationError and leaves the current certificates unchanged.
func (c *Certificates) Prepare(cfg *config.Config) (digest string, apply func(), err error) {
	digest, next, errs := c.load(cfg)
	if len(errs) > 0 {
		fes := make([]config.FieldError, len(errs))
		for i, e := range errs {
			fes[i] = e.fe
		}
		return "", nil, &config.ValidationError{Errors: fes}
	}
	return digest, func() { c.install(next) }, nil
}

type pairError struct {
	listener int
	fe       config.FieldError
}

type pair struct {
	digest string
	cert   *tls.Certificate
}

func (c *Certificates) load(cfg *config.Config) (digest string, next map[string]*pair, errs []pairError) {
	c.mu.Lock()
	defer c.mu.Unlock()
	next = map[string]*pair{}
	all := sha256.New()
	for i, l := range cfg.Listeners {
		if l.TLS == nil {
			continue
		}
		certPath := fmt.Sprintf("listeners[%d].tls.cert_file", i)
		keyPath := fmt.Sprintf("listeners[%d].tls.key_file", i)
		certPEM, certErr := os.ReadFile(l.TLS.CertFile)
		if certErr != nil {
			errs = append(errs, pairError{listener: i, fe: config.FieldError{
				Path: certPath, Line: cfg.Line(certPath), Msg: "cannot read certificate: " + certErr.Error(),
			}})
		}
		keyPEM, keyErr := os.ReadFile(l.TLS.KeyFile)
		if keyErr != nil {
			errs = append(errs, pairError{listener: i, fe: config.FieldError{
				Path: keyPath, Line: cfg.Line(keyPath), Msg: "cannot read key: " + keyErr.Error(),
			}})
		}
		if certErr != nil || keyErr != nil {
			continue
		}
		h := sha256.New()
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(certPEM)))
		h.Write(n[:])
		h.Write(certPEM)
		h.Write(keyPEM)
		d := hex.EncodeToString(h.Sum(nil))
		cert := c.current[d]
		if cert == nil {
			kp, err := tls.X509KeyPair(certPEM, keyPEM)
			c.parses.Add(1)
			if err != nil {
				errs = append(errs, pairError{listener: i, fe: config.FieldError{
					Path: certPath, Line: cfg.Line(certPath), Msg: "invalid certificate or key: " + err.Error(),
				}})
				continue
			}
			if kp.Leaf != nil && c.clk.Now().After(kp.Leaf.NotAfter) {
				c.logger.Warn("certificate expired", "listener", l.Name, "not_after", kp.Leaf.NotAfter)
			}
			cert = &kp
		}
		next[l.Name] = &pair{digest: d, cert: cert}
		all.Write([]byte(l.Name + "=" + d + "\n"))
	}
	return hex.EncodeToString(all.Sum(nil)), next, errs
}

func (c *Certificates) install(next map[string]*pair) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cur := map[string]*tls.Certificate{}
	for name, p := range next {
		c.holder(name).cur.Store(p.cert)
		cur[p.digest] = p.cert
	}
	c.current = cur
}

// holder returns the certificate holder for a listener, creating it if needed. The caller must hold c.mu.
func (c *Certificates) holder(name string) *certificate {
	h := c.holders[name]
	if h == nil {
		h = &certificate{}
		c.holders[name] = h
	}
	return h
}

func (c *Certificates) getter(name string) func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.holder(name).get
}

func (c *Certificates) loaded(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	h, ok := c.holders[name]
	return ok && h.cur.Load() != nil
}
