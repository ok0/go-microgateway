package redis

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
)

// NewFromConfig returns a Store built from cfg. It performs no network I/O;
// connections are dialed on first use. A nil logger discards diagnostics.
// Its dial, read and write limits run on the real clock.
// NewFromConfig returns an error for an invalid configuration; the error text
// never contains a credential or a file path.
func NewFromConfig(cfg *config.RedisStore, logger *slog.Logger) (*Store, error) {
	return newFromConfig(cfg, logger, clock.Real())
}

// newFromConfig is NewFromConfig with the dial, read and write limits of the client running on clk.
func newFromConfig(cfg *config.RedisStore, logger *slog.Logger, clk clock.Clock) (*Store, error) {
	if cfg == nil {
		return nil, errors.New("redis: nil configuration")
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	prefix := DefaultKeyPrefix
	if cfg.KeyPrefix != nil {
		prefix = *cfg.KeyPrefix
	}
	if prefix == "" || strings.ContainsAny(prefix, "{}") {
		return nil, errors.New("redis: key_prefix must be non-empty and must not contain { or }")
	}
	tlsCfg, err := tlsConfig(cfg.TLS)
	if err != nil {
		return nil, err
	}
	opts, err := clientOptions(cfg, tlsCfg, clk)
	if err != nil {
		return nil, err
	}
	mode := cfg.Mode
	if mode == "" {
		mode = "standalone"
	}
	s := newStore(newClient(opts), true, prefix, logger)
	logger.Info("redis store configured", "mode", mode, "addresses", len(cfg.Addresses), "tls", tlsCfg != nil, "key_prefix", prefix)
	return s, nil
}

// clientOptions returns the go-redis options for the configured mode as one of
// *goredis.Options, *goredis.FailoverOptions or *goredis.ClusterOptions.
// The options turn off go-redis's own time limits; the dialer returned by newDialer runs the configured limits on clk.
func clientOptions(cfg *config.RedisStore, tlsCfg *tls.Config, clk clock.Clock) (any, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = "standalone"
	}
	if len(cfg.Addresses) == 0 {
		return nil, errors.New("redis: at least one address is required")
	}
	addrs := slices.Clone(cfg.Addresses)
	dialer := newDialer(clk, limitsOf(cfg), tlsCfg)
	disabled := &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	switch mode {
	case "standalone":
		if len(addrs) != 1 {
			return nil, errors.New("redis: standalone mode takes exactly one address")
		}
		return &goredis.Options{
			Addr:                     addrs[0],
			Username:                 cfg.Username,
			Password:                 cfg.Password.Reveal(),
			DB:                       cfg.DB,
			Protocol:                 2,
			MaxRetries:               -1,
			DialerRetries:            1,
			Dialer:                   dialer,
			DialTimeout:              ownDialer,
			ReadTimeout:              noSocketDeadline,
			WriteTimeout:             noSocketDeadline,
			PoolTimeout:              poolWaitForever,
			ConnMaxIdleTime:          keepIdleConns,
			ContextTimeoutEnabled:    true,
			PoolSize:                 cfg.PoolSize,
			MinIdleConns:             cfg.MinIdleConns,
			TLSConfig:                tlsCfg,
			DisableIdentity:          true,
			MaintNotificationsConfig: disabled,
		}, nil
	case "sentinel":
		if cfg.MasterName == "" {
			return nil, errors.New("redis: master_name is required in sentinel mode")
		}
		return &goredis.FailoverOptions{
			MasterName:            cfg.MasterName,
			SentinelAddrs:         addrs,
			SentinelUsername:      cfg.SentinelUsername,
			SentinelPassword:      cfg.SentinelPassword.Reveal(),
			Username:              cfg.Username,
			Password:              cfg.Password.Reveal(),
			DB:                    cfg.DB,
			Protocol:              2,
			MaxRetries:            -1,
			DialerRetries:         1,
			Dialer:                dialer,
			DialTimeout:           ownDialer,
			ReadTimeout:           noSocketDeadline,
			WriteTimeout:          noSocketDeadline,
			PoolTimeout:           poolWaitForever,
			ConnMaxIdleTime:       keepIdleConns,
			ContextTimeoutEnabled: true,
			PoolSize:              cfg.PoolSize,
			MinIdleConns:          cfg.MinIdleConns,
			TLSConfig:             tlsCfg,
			DisableIdentity:       true,
		}, nil
	case "cluster":
		if cfg.DB != 0 {
			return nil, errors.New("redis: db must be 0 in cluster mode")
		}
		return &goredis.ClusterOptions{
			Addrs:                    addrs,
			Username:                 cfg.Username,
			Password:                 cfg.Password.Reveal(),
			Protocol:                 2,
			MaxRetries:               -1,
			MaxRedirects:             -1,
			DialerRetries:            1,
			Dialer:                   dialer,
			DialTimeout:              ownDialer,
			ReadTimeout:              noSocketDeadline,
			WriteTimeout:             noSocketDeadline,
			PoolTimeout:              poolWaitForever,
			ConnMaxIdleTime:          keepIdleConns,
			ContextTimeoutEnabled:    true,
			PoolSize:                 cfg.PoolSize,
			MinIdleConns:             cfg.MinIdleConns,
			TLSConfig:                tlsCfg,
			DisableIdentity:          true,
			MaintNotificationsConfig: disabled,
		}, nil
	}
	return nil, errors.New("redis: mode must be standalone, sentinel or cluster")
}

// newClient returns the client for opts, which must come from clientOptions.
// It panics for any other type.
func newClient(opts any) goredis.UniversalClient {
	switch o := opts.(type) {
	case *goredis.Options:
		return goredis.NewClient(o)
	case *goredis.FailoverOptions:
		return goredis.NewFailoverClient(o)
	case *goredis.ClusterOptions:
		return goredis.NewClusterClient(o)
	}
	panic("redis: unsupported client options")
}

// dur returns the duration held by p, or def when p is nil.
func dur(p *config.Duration, def time.Duration) time.Duration {
	if p == nil {
		return def
	}
	return p.Std()
}

// tlsConfig returns the TLS configuration for t, or nil when TLS is disabled.
// Certificate and key files are read here, once, when the store is built.
func tlsConfig(t *config.RedisTLS) (*tls.Config, error) {
	if t == nil || !t.Enabled {
		return nil, nil
	}
	c := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName,
		InsecureSkipVerify: t.InsecureSkipVerify,
	}
	if t.CAFile != "" {
		pem, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, errors.New("redis: tls: ca_file cannot be read")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("redis: tls: ca_file contains no PEM certificate")
		}
		c.RootCAs = pool
	}
	if (t.CertFile == "") != (t.KeyFile == "") {
		return nil, errors.New("redis: tls: cert_file and key_file must be set together")
	}
	if t.CertFile != "" {
		pair, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, errors.New("redis: tls: cert_file and key_file do not form a valid key pair")
		}
		c.Certificates = []tls.Certificate{pair}
	}
	return c, nil
}
