package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/leakcheck"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/store"
)

func checkEq[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, want %v", name, got, want)
	}
}

func durPtr(d time.Duration) *config.Duration {
	v := config.Duration(d)
	return &v
}

func checkDisabledMaint(t *testing.T, c *maintnotifications.Config) {
	t.Helper()
	if c == nil {
		t.Fatal("MaintNotificationsConfig is nil")
	}
	checkEq(t, "MaintNotificationsConfig.Mode", c.Mode, maintnotifications.ModeDisabled)
}

// checkSentinels checks the go-redis option values that turn off go-redis's own limits on the wall clock.
func checkSentinels(t *testing.T, dial, read, write, pool, idle time.Duration, dialer bool) {
	t.Helper()
	checkEq(t, "DialTimeout negative", dial < 0, true)
	checkEq(t, "Dialer set", dialer, true)
	checkEq(t, "ReadTimeout", read, -2)
	checkEq(t, "WriteTimeout", write, -2)
	checkEq(t, "PoolTimeout at least 100 years", pool >= 100*365*24*time.Hour, true)
	checkEq(t, "ConnMaxIdleTime", idle, -1)
}

// TestClientOptionSentinels pins how go-redis interprets the sentinel values: -2 becomes its "no deadline call"
// value -1, and a negative dial timeout, the pool wait and -1 for idle connections are kept as given.
func TestClientOptionSentinels(t *testing.T) {
	addrs := []string{"127.0.0.1:1"}
	build := func(t *testing.T, cfg *config.RedisStore) any {
		t.Helper()
		opts, err := clientOptions(cfg, nil, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		return opts
	}
	t.Run("standalone", func(t *testing.T) {
		o := build(t, &config.RedisStore{Mode: "standalone", Addresses: addrs}).(*goredis.Options)
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
		c := goredis.NewClient(o)
		defer c.Close()
		n := c.Options()
		checkEq(t, "normalized ReadTimeout", n.ReadTimeout, -1)
		checkEq(t, "normalized WriteTimeout", n.WriteTimeout, -1)
		checkEq(t, "normalized DialTimeout negative", n.DialTimeout < 0, true)
		checkEq(t, "normalized PoolTimeout", n.PoolTimeout, poolWaitForever)
		checkEq(t, "normalized ConnMaxIdleTime", n.ConnMaxIdleTime, -1)
	})
	t.Run("sentinel", func(t *testing.T) {
		o := build(t, &config.RedisStore{Mode: "sentinel", MasterName: "mymaster", Addresses: addrs}).(*goredis.FailoverOptions)
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
		c := goredis.NewFailoverClient(o)
		defer c.Close()
		n := c.Options()
		checkEq(t, "normalized ReadTimeout", n.ReadTimeout, -1)
		checkEq(t, "normalized WriteTimeout", n.WriteTimeout, -1)
		checkEq(t, "normalized DialTimeout negative", n.DialTimeout < 0, true)
		checkEq(t, "normalized PoolTimeout", n.PoolTimeout, poolWaitForever)
		checkEq(t, "normalized ConnMaxIdleTime", n.ConnMaxIdleTime, -1)
	})
	t.Run("cluster", func(t *testing.T) {
		o := build(t, &config.RedisStore{Mode: "cluster", Addresses: addrs}).(*goredis.ClusterOptions)
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
		c := goredis.NewClusterClient(o)
		defer c.Close()
		// The cluster keeps the values and hands them to each node client, which normalizes them as above.
		n := c.Options()
		checkEq(t, "cluster ReadTimeout", n.ReadTimeout, -2)
		checkEq(t, "cluster WriteTimeout", n.WriteTimeout, -2)
		checkEq(t, "cluster DialTimeout negative", n.DialTimeout < 0, true)
		checkEq(t, "cluster PoolTimeout", n.PoolTimeout, poolWaitForever)
		checkEq(t, "cluster ConnMaxIdleTime", n.ConnMaxIdleTime, -1)
	})
}

func TestClientOptions(t *testing.T) {
	pw := uniqueSecret(t, "pw-")
	spw := uniqueSecret(t, "spw-")
	tlsCfg := &tls.Config{ServerName: "redis.test"}

	t.Run("standalone explicit", func(t *testing.T) {
		cfg := &config.RedisStore{
			Mode:         "standalone",
			Addresses:    []string{"10.0.0.1:6380"},
			Username:     "app",
			Password:     config.NewSecret(pw),
			DB:           3,
			DialTimeout:  durPtr(2 * time.Second),
			ReadTimeout:  durPtr(3 * time.Second),
			WriteTimeout: durPtr(4 * time.Second),
			PoolSize:     7,
			MinIdleConns: 2,
		}
		opts, err := clientOptions(cfg, tlsCfg, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		o, ok := opts.(*goredis.Options)
		if !ok {
			t.Fatalf("options type %T, want *goredis.Options", opts)
		}
		checkEq(t, "Addr", o.Addr, "10.0.0.1:6380")
		checkEq(t, "Username", o.Username, "app")
		checkEq(t, "Password", o.Password == pw, true)
		checkEq(t, "DB", o.DB, 3)
		checkEq(t, "Protocol", o.Protocol, 2)
		checkEq(t, "MaxRetries", o.MaxRetries, -1)
		checkEq(t, "DialerRetries", o.DialerRetries, 1)
		checkEq(t, "limits", limitsOf(cfg), connLimits{dial: 2 * time.Second, read: 3 * time.Second, write: 4 * time.Second})
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
		checkEq(t, "ContextTimeoutEnabled", o.ContextTimeoutEnabled, true)
		checkEq(t, "PoolSize", o.PoolSize, 7)
		checkEq(t, "MinIdleConns", o.MinIdleConns, 2)
		checkEq(t, "TLSConfig", o.TLSConfig == tlsCfg, true)
		checkEq(t, "DisableIdentity", o.DisableIdentity, true)
		checkDisabledMaint(t, o.MaintNotificationsConfig)
	})

	t.Run("standalone defaults", func(t *testing.T) {
		cfg := &config.RedisStore{Addresses: []string{"10.0.0.2:6379"}}
		opts, err := clientOptions(cfg, nil, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		o, ok := opts.(*goredis.Options)
		if !ok {
			t.Fatalf("options type %T, want *goredis.Options", opts)
		}
		checkEq(t, "Addr", o.Addr, "10.0.0.2:6379")
		checkEq(t, "limits", limitsOf(cfg), connLimits{dial: time.Second, read: 500 * time.Millisecond, write: 500 * time.Millisecond})
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
		checkEq(t, "TLSConfig nil", o.TLSConfig == nil, true)
		checkDisabledMaint(t, o.MaintNotificationsConfig)
	})

	t.Run("empty mode is standalone", func(t *testing.T) {
		opts, err := clientOptions(&config.RedisStore{Addresses: []string{"10.0.0.3:6379"}}, nil, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := opts.(*goredis.Options); !ok {
			t.Fatalf("options type %T, want *goredis.Options", opts)
		}
	})

	t.Run("sentinel explicit", func(t *testing.T) {
		cfg := &config.RedisStore{
			Mode:             "sentinel",
			MasterName:       "mymaster",
			Addresses:        []string{"s1:26379", "s2:26379"},
			SentinelUsername: "sentinel-user",
			SentinelPassword: config.NewSecret(spw),
			Username:         "app",
			Password:         config.NewSecret(pw),
			DB:               3,
			DialTimeout:      durPtr(2 * time.Second),
			ReadTimeout:      durPtr(3 * time.Second),
			WriteTimeout:     durPtr(4 * time.Second),
			PoolSize:         7,
			MinIdleConns:     2,
		}
		opts, err := clientOptions(cfg, tlsCfg, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		o, ok := opts.(*goredis.FailoverOptions)
		if !ok {
			t.Fatalf("options type %T, want *goredis.FailoverOptions", opts)
		}
		checkEq(t, "MasterName", o.MasterName, "mymaster")
		checkEq(t, "SentinelAddrs", slices.Equal(o.SentinelAddrs, []string{"s1:26379", "s2:26379"}), true)
		checkEq(t, "SentinelUsername", o.SentinelUsername, "sentinel-user")
		checkEq(t, "SentinelPassword", o.SentinelPassword == spw, true)
		checkEq(t, "Username", o.Username, "app")
		checkEq(t, "Password", o.Password == pw, true)
		checkEq(t, "DB", o.DB, 3)
		checkEq(t, "Protocol", o.Protocol, 2)
		checkEq(t, "MaxRetries", o.MaxRetries, -1)
		checkEq(t, "DialerRetries", o.DialerRetries, 1)
		checkEq(t, "limits", limitsOf(cfg), connLimits{dial: 2 * time.Second, read: 3 * time.Second, write: 4 * time.Second})
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
		checkEq(t, "ContextTimeoutEnabled", o.ContextTimeoutEnabled, true)
		checkEq(t, "PoolSize", o.PoolSize, 7)
		checkEq(t, "MinIdleConns", o.MinIdleConns, 2)
		checkEq(t, "TLSConfig", o.TLSConfig == tlsCfg, true)
		checkEq(t, "DisableIdentity", o.DisableIdentity, true)

		cfg.Addresses[0] = "changed:1"
		checkEq(t, "SentinelAddrs copied", o.SentinelAddrs[0], "s1:26379")
	})

	t.Run("sentinel defaults", func(t *testing.T) {
		cfg := &config.RedisStore{Mode: "sentinel", MasterName: "mymaster", Addresses: []string{"s1:26379"}}
		opts, err := clientOptions(cfg, nil, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		o, ok := opts.(*goredis.FailoverOptions)
		if !ok {
			t.Fatalf("options type %T, want *goredis.FailoverOptions", opts)
		}
		checkEq(t, "limits", limitsOf(cfg), connLimits{dial: time.Second, read: 500 * time.Millisecond, write: 500 * time.Millisecond})
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
	})

	t.Run("cluster explicit", func(t *testing.T) {
		cfg := &config.RedisStore{
			Mode:         "cluster",
			Addresses:    []string{"c1:6379", "c2:6379"},
			Username:     "app",
			Password:     config.NewSecret(pw),
			DialTimeout:  durPtr(2 * time.Second),
			ReadTimeout:  durPtr(3 * time.Second),
			WriteTimeout: durPtr(4 * time.Second),
			PoolSize:     7,
			MinIdleConns: 2,
		}
		opts, err := clientOptions(cfg, tlsCfg, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		o, ok := opts.(*goredis.ClusterOptions)
		if !ok {
			t.Fatalf("options type %T, want *goredis.ClusterOptions", opts)
		}
		checkEq(t, "Addrs", slices.Equal(o.Addrs, []string{"c1:6379", "c2:6379"}), true)
		checkEq(t, "Username", o.Username, "app")
		checkEq(t, "Password", o.Password == pw, true)
		checkEq(t, "Protocol", o.Protocol, 2)
		checkEq(t, "MaxRetries", o.MaxRetries, -1)
		checkEq(t, "MaxRedirects", o.MaxRedirects, -1)
		checkEq(t, "DialerRetries", o.DialerRetries, 1)
		checkEq(t, "limits", limitsOf(cfg), connLimits{dial: 2 * time.Second, read: 3 * time.Second, write: 4 * time.Second})
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
		checkEq(t, "ContextTimeoutEnabled", o.ContextTimeoutEnabled, true)
		checkEq(t, "PoolSize", o.PoolSize, 7)
		checkEq(t, "MinIdleConns", o.MinIdleConns, 2)
		checkEq(t, "TLSConfig", o.TLSConfig == tlsCfg, true)
		checkEq(t, "DisableIdentity", o.DisableIdentity, true)
		checkDisabledMaint(t, o.MaintNotificationsConfig)

		cfg.Addresses[0] = "changed:1"
		checkEq(t, "Addrs copied", o.Addrs[0], "c1:6379")
	})

	t.Run("cluster defaults", func(t *testing.T) {
		cfg := &config.RedisStore{Mode: "cluster", Addresses: []string{"c1:6379"}}
		opts, err := clientOptions(cfg, nil, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		o, ok := opts.(*goredis.ClusterOptions)
		if !ok {
			t.Fatalf("options type %T, want *goredis.ClusterOptions", opts)
		}
		checkEq(t, "limits", limitsOf(cfg), connLimits{dial: time.Second, read: 500 * time.Millisecond, write: 500 * time.Millisecond})
		checkSentinels(t, o.DialTimeout, o.ReadTimeout, o.WriteTimeout, o.PoolTimeout, o.ConnMaxIdleTime, o.Dialer != nil)
		checkEq(t, "MaxRedirects", o.MaxRedirects, -1)
	})
}

func TestNewFromConfigErrors(t *testing.T) {
	pw := uniqueSecret(t, "pw-")
	spw := uniqueSecret(t, "spw-")
	addrs := []string{"127.0.0.1:6379"}
	empty := ""
	braced := "a{b"
	cases := []struct {
		name string
		cfg  *config.RedisStore
	}{
		{"nil config", nil},
		{"unknown mode", &config.RedisStore{Mode: "bogus", Addresses: addrs, Password: config.NewSecret(pw)}},
		{"no addresses", &config.RedisStore{Mode: "standalone", Password: config.NewSecret(pw)}},
		{"standalone with two addresses", &config.RedisStore{Mode: "standalone", Addresses: []string{"a:1", "b:2"}, Password: config.NewSecret(pw)}},
		{"sentinel without master", &config.RedisStore{Mode: "sentinel", Addresses: addrs, Password: config.NewSecret(pw), SentinelPassword: config.NewSecret(spw)}},
		{"cluster with db 1", &config.RedisStore{Mode: "cluster", Addresses: addrs, DB: 1, Password: config.NewSecret(pw)}},
		{"empty key prefix", &config.RedisStore{Mode: "standalone", Addresses: addrs, Password: config.NewSecret(pw), KeyPrefix: &empty}},
		{"key prefix with brace", &config.RedisStore{Mode: "standalone", Addresses: addrs, Password: config.NewSecret(pw), KeyPrefix: &braced}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewFromConfig(tc.cfg, nil)
			if err == nil {
				_ = s.Close()
				t.Fatal("NewFromConfig returned no error")
			}
			for _, secret := range []string{pw, spw} {
				for _, f := range leakcheck.Forms(secret) {
					if strings.Contains(err.Error(), f) {
						t.Fatal("error text contains a form of a secret")
					}
				}
			}
		})
	}
}

func TestNewFromConfigModes(t *testing.T) {
	cases := []struct {
		name string
		cfg  *config.RedisStore
		ok   func(goredis.UniversalClient) bool
	}{
		{
			name: "standalone",
			cfg:  &config.RedisStore{Mode: "standalone", Addresses: []string{"127.0.0.1:1"}},
			ok:   func(c goredis.UniversalClient) bool { _, is := c.(*goredis.Client); return is },
		},
		{
			name: "sentinel",
			cfg:  &config.RedisStore{Mode: "sentinel", MasterName: "mymaster", Addresses: []string{"127.0.0.1:1"}},
			ok:   func(c goredis.UniversalClient) bool { _, is := c.(*goredis.Client); return is },
		},
		{
			name: "cluster",
			cfg:  &config.RedisStore{Mode: "cluster", Addresses: []string{"127.0.0.1:1"}},
			ok:   func(c goredis.UniversalClient) bool { _, is := c.(*goredis.ClusterClient); return is },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewFromConfig(tc.cfg, nil)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.ok(s.c) {
				t.Fatalf("client type %T is not the expected one", s.c)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if !errors.Is(s.c.Ping(context.Background()).Err(), goredis.ErrClosed) {
				t.Error("client is still open after Close")
			}
		})
	}
}

func TestNewFromConfigNoRoundTrip(t *testing.T) {
	addr, accepted := blackhole(t)
	s, err := NewFromConfig(&config.RedisStore{Mode: "standalone", Addresses: []string{addr}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	select {
	case <-accepted:
		t.Fatal("a connection was opened before the first Take")
	default:
	}
}

func TestTLS(t *testing.T) {
	certFile, keyFile, _ := testcert.Write(t, t.TempDir())
	dir := t.TempDir()

	writeFile := func(name string, data []byte) string {
		t.Helper()
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	checkTLSErr := func(t *testing.T, err error, want string) {
		t.Helper()
		if err == nil {
			t.Fatal("tlsConfig returned no error")
		}
		if err.Error() != want {
			t.Errorf("error = %q, want %q", err.Error(), want)
		}
		if strings.Contains(err.Error(), "-----BEGIN") {
			t.Error("error text contains PEM bytes")
		}
	}

	t.Run("ca file", func(t *testing.T) {
		c, err := tlsConfig(&config.RedisTLS{Enabled: true, CAFile: certFile, ServerName: "redis.test", InsecureSkipVerify: true})
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "RootCAs set", c.RootCAs != nil, true)
		checkEq(t, "MinVersion", c.MinVersion, uint16(tls.VersionTLS12))
		checkEq(t, "ServerName", c.ServerName, "redis.test")
		checkEq(t, "InsecureSkipVerify", c.InsecureSkipVerify, true)
	})

	t.Run("client certificate", func(t *testing.T) {
		c, err := tlsConfig(&config.RedisTLS{Enabled: true, CertFile: certFile, KeyFile: keyFile})
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "certificates", len(c.Certificates), 1)
	})

	t.Run("missing ca file", func(t *testing.T) {
		_, err := tlsConfig(&config.RedisTLS{Enabled: true, CAFile: filepath.Join(dir, "missing.pem")})
		checkTLSErr(t, err, "redis: tls: ca_file cannot be read")
	})

	t.Run("non-pem ca file", func(t *testing.T) {
		p := writeFile("not-pem.pem", []byte("this is not a certificate"))
		_, err := tlsConfig(&config.RedisTLS{Enabled: true, CAFile: p})
		checkTLSErr(t, err, "redis: tls: ca_file contains no PEM certificate")
	})

	t.Run("cert without key", func(t *testing.T) {
		_, err := tlsConfig(&config.RedisTLS{Enabled: true, CertFile: certFile})
		checkTLSErr(t, err, "redis: tls: cert_file and key_file must be set together")
	})

	t.Run("mismatched pair", func(t *testing.T) {
		_, otherKey := testcert.Generate(t, 2, time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC))
		p := writeFile("other-key.pem", otherKey)
		_, err := tlsConfig(&config.RedisTLS{Enabled: true, CertFile: certFile, KeyFile: p})
		checkTLSErr(t, err, "redis: tls: cert_file and key_file do not form a valid key pair")
	})

	t.Run("disabled", func(t *testing.T) {
		c, err := tlsConfig(&config.RedisTLS{Enabled: false, CAFile: certFile})
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "config nil", c == nil, true)
		c, err = tlsConfig(nil)
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "nil config", c == nil, true)
	})

	t.Run("take over tls", func(t *testing.T) {
		pair, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			t.Fatal(err)
		}
		m, err := miniredis.RunTLS(&tls.Config{Certificates: []tls.Certificate{pair}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(m.Close)
		m.SetTime(miniStart)
		s, err := newFromConfig(&config.RedisStore{
			Mode:      "standalone",
			Addresses: []string{m.Addr()},
			TLS:       &config.RedisTLS{Enabled: true, CAFile: certFile},
		}, nil, clock.NewFake(miniStart))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		d, err := s.Take(context.Background(), "tls-user", store.Limit{
			Algorithm: store.TokenBucket,
			Count:     1,
			Period:    time.Hour,
			Burst:     1,
		})
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "Allowed", d.Allowed, true)
	})
}

func storeWithAuth(t *testing.T, addr, username, password string, db int) *Store {
	t.Helper()
	s, err := newFromConfig(&config.RedisStore{
		Mode:      "standalone",
		Addresses: []string{addr},
		Username:  username,
		Password:  config.NewSecret(password),
		DB:        db,
	}, nil, clock.NewFake(miniStart))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestAuthAndDB(t *testing.T) {
	pw := uniqueSecret(t, "pw-")
	wrong := uniqueSecret(t, "wpw-")
	limit := store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: time.Hour, Burst: 1}
	ctx := context.Background()

	t.Run("correct password", func(t *testing.T) {
		m := newMini(t)
		m.RequireAuth(pw)
		s := storeWithAuth(t, m.Addr(), "", pw, 0)
		d, err := s.Take(ctx, "auth-ok", limit)
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "Allowed", d.Allowed, true)
	})

	t.Run("wrong password", func(t *testing.T) {
		m := newMini(t)
		m.RequireAuth(pw)
		s := storeWithAuth(t, m.Addr(), "", wrong, 0)
		_, err := s.Take(ctx, "auth-bad", limit)
		if !errors.Is(err, store.ErrUnavailable) {
			t.Fatalf("error = %v, want ErrUnavailable", err)
		}
		if !strings.Contains(err.Error(), "server error") {
			t.Errorf("error %q lacks the server error category", err.Error())
		}
		for _, secret := range []string{pw, wrong} {
			for _, f := range leakcheck.Forms(secret) {
				if strings.Contains(err.Error(), f) {
					t.Fatal("error text contains a form of a secret")
				}
			}
		}
	})

	t.Run("user and password", func(t *testing.T) {
		m := newMini(t)
		m.RequireUserAuth("svc", pw)
		s := storeWithAuth(t, m.Addr(), "svc", pw, 0)
		d, err := s.Take(ctx, "auth-user", limit)
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "Allowed", d.Allowed, true)
	})

	t.Run("database 3", func(t *testing.T) {
		m := newMini(t)
		s := storeWithAuth(t, m.Addr(), "", "", 3)
		if _, err := s.Take(ctx, "auth-db", limit); err != nil {
			t.Fatal(err)
		}
		checkEq(t, "keys in database 3", len(m.DB(3).Keys()) > 0, true)
	})
}

func TestOptionCapabilities(t *testing.T) {
	checkEq(t, "capabilities", capabilities, store.CapRateLimit|store.CapCache)
}

func TestOptionYAML(t *testing.T) {
	pw := uniqueSecret(t, "pw-")
	m := newMini(t)
	m.RequireAuth(pw)
	doc := gwDoc(
		redisStoreYAML("shared", []string{m.Addr()}, "password: "+pw+"\nkey_prefix: mgtest\n", ""),
		routeYAML("rl", "rate: 1\nper: 1h\nburst: 2\nstore: shared\nstore_timeout: 2s\n"),
		upstream(t),
	)
	g := mustGateway(t, doc, clock.NewFake(miniStart), nil)
	remote := "192.0.2.10:4000"
	for _, want := range []int{200, 200, 429} {
		rec := doGet(g, "/rl", remote)
		if rec.Code != want {
			t.Fatalf("status %d, want %d", rec.Code, want)
		}
	}
	keys := m.Keys()
	checkEq(t, "keys written", len(keys) > 0, true)
	for _, k := range keys {
		if !strings.HasPrefix(k, "mgtest:rl:{") {
			t.Errorf("key %q lacks the configured prefix", k)
		}
	}
}

func TestOptionUnregistered(t *testing.T) {
	m := newMini(t)
	doc := gwDoc(
		redisStoreYAML("shared", []string{m.Addr()}, "", ""),
		routeYAML("rl", "rate: 1\nper: 1h\nburst: 2\nstore: shared\n"),
		upstream(t),
	)
	_, err := newGateway(t, doc, false, clock.NewFake(miniStart), nil)
	if err == nil || !strings.Contains(err.Error(), `store type "redis" is not registered`) {
		t.Fatalf("error = %v, want store type \"redis\" is not registered", err)
	}
}

type countingStore struct {
	*Store
	closed atomic.Int32
}

func (c *countingStore) Close() error {
	c.closed.Add(1)
	return c.Store.Close()
}

func TestReloadReuseAndRecreate(t *testing.T) {
	m := newMini(t)
	srv := upstream(t)
	var built []*countingStore
	counting := func(p microgateway.StoreParams) (store.Backend, error) {
		b, err := factory(p)
		if err != nil {
			return nil, err
		}
		c := &countingStore{Store: b.(*Store)}
		built = append(built, c)
		return c, nil
	}
	rl := routeYAML("rl", "rate: 1\nper: 1h\nburst: 2\nstore: shared\nstore_timeout: 2s\n")
	other := routeYAML("other", "rate: 5\nper: 1s\n")
	remote := "192.0.2.10:4000"

	g, err := newGateway(t, gwDoc(redisStoreYAML("shared", []string{m.Addr()}, "key_prefix: first\n", ""), rl, srv),
		false, clock.NewFake(miniStart), nil, microgateway.WithStoreType("redis", capabilities, counting))
	if err != nil {
		t.Fatal(err)
	}
	checkEq(t, "factory calls after build", len(built), 1)

	if err := g.ReloadYAML([]byte(gwDoc(redisStoreYAML("shared", []string{m.Addr()}, "key_prefix: first\n", ""), rl+other, srv))); err != nil {
		t.Fatal(err)
	}
	checkEq(t, "factory calls after unrelated reload", len(built), 1)
	checkEq(t, "first store closes after unrelated reload", built[0].closed.Load(), int32(0))

	if err := g.ReloadYAML([]byte(gwDoc(redisStoreYAML("shared", []string{m.Addr()}, "key_prefix: second\n", ""), rl+other, srv))); err != nil {
		t.Fatal(err)
	}
	checkEq(t, "factory calls after prefix change", len(built), 2)
	checkEq(t, "first store closes after prefix change", built[0].closed.Load(), int32(1))
	checkEq(t, "second store closes before Close", built[1].closed.Load(), int32(0))
	rec := doGet(g, "/rl", remote)
	checkEq(t, "status under new prefix", rec.Code, http.StatusOK)
	found := false
	for _, k := range m.Keys() {
		if strings.HasPrefix(k, "second:rl:{") {
			found = true
		}
	}
	checkEq(t, "key under new prefix", found, true)

	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	checkEq(t, "second store closes once", built[1].closed.Load(), int32(1))
	checkEq(t, "first store closes once", built[0].closed.Load(), int32(1))
}

func TestCacheStoreWiring(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		_, _ = io.WriteString(w, "cached")
	}))
	t.Cleanup(origin.Close)
	clk := clock.NewFake(miniStart)
	remote := "192.0.2.10:4000"
	const bothRoute = "  - name: both\n    match:\n      path: /both\n    upstream: up\n" +
		"    rate_limit:\n      rate: 100\n      per: 1h\n      burst: 100\n      store: shared\n      store_timeout: 2s\n" +
		"    cache:\n      enabled: true\n"
	const cachedRoute = "  - name: cached\n    match:\n      path: /cached\n    upstream: up\n    cache:\n      enabled: true\n"
	const extraRoute = "  - name: extra\n    match:\n      path: /extra\n    upstream: up\n"
	wiring := func(addr, prefix, routes string) string {
		doc := gwDoc(redisStoreYAML("shared", []string{addr}, "key_prefix: "+prefix+"\n", ""), routes, origin.URL)
		return strings.Replace(doc, "listeners:\n", "cache:\n  store: shared\nlisteners:\n", 1)
	}
	lastStatus := func(t *testing.T, rec *httptest.ResponseRecorder) httpcache.Status {
		t.Helper()
		sts, err := httpcache.ParseStatus(strings.Join(rec.Header().Values("Cache-Status"), ", "))
		if err != nil || len(sts) == 0 {
			t.Fatalf("Cache-Status %q: %v", rec.Header().Values("Cache-Status"), err)
		}
		return sts[len(sts)-1]
	}

	t.Run("one store serves rate limit and cache", func(t *testing.T) {
		m := newMini(t)
		built := 0
		counting := func(p microgateway.StoreParams) (store.Backend, error) {
			built++
			return factory(p)
		}
		g, err := newGateway(t, wiring(m.Addr(), "mgtest", bothRoute), false, clk, nil,
			microgateway.WithStoreType("redis", capabilities, counting))
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "factory calls after build", built, 1)

		miss := doGet(g, "/both", remote)
		checkEq(t, "miss status", miss.Code, http.StatusOK)
		if st := lastStatus(t, miss); st.Hit || !st.Stored {
			t.Fatalf("first Cache-Status %+v, want a stored miss", st)
		}
		hit := doGet(g, "/both", remote)
		checkEq(t, "hit status", hit.Code, http.StatusOK)
		if st := lastStatus(t, hit); !st.Hit {
			t.Fatalf("second Cache-Status %+v, want a hit", st)
		}
		checkEq(t, "hit body", hit.Body.String(), "cached")
		checkEq(t, "factory calls after requests", built, 1)

		var rl, c int
		for _, k := range m.Keys() {
			switch {
			case strings.HasPrefix(k, "mgtest:rl:{"):
				rl++
			case strings.HasPrefix(k, "mgtest:c:"):
				c++
			default:
				t.Errorf("key %q is under neither :rl: nor :c:", k)
			}
		}
		checkEq(t, "rate-limit keys written", rl > 0, true)
		checkEq(t, "cache keys written", c > 0, true)
	})

	t.Run("rate-limit-only type rejected as cache store", func(t *testing.T) {
		plain := func(microgateway.StoreParams) (store.Backend, error) {
			return nil, errors.New("unexpected build")
		}
		doc := gwDoc("  - name: other\n    type: plain\n", cachedRoute, origin.URL)
		doc = strings.Replace(doc, "listeners:\n", "cache:\n  store: other\nlisteners:\n", 1)
		_, err := newGateway(t, doc, false, clk, nil, microgateway.WithStoreType("plain", store.CapRateLimit, plain))
		var ve *config.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("error %v, want a *config.ValidationError", err)
		}
		line := 1 + strings.Count(doc[:strings.Index(doc, "  store: other")], "\n")
		want := config.FieldError{Path: "cache.store", Line: line, Msg: `store "other" (type "plain") does not support caching`}
		checkEq(t, "errors", slices.Equal(ve.Errors, []config.FieldError{want}), true)
	})

	t.Run("validate and load do not connect", func(t *testing.T) {
		addr, accepted := blackhole(t)
		doc := wiring(addr, "mgtest", bothRoute)
		cfg, err := config.Parse([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		if err := microgateway.Validate(cfg, Option()); err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if _, err := newGateway(t, doc, true, clk, nil); err != nil {
			t.Fatal(err)
		}
		select {
		case <-accepted:
			t.Fatal("a connection was opened before the first request")
		default:
		}
	})

	t.Run("reload reuses an unchanged store and recreates a changed one", func(t *testing.T) {
		m := newMini(t)
		var built []*countingStore
		counting := func(p microgateway.StoreParams) (store.Backend, error) {
			b, err := factory(p)
			if err != nil {
				return nil, err
			}
			c := &countingStore{Store: b.(*Store)}
			built = append(built, c)
			return c, nil
		}
		routes := bothRoute + cachedRoute
		g, err := newGateway(t, wiring(m.Addr(), "first", routes), false, clk, nil,
			microgateway.WithStoreType("redis", capabilities, counting))
		if err != nil {
			t.Fatal(err)
		}
		checkEq(t, "factory calls after build", len(built), 1)

		if err := g.ReloadYAML([]byte(wiring(m.Addr(), "first", routes+extraRoute))); err != nil {
			t.Fatal(err)
		}
		checkEq(t, "factory calls after unrelated reload", len(built), 1)
		checkEq(t, "first store closes after unrelated reload", built[0].closed.Load(), int32(0))

		if err := g.ReloadYAML([]byte(wiring(m.Addr(), "second", routes+extraRoute))); err != nil {
			t.Fatal(err)
		}
		checkEq(t, "factory calls after prefix change", len(built), 2)
		checkEq(t, "first store closes after prefix change", built[0].closed.Load(), int32(1))
		checkEq(t, "second store closes before Close", built[1].closed.Load(), int32(0))
		rec := doGet(g, "/both", remote)
		checkEq(t, "status under new prefix", rec.Code, http.StatusOK)
		found := false
		for _, k := range m.Keys() {
			if strings.HasPrefix(k, "second:c:") {
				found = true
			}
		}
		checkEq(t, "cache key under new prefix", found, true)

		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		checkEq(t, "second store closes once", built[1].closed.Load(), int32(1))
		checkEq(t, "first store closes once", built[0].closed.Load(), int32(1))
	})
}
