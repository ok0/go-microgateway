package config

import (
	"time"
)

func boolPtr(b bool) *bool {
	return &b
}

func intPtr(v int) *int {
	return &v
}

func defStr(p *string, v string) {
	if *p == "" {
		*p = v
	}
}

func defInt(p *int, v int) {
	if *p == 0 {
		*p = v
	}
}

func defFloat(p *float64, v float64) {
	if *p == 0 {
		*p = v
	}
}

func floatPtr(v float64) *float64 {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func defDur(p *Duration, v time.Duration) {
	if *p == 0 {
		*p = Duration(v)
	}
}

func defBool(p **bool, v bool) {
	if *p == nil {
		*p = boolPtr(v)
	}
}

func defDurPtr(p **Duration, v time.Duration) {
	if *p == nil {
		d := Duration(v)
		*p = &d
	}
}

func defIntPtr(p **int, v int) {
	if *p == nil {
		*p = intPtr(v)
	}
}

func defStrPtr(p **string, v string) {
	if *p == nil {
		s := v
		*p = &s
	}
}

// applyDefaults sets every default documented in types.go on fields that hold their zero value.
// Optional blocks that are nil are left nil.
func (c *Config) applyDefaults() {
	g := &c.Global
	defStr(&g.RequestIDHeader, "X-Request-ID")
	if g.StripIdentityHeaders == nil {
		g.StripIdentityHeaders = []string{"X-Consumer-ID", "X-Consumer-Metadata", "X-User-ID", "X-Auth-Subject", "X-Auth-Claims"}
	}
	defDur(&g.DrainTimeout, 30*time.Second)
	defStr(&g.Log.Level, "info")
	defStr(&g.Log.Format, "json")
	defBool(&g.Log.AccessLog, true)
	defDur(&g.Reload.Debounce, 250*time.Millisecond)
	defDur(&g.Reload.PollInterval, 5*time.Second)

	for i := range c.Stores {
		c.Stores[i].applyDefaults()
	}
	c.Cache.applyDefaults()

	for i := range c.Listeners {
		l := &c.Listeners[i]
		defBool(&l.HTTP2, true)
		defDur(&l.ReadHeaderTimeout, 10*time.Second)
		defDur(&l.IdleTimeout, 120*time.Second)
		defInt(&l.MaxHeaderBytes, 1048576)
		if l.TLS != nil {
			defStr(&l.TLS.MinVersion, "1.2")
		}
	}

	defBool(&c.Admin.Enabled, true)
	defStr(&c.Admin.Address, "127.0.0.1:9901")

	for i := range c.Upstreams {
		u := &c.Upstreams[i]
		for j := range u.Hosts {
			if u.Hosts[j].Weight == nil {
				u.Hosts[j].Weight = intPtr(1)
			}
		}
		if d := u.DNS; d != nil {
			defStr(&d.Scheme, "http")
			defDur(&d.RefreshInterval, 30*time.Second)
		}
		defStr(&u.LoadBalancer.Algorithm, "p2c")
		defFloat(&u.LoadBalancer.BoundedLoadFactor, 1.25)
		defInt(&u.LoadBalancer.VirtualNodes, 160)
		defDur(&u.Timeouts.Connect, 5*time.Second)
		defDur(&u.Timeouts.TLSHandshake, 10*time.Second)
		defDur(&u.Timeouts.ResponseHeader, 30*time.Second)
		defDur(&u.Timeouts.Idle, 90*time.Second)
		defInt(&u.Pool.MaxIdlePerHost, 64)
		if p := u.PassiveHealth; p != nil {
			defInt(&p.ConsecutiveFailures, 5)
			defDur(&p.BaseEjectionTime, 30*time.Second)
			defDur(&p.MaxEjectionTime, 300*time.Second)
			if p.MaxEjectionPercent == nil {
				p.MaxEjectionPercent = intPtr(50)
			}
		}
		if a := u.ActiveHealth; a != nil {
			defStr(&a.Path, "/health")
			defDur(&a.Interval, 10*time.Second)
			if a.Jitter == nil {
				a.Jitter = floatPtr(0.1)
			}
			defDur(&a.Timeout, 2*time.Second)
			defIntPtr(&a.HealthyThreshold, 2)
			defIntPtr(&a.UnhealthyThreshold, 3)
			if a.ExpectedStatuses == nil {
				a.ExpectedStatuses = []string{"200-399"}
			}
		}
		if cb := u.CircuitBreaker; cb != nil {
			defDur(&cb.Window, 10*time.Second)
			if cb.Buckets == nil {
				cb.Buckets = intPtr(10)
			}
			if cb.MinRequests == nil {
				cb.MinRequests = intPtr(20)
			}
			if cb.FailureRatio == nil {
				cb.FailureRatio = floatPtr(0.5)
			}
			if cb.ConsecutiveFailures == nil {
				cb.ConsecutiveFailures = intPtr(5)
			}
			defDur(&cb.OpenTimeout, 30*time.Second)
			if cb.HalfOpenProbes == nil {
				cb.HalfOpenProbes = intPtr(3)
			}
			if cb.SuccessThreshold == nil {
				cb.SuccessThreshold = intPtr(2)
			}
		}
	}

	for i := range c.Auth.JWT {
		p := &c.Auth.JWT[i]
		defDurPtr(&p.Leeway, 30*time.Second)
		defStr(&p.Header, "Authorization")
		defStr(&p.ScopeClaim, "scope")
		defStr(&p.RolesClaim, "roles")
		defIntPtr(&p.MaxTokenBytes, 4096)
		defBool(&p.ForwardCredentials, false)
		if j := p.JWKS; j != nil {
			defDurPtr(&j.TTL, 10*time.Minute)
			defDurPtr(&j.MinRefreshInterval, 30*time.Second)
			defDurPtr(&j.Timeout, 5*time.Second)
		}
	}
	for i := range c.Auth.APIKeys {
		p := &c.Auth.APIKeys[i]
		defStrPtr(&p.Header, "X-API-Key")
		defStr(&p.ConsumerHeader, "X-Consumer-ID")
		defBool(&p.ForwardCredentials, false)
	}

	for i := range c.Routes {
		r := &c.Routes[i]
		defStr(&r.Match.Path, "/")
		defStr(&r.Match.PathType, "prefix")
		if a := r.Auth; a != nil {
			defStr(&a.Mode, "none")
		}
		if rl := r.RateLimit; rl != nil {
			defStr(&rl.Algorithm, "token_bucket")
			switch rl.Algorithm {
			case "token_bucket":
				defDurPtr(&rl.Per, time.Second)
				if rl.Burst == nil && rl.Rate != nil && *rl.Rate >= 1 {
					rl.Burst = int64Ptr(*rl.Rate)
				}
			case "sliding_window":
				defDurPtr(&rl.Window, time.Minute)
			}
			defStr(&rl.Key.Source, "ip")
			defIntPtr(&rl.MaxKeys, 100000)
			if rl.Store != "" {
				defStr(&rl.OnStoreError, "local")
				defDurPtr(&rl.StoreTimeout, 50*time.Millisecond)
			}
		}
		if rc := r.Cache; rc != nil {
			rc.applyDefaults()
		}
		if rb := r.CircuitBreaker; rb != nil {
			defBool(&rb.Enabled, true)
		}
		if rt := r.Retry; rt != nil {
			if rt.Attempts == nil {
				rt.Attempts = intPtr(3)
			}
			defDur(&rt.BaseDelay, 25*time.Millisecond)
			defDur(&rt.MaxDelay, 250*time.Millisecond)
			if rt.OnStatuses == nil {
				rt.OnStatuses = []int{502, 503, 504}
			}
			if rt.Methods == nil {
				rt.Methods = []string{"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}
			}
			defStr(&rt.IdempotencyKey.Header, "Idempotency-Key")
			if rt.Budget.Ratio == nil {
				rt.Budget.Ratio = floatPtr(0.1)
			}
			if rt.Budget.MinPerSecond == nil {
				rt.Budget.MinPerSecond = floatPtr(10)
			}
			defDur(&rt.Budget.Window, 10*time.Second)
			if rt.MaxBodyBytes == nil {
				rt.MaxBodyBytes = int64Ptr(65536)
			}
		}
	}
}
