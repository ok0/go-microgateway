package config

import (
	"math"
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

func burstFor(rate float64) int {
	if !(rate > 1) || math.IsInf(rate, 0) {
		return 1
	}
	return int(math.Ceil(rate))
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
			defInt(&p.MaxEjectionPercent, 50)
		}
		if a := u.ActiveHealth; a != nil {
			defStr(&a.Path, "/health")
			defDur(&a.Interval, 10*time.Second)
			defFloat(&a.Jitter, 0.1)
			defDur(&a.Timeout, 2*time.Second)
			defInt(&a.HealthyThreshold, 2)
			defInt(&a.UnhealthyThreshold, 3)
			if a.ExpectedStatuses == nil {
				a.ExpectedStatuses = []string{"200-399"}
			}
		}
		if cb := u.CircuitBreaker; cb != nil {
			defDur(&cb.Window, 10*time.Second)
			defInt(&cb.Buckets, 10)
			defInt(&cb.MinRequests, 20)
			defFloat(&cb.FailureRatio, 0.5)
			defInt(&cb.ConsecutiveFailures, 5)
			defDur(&cb.OpenTimeout, 30*time.Second)
			defInt(&cb.HalfOpenProbes, 3)
			defInt(&cb.SuccessThreshold, 2)
		}
	}

	for i := range c.Auth.JWT {
		p := &c.Auth.JWT[i]
		defDur(&p.Leeway, 30*time.Second)
		defStr(&p.Header, "Authorization")
		defStr(&p.ScopeClaim, "scope")
		defStr(&p.RolesClaim, "roles")
		if j := p.JWKS; j != nil {
			defDur(&j.TTL, 5*time.Minute)
			defDur(&j.MinRefreshInterval, 30*time.Second)
			defDur(&j.Timeout, 5*time.Second)
		}
	}
	for i := range c.Auth.APIKeys {
		p := &c.Auth.APIKeys[i]
		defStr(&p.Header, "X-API-Key")
		defStr(&p.ConsumerHeader, "X-Consumer-ID")
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
			defDur(&rl.Per, time.Second)
			if rl.Algorithm == "token_bucket" && rl.Burst == 0 {
				rl.Burst = burstFor(rl.Rate)
			}
			defDur(&rl.Window, time.Minute)
			defStr(&rl.Key.Source, "ip")
			defInt(&rl.MaxKeys, 100000)
			defDur(&rl.IdleTTL, 10*time.Minute)
		}
		if rb := r.CircuitBreaker; rb != nil {
			defBool(&rb.Enabled, true)
		}
		if rt := r.Retry; rt != nil {
			defInt(&rt.Attempts, 3)
			defDur(&rt.BaseDelay, 25*time.Millisecond)
			defDur(&rt.MaxDelay, 250*time.Millisecond)
			if rt.OnStatuses == nil {
				rt.OnStatuses = []int{502, 503, 504}
			}
			if rt.Methods == nil {
				rt.Methods = []string{"GET", "HEAD", "OPTIONS", "PUT", "DELETE"}
			}
			defStr(&rt.IdempotencyKey.Header, "Idempotency-Key")
			defFloat(&rt.Budget.Ratio, 0.1)
			defFloat(&rt.Budget.MinPerSecond, 10)
			defDur(&rt.Budget.Window, 10*time.Second)
			if rt.MaxBodyBytes == 0 {
				rt.MaxBodyBytes = 65536
			}
		}
	}
}
