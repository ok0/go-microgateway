package config

import (
	"fmt"
	"maps"
	"math"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ok0/lychgate/internal/router"
)

// Validate applies defaults to c, then checks it; it returns a *ValidationError (line numbers are 0) or nil.
func (c *Config) Validate() error {
	c.applyDefaults()
	errs := c.validate()
	if len(errs) == 0 {
		return nil
	}
	return newValidationError(errs)
}

var (
	jwtAlgorithms = []string{"HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES384", "ES512", "EdDSA"}
	lbAlgorithms  = []string{"p2c", "round_robin", "weighted_round_robin", "random", "consistent_hash"}
	apiKeyHash    = regexp.MustCompile(`^sha256:[0-9a-fA-F]{64}$`)
	statusPattern = regexp.MustCompile(`^\d{3}(-\d{3})?$`)
)

type checker struct {
	errs []FieldError
}

func (ck *checker) add(path, msg string) {
	ck.errs = append(ck.errs, FieldError{Path: path, Msg: msg})
}

// validate collects every problem in c without stopping at the first one. It expects defaults to be applied.
func (c *Config) validate() []FieldError {
	ck := &checker{}
	if c.Version != 1 {
		ck.add("version", fmt.Sprintf("unsupported version %d (must be 1)", c.Version))
	}
	checkGlobal(ck, &c.Global)
	checkListeners(ck, c)
	checkUpstreams(ck, c.Upstreams)
	kinds := checkAuth(ck, &c.Auth)
	checkMiddlewares(ck, c.Middlewares)
	checkRoutes(ck, c, kinds)
	return ck.errs
}

func checkName(ck *checker, path, name string, seen map[string]bool) {
	if name == "" {
		ck.add(path, "is required")
		return
	}
	if seen[name] {
		ck.add(path, fmt.Sprintf("duplicate name %q", name))
		return
	}
	seen[name] = true
}

func requirePositive(ck *checker, path string, d Duration) {
	if d <= 0 {
		ck.add(path, "must be positive")
	}
}

func requireNonNegative(ck *checker, path string, d Duration) {
	if d < 0 {
		ck.add(path, "must not be negative")
	}
}

func validAddress(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	_, err = strconv.ParseUint(port, 10, 16)
	return err == nil
}

func validHost(h string) bool {
	name := h
	if rest, ok := strings.CutPrefix(h, "*."); ok {
		name = rest
	}
	if name == "" {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if label == "" || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return false
			}
		}
	}
	return true
}

func isUpperToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func statusRangeProblem(s string) string {
	if !statusPattern.MatchString(s) {
		return fmt.Sprintf("invalid status range %q (use NNN or NNN-NNN)", s)
	}
	lo, _ := strconv.Atoi(s[:3])
	hi := lo
	if len(s) > 3 {
		hi, _ = strconv.Atoi(s[4:])
	}
	if lo < 100 || hi > 599 || lo > hi {
		return fmt.Sprintf("invalid status range %q", s)
	}
	return ""
}

func checkGlobal(ck *checker, g *Global) {
	if g.RequestIDHeader == "" {
		ck.add("global.request_id_header", "is required")
	}
	for i, p := range g.TrustedProxies {
		_, perr := netip.ParsePrefix(p)
		_, aerr := netip.ParseAddr(p)
		if perr != nil && aerr != nil {
			ck.add(fmt.Sprintf("global.trusted_proxies[%d]", i), fmt.Sprintf("invalid CIDR %q", p))
		}
	}
	requirePositive(ck, "global.drain_timeout", g.DrainTimeout)
	requireNonNegative(ck, "global.shutdown_delay", g.ShutdownDelay)
	if !slices.Contains([]string{"debug", "info", "warn", "error"}, g.Log.Level) {
		ck.add("global.log.level", "must be one of debug, info, warn, error")
	}
	if !slices.Contains([]string{"json", "text"}, g.Log.Format) {
		ck.add("global.log.format", "must be one of json, text")
	}
	requirePositive(ck, "global.reload.debounce", g.Reload.Debounce)
	requirePositive(ck, "global.reload.poll_interval", g.Reload.PollInterval)
}

func checkListeners(ck *checker, c *Config) {
	if len(c.Listeners) == 0 {
		ck.add("listeners", "at least one listener is required")
	}
	names := map[string]bool{}
	for i := range c.Listeners {
		l := &c.Listeners[i]
		p := fmt.Sprintf("listeners[%d]", i)
		checkName(ck, p+".name", l.Name, names)
		if !validAddress(l.Address) {
			ck.add(p+".address", fmt.Sprintf("invalid address %q", l.Address))
		}
		if t := l.TLS; t != nil {
			if t.CertFile == "" {
				ck.add(p+".tls.cert_file", "is required")
			}
			if t.KeyFile == "" {
				ck.add(p+".tls.key_file", "is required")
			}
			if !slices.Contains([]string{"1.2", "1.3"}, t.MinVersion) {
				ck.add(p+".tls.min_version", "must be one of 1.2, 1.3")
			}
		}
		requirePositive(ck, p+".read_header_timeout", l.ReadHeaderTimeout)
		requirePositive(ck, p+".idle_timeout", l.IdleTimeout)
		requireNonNegative(ck, p+".read_timeout", l.ReadTimeout)
		requireNonNegative(ck, p+".write_timeout", l.WriteTimeout)
		if l.MaxHeaderBytes <= 0 {
			ck.add(p+".max_header_bytes", "must be positive")
		}
	}
	if c.Admin.Enabled != nil && *c.Admin.Enabled && !validAddress(c.Admin.Address) {
		ck.add("admin.address", fmt.Sprintf("invalid address %q", c.Admin.Address))
	}
}

func checkUpstreams(ck *checker, ups []Upstream) {
	names := map[string]bool{}
	for i := range ups {
		u := &ups[i]
		p := fmt.Sprintf("upstreams[%d]", i)
		checkName(ck, p+".name", u.Name, names)
		hasDNS := u.DNS != nil
		switch {
		case len(u.Hosts) == 0 && !hasDNS:
			ck.add(p, "either hosts or dns is required")
		case len(u.Hosts) > 0 && hasDNS:
			ck.add(p, "hosts and dns are mutually exclusive")
		}
		for j := range u.Hosts {
			checkHost(ck, fmt.Sprintf("%s.hosts[%d]", p, j), &u.Hosts[j])
		}
		if u.DNS != nil {
			checkDNS(ck, p+".dns", u.DNS)
		}
		checkLoadBalancer(ck, p+".load_balancer", &u.LoadBalancer)
		checkUpstreamTimeouts(ck, p, u)
		checkUpstreamHealth(ck, p, u)
	}
}

func checkHost(ck *checker, p string, h *Host) {
	u, err := url.Parse(h.URL)
	switch {
	case err != nil:
		ck.add(p+".url", fmt.Sprintf("invalid URL: %v", err))
	case u.Scheme != "http" && u.Scheme != "https":
		ck.add(p+".url", "scheme must be http or https")
	case u.Host == "":
		ck.add(p+".url", "missing host")
	}
	if h.Weight < 0 {
		ck.add(p+".weight", "must not be negative")
	}
}

func checkDNS(ck *checker, p string, d *DNS) {
	if d.Hostname == "" {
		ck.add(p+".hostname", "is required")
	}
	if d.Port < 1 || d.Port > 65535 {
		ck.add(p+".port", "must be between 1 and 65535")
	}
	if d.Scheme != "http" && d.Scheme != "https" {
		ck.add(p+".scheme", "must be one of http, https")
	}
	requirePositive(ck, p+".refresh_interval", d.RefreshInterval)
}

func checkLoadBalancer(ck *checker, p string, lb *LoadBalancer) {
	if !slices.Contains(lbAlgorithms, lb.Algorithm) {
		ck.add(p+".algorithm", "must be one of p2c, round_robin, weighted_round_robin, random, consistent_hash")
	}
	if lb.Algorithm == "consistent_hash" || lb.Hash.Source != "" {
		hp := p + ".hash"
		switch lb.Hash.Source {
		case "header", "cookie":
			if lb.Hash.Name == "" {
				ck.add(hp+".name", "is required for header and cookie sources")
			}
		case "ip", "path":
		default:
			ck.add(hp+".source", "must be one of header, cookie, ip, path")
		}
	}
	if f := lb.BoundedLoadFactor; !(f >= 1) || math.IsInf(f, 0) {
		ck.add(p+".bounded_load_factor", "must be >= 1")
	}
	if lb.VirtualNodes < 1 {
		ck.add(p+".virtual_nodes", "must be >= 1")
	}
}

func checkUpstreamTimeouts(ck *checker, p string, u *Upstream) {
	tp := p + ".timeouts"
	requirePositive(ck, tp+".connect", u.Timeouts.Connect)
	requirePositive(ck, tp+".tls_handshake", u.Timeouts.TLSHandshake)
	requirePositive(ck, tp+".response_header", u.Timeouts.ResponseHeader)
	requirePositive(ck, tp+".idle", u.Timeouts.Idle)
	if u.Pool.MaxIdlePerHost < 0 {
		ck.add(p+".pool.max_idle_per_host", "must not be negative")
	}
	if u.Pool.MaxConnsPerHost < 0 {
		ck.add(p+".pool.max_conns_per_host", "must not be negative")
	}
}

func checkUpstreamHealth(ck *checker, p string, u *Upstream) {
	if ph := u.PassiveHealth; ph != nil {
		pp := p + ".passive_health"
		if ph.ConsecutiveFailures < 1 {
			ck.add(pp+".consecutive_failures", "must be >= 1")
		}
		requirePositive(ck, pp+".base_ejection_time", ph.BaseEjectionTime)
		if ph.MaxEjectionTime < ph.BaseEjectionTime {
			ck.add(pp+".max_ejection_time", "must be >= base_ejection_time")
		}
		if ph.MaxEjectionPercent < 1 || ph.MaxEjectionPercent > 100 {
			ck.add(pp+".max_ejection_percent", "must be between 1 and 100")
		}
	}
	if ah := u.ActiveHealth; ah != nil {
		ap := p + ".active_health"
		if !strings.HasPrefix(ah.Path, "/") {
			ck.add(ap+".path", `must start with "/"`)
		}
		requirePositive(ck, ap+".interval", ah.Interval)
		requirePositive(ck, ap+".timeout", ah.Timeout)
		if j := ah.Jitter; !(j >= 0 && j < 1) || math.IsInf(j, 0) {
			ck.add(ap+".jitter", "must be in [0, 1)")
		}
		if ah.HealthyThreshold < 1 {
			ck.add(ap+".healthy_threshold", "must be >= 1")
		}
		if ah.UnhealthyThreshold < 1 {
			ck.add(ap+".unhealthy_threshold", "must be >= 1")
		}
		for k, s := range ah.ExpectedStatuses {
			if msg := statusRangeProblem(s); msg != "" {
				ck.add(fmt.Sprintf("%s.expected_statuses[%d]", ap, k), msg)
			}
		}
	}
	if cb := u.CircuitBreaker; cb != nil {
		cp := p + ".circuit_breaker"
		requirePositive(ck, cp+".window", cb.Window)
		if cb.Buckets < 1 {
			ck.add(cp+".buckets", "must be >= 1")
		}
		if cb.MinRequests < 1 {
			ck.add(cp+".min_requests", "must be >= 1")
		}
		if f := cb.FailureRatio; !(f > 0 && f <= 1) || math.IsInf(f, 0) {
			ck.add(cp+".failure_ratio", "must be in (0, 1]")
		}
		if cb.ConsecutiveFailures < 1 {
			ck.add(cp+".consecutive_failures", "must be >= 1")
		}
		requirePositive(ck, cp+".open_timeout", cb.OpenTimeout)
		if cb.HalfOpenProbes < 1 {
			ck.add(cp+".half_open_probes", "must be >= 1")
		}
		if cb.SuccessThreshold < 1 {
			ck.add(cp+".success_threshold", "must be >= 1")
		}
		if cb.HalfOpenProbes < cb.SuccessThreshold {
			ck.add(cp+".half_open_probes", "must be >= success_threshold")
		}
	}
}

// checkAuth validates every provider and returns the kind ("jwt" or "apikey") of each provider name.
func checkAuth(ck *checker, a *Auth) map[string]string {
	kinds := map[string]string{}
	names := map[string]bool{}
	for i := range a.JWT {
		p := fmt.Sprintf("auth.jwt[%d]", i)
		checkName(ck, p+".name", a.JWT[i].Name, names)
		if a.JWT[i].Name != "" {
			if _, ok := kinds[a.JWT[i].Name]; !ok {
				kinds[a.JWT[i].Name] = "jwt"
			}
		}
		checkJWT(ck, p, &a.JWT[i])
	}
	for i := range a.APIKeys {
		p := fmt.Sprintf("auth.api_keys[%d]", i)
		checkName(ck, p+".name", a.APIKeys[i].Name, names)
		if a.APIKeys[i].Name != "" {
			if _, ok := kinds[a.APIKeys[i].Name]; !ok {
				kinds[a.APIKeys[i].Name] = "apikey"
			}
		}
		checkAPIKeys(ck, p, &a.APIKeys[i])
	}
	return kinds
}

func checkJWT(ck *checker, p string, j *JWTProvider) {
	if j.Issuer == "" {
		ck.add(p+".issuer", "is required")
	}
	if len(j.Audiences) == 0 {
		ck.add(p+".audiences", "at least one audience is required")
	}
	switch {
	case j.Leeway < 0:
		ck.add(p+".leeway", "must not be negative")
	case j.Leeway > Duration(5*time.Minute):
		ck.add(p+".leeway", "must not exceed 5m")
	}
	if j.Header == "" {
		ck.add(p+".header", "is required")
	}
	if j.ScopeClaim == "" {
		ck.add(p+".scope_claim", "is required")
	}
	if j.RolesClaim == "" {
		ck.add(p+".roles_claim", "is required")
	}
	if len(j.Keys) == 0 && j.JWKS == nil {
		ck.add(p, "keys or jwks is required")
	}
	kids := map[string]bool{}
	for k := range j.Keys {
		key := &j.Keys[k]
		kp := fmt.Sprintf("%s.keys[%d]", p, k)
		if len(j.Keys) > 1 {
			switch {
			case key.KID == "":
				ck.add(kp+".kid", "is required when there is more than one key")
			case kids[key.KID]:
				ck.add(kp+".kid", fmt.Sprintf("duplicate kid %q", key.KID))
			default:
				kids[key.KID] = true
			}
		}
		if !slices.Contains(jwtAlgorithms, key.Algorithm) {
			ck.add(kp+".algorithm", "must be one of HS256, HS384, HS512, RS256, RS384, RS512, PS256, PS384, PS512, ES256, ES384, ES512, EdDSA")
		}
		n := 0
		for _, s := range []string{key.Secret, key.PublicKey, key.PublicKeyFile} {
			if s != "" {
				n++
			}
		}
		if n != 1 {
			ck.add(kp, "exactly one of secret, public_key, public_key_file must be set")
		}
		hmac := strings.HasPrefix(key.Algorithm, "HS")
		if hmac && key.Secret == "" {
			ck.add(kp+".secret", "requires a secret for HS algorithms")
		}
		if !hmac && key.Secret != "" {
			ck.add(kp+".algorithm", "requires public_key or public_key_file")
		}
	}
	if jw := j.JWKS; jw != nil {
		jp := p + ".jwks"
		u, err := url.Parse(jw.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			ck.add(jp+".url", "must be an http or https URL")
		}
		requirePositive(ck, jp+".ttl", jw.TTL)
		requirePositive(ck, jp+".min_refresh_interval", jw.MinRefreshInterval)
		requirePositive(ck, jp+".timeout", jw.Timeout)
	}
	for _, claim := range slices.Sorted(maps.Keys(j.ForwardClaims)) {
		if j.ForwardClaims[claim] == "" {
			ck.add(p+".forward_claims."+claim, "must not be empty")
		}
	}
}

func checkAPIKeys(ck *checker, p string, a *APIKeyProvider) {
	if a.Header == "" {
		ck.add(p+".header", "is required")
	}
	if a.ConsumerHeader == "" {
		ck.add(p+".consumer_header", "is required")
	}
	if len(a.Keys) == 0 && a.KeyFile == "" {
		ck.add(p, "keys or key_file is required")
	}
	ids := map[string]bool{}
	for k := range a.Keys {
		key := &a.Keys[k]
		kp := fmt.Sprintf("%s.keys[%d]", p, k)
		switch {
		case key.ID == "":
			ck.add(kp+".id", "is required")
		case ids[key.ID]:
			ck.add(kp+".id", fmt.Sprintf("duplicate id %q", key.ID))
		default:
			ids[key.ID] = true
		}
		if !apiKeyHash.MatchString(key.Hash) {
			ck.add(kp+".hash", "must have the form sha256:<64 hex digits>")
		}
		if key.Consumer == "" {
			ck.add(kp+".consumer", "is required")
		}
	}
}

func checkMiddlewares(ck *checker, ms []Middleware) {
	names := map[string]bool{}
	for i := range ms {
		p := fmt.Sprintf("middlewares[%d]", i)
		checkName(ck, p+".name", ms[i].Name, names)
		if ms[i].Type == "" {
			ck.add(p+".type", "is required")
		}
		if hasNonStringKeys(ms[i].Params) {
			ck.add(p+".params", "keys must be strings")
		}
	}
}

func hasNonStringKeys(v any) bool {
	switch t := v.(type) {
	case map[any]any:
		return true
	case map[string]any:
		for _, x := range t {
			if hasNonStringKeys(x) {
				return true
			}
		}
	case []any:
		for _, x := range t {
			if hasNonStringKeys(x) {
				return true
			}
		}
	}
	return false
}

func checkRoutes(ck *checker, c *Config, kinds map[string]string) {
	upstreams := map[string]bool{}
	for _, u := range c.Upstreams {
		upstreams[u.Name] = true
	}
	mws := map[string]bool{}
	for _, m := range c.Middlewares {
		mws[m.Name] = true
	}
	names := map[string]bool{}
	seenKeys := map[string]string{}
	for i := range c.Routes {
		r := &c.Routes[i]
		p := fmt.Sprintf("routes[%d]", i)
		checkName(ck, p+".name", r.Name, names)
		switch {
		case r.Upstream == "":
			ck.add(p+".upstream", "is required")
		case !upstreams[r.Upstream]:
			ck.add(p+".upstream", fmt.Sprintf("unknown upstream %q", r.Upstream))
		}
		checkRouteMatch(ck, p, &r.Match)
		checkRewrite(ck, p, &r.Rewrite)
		requireNonNegative(ck, p+".timeout", r.Timeout)
		if r.Auth != nil {
			checkRouteAuth(ck, p+".auth", r.Auth, kinds)
		}
		if r.RateLimit != nil {
			checkRateLimit(ck, p+".rate_limit", r.RateLimit)
		}
		if r.Retry != nil {
			checkRetry(ck, p+".retry", r.Retry)
		}
		for j, m := range r.Middlewares {
			if !mws[m] {
				ck.add(fmt.Sprintf("%s.middlewares[%d]", p, j), fmt.Sprintf("unknown middleware %q", m))
			}
		}
		checkPipeline(ck, p+".pipeline", r.Pipeline, mws)
		key := routeKey(r)
		if earlier, ok := seenKeys[key]; ok {
			ck.add(p, fmt.Sprintf("conflicts with route %q", earlier))
		} else {
			seenKeys[key] = r.Name
		}
	}
}

func checkRouteMatch(ck *checker, p string, m *Match) {
	mp := p + ".match"
	if !slices.Contains([]string{"exact", "prefix", "pattern"}, m.PathType) {
		ck.add(mp+".path_type", "must be one of exact, prefix, pattern")
	} else if _, err := router.Compile(m.Path, router.PathType(m.PathType)); err != nil {
		ck.add(mp+".path", "invalid path: "+err.Error())
	}
	for j, h := range m.Hosts {
		if !validHost(h) {
			ck.add(fmt.Sprintf("%s.hosts[%d]", mp, j), fmt.Sprintf("invalid host %q", h))
		}
	}
	for j, meth := range m.Methods {
		if !isUpperToken(meth) {
			ck.add(fmt.Sprintf("%s.methods[%d]", mp, j), fmt.Sprintf("invalid method %q (must be an uppercase token)", meth))
		}
	}
}

func checkRewrite(ck *checker, p string, rw *Rewrite) {
	rp := p + ".rewrite"
	if rw.StripPrefix != "" && !strings.HasPrefix(rw.StripPrefix, "/") {
		ck.add(rp+".strip_prefix", `must start with "/"`)
	}
	rep := rw.ReplacePrefix
	if (rep.From == "") != (rep.To == "") {
		ck.add(rp+".replace_prefix", "from and to must both be set or both be empty")
	}
	if rep.From != "" && !strings.HasPrefix(rep.From, "/") {
		ck.add(rp+".replace_prefix.from", `must start with "/"`)
	}
	if rw.StripPrefix != "" && (rep.From != "" || rep.To != "") {
		ck.add(rp, "strip_prefix and replace_prefix are mutually exclusive")
	}
}

func checkRouteAuth(ck *checker, p string, a *RouteAuth, kinds map[string]string) {
	if !slices.Contains([]string{"none", "jwt", "apikey", "any"}, a.Mode) {
		ck.add(p+".mode", "must be one of none, jwt, apikey, any")
	}
	switch {
	case a.Mode == "none" && len(a.Providers) > 0:
		ck.add(p+".providers", "must be empty when mode is none")
	case a.Mode != "none" && len(a.Providers) == 0:
		ck.add(p+".providers", "at least one provider is required")
	}
	for j, name := range a.Providers {
		pp := fmt.Sprintf("%s.providers[%d]", p, j)
		kind, ok := kinds[name]
		switch {
		case !ok:
			ck.add(pp, fmt.Sprintf("unknown auth provider %q", name))
		case a.Mode == "jwt" && kind == "apikey":
			ck.add(pp, fmt.Sprintf("%q is not a jwt provider", name))
		case a.Mode == "apikey" && kind == "jwt":
			ck.add(pp, fmt.Sprintf("%q is not an apikey provider", name))
		}
	}
}

func checkRateLimit(ck *checker, p string, rl *RateLimit) {
	switch rl.Algorithm {
	case "token_bucket":
		if !(rl.Rate > 0) || math.IsInf(rl.Rate, 0) {
			ck.add(p+".rate", "must be a positive finite number")
		}
		requirePositive(ck, p+".per", rl.Per)
		if rl.Burst < 1 {
			ck.add(p+".burst", "must be >= 1")
		}
	case "sliding_window":
		if rl.Limit < 1 {
			ck.add(p+".limit", "must be >= 1")
		}
		requirePositive(ck, p+".window", rl.Window)
	default:
		ck.add(p+".algorithm", "must be one of token_bucket, sliding_window")
	}
	switch rl.Key.Source {
	case "ip", "consumer", "route":
	case "header":
		if rl.Key.Name == "" {
			ck.add(p+".key.name", "is required when key source is header")
		}
	default:
		ck.add(p+".key.source", "must be one of ip, consumer, header, route")
	}
	if rl.MaxKeys < 1 {
		ck.add(p+".max_keys", "must be >= 1")
	}
	requirePositive(ck, p+".idle_ttl", rl.IdleTTL)
}

func checkRetry(ck *checker, p string, rt *Retry) {
	if rt.Attempts < 1 || rt.Attempts > 10 {
		ck.add(p+".attempts", "must be between 1 and 10")
	}
	requirePositive(ck, p+".base_delay", rt.BaseDelay)
	if rt.MaxDelay < rt.BaseDelay {
		ck.add(p+".max_delay", "must be >= base_delay")
	}
	for j, s := range rt.OnStatuses {
		if s < 100 || s > 599 {
			ck.add(fmt.Sprintf("%s.on_statuses[%d]", p, j), "must be between 100 and 599")
		}
	}
	for j, m := range rt.Methods {
		if !isUpperToken(m) {
			ck.add(fmt.Sprintf("%s.methods[%d]", p, j), fmt.Sprintf("invalid method %q (must be an uppercase token)", m))
		}
	}
	if rt.IdempotencyKey.Enabled && rt.IdempotencyKey.Header == "" {
		ck.add(p+".idempotency_key.header", "is required when enabled")
	}
	b := rt.Budget
	if !(b.Ratio >= 0 && b.Ratio <= 1) || math.IsInf(b.Ratio, 0) {
		ck.add(p+".budget.ratio", "must be in [0, 1]")
	}
	if !(b.MinPerSecond >= 0) || math.IsInf(b.MinPerSecond, 0) {
		ck.add(p+".budget.min_per_second", "must be a finite number >= 0")
	}
	requirePositive(ck, p+".budget.window", b.Window)
	if rt.MaxBodyBytes < 0 {
		ck.add(p+".max_body_bytes", "must not be negative")
	}
}

func checkPipeline(ck *checker, p string, stages []string, mws map[string]bool) {
	seen := map[string]bool{}
	for j, s := range stages {
		sp := fmt.Sprintf("%s[%d]", p, j)
		if !slices.Contains(BuiltinStages, s) && !mws[s] {
			ck.add(sp, fmt.Sprintf("unknown pipeline stage %q", s))
		}
		if seen[s] {
			ck.add(sp, fmt.Sprintf("duplicate stage %q", s))
		}
		seen[s] = true
		if s == "retry" && j != len(stages)-1 {
			ck.add(sp, `"retry" must be the last stage`)
		}
	}
}

// routeKey identifies the match a route makes; two routes with the same key conflict.
func routeKey(r *Route) string {
	hosts := make([]string, len(r.Match.Hosts))
	for i, h := range r.Match.Hosts {
		hosts[i] = strings.ToLower(h)
	}
	slices.Sort(hosts)
	methods := slices.Clone(r.Match.Methods)
	slices.Sort(methods)
	headers := make([]string, 0, len(r.Match.Headers))
	for k, v := range r.Match.Headers {
		headers = append(headers, strings.ToLower(k)+"="+v)
	}
	slices.Sort(headers)
	return fmt.Sprintf("%s|%s|%s|%s|%s|%d",
		strings.Join(hosts, ","), r.Match.Path, r.Match.PathType,
		strings.Join(methods, ","), strings.Join(headers, ","), r.Priority)
}
