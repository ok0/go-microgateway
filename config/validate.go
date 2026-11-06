package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ok0/go-microgateway/internal/reqpath"
	"github.com/ok0/go-microgateway/internal/router"
	"github.com/ok0/go-microgateway/store"
)

// Validate reloads the key files referenced by c, applies defaults to c, then checks it. It returns a
// *ValidationError or nil. Line numbers come from the document c was parsed from (Parse and LoadFile, kept by
// Clone): each error carries the line of its field or of the nearest enclosing field declared there. They are 0
// for configurations built in Go. Relative key file paths resolve against the working directory.
func (c *Config) Validate() error {
	errs := loadKeyMaterial(c, "")
	c.applyDefaults()
	errs = append(errs, c.validate()...)
	if len(errs) == 0 {
		return nil
	}
	for i := range errs {
		if errs[i].Line == 0 && errs[i].Path != "" {
			errs[i].Line = c.lines.lineFor(errs[i].Path)
		}
	}
	return newValidationError(errs)
}

var (
	lbAlgorithms  = []string{"p2c", "round_robin", "weighted_round_robin", "random", "consistent_hash"}
	apiKeyHash    = regexp.MustCompile(`^sha256:[0-9a-fA-F]{64}$`)
	statusPattern = regexp.MustCompile(`^\d{3}(-\d{3})?$`)
)

// msgCacheBeforeAuth is reported at the first "cache" entry of a pipeline that lists "auth" after it.
const msgCacheBeforeAuth = `stage "cache" must come after "auth": cache hits would be served without authentication`

// canonicalPathMsg is reported at a rewrite prefix that the request path canonicalization would change or reject.
const canonicalPathMsg = "must be a canonical path: no \".\" or \"..\" segment, no empty segment, no backslash, no encoded \"/\" or \"\\\", no control byte, and escapes only for bytes that need them, in upper case"

type checker struct {
	errs []FieldError
}

func (ck *checker) add(path, msg string) {
	ck.errs = append(ck.errs, FieldError{Path: path, Msg: msg})
}

// reported reports whether an error was already added at path or at a path nested under it.
func (ck *checker) reported(path string) bool {
	for _, fe := range ck.errs {
		if fe.Path == path || strings.HasPrefix(fe.Path, path+".") || strings.HasPrefix(fe.Path, path+"[") {
			return true
		}
	}
	return false
}

// validate collects every problem in c without stopping at the first one. It expects defaults to be applied.
func (c *Config) validate() []FieldError {
	ck := &checker{}
	if c.Version != 1 {
		ck.add("version", fmt.Sprintf("unsupported version %d (must be 1)", c.Version))
	}
	checkGlobal(ck, &c.Global)
	checkListeners(ck, c)
	checkAdminAuth(ck, &c.Admin.Auth)
	checkUpstreams(ck, c.Upstreams)
	kinds := checkAuth(ck, &c.Auth)
	checkIdentityHeaders(ck, c)
	checkMiddlewares(ck, c.Middlewares)
	names := checkStores(ck, c.Stores)
	checkCache(ck, &c.Cache, names)
	checkRoutes(ck, c, kinds, names)
	checkCacheCollisions(ck, c)
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
			if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' {
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
		if (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
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
	raw := h.URL.Reveal()
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		if RedactURL(raw) != raw {
			ck.add(p+".url", "invalid URL")
			break
		}
		detail := err
		var ue *url.Error
		if errors.As(err, &ue) {
			detail = ue.Err
		}
		ck.add(p+".url", fmt.Sprintf("invalid URL: %v", detail))
	case u.Scheme != "http" && u.Scheme != "https":
		ck.add(p+".url", "scheme must be http or https")
	case u.Host == "":
		ck.add(p+".url", "missing host")
	}
	if h.Weight != nil && *h.Weight < 0 {
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
	if d.RefreshInterval < Duration(time.Second) {
		ck.add(p+".refresh_interval", "must be at least 1s")
	}
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
		if v := ph.MaxEjectionPercent; v != nil && (*v < 0 || *v > 100) {
			ck.add(pp+".max_ejection_percent", "must be between 0 and 100")
		}
	}
	if ah := u.ActiveHealth; ah != nil {
		ap := p + ".active_health"
		if !strings.HasPrefix(ah.Path, "/") {
			ck.add(ap+".path", `must start with "/"`)
		}
		requirePositive(ck, ap+".interval", ah.Interval)
		requirePositive(ck, ap+".timeout", ah.Timeout)
		if ah.Interval > 0 && ah.Timeout > 0 && ah.Timeout >= ah.Interval {
			ck.add(ap+".timeout", "must be less than interval")
		}
		if j := ah.Jitter; j != nil && (!(*j >= 0 && *j < 1) || math.IsInf(*j, 0)) {
			ck.add(ap+".jitter", "must be in [0, 1)")
		}
		if *ah.HealthyThreshold < 1 {
			ck.add(ap+".healthy_threshold", "must be >= 1")
		}
		if *ah.UnhealthyThreshold < 1 {
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
		buckets := *cb.Buckets
		switch {
		case buckets < 1:
			ck.add(cp+".buckets", "must be >= 1")
		case buckets > 1000:
			ck.add(cp+".buckets", "must be <= 1000")
		case cb.Window > 0 && time.Duration(cb.Window)/time.Duration(buckets) < time.Millisecond:
			ck.add(cp+".buckets", "window / buckets must be at least 1ms")
		}
		if *cb.MinRequests < 1 {
			ck.add(cp+".min_requests", "must be >= 1")
		}
		if f := *cb.FailureRatio; !(f > 0 && f <= 1) || math.IsInf(f, 0) {
			ck.add(cp+".failure_ratio", "must be in (0, 1]")
		}
		if v := cb.ConsecutiveFailures; v != nil && *v < 0 {
			ck.add(cp+".consecutive_failures", "must be >= 0")
		}
		requirePositive(ck, cp+".open_timeout", cb.OpenTimeout)
		if *cb.HalfOpenProbes < 1 {
			ck.add(cp+".half_open_probes", "must be >= 1")
		}
		if *cb.SuccessThreshold < 1 {
			ck.add(cp+".success_threshold", "must be >= 1")
		}
		if *cb.HalfOpenProbes < *cb.SuccessThreshold {
			ck.add(cp+".half_open_probes", "must be >= success_threshold")
		}
	}
}

func checkMiddlewares(ck *checker, ms []Middleware) {
	names := map[string]bool{}
	for i := range ms {
		p := fmt.Sprintf("middlewares[%d]", i)
		checkName(ck, p+".name", ms[i].Name, names)
		if slices.Contains(BuiltinStages, ms[i].Name) {
			ck.add(p+".name", fmt.Sprintf("%q is reserved for a built-in stage", ms[i].Name))
		}
		if ms[i].Type == "" {
			ck.add(p+".type", "is required")
		}
		if hasNonStringKeys(ms[i].Params.Reveal()) {
			ck.add(p+".params", "keys must be strings")
		} else if slices.Contains(BuiltinMiddlewareTypes, ms[i].Type) {
			checkBuiltinParams(ck, p, &ms[i])
		}
	}
}

func checkBuiltinParams(ck *checker, p string, m *Middleware) {
	_, err := m.BuiltinParams()
	if err == nil {
		return
	}
	var pe *ParamsError
	if !errors.As(err, &pe) {
		ck.add(p+".params", err.Error())
		return
	}
	for _, fe := range pe.Errors {
		suffix := ""
		switch {
		case fe.Path == "":
		case strings.HasPrefix(fe.Path, "["):
			suffix = fe.Path
		default:
			suffix = "." + fe.Path
		}
		ck.add(p+".params"+suffix, fe.Msg)
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

func checkRoutes(ck *checker, c *Config, kinds map[string]string, storeNames map[string]bool) {
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
			checkRateLimit(ck, p+".rate_limit", r)
			checkRateLimitStore(ck, p+".rate_limit", r.RateLimit, storeNames)
		}
		if r.Cache != nil {
			checkRouteCache(ck, p+".cache", r.Cache)
		}
		if r.Retry != nil {
			checkRetry(ck, p+".retry", r.Retry)
		}
		usedMws := map[string]bool{}
		for j, m := range r.Middlewares {
			if !mws[m] {
				ck.add(fmt.Sprintf("%s.middlewares[%d]", p, j), fmt.Sprintf("unknown middleware %q", m))
			}
			if usedMws[m] {
				ck.add(fmt.Sprintf("%s.middlewares[%d]", p, j), fmt.Sprintf("duplicate middleware %q", m))
			}
			usedMws[m] = true
		}
		checkPipeline(ck, p+".pipeline", r.Pipeline, mws)
		checkPipelineOrder(ck, p+".pipeline", r.Pipeline)
		if r.Auth != nil && r.Auth.Mode != "none" && len(r.Pipeline) > 0 && !slices.Contains(r.Pipeline, "auth") {
			ck.add(p+".pipeline", `must include "auth" when auth mode is not none`)
		}
		if r.RateLimit != nil && len(r.Pipeline) > 0 && !slices.Contains(r.Pipeline, "rate_limit") {
			ck.add(p+".pipeline", `must include "rate_limit" because the route has a rate_limit block`)
		}
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
	if rw.StripPrefix != "" && strings.HasPrefix(rw.StripPrefix, "/") && !reqpath.IsCanonical(rw.StripPrefix) {
		ck.add(rp+".strip_prefix", canonicalPathMsg)
	}
	rep := rw.ReplacePrefix
	if (rep.From == "") != (rep.To == "") {
		ck.add(rp+".replace_prefix", "from and to must both be set or both be empty")
	}
	if rep.From != "" && !strings.HasPrefix(rep.From, "/") {
		ck.add(rp+".replace_prefix.from", `must start with "/"`)
	}
	if rep.From != "" && strings.HasPrefix(rep.From, "/") && !reqpath.IsCanonical(rep.From) {
		ck.add(rp+".replace_prefix.from", canonicalPathMsg)
	}
	if rep.To != "" && !reqpath.IsCanonical(rep.To) {
		ck.add(rp+".replace_prefix.to", canonicalPathMsg)
	}
	if rw.StripPrefix != "" && (rep.From != "" || rep.To != "") {
		ck.add(rp, "strip_prefix and replace_prefix are mutually exclusive")
	}
}

func checkRateLimit(ck *checker, p string, r *Route) {
	rl := r.RateLimit
	start := len(ck.errs)
	ok := true
	switch rl.Algorithm {
	case "token_bucket":
		if rl.Rate == nil {
			ck.add(p+".rate", "is required for token_bucket")
			ok = false
		} else if *rl.Rate < 1 {
			ck.add(p+".rate", "must be >= 1")
			ok = false
		}
		if rl.Per != nil && *rl.Per <= 0 {
			ck.add(p+".per", "must be positive")
			ok = false
		}
		if rl.Burst != nil && *rl.Burst < 1 {
			ck.add(p+".burst", "must be >= 1")
			ok = false
		}
		if rl.Limit != nil {
			ck.add(p+".limit", "is only valid with algorithm sliding_window")
		}
		if rl.Window != nil {
			ck.add(p+".window", "is only valid with algorithm sliding_window")
		}
		if ok {
			t := int64(rl.Per.Std()) / *rl.Rate
			if t < int64(store.MinInterval) {
				ck.add(p+".rate", "per/rate must be at least 1µs")
			} else if *rl.Burst > int64(store.MaxSpan)/t {
				ck.add(p+".burst", "burst × per/rate must not exceed 24h")
			}
		}
	case "sliding_window":
		if rl.Limit == nil {
			ck.add(p+".limit", "is required for sliding_window")
			ok = false
		} else if *rl.Limit < 1 {
			ck.add(p+".limit", "must be >= 1")
			ok = false
		}
		if rl.Window != nil && *rl.Window <= 0 {
			ck.add(p+".window", "must be positive")
			ok = false
		}
		if rl.Rate != nil {
			ck.add(p+".rate", "is only valid with algorithm token_bucket")
		}
		if rl.Per != nil {
			ck.add(p+".per", "is only valid with algorithm token_bucket")
		}
		if rl.Burst != nil {
			ck.add(p+".burst", "is only valid with algorithm token_bucket")
		}
		if ok {
			if rl.Window.Std() > store.MaxSpan {
				ck.add(p+".window", "must not exceed 24h")
			} else if int64(*rl.Window) / *rl.Limit < int64(store.MinInterval) {
				ck.add(p+".limit", "window/limit must be at least 1µs")
			}
		}
	default:
		ck.add(p+".algorithm", "must be one of token_bucket, sliding_window")
	}
	if len(ck.errs) == start {
		if l, valid := rl.StoreLimit(); valid {
			if err := l.Validate(); err != nil {
				ck.add(p, err.Error())
			}
		}
	}
	switch rl.Key.Source {
	case "ip", "consumer", "route":
		if rl.Key.Name != "" {
			ck.add(p+".key.name", "is only valid with key source header")
		}
	case "header":
		if rl.Key.Name == "" {
			ck.add(p+".key.name", "is required when key source is header")
		}
	default:
		ck.add(p+".key.source", "must be one of ip, consumer, header, route")
	}
	if rl.Key.Source == "consumer" && (r.Auth == nil || r.Auth.Mode == "none") {
		ck.add(p+".key.source", "consumer requires route authentication (auth.mode is none, so every request would fall back to the client IP)")
	}
	if *rl.MaxKeys < 1 {
		ck.add(p+".max_keys", "must be >= 1")
	}
}

func checkRateLimitStore(ck *checker, p string, rl *RateLimit, names map[string]bool) {
	if rl.Store == "" {
		if rl.OnStoreError != "" {
			ck.add(p+".on_store_error", "is only valid with store")
		}
		if rl.StoreTimeout != nil {
			ck.add(p+".store_timeout", "is only valid with store")
		}
		return
	}
	if !names[rl.Store] {
		ck.add(p+".store", fmt.Sprintf("store %q is not defined in stores", rl.Store))
	}
	if !slices.Contains([]string{"local", "allow", "deny"}, rl.OnStoreError) {
		ck.add(p+".on_store_error", "must be one of local, allow, deny")
	}
	if rl.StoreTimeout != nil && *rl.StoreTimeout <= 0 {
		ck.add(p+".store_timeout", "must be positive")
	}
}

func checkRetry(ck *checker, p string, rt *Retry) {
	if a := *rt.Attempts; a < 1 || a > 10 {
		ck.add(p+".attempts", "must be between 1 and 10")
	}
	requirePositive(ck, p+".base_delay", rt.BaseDelay)
	if rt.MaxDelay < rt.BaseDelay {
		ck.add(p+".max_delay", "must be >= base_delay")
	}
	for j, s := range rt.OnStatuses {
		if s < 400 || s > 599 {
			ck.add(fmt.Sprintf("%s.on_statuses[%d]", p, j), "must be between 400 and 599")
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
	if v := b.Ratio; v != nil && (!(*v >= 0 && *v <= 1) || math.IsInf(*v, 0)) {
		ck.add(p+".budget.ratio", "must be in [0, 1]")
	}
	if v := b.MinPerSecond; v != nil && (!(*v >= 0) || math.IsInf(*v, 0)) {
		ck.add(p+".budget.min_per_second", "must be a finite number >= 0")
	}
	requirePositive(ck, p+".budget.window", b.Window)
	if v := rt.MaxBodyBytes; v != nil && *v < 0 {
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
	if ci := slices.Index(stages, "cache"); ci >= 0 {
		for _, later := range []string{"circuit_breaker", "retry"} {
			if i := slices.Index(stages, later); i >= 0 && i < ci {
				ck.add(p, `"cache" must come before "circuit_breaker" and "retry"`)
				break
			}
		}
	}
}

// checkPipelineOrder rejects a pipeline at path p that lists "cache" before "auth", since the cache stage would
// then answer hits before authentication runs. The error goes on the first "cache" entry unless that entry already
// has an error of its own. A pipeline without "auth" or without "cache" is accepted.
func checkPipelineOrder(ck *checker, p string, stages []string) {
	ci := slices.Index(stages, "cache")
	if ci < 0 || !slices.Contains(stages[ci+1:], "auth") {
		return
	}
	sp := fmt.Sprintf("%s[%d]", p, ci)
	if ck.reported(sp) {
		return
	}
	ck.add(sp, msgCacheBeforeAuth)
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
		headers = append(headers, strings.ToLower(k)+"="+v.Reveal())
	}
	slices.Sort(headers)
	return fmt.Sprintf("%s|%s|%s|%s|%s|%d",
		strings.Join(hosts, ","), r.Match.Path, r.Match.PathType,
		strings.Join(methods, ","), strings.Join(headers, ","), r.Priority)
}
