package config

import (
	"fmt"
	"strings"
)

var cacheShapeFields = []struct {
	name  string
	value func(r *Route) any
}{
	{"match.hosts", func(r *Route) any { return r.Match.Hosts }},
	{"match.path", func(r *Route) any { return r.Match.Path }},
	{"match.path_type", func(r *Route) any { return r.Match.PathType }},
	{"match.methods", func(r *Route) any { return r.Match.Methods }},
	{"upstream", func(r *Route) any { return r.Upstream }},
	{"preserve_host", func(r *Route) any { return r.PreserveHost }},
	{"rewrite", func(r *Route) any { return r.Rewrite }},
	{"request_headers", func(r *Route) any { return r.RequestHeaders }},
	{"response_headers", func(r *Route) any { return r.ResponseHeaders }},
	{"timeout", func(r *Route) any { return r.Timeout }},
	{"auth", func(r *Route) any { return r.Auth }},
	{"rate_limit", func(r *Route) any { return r.RateLimit }},
	{"cache", func(r *Route) any { return r.Cache }},
	{"circuit_breaker", func(r *Route) any { return r.CircuitBreaker }},
	{"retry", func(r *Route) any { return r.Retry }},
	{"middlewares", func(r *Route) any { return r.Middlewares }},
	{"pipeline", func(r *Route) any { return r.Pipeline }},
}

func checkCacheCollisions(ck *checker, c *Config) {
	for i := range c.Routes {
		ri := &c.Routes[i]
		if ri.Cache == nil || !ri.Cache.Enabled {
			continue
		}
		for j := i + 1; j < len(c.Routes); j++ {
			rj := &c.Routes[j]
			if rj.Cache == nil || !rj.Cache.Enabled {
				continue
			}
			if !routesMayCollide(ri, rj) || routeShape(*ri) == routeShape(*rj) {
				continue
			}
			field := firstDifferingField(ri, rj)
			if field == "" {
				continue
			}
			ck.add(fmt.Sprintf("routes[%d].cache", j), fmt.Sprintf(
				"route %q can serve the same cached URLs as route %q (routes[%d].cache) but differs in %s; such routes must be identical except name, match.headers and priority",
				rj.Name, ri.Name, i, field))
			ck.add(fmt.Sprintf("routes[%d].cache", i), fmt.Sprintf(
				"route %q can serve the same cached URLs as route %q (routes[%d].cache) but differs in %s; such routes must be identical except name, match.headers and priority",
				ri.Name, rj.Name, j, field))
		}
	}
}

func routesMayCollide(a, b *Route) bool {
	if len(a.Match.Headers) == 0 && len(b.Match.Headers) == 0 {
		return false
	}
	getA, headA := getHeadMethods(a.Match.Methods)
	getB, headB := getHeadMethods(b.Match.Methods)
	if !(getA && getB || headA && headB) {
		return false
	}
	return hostsMayOverlap(a.Match.Hosts, b.Match.Hosts) && pathsMayOverlap(&a.Match, &b.Match)
}

func getHeadMethods(m []string) (get, head bool) {
	if len(m) == 0 {
		return true, true
	}
	for _, method := range m {
		switch method {
		case "GET":
			get = true
		case "HEAD":
			head = true
		}
	}
	return get, head
}

func hostsMayOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return true
	}
	for _, x := range a {
		for _, y := range b {
			if hostPairMayOverlap(normalizeHost(x), normalizeHost(y)) {
				return true
			}
		}
	}
	return false
}

func normalizeHost(h string) string {
	return strings.TrimSuffix(strings.ToLower(h), ".")
}

func hostPairMayOverlap(x, y string) bool {
	xw, yw := strings.HasPrefix(x, "*."), strings.HasPrefix(y, "*.")
	switch {
	case xw && yw:
		return strings.HasSuffix(x[1:], y[1:]) || strings.HasSuffix(y[1:], x[1:])
	case xw:
		return len(y) > len(x)-1 && strings.HasSuffix(y, x[1:])
	case yw:
		return len(x) > len(y)-1 && strings.HasSuffix(x, y[1:])
	default:
		return x == y
	}
}

func pathsMayOverlap(a, b *Match) bool {
	la, pa := pathLiteral(a)
	lb, pb := pathLiteral(b)
	switch {
	case !pa && !pb:
		return la == lb
	case !pa:
		return strings.HasPrefix(la, lb)
	case !pb:
		return strings.HasPrefix(lb, la)
	default:
		return strings.HasPrefix(la, lb) || strings.HasPrefix(lb, la)
	}
}

func pathLiteral(m *Match) (literal string, prefix bool) {
	switch m.PathType {
	case "exact":
		return m.Path, false
	case "pattern":
		if i := strings.IndexAny(m.Path, ":*"); i >= 0 {
			return m.Path[:i], true
		}
		return m.Path, true
	default:
		return m.Path, true
	}
}

func routeShape(r Route) string {
	r.Name = ""
	r.Match.Headers = nil
	r.Priority = 0
	return FingerprintOf(r)
}

func firstDifferingField(a, b *Route) string {
	for _, f := range cacheShapeFields {
		if FingerprintOf(f.value(a)) != FingerprintOf(f.value(b)) {
			return f.name
		}
	}
	return ""
}
