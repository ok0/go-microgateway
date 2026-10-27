package router

import (
	"cmp"
	"fmt"
	"net/http"
	"net/textproto"
	"slices"
	"strings"

	"github.com/ok0/lychgate/internal/reqctx"
)

// Router matches requests to routes by host, path, method and headers.
//
// Precedence is deterministic: exact host > longest wildcard host > any host; then exact path > pattern (more literal segments, then fewer params) > prefix (longest); then more constraints (methods/headers) first; then higher `priority`; then config order. A method or header mismatch falls through to the next candidate. Host matching ignores port and case.
//
// Additional rules:
//   - constraint count = (1 if methods set) + number of header matchers
//   - glob counts as a param
//   - a wildcard `*.example.com` matches one or more labels before `.example.com` and not `example.com` itself
//   - exact paths compare the request path verbatim
//   - prefix matches on segment boundaries; a trailing slash in a prefix is ignored
type Router struct {
	exact map[string]*bucket
	wild  []wildBucket
	any   *bucket
}

type Matcher struct {
	Hosts    []string
	Path     string
	PathType PathType
	Methods  []string
	Headers  map[string]string
	Priority int
	Order    int
}

type Entry struct {
	Matcher
	Value any
}

type Params []reqctx.Param

func (p Params) Get(name string) string {
	for _, x := range p {
		if x.Name == name {
			return x.Value
		}
	}
	return ""
}

type route struct {
	value       any
	pat         *Pattern
	methods     []string
	headers     [][2]string
	constraints int
	priority    int
	order       int
	literals    int
	params      int
}

type node struct {
	children   map[string]*node
	param      *node
	patternEnd []*route
	globEnd    []*route
	prefixEnd  []*route
}

type bucket struct {
	exact    map[string][]*route
	patterns *node
	prefixes *node
}

type wildBucket struct {
	suffix string
	b      *bucket
}

func newBucket() *bucket {
	return &bucket{exact: make(map[string][]*route), patterns: &node{}, prefixes: &node{}}
}

func New(entries []Entry) (*Router, error) {
	rt := &Router{exact: make(map[string]*bucket)}
	wildByKey := make(map[string]*bucket)
	for _, e := range entries {
		pat, err := Compile(e.Path, e.PathType)
		if err != nil {
			return nil, fmt.Errorf("route %d: %w", e.Order, err)
		}
		r := &route{
			value:       e.Value,
			pat:         pat,
			methods:     e.Methods,
			constraints: btoi(len(e.Methods) > 0) + len(e.Headers),
			priority:    e.Priority,
			order:       e.Order,
		}
		for k, v := range e.Headers {
			r.headers = append(r.headers, [2]string{textproto.CanonicalMIMEHeaderKey(k), v})
		}
		slices.SortFunc(r.headers, func(a, b [2]string) int {
			return strings.Compare(a[0], b[0])
		})
		for _, s := range pat.Segments {
			if s.Kind == SegLiteral {
				r.literals++
			} else {
				r.params++
			}
		}

		if len(e.Hosts) == 0 {
			if rt.any == nil {
				rt.any = newBucket()
			}
			rt.any.insert(r)
		}
		for _, h := range e.Hosts {
			switch {
			case h == "" || h == "*" || h == "*.":
				return nil, fmt.Errorf("invalid host %q", h)
			case strings.HasPrefix(h, "*."):
				name := strings.ToLower(strings.TrimSuffix(h[2:], "."))
				if name == "" {
					return nil, fmt.Errorf("invalid host %q", h)
				}
				key := "." + name
				b := wildByKey[key]
				if b == nil {
					b = newBucket()
					wildByKey[key] = b
				}
				b.insert(r)
			default:
				key := strings.ToLower(strings.TrimSuffix(h, "."))
				b := rt.exact[key]
				if b == nil {
					b = newBucket()
					rt.exact[key] = b
				}
				b.insert(r)
			}
		}
	}

	for key, b := range wildByKey {
		rt.wild = append(rt.wild, wildBucket{suffix: key, b: b})
	}
	slices.SortFunc(rt.wild, func(a, b wildBucket) int {
		if c := cmp.Compare(len(b.suffix), len(a.suffix)); c != 0 {
			return c
		}
		return strings.Compare(a.suffix, b.suffix)
	})

	for _, b := range rt.exact {
		sortBucket(b)
	}
	for _, w := range rt.wild {
		sortBucket(w.b)
	}
	if rt.any != nil {
		sortBucket(rt.any)
	}
	return rt, nil
}

func (b *bucket) insert(r *route) {
	switch r.pat.Type {
	case PathExact:
		b.exact[r.pat.Raw] = append(b.exact[r.pat.Raw], r)
	case PathPrefix:
		n := b.prefixes
		for _, s := range r.pat.Segments {
			n = n.child(s.Value)
		}
		n.prefixEnd = append(n.prefixEnd, r)
	case PathPattern:
		n := b.patterns
		for _, s := range r.pat.Segments {
			switch s.Kind {
			case SegLiteral:
				n = n.child(s.Value)
			case SegParam:
				n = n.paramChild()
			case SegGlob:
				n.globEnd = append(n.globEnd, r)
				return
			}
		}
		n.patternEnd = append(n.patternEnd, r)
	}
}

func (n *node) child(seg string) *node {
	if n.children == nil {
		n.children = make(map[string]*node)
	}
	c := n.children[seg]
	if c == nil {
		c = &node{}
		n.children[seg] = c
	}
	return c
}

func (n *node) paramChild() *node {
	if n.param == nil {
		n.param = &node{}
	}
	return n.param
}

func sortBucket(b *bucket) {
	for _, list := range b.exact {
		slices.SortStableFunc(list, cmpRoute)
	}
	sortNode(b.patterns)
	sortNode(b.prefixes)
}

func sortNode(n *node) {
	if n == nil {
		return
	}
	slices.SortStableFunc(n.patternEnd, cmpRoute)
	slices.SortStableFunc(n.globEnd, cmpRoute)
	slices.SortStableFunc(n.prefixEnd, cmpRoute)
	for _, c := range n.children {
		sortNode(c)
	}
	sortNode(n.param)
}

func cmpRoute(a, b *route) int {
	if c := cmp.Compare(b.constraints, a.constraints); c != 0 {
		return c
	}
	if c := cmp.Compare(b.priority, a.priority); c != 0 {
		return c
	}
	return cmp.Compare(a.order, b.order)
}

func rankPattern(a, b *route) int {
	if c := cmp.Compare(b.literals, a.literals); c != 0 {
		return c
	}
	if c := cmp.Compare(a.params, b.params); c != 0 {
		return c
	}
	return cmpRoute(a, b)
}

func (rt *Router) Lookup(r *http.Request) (value any, params Params, ok bool) {
	if r.URL == nil {
		return nil, nil, false
	}
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		return nil, nil, false
	}
	host := normalizeHost(r.Host)

	if b := rt.exact[host]; b != nil {
		if v, p, found := b.lookup(path, r); found {
			return v, p, true
		}
	}
	for _, w := range rt.wild {
		if len(host) > len(w.suffix) && strings.HasSuffix(host, w.suffix) {
			if v, p, found := w.b.lookup(path, r); found {
				return v, p, true
			}
		}
	}
	if rt.any != nil {
		return rt.any.lookup(path, r)
	}
	return nil, nil, false
}

func normalizeHost(h string) string {
	if strings.HasPrefix(h, "[") {
		if i := strings.IndexByte(h, ']'); i >= 0 {
			h = h[:i+1]
		}
	} else if strings.Count(h, ":") == 1 {
		h, _, _ = strings.Cut(h, ":")
	}
	return strings.TrimSuffix(strings.ToLower(h), ".")
}

func (b *bucket) lookup(path string, r *http.Request) (any, Params, bool) {
	for _, c := range b.exact[path] {
		if c.accepts(r) {
			return c.value, nil, true
		}
	}

	if best := walk(b.patterns, path[1:], path == "/", r, nil); best != nil {
		return best.value, extract(best, path), true
	}

	var best *route
	n := b.prefixes
	rest, done := path[1:], path == "/"
	for {
		if c := firstAccepting(n.prefixEnd, r); c != nil {
			best = c
		}
		if done {
			break
		}
		var seg string
		seg, rest, done = nextSeg(rest)
		n = n.children[seg]
		if n == nil {
			break
		}
	}
	if best == nil {
		return nil, nil, false
	}
	return best.value, nil, true
}

func walk(n *node, rest string, done bool, r *http.Request, best *route) *route {
	best = takeBetter(n.globEnd, r, best)
	if done {
		return takeBetter(n.patternEnd, r, best)
	}
	seg, next, last := nextSeg(rest)
	if c := n.children[seg]; c != nil {
		best = walk(c, next, last, r, best)
	}
	if seg != "" && n.param != nil {
		best = walk(n.param, next, last, r, best)
	}
	return best
}

func takeBetter(list []*route, r *http.Request, best *route) *route {
	c := firstAccepting(list, r)
	if c == nil {
		return best
	}
	if best == nil || rankPattern(c, best) < 0 {
		return c
	}
	return best
}

func firstAccepting(list []*route, r *http.Request) *route {
	for _, c := range list {
		if c.accepts(r) {
			return c
		}
	}
	return nil
}

func nextSeg(rest string) (seg, next string, last bool) {
	i := strings.IndexByte(rest, '/')
	if i < 0 {
		return rest, "", true
	}
	return rest[:i], rest[i+1:], false
}

func (c *route) accepts(r *http.Request) bool {
	if len(c.methods) > 0 && !slices.Contains(c.methods, r.Method) {
		return false
	}
	for _, h := range c.headers {
		if r.Header.Get(h[0]) != h[1] {
			return false
		}
	}
	return true
}

func extract(rt *route, path string) Params {
	out := make(Params, 0, rt.params)
	rest := path[1:]
	for _, s := range rt.pat.Segments {
		if s.Kind == SegGlob {
			out = append(out, reqctx.Param{Name: s.Value, Value: rest})
			break
		}
		var seg string
		seg, rest, _ = nextSeg(rest)
		if s.Kind == SegParam {
			out = append(out, reqctx.Param{Name: s.Value, Value: seg})
		}
	}
	return out
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
