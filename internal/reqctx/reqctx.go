// Package reqctx carries request-scoped values in a request context and derives the client address.
package reqctx

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync/atomic"
)

type ctxKey int

const (
	keyRequestID ctxKey = iota
	keyRoute
	keyParams
	keyClientIP
	keyIdentity
	keyInfo
	keyExchange
)

// Param is a single named path parameter captured by a route pattern.
type Param struct {
	Name  string
	Value string
}

// Identity describes the authenticated caller of a request.
type Identity struct {
	Provider string
	Consumer string
	Subject  string
	Claims   map[string]any
	Metadata map[string]string
}

// Info is filled in by inner request stages and read by the outer observation stage once the request completes. Only the goroutine serving the request may write it.
type Info struct {
	Route    string
	Upstream string
	// Error is the gateway error code of a request rejected before routing; empty otherwise.
	Error string
}

// WithIdentity returns a context carrying the caller identity id.
func WithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, keyIdentity, id)
}

// IdentityFrom returns the caller identity stored in ctx and whether one is present.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	v, ok := ctx.Value(keyIdentity).(Identity)
	return v, ok
}

// WithInfo returns a context carrying the mutable request record in.
func WithInfo(ctx context.Context, in *Info) context.Context {
	return context.WithValue(ctx, keyInfo, in)
}

// InfoFrom returns the request record stored in ctx, or nil if absent.
func InfoFrom(ctx context.Context) *Info {
	v, _ := ctx.Value(keyInfo).(*Info)
	return v
}

// Exchange records whether an upstream response was received for the request and whether the last forwarded upstream response body was read to its end.
type Exchange struct {
	responded atomic.Bool
	done      atomic.Bool
}

// WithExchange returns a context carrying the upstream exchange record e.
func WithExchange(ctx context.Context, e *Exchange) context.Context {
	return context.WithValue(ctx, keyExchange, e)
}

// ExchangeFrom returns the upstream exchange record stored in ctx, or nil if absent.
func ExchangeFrom(ctx context.Context) *Exchange {
	v, _ := ctx.Value(keyExchange).(*Exchange)
	return v
}

// Begin clears the completion flag before an upstream response body is forwarded.
func (e *Exchange) Begin() {
	e.responded.Store(true)
	e.done.Store(false)
}

// Responded reports whether an upstream response was received for the request (Begin was called), as opposed to an error produced by the gateway.
func (e *Exchange) Responded() bool {
	return e.responded.Load()
}

// Finish marks the forwarded upstream response body as read to its end.
func (e *Exchange) Finish() {
	e.done.Store(true)
}

// Complete reports whether the last forwarded upstream response body was read to its end.
func (e *Exchange) Complete() bool {
	return e.done.Load()
}

// WithRequestID returns a context carrying the request ID id.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, keyRequestID, id)
}

// RequestID returns the request ID stored in ctx, or "" if absent.
func RequestID(ctx context.Context) string {
	v, _ := ctx.Value(keyRequestID).(string)
	return v
}

// WithRoute returns a context carrying the matched route name.
func WithRoute(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, keyRoute, name)
}

// Route returns the matched route name stored in ctx, or "" if absent.
func Route(ctx context.Context) string {
	v, _ := ctx.Value(keyRoute).(string)
	return v
}

// WithParams returns a context carrying the path parameters p.
func WithParams(ctx context.Context, p []Param) context.Context {
	return context.WithValue(ctx, keyParams, p)
}

// Params returns the path parameters stored in ctx, or nil if absent.
func Params(ctx context.Context) []Param {
	v, _ := ctx.Value(keyParams).([]Param)
	return v
}

// ParamValue returns the value of the path parameter name, or "" if absent.
func ParamValue(ctx context.Context, name string) string {
	for _, p := range Params(ctx) {
		if p.Name == name {
			return p.Value
		}
	}
	return ""
}

// WithClientIP returns a context carrying the derived client address ip.
func WithClientIP(ctx context.Context, ip netip.Addr) context.Context {
	return context.WithValue(ctx, keyClientIP, ip)
}

// ClientIP returns the client address stored in ctx, or the zero Addr if absent.
func ClientIP(ctx context.Context) netip.Addr {
	v, _ := ctx.Value(keyClientIP).(netip.Addr)
	return v
}

// TrustedProxies is a set of address prefixes whose peers are trusted proxies.
type TrustedProxies struct {
	prefixes []netip.Prefix
}

// ParseTrusted builds a TrustedProxies from CIDR prefixes or single addresses.
func ParseTrusted(list []string) (*TrustedProxies, error) {
	t := &TrustedProxies{}
	for _, s := range list {
		if p, err := netip.ParsePrefix(s); err == nil {
			t.prefixes = append(t.prefixes, unmapPrefix(p))
			continue
		}
		a, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy %q", s)
		}
		a = a.Unmap()
		t.prefixes = append(t.prefixes, netip.PrefixFrom(a, a.BitLen()))
	}
	return t, nil
}

func unmapPrefix(p netip.Prefix) netip.Prefix {
	a := p.Addr()
	if a.Is4In6() && p.Bits() >= 96 {
		return netip.PrefixFrom(a.Unmap(), p.Bits()-96).Masked()
	}
	return p.Masked()
}

// Contains reports whether a is inside one of the trusted prefixes.
func (t *TrustedProxies) Contains(a netip.Addr) bool {
	if t == nil || !a.IsValid() {
		return false
	}
	a = a.Unmap()
	for _, p := range t.prefixes {
		if p.Contains(a) {
			return true
		}
	}
	return false
}

// PeerAddr returns the address of the direct peer of r, or the zero Addr if it cannot be parsed.
func PeerAddr(r *http.Request) netip.Addr {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	a, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}
	}
	return a.Unmap()
}

// ClientIP derives the client address of r, following X-Forwarded-For only through trusted proxies.
func (t *TrustedProxies) ClientIP(r *http.Request) netip.Addr {
	peer := PeerAddr(r)
	if !t.Contains(peer) {
		return peer
	}
	var hops []string
	for _, v := range r.Header.Values("X-Forwarded-For") {
		for _, h := range strings.Split(v, ",") {
			hops = append(hops, strings.TrimSpace(h))
		}
	}
	last := peer
	for i := len(hops) - 1; i >= 0; i-- {
		a, err := netip.ParseAddr(hops[i])
		if err != nil {
			return last
		}
		a = a.Unmap()
		if !t.Contains(a) {
			return a
		}
		last = a
	}
	return last
}
