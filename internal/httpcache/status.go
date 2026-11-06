package httpcache

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Fwd values of a Cache-Status member (RFC 9211) that name why the request was forwarded to the origin.
const (
	FwdBypass   = "bypass"
	FwdMethod   = "method"
	FwdURIMiss  = "uri-miss"
	FwdVaryMiss = "vary-miss"
	FwdMiss     = "miss"
	FwdRequest  = "request"
	FwdStale    = "stale"
	FwdPartial  = "partial"
)

// Limits on a Cache-Status field: its size in bytes and its number of members.
const (
	MaxStatusBytes   = 64 << 10
	MaxStatusMembers = 64
	maxTTL           = 999999999999999
)

// Status is one member of a Cache-Status list (RFC 9211). Empty Fwd, Key and
// Detail mean the parameter is absent.
type Status struct {
	Name         string
	Hit          bool
	Fwd          string
	FwdStatus    int
	HasFwdStatus bool
	TTL          int64
	HasTTL       bool
	Stored       bool
	Collapsed    bool
	Key          string
	Detail       string
}

// String renders the member as an RFC 8941 item with its parameters in a fixed order.
func (s Status) String() string {
	var b strings.Builder
	if validToken(s.Name) {
		b.WriteString(s.Name)
	} else {
		writeSFString(&b, s.Name)
	}
	if s.Hit {
		b.WriteString("; hit")
	}
	if validToken(s.Fwd) {
		b.WriteString("; fwd=")
		b.WriteString(s.Fwd)
	}
	if s.HasFwdStatus && s.FwdStatus >= 100 && s.FwdStatus <= 599 {
		b.WriteString("; fwd-status=")
		b.WriteString(strconv.Itoa(s.FwdStatus))
	}
	if s.HasTTL {
		b.WriteString("; ttl=")
		b.WriteString(strconv.FormatInt(max(-maxTTL, min(maxTTL, s.TTL)), 10))
	}
	if s.Stored {
		b.WriteString("; stored")
	}
	if s.Collapsed {
		b.WriteString("; collapsed")
	}
	if s.Key != "" {
		b.WriteString("; key=")
		writeSFString(&b, s.Key)
	}
	if s.Detail != "" {
		b.WriteString("; detail=")
		writeSFString(&b, s.Detail)
	}
	return b.String()
}

// FormatStatus renders the members as one Cache-Status field value.
func FormatStatus(members ...Status) string {
	var b strings.Builder
	for i, m := range members {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(m.String())
	}
	return b.String()
}

// ErrMalformedStatus is wrapped by every error that ParseStatus returns.
var ErrMalformedStatus = errors.New("httpcache: malformed Cache-Status")

// ParseStatus parses a Cache-Status field value as a strict RFC 8941 list.
func ParseStatus(s string) ([]Status, error) {
	if len(s) > MaxStatusBytes {
		return nil, fmt.Errorf("%w: field exceeds %d bytes at offset %d", ErrMalformedStatus, MaxStatusBytes, MaxStatusBytes)
	}
	p := statusParser{s: s}
	p.skipSP()
	if p.end() {
		return nil, nil
	}
	var list []Status
	for {
		if len(list) == MaxStatusMembers {
			return nil, p.fail("too many members")
		}
		st, err := p.member()
		if err != nil {
			return nil, err
		}
		list = append(list, st)
		p.skipOWS()
		if p.end() {
			return list, nil
		}
		if p.s[p.pos] != ',' {
			return nil, p.fail("expected comma")
		}
		p.pos++
		p.skipOWS()
		if p.end() {
			return nil, p.fail("trailing comma")
		}
	}
}

type itemKind uint8

const (
	kindToken itemKind = iota + 1
	kindString
	kindInt
	kindDecimal
	kindBytes
	kindBool
)

type item struct {
	kind itemKind
	text string
	num  int64
	flag bool
}

type statusParser struct {
	s   string
	pos int
}

func (p *statusParser) fail(reason string) error {
	return fmt.Errorf("%w: %s at offset %d", ErrMalformedStatus, reason, p.pos)
}

func (p *statusParser) end() bool {
	return p.pos >= len(p.s)
}

func (p *statusParser) skipSP() {
	for !p.end() && p.s[p.pos] == ' ' {
		p.pos++
	}
}

func (p *statusParser) skipOWS() {
	for !p.end() && (p.s[p.pos] == ' ' || p.s[p.pos] == '\t') {
		p.pos++
	}
}

func (p *statusParser) member() (Status, error) {
	var st Status
	it, err := p.bareItem()
	if err != nil {
		return st, err
	}
	if it.kind != kindToken && it.kind != kindString {
		return st, p.fail("member must be a token or a string")
	}
	st.Name = it.text
	for !p.end() && p.s[p.pos] == ';' {
		p.pos++
		p.skipSP()
		key, err := p.key()
		if err != nil {
			return st, err
		}
		val := item{kind: kindBool, flag: true}
		if !p.end() && p.s[p.pos] == '=' {
			p.pos++
			val, err = p.bareItem()
			if err != nil {
				return st, err
			}
		}
		if err := p.apply(&st, key, val); err != nil {
			return st, err
		}
	}
	return st, nil
}

func (p *statusParser) key() (string, error) {
	if p.end() {
		return "", p.fail("expected parameter key")
	}
	c := p.s[p.pos]
	if !isLowerAlpha(c) && c != '*' {
		return "", p.fail("invalid parameter key")
	}
	start := p.pos
	p.pos++
	for !p.end() && isKeyChar(p.s[p.pos]) {
		p.pos++
	}
	return p.s[start:p.pos], nil
}

func (p *statusParser) apply(st *Status, key string, v item) error {
	switch key {
	case "hit", "stored", "collapsed":
		if v.kind != kindBool {
			return p.fail(key + " must be a boolean")
		}
		switch key {
		case "hit":
			st.Hit = v.flag
		case "stored":
			st.Stored = v.flag
		default:
			st.Collapsed = v.flag
		}
	case "fwd":
		if v.kind != kindToken {
			return p.fail("fwd must be a token")
		}
		st.Fwd = v.text
	case "fwd-status":
		if v.kind != kindInt || v.num < 100 || v.num > 599 {
			return p.fail("fwd-status must be an integer from 100 to 599")
		}
		st.FwdStatus = int(v.num)
		st.HasFwdStatus = true
	case "ttl":
		if v.kind != kindInt {
			return p.fail("ttl must be an integer")
		}
		st.TTL = v.num
		st.HasTTL = true
	case "key":
		if v.kind != kindString {
			return p.fail("key must be a string")
		}
		st.Key = v.text
	case "detail":
		if v.kind != kindString && v.kind != kindToken {
			return p.fail("detail must be a string or a token")
		}
		st.Detail = v.text
	}
	return nil
}

func (p *statusParser) bareItem() (item, error) {
	if p.end() {
		return item{}, p.fail("expected value")
	}
	c := p.s[p.pos]
	switch {
	case c == '"':
		return p.str()
	case c == '*' || isAlpha(c):
		return p.token(), nil
	case c == '-' || isDigit(c):
		return p.number()
	case c == ':':
		return p.bytes()
	case c == '?':
		return p.boolean()
	case c == '(':
		return item{}, p.fail("inner lists are not supported")
	}
	return item{}, p.fail("invalid value")
}

func (p *statusParser) token() item {
	start := p.pos
	p.pos++
	for !p.end() && isTokenChar(p.s[p.pos]) {
		p.pos++
	}
	return item{kind: kindToken, text: p.s[start:p.pos]}
}

func (p *statusParser) str() (item, error) {
	p.pos++
	var b strings.Builder
	for {
		if p.end() {
			return item{}, p.fail("unterminated string")
		}
		c := p.s[p.pos]
		p.pos++
		switch {
		case c == '"':
			return item{kind: kindString, text: b.String()}, nil
		case c == '\\':
			if p.end() {
				return item{}, p.fail("unterminated string")
			}
			n := p.s[p.pos]
			if n != '"' && n != '\\' {
				return item{}, p.fail("invalid escape in string")
			}
			b.WriteByte(n)
			p.pos++
		case c < 0x20 || c > 0x7e:
			return item{}, p.fail("invalid character in string")
		default:
			b.WriteByte(c)
		}
	}
}

func (p *statusParser) number() (item, error) {
	neg := p.s[p.pos] == '-'
	if neg {
		p.pos++
	}
	start := p.pos
	for !p.end() && isDigit(p.s[p.pos]) {
		p.pos++
	}
	intDigits := p.pos - start
	if intDigits == 0 {
		return item{}, p.fail("invalid number")
	}
	if !p.end() && p.s[p.pos] == '.' {
		if intDigits > 12 {
			return item{}, p.fail("decimal integer part exceeds 12 digits")
		}
		p.pos++
		fracStart := p.pos
		for !p.end() && isDigit(p.s[p.pos]) {
			p.pos++
		}
		if n := p.pos - fracStart; n == 0 || n > 3 {
			return item{}, p.fail("decimal fraction must have 1 to 3 digits")
		}
		return item{kind: kindDecimal}, nil
	}
	if intDigits > 15 {
		return item{}, p.fail("integer exceeds 15 digits")
	}
	n, err := strconv.ParseInt(p.s[start:p.pos], 10, 64)
	if err != nil {
		return item{}, p.fail("invalid integer")
	}
	if neg {
		n = -n
	}
	return item{kind: kindInt, num: n}, nil
}

func (p *statusParser) bytes() (item, error) {
	p.pos++
	for !p.end() && isBase64Char(p.s[p.pos]) {
		p.pos++
	}
	if p.end() || p.s[p.pos] != ':' {
		return item{}, p.fail("unterminated byte sequence")
	}
	p.pos++
	return item{kind: kindBytes}, nil
}

func (p *statusParser) boolean() (item, error) {
	if p.pos+1 >= len(p.s) {
		return item{}, p.fail("invalid boolean")
	}
	var flag bool
	switch p.s[p.pos+1] {
	case '0':
	case '1':
		flag = true
	default:
		return item{}, p.fail("invalid boolean")
	}
	p.pos += 2
	return item{kind: kindBool, flag: flag}, nil
}

func validToken(s string) bool {
	if s == "" || (!isAlpha(s[0]) && s[0] != '*') {
		return false
	}
	for i := 1; i < len(s); i++ {
		if !isTokenChar(s[i]) {
			return false
		}
	}
	return true
}

func writeSFString(b *strings.Builder, s string) {
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c < 0x20 || c > 0x7e:
			b.WriteByte('?')
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isLowerAlpha(c byte) bool {
	return c >= 'a' && c <= 'z'
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func isTokenChar(c byte) bool {
	return isAlpha(c) || isDigit(c) || strings.IndexByte("!#$%&'*+-.^_`|~:/", c) >= 0
}

func isKeyChar(c byte) bool {
	return isLowerAlpha(c) || isDigit(c) || c == '_' || c == '-' || c == '.' || c == '*'
}

func isBase64Char(c byte) bool {
	return isAlpha(c) || isDigit(c) || c == '+' || c == '/' || c == '='
}
