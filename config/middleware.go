package config

import (
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/ok0/go-microgateway/internal/yamlx"
)

// BuiltinMiddlewareTypes lists the middleware types implemented by the gateway itself.
var BuiltinMiddlewareTypes = []string{"cors", "headers", "request_size_limit"}

// CORSParams configures the cors middleware type.
type CORSParams struct {
	// AllowedOrigins lists exact origins or "*".
	AllowedOrigins []string `yaml:"allowed_origins" json:"allowed_origins"`
	// AllowedMethods lists the methods allowed for cross-origin requests. Default GET, HEAD, POST.
	AllowedMethods []string `yaml:"allowed_methods" json:"allowed_methods"`
	// AllowedHeaders lists the request headers allowed for cross-origin requests.
	AllowedHeaders []string `yaml:"allowed_headers" json:"allowed_headers"`
	// ExposeHeaders lists the response headers exposed to cross-origin callers.
	ExposeHeaders []string `yaml:"expose_headers" json:"expose_headers"`
	// AllowCredentials permits credentialed cross-origin requests.
	AllowCredentials bool `yaml:"allow_credentials" json:"allow_credentials"`
	// MaxAge is the preflight cache lifetime in seconds; 0 omits the header.
	MaxAge int `yaml:"max_age" json:"max_age"`
}

// HeadersParams configures the headers middleware type.
type HeadersParams struct {
	// Request edits headers of the forwarded request.
	Request HeaderOps `yaml:"request" json:"request"`
	// Response edits headers of the response.
	Response HeaderOps `yaml:"response" json:"response"`
}

// RequestSizeLimitParams configures the request_size_limit middleware type.
type RequestSizeLimitParams struct {
	// MaxBytes is the largest accepted request body in bytes.
	MaxBytes int64 `yaml:"max_bytes" json:"max_bytes"`
}

// ParamsError reports problems in middleware params. Paths are relative to params ("" = params itself); lines are 0.
type ParamsError struct {
	// Errors lists the problems found.
	Errors []FieldError
}

// Error joins the problems as "params.<path>: <msg>" or "params: <msg>", separated by "; ".
func (e *ParamsError) Error() string {
	parts := make([]string, 0, len(e.Errors))
	for _, fe := range e.Errors {
		if fe.Path == "" {
			parts = append(parts, "params: "+fe.Msg)
			continue
		}
		parts = append(parts, "params."+fe.Path+": "+fe.Msg)
	}
	return strings.Join(parts, "; ")
}

// DecodeParams strictly decodes params into v; absent or empty params leave v unchanged.
func DecodeParams(params map[string]any, v any) error {
	err := yamlx.DecodeValue(params, v)
	if err == nil {
		return nil
	}
	var ve *yamlx.ValueError
	if errors.As(err, &ve) {
		pe := &ParamsError{}
		for _, e := range ve.Errors {
			pe.Errors = append(pe.Errors, FieldError{Path: e.Path, Msg: e.Msg})
		}
		return pe
	}
	return &ParamsError{Errors: []FieldError{{Msg: err.Error()}}}
}

// BuiltinParams decodes, defaults and checks the params of a built-in type, returning *CORSParams, *HeadersParams or *RequestSizeLimitParams. Decode and semantic problems are reported together in one *ParamsError.
func (m Middleware) BuiltinParams() (any, error) {
	switch m.Type {
	case "cors":
		p := &CORSParams{}
		derr := DecodeParams(m.Params.Reveal(), p)
		if wholeParamsFailure(derr) {
			return nil, derr
		}
		if p.AllowedMethods == nil {
			p.AllowedMethods = []string{"GET", "HEAD", "POST"}
		}
		if err := mergeParamsErrors(derr, checkCORSParams(p)); err != nil {
			return nil, err
		}
		return p, nil
	case "headers":
		p := &HeadersParams{}
		derr := DecodeParams(m.Params.Reveal(), p)
		if wholeParamsFailure(derr) {
			return nil, derr
		}
		semantic := appendHeaderOpsErrors(nil, "request", p.Request)
		semantic = appendHeaderOpsErrors(semantic, "response", p.Response)
		if err := mergeParamsErrors(derr, semantic); err != nil {
			return nil, err
		}
		return p, nil
	case "request_size_limit":
		p := &RequestSizeLimitParams{}
		derr := DecodeParams(m.Params.Reveal(), p)
		if wholeParamsFailure(derr) {
			return nil, derr
		}
		var semantic []FieldError
		if p.MaxBytes <= 0 {
			semantic = []FieldError{{Path: "max_bytes", Msg: "must be positive"}}
		}
		if err := mergeParamsErrors(derr, semantic); err != nil {
			return nil, err
		}
		return p, nil
	}
	return nil, fmt.Errorf("middleware type %q is not built in", m.Type)
}

// wholeParamsFailure reports whether a decode error leaves the typed params unpopulated.
func wholeParamsFailure(err error) bool {
	if err == nil {
		return false
	}
	var pe *ParamsError
	if !errors.As(err, &pe) {
		return true
	}
	for _, fe := range pe.Errors {
		if fe.Path == "" {
			return true
		}
	}
	return false
}

// mergeParamsErrors combines decode problems with semantic problems. Semantic problems on a path
// already reported by decoding are dropped.
func mergeParamsErrors(decodeErr error, semantic []FieldError) error {
	var pe *ParamsError
	if decodeErr != nil && !errors.As(decodeErr, &pe) {
		return decodeErr
	}
	var decodeErrs []FieldError
	if pe != nil {
		decodeErrs = pe.Errors
	}
	out := slices.Clone(decodeErrs)
	for _, s := range semantic {
		shadowed := false
		for _, d := range decodeErrs {
			if s.Path == d.Path || strings.HasPrefix(s.Path, d.Path+".") || strings.HasPrefix(s.Path, d.Path+"[") {
				shadowed = true
				break
			}
		}
		if !shadowed {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return &ParamsError{Errors: out}
}

func checkCORSParams(p *CORSParams) []FieldError {
	var errs []FieldError
	for k, o := range p.AllowedOrigins {
		if o != "*" && !validOrigin(o) {
			errs = append(errs, FieldError{
				Path: fmt.Sprintf("allowed_origins[%d]", k),
				Msg:  `must be "*" or an origin such as https://app.example.com`,
			})
		}
	}
	for k, meth := range p.AllowedMethods {
		if !isUpperToken(meth) {
			errs = append(errs, FieldError{Path: fmt.Sprintf("allowed_methods[%d]", k), Msg: "must be an upper-case HTTP method"})
		}
	}
	for k, h := range p.AllowedHeaders {
		if !validHeaderName(h) {
			errs = append(errs, FieldError{Path: fmt.Sprintf("allowed_headers[%d]", k), Msg: fmt.Sprintf("invalid header name %q", h)})
		}
	}
	for k, h := range p.ExposeHeaders {
		if !validHeaderName(h) {
			errs = append(errs, FieldError{Path: fmt.Sprintf("expose_headers[%d]", k), Msg: fmt.Sprintf("invalid header name %q", h)})
		}
	}
	if p.MaxAge < 0 {
		errs = append(errs, FieldError{Path: "max_age", Msg: "must not be negative"})
	}
	if p.AllowCredentials && slices.Contains(p.AllowedOrigins, "*") {
		errs = append(errs, FieldError{Path: "allow_credentials", Msg: `cannot be true when allowed_origins contains "*"`})
	}
	return errs
}

func appendHeaderOpsErrors(errs []FieldError, prefix string, ops HeaderOps) []FieldError {
	for _, k := range slices.Sorted(maps.Keys(ops.Add)) {
		if !validHeaderName(k) {
			errs = append(errs, FieldError{Path: prefix + ".add." + k, Msg: fmt.Sprintf("invalid header name %q", k)})
		}
	}
	for _, k := range slices.Sorted(maps.Keys(ops.Set)) {
		if !validHeaderName(k) {
			errs = append(errs, FieldError{Path: prefix + ".set." + k, Msg: fmt.Sprintf("invalid header name %q", k)})
		}
	}
	for k, name := range ops.Remove {
		if !validHeaderName(name) {
			errs = append(errs, FieldError{Path: fmt.Sprintf("%s.remove[%d]", prefix, k), Msg: fmt.Sprintf("invalid header name %q", name)})
		}
	}
	return errs
}

func validOrigin(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return (u.Scheme == "http" || u.Scheme == "https") &&
		u.Host != "" && u.Path == "" && u.RawQuery == "" && u.Fragment == "" && u.User == nil
}

// validHeaderName reports whether s is a non-empty RFC 9110 token.
func validHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}
