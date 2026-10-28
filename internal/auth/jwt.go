package auth

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/lychgate/internal/authkey"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/reqctx"
)

type JWTConfig struct {
	Name               string
	Issuer             string
	Audiences          []string
	Leeway             time.Duration
	Header             string
	MaxTokenBytes      int
	Algorithms         []string
	ScopeClaim         string
	RolesClaim         string
	ForwardClaims      map[string]string
	ForwardCredentials bool
}

type JWT struct {
	cfg    JWTConfig
	keys   KeySource
	parser *jwt.Parser
}

var (
	errBadKID      = errors.New("key id is not a string")
	errAlgMismatch = errors.New("algorithm does not match key")
)

func NewJWT(c JWTConfig, keys KeySource, clk clock.Clock) (*JWT, error) {
	if c.Name == "" {
		return nil, errors.New("jwt: name is required")
	}
	if c.Issuer == "" {
		return nil, errors.New("jwt: issuer is required")
	}
	if len(c.Audiences) == 0 {
		return nil, errors.New("jwt: audience is required")
	}
	if c.Header == "" {
		return nil, errors.New("jwt: header is required")
	}
	if len(c.Algorithms) == 0 {
		return nil, errors.New("jwt: at least one algorithm is required")
	}
	for _, a := range c.Algorithms {
		if !authkey.Supported(a) {
			return nil, fmt.Errorf("jwt: unsupported algorithm %q", a)
		}
	}
	if c.MaxTokenBytes <= 0 {
		return nil, errors.New("jwt: max token bytes must be positive")
	}
	if keys == nil {
		return nil, errors.New("jwt: key source is required")
	}
	if clk == nil {
		clk = clock.Real()
	}
	c.Algorithms = slices.Clone(c.Algorithms)
	c.Audiences = slices.Clone(c.Audiences)
	parser := jwt.NewParser(
		jwt.WithValidMethods(c.Algorithms),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(c.Issuer),
		jwt.WithAudience(c.Audiences...),
		jwt.WithLeeway(c.Leeway),
		jwt.WithTimeFunc(clk.Now),
	)
	return &JWT{cfg: c, keys: keys, parser: parser}, nil
}

func (j *JWT) Name() string {
	return j.cfg.Name
}

func (j *JWT) Challenge() string {
	return "Bearer"
}

func (j *JWT) Credentials() (string, string) {
	return j.cfg.Header, ""
}

func (j *JWT) ForwardCredentials() bool {
	return j.cfg.ForwardCredentials
}

func (j *JWT) Authenticate(r *http.Request) (Result, error) {
	vals := r.Header.Values(j.cfg.Header)
	if len(vals) == 0 {
		return Result{}, ErrNoCredentials
	}
	var token string
	if strings.EqualFold(j.cfg.Header, "Authorization") {
		scheme, rest, _ := strings.Cut(vals[0], " ")
		if !strings.EqualFold(scheme, "Bearer") {
			return Result{}, ErrNoCredentials
		}
		token = strings.TrimLeft(rest, " ")
	} else {
		token = strings.TrimSpace(vals[0])
		if token == "" {
			return Result{}, ErrNoCredentials
		}
	}
	if token == "" || len(vals) > 1 {
		return Result{}, Fail(ReasonMalformed, nil)
	}
	if len(token) > j.cfg.MaxTokenBytes {
		return Result{}, Fail(ReasonTooLarge, nil)
	}

	ctx := r.Context()
	var kerr error
	claims := jwt.MapClaims{}
	tok, err := j.parser.ParseWithClaims(token, claims, func(t *jwt.Token) (any, error) {
		kid := ""
		if v, ok := t.Header["kid"]; ok {
			s, isString := v.(string)
			if !isString {
				kerr = errBadKID
				return nil, kerr
			}
			kid = s
		}
		k, err := j.keys.Lookup(ctx, kid)
		if err != nil {
			kerr = err
			return nil, err
		}
		alg := t.Method.Alg()
		if !keyAllows(k, alg) {
			kerr = errAlgMismatch
			return nil, kerr
		}
		return k.Material, nil
	})
	if err != nil {
		return Result{}, Fail(j.classify(err, kerr, tok), err)
	}

	sub, _ := claims["sub"].(string)
	res := Result{
		Identity: reqctx.Identity{
			Provider: j.cfg.Name,
			Subject:  sub,
			Consumer: sub,
			Claims:   map[string]any(claims),
		},
		Scopes:  stringList(claims[j.cfg.ScopeClaim]),
		Roles:   stringList(claims[j.cfg.RolesClaim]),
		Values:  claims,
		Forward: make(map[string]string),
	}
	for name, hdr := range j.cfg.ForwardClaims {
		v, ok := claims[name]
		if !ok {
			continue
		}
		if s, ok := forwardValue(v); ok {
			res.Forward[hdr] = s
		}
	}
	return res, nil
}

func keyAllows(k Key, alg string) bool {
	if k.Algorithm != "" && k.Algorithm != alg {
		return false
	}
	if k.Algorithm == "" && !slices.Contains(k.Allowed, alg) {
		return false
	}
	return authkey.Check(alg, k.Material) == nil
}

func (j *JWT) classify(err, kerr error, tok *jwt.Token) Reason {
	switch {
	case errors.Is(kerr, ErrUnknownKID):
		return ReasonUnknownKID
	case errors.Is(kerr, ErrKeysUnavailable):
		return ReasonKeysUnavailable
	case errors.Is(kerr, errAlgMismatch):
		return ReasonAlgorithm
	case errors.Is(kerr, errBadKID):
		return ReasonMalformed
	case errors.Is(err, jwt.ErrTokenMalformed):
		return ReasonMalformed
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		if tok != nil && tok.Method != nil && !slices.Contains(j.cfg.Algorithms, tok.Method.Alg()) {
			return ReasonAlgorithm
		}
		return ReasonSignature
	case errors.Is(err, jwt.ErrTokenUnverifiable):
		return ReasonAlgorithm
	case errors.Is(err, jwt.ErrTokenExpired), errors.Is(err, jwt.ErrTokenNotValidYet):
		return ReasonExpired
	case errors.Is(err, jwt.ErrTokenInvalidAudience),
		errors.Is(err, jwt.ErrTokenInvalidIssuer),
		errors.Is(err, jwt.ErrTokenRequiredClaimMissing),
		errors.Is(err, jwt.ErrTokenInvalidClaims),
		errors.Is(err, jwt.ErrInvalidType),
		errors.Is(err, jwt.ErrTokenUsedBeforeIssued):
		return ReasonClaims
	}
	return ReasonMalformed
}
