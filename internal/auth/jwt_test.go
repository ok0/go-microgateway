package auth

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/lychgate/internal/authkey"
	"github.com/ok0/lychgate/internal/clock"
)

const testIssuer = "https://issuer.test"

type algCase struct {
	alg    string
	kid    string
	signer any
	key    any
}

func algCases(ks *keyset) []algCase {
	return []algCase{
		{"HS256", "hs256", ks.hmac, ks.hmac},
		{"HS384", "hs384", ks.hmac, ks.hmac},
		{"HS512", "hs512", ks.hmac, ks.hmac},
		{"RS256", "rs256", ks.rsa1, &ks.rsa1.PublicKey},
		{"RS384", "rs384", ks.rsa1, &ks.rsa1.PublicKey},
		{"RS512", "rs512", ks.rsa1, &ks.rsa1.PublicKey},
		{"ES256", "es256", ks.p256, &ks.p256.PublicKey},
		{"ES384", "es384", ks.p384, &ks.p384.PublicKey},
		{"ES512", "es512", ks.p521, &ks.p521.PublicKey},
		{"EdDSA", "eddsa", ks.edPriv, ks.edPub},
	}
}

func allStatic(t *testing.T) *StaticKeys {
	t.Helper()
	var keys []Key
	for _, c := range algCases(loadKeyset()) {
		keys = append(keys, Key{ID: c.kid, Algorithm: c.alg, Material: c.key})
	}
	s, err := NewStaticKeys(keys)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testJWTConfig(algs []string) JWTConfig {
	return JWTConfig{
		Name:          "jwt",
		Issuer:        testIssuer,
		Audiences:     []string{"api"},
		Header:        "Authorization",
		MaxTokenBytes: 8192,
		Algorithms:    algs,
		ScopeClaim:    "scope",
		RolesClaim:    "roles",
		ForwardClaims: map[string]string{
			"tenant":  "X-Tenant",
			"level":   "X-Level",
			"groups":  "X-Groups",
			"missing": "X-Missing",
			"bad":     "X-Bad",
		},
	}
}

func baseClaims(now time.Time) jwt.MapClaims {
	return jwt.MapClaims{
		"iss": testIssuer,
		"aud": "api",
		"sub": "user-1",
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}
}

func bearerReq(t *testing.T, tok string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodGet, "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+tok)
	return r
}

func rawToken(header, payload string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(header)) + "." + enc([]byte(payload)) + ".AAAA"
}

func TestJWTValid(t *testing.T) {
	clk := clock.NewFake(testNow)
	p, err := NewJWT(testJWTConfig(authkey.Algorithms), allStatic(t), clk)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range algCases(loadKeyset()) {
		t.Run(c.alg, func(t *testing.T) {
			claims := baseClaims(testNow)
			claims["scope"] = "read write"
			claims["roles"] = []string{"admin"}
			claims["tenant"] = "acme"
			claims["level"] = 3
			claims["groups"] = []any{"a", 1}
			claims["bad"] = "a\r\nb"
			tok := signToken(t, c.alg, c.kid, c.signer, claims)

			res, err := p.Authenticate(bearerReq(t, tok))
			if err != nil {
				t.Fatalf("Authenticate: %v", err)
			}
			id := res.Identity
			if id.Provider != "jwt" || id.Subject != "user-1" || id.Consumer != "user-1" {
				t.Fatalf("identity = %+v", id)
			}
			if id.Claims["sub"] != "user-1" {
				t.Fatalf("claims sub = %v", id.Claims["sub"])
			}
			if !slices.Equal(res.Scopes, []string{"read", "write"}) {
				t.Fatalf("scopes = %v", res.Scopes)
			}
			if !slices.Equal(res.Roles, []string{"admin"}) {
				t.Fatalf("roles = %v", res.Roles)
			}
			if res.Values["tenant"] != "acme" {
				t.Fatalf("values tenant = %v", res.Values["tenant"])
			}
			want := map[string]string{"X-Tenant": "acme", "X-Level": "3", "X-Groups": `["a",1]`}
			if !maps.Equal(res.Forward, want) {
				t.Fatalf("forward = %v, want %v", res.Forward, want)
			}
		})
	}
}

func TestJWTRejects(t *testing.T) {
	ks := loadKeyset()
	now := testNow
	tokenWith := func(mut func(jwt.MapClaims)) func(t *testing.T) string {
		return func(t *testing.T) string {
			claims := baseClaims(now)
			mut(claims)
			return signToken(t, "HS256", "hs256", ks.hmac, claims)
		}
	}
	cases := []struct {
		name  string
		mut   func(*JWTConfig)
		keys  func(t *testing.T) KeySource
		token func(t *testing.T) string
		want  Reason
	}{
		{
			name: "expired",
			token: tokenWith(func(c jwt.MapClaims) {
				c["exp"] = now.Add(-time.Minute).Unix()
			}),
			want: ReasonExpired,
		},
		{
			name: "not yet valid",
			token: tokenWith(func(c jwt.MapClaims) {
				c["nbf"] = now.Add(time.Hour).Unix()
			}),
			want: ReasonExpired,
		},
		{
			name: "wrong issuer",
			token: tokenWith(func(c jwt.MapClaims) {
				c["iss"] = "https://evil.test"
			}),
			want: ReasonClaims,
		},
		{
			name: "wrong audience",
			token: tokenWith(func(c jwt.MapClaims) {
				c["aud"] = "other"
			}),
			want: ReasonClaims,
		},
		{
			name: "missing exp",
			token: tokenWith(func(c jwt.MapClaims) {
				delete(c, "exp")
			}),
			want: ReasonClaims,
		},
		{
			name: "exp of wrong type",
			token: tokenWith(func(c jwt.MapClaims) {
				c["exp"] = "soon"
			}),
			want: ReasonClaims,
		},
		{
			name: "bad signature",
			token: func(t *testing.T) string {
				return signToken(t, "RS256", "rs256", ks.rsa2, baseClaims(now))
			},
			want: ReasonSignature,
		},
		{
			name: "alg none",
			token: func(t *testing.T) string {
				return signToken(t, "none", "", nil, baseClaims(now))
			},
			want: ReasonAlgorithm,
		},
		{
			name: "unlisted HS384 on HS256 provider",
			mut:  func(c *JWTConfig) { c.Algorithms = []string{"HS256"} },
			token: func(t *testing.T) string {
				return signToken(t, "HS384", "hs384", ks.hmac, baseClaims(now))
			},
			want: ReasonAlgorithm,
		},
		{
			name: "algorithm does not match pinned key",
			token: func(t *testing.T) string {
				return signToken(t, "RS384", "rs256", ks.rsa1, baseClaims(now))
			},
			want: ReasonAlgorithm,
		},
		{
			name: "RSA public PEM used as HS256 secret",
			token: func(t *testing.T) string {
				der, err := x509.MarshalPKIXPublicKey(&ks.rsa1.PublicKey)
				if err != nil {
					t.Fatal(err)
				}
				secret := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
				return signToken(t, "HS256", "rs256", secret, baseClaims(now))
			},
			want: ReasonAlgorithm,
		},
		{
			name: "unknown algorithm name",
			token: func(t *testing.T) string {
				return rawToken(`{"alg":"XX1","kid":"hs256"}`, `{"sub":"x"}`)
			},
			want: ReasonAlgorithm,
		},
		{
			name: "unknown key id",
			token: func(t *testing.T) string {
				return signToken(t, "HS256", "nope", ks.hmac, baseClaims(now))
			},
			want: ReasonUnknownKID,
		},
		{
			name: "no key id with several keys",
			token: func(t *testing.T) string {
				return signToken(t, "HS256", "", ks.hmac, baseClaims(now))
			},
			want: ReasonUnknownKID,
		},
		{
			name: "garbage token",
			token: func(t *testing.T) string {
				return "garbage"
			},
			want: ReasonMalformed,
		},
		{
			name: "non-string key id",
			token: func(t *testing.T) string {
				tok := jwt.NewWithClaims(jwt.SigningMethodHS256, baseClaims(now))
				tok.Header["kid"] = 7
				s, err := tok.SignedString(ks.hmac)
				if err != nil {
					t.Fatal(err)
				}
				return s
			},
			want: ReasonMalformed,
		},
		{
			name: "token larger than limit",
			mut:  func(c *JWTConfig) { c.MaxTokenBytes = 16 },
			token: func(t *testing.T) string {
				return signToken(t, "HS256", "hs256", ks.hmac, baseClaims(now))
			},
			want: ReasonTooLarge,
		},
		{
			name: "keys unavailable",
			keys: func(t *testing.T) KeySource {
				mf := newMemFetcher(nil)
				mf.set(nil, errTestFetch)
				j, err := NewJWKS(testJWKSConfig(), JWKSDeps{Fetcher: mf, Clock: clock.NewFake(now)})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = j.Close() })
				return j
			},
			token: func(t *testing.T) string {
				return signToken(t, "HS256", "x", ks.hmac, baseClaims(now))
			},
			want: ReasonKeysUnavailable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testJWTConfig(authkey.Algorithms)
			if tc.mut != nil {
				tc.mut(&cfg)
			}
			var keys KeySource = allStatic(t)
			if tc.keys != nil {
				keys = tc.keys(t)
			}
			p, err := NewJWT(cfg, keys, clock.NewFake(now))
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.Authenticate(bearerReq(t, tc.token(t)))
			if err == nil {
				t.Fatal("Authenticate succeeded, want rejection")
			}
			if got := ReasonOf(err); got != tc.want {
				t.Fatalf("reason = %q, want %q (err %v)", got, tc.want, err)
			}
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("error %v does not wrap ErrInvalid", err)
			}
		})
	}
}

func TestJWTHeaderForms(t *testing.T) {
	ks := loadKeyset()
	now := testNow
	valid := signToken(t, "HS256", "hs256", ks.hmac, baseClaims(now))

	cases := []struct {
		name   string
		header string
		values []string
		want   Reason
		none   bool
	}{
		{name: "missing header", header: "Authorization", none: true},
		{name: "basic scheme", header: "Authorization", values: []string{"Basic abc"}, none: true},
		{name: "bearer without token", header: "Authorization", values: []string{"Bearer"}, want: ReasonMalformed},
		{name: "bearer with blank token", header: "Authorization", values: []string{"Bearer   "}, want: ReasonMalformed},
		{name: "two authorization values", header: "Authorization", values: []string{"Bearer " + valid, "Bearer " + valid}, want: ReasonMalformed},
		{name: "lower case scheme", header: "Authorization", values: []string{"bearer " + valid}},
		{name: "extra spaces", header: "Authorization", values: []string{"Bearer    " + valid}},
		{name: "custom header whole value", header: "X-Token", values: []string{"  " + valid + "  "}},
		{name: "custom header empty", header: "X-Token", values: []string{"   "}, none: true},
		{name: "custom header two values", header: "X-Token", values: []string{valid, valid}, want: ReasonMalformed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testJWTConfig(authkey.Algorithms)
			cfg.Header = tc.header
			p, err := NewJWT(cfg, allStatic(t), clock.NewFake(now))
			if err != nil {
				t.Fatal(err)
			}
			r, err := http.NewRequest(http.MethodGet, "/", nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range tc.values {
				r.Header.Add(tc.header, v)
			}
			_, err = p.Authenticate(r)
			switch {
			case tc.none:
				if !errors.Is(err, ErrNoCredentials) {
					t.Fatalf("err = %v, want ErrNoCredentials", err)
				}
			case tc.want != "":
				if got := ReasonOf(err); got != tc.want {
					t.Fatalf("reason = %q (err %v), want %q", got, err, tc.want)
				}
			default:
				if err != nil {
					t.Fatalf("Authenticate: %v", err)
				}
			}
		})
	}
}

func TestJWTLeeway(t *testing.T) {
	ks := loadKeyset()
	claims := baseClaims(testNow)
	claims["exp"] = testNow.Unix()
	tok := signToken(t, "HS256", "hs256", ks.hmac, claims)

	clk := clock.NewFake(testNow)
	clk.Advance(20 * time.Second)

	withLeeway := testJWTConfig(authkey.Algorithms)
	withLeeway.Leeway = 30 * time.Second
	p, err := NewJWT(withLeeway, allStatic(t), clk)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Authenticate(bearerReq(t, tok)); err != nil {
		t.Fatalf("with 30s leeway: %v", err)
	}

	strict := testJWTConfig(authkey.Algorithms)
	p, err = NewJWT(strict, allStatic(t), clk)
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.Authenticate(bearerReq(t, tok))
	if got := ReasonOf(err); got != ReasonExpired {
		t.Fatalf("without leeway: reason = %q, want expired", got)
	}
}

func TestJWTAccessors(t *testing.T) {
	cfg := testJWTConfig(authkey.Algorithms)
	cfg.ForwardCredentials = true
	p, err := NewJWT(cfg, allStatic(t), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "jwt" || p.Challenge() != "Bearer" || !p.ForwardCredentials() {
		t.Fatalf("accessors wrong: name=%q challenge=%q forward=%v", p.Name(), p.Challenge(), p.ForwardCredentials())
	}
	if h, q := p.Credentials(); h != "Authorization" || q != "" {
		t.Fatalf("credentials = %q, %q", h, q)
	}
}

func TestJWTWithJWKS(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	j, err := NewJWKS(testJWKSConfig(), JWKSDeps{Fetcher: mf, Clock: clock.NewFake(testNow)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })

	p, err := NewJWT(testJWTConfig(authkey.Algorithms), &Combined{JWKS: j}, clock.NewFake(testNow))
	if err != nil {
		t.Fatal(err)
	}
	tok := signToken(t, "RS256", "rs256", ks.rsa1, baseClaims(testNow))
	res, err := p.Authenticate(bearerReq(t, tok))
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if res.Identity.Subject != "user-1" {
		t.Fatalf("subject = %q", res.Identity.Subject)
	}
}

func TestCombinedLookup(t *testing.T) {
	ks := loadKeyset()
	ctx := t.Context()
	static, err := NewStaticKeys([]Key{{ID: "hs256", Algorithm: "HS256", Material: ks.hmac}})
	if err != nil {
		t.Fatal(err)
	}
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	j, err := NewJWKS(testJWKSConfig(), JWKSDeps{Fetcher: mf, Clock: clock.NewFake(testNow)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })

	c := &Combined{Static: static, JWKS: j}
	if k, err := c.Lookup(ctx, "hs256"); err != nil || k.Algorithm != "HS256" {
		t.Fatalf("static hit: %+v, %v", k, err)
	}
	if k, err := c.Lookup(ctx, "rs256"); err != nil || k.Algorithm != "RS256" {
		t.Fatalf("jwks hit: %+v, %v", k, err)
	}
	if _, err := c.Lookup(ctx, "nope"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("unknown kid err = %v", err)
	}

	solo := &Combined{Static: static}
	if k, err := solo.Lookup(ctx, ""); err != nil || k.ID != "hs256" {
		t.Fatalf("sole static key without kid: %+v, %v", k, err)
	}
	if _, err := solo.Lookup(ctx, "nope"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("nil jwks kid miss err = %v", err)
	}

	both := &Combined{Static: static, JWKS: j}
	if _, err := both.Lookup(ctx, ""); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("no kid with two candidates: err = %v", err)
	}

	empty := &Combined{}
	if _, err := empty.Lookup(ctx, ""); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("empty source: err = %v", err)
	}
	if _, err := empty.Lookup(ctx, "x"); !errors.Is(err, ErrUnknownKID) {
		t.Fatalf("empty source with kid: err = %v", err)
	}
}

func TestCombinedLookupNoKidSingleJWKS(t *testing.T) {
	ks := loadKeyset()
	mf := newMemFetcher(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey)))
	j, err := NewJWKS(testJWKSConfig(), JWKSDeps{Fetcher: mf, Clock: clock.NewFake(testNow)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })

	k, err := (&Combined{JWKS: j}).Lookup(t.Context(), "")
	if err != nil || k.ID != "rs256" {
		t.Fatalf("sole jwks key without kid: %+v, %v", k, err)
	}

	mf.set(nil, errTestFetch)
	failing, err := NewJWKS(testJWKSConfig(), JWKSDeps{Fetcher: mf, Clock: clock.NewFake(testNow)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = failing.Close() })
	if _, err := (&Combined{JWKS: failing}).Lookup(t.Context(), ""); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("failing jwks without kid: err = %v", err)
	}
}

func TestNewJWTErrors(t *testing.T) {
	cases := []struct {
		name    string
		mut     func(*JWTConfig)
		nilKeys bool
	}{
		{name: "name", mut: func(c *JWTConfig) { c.Name = "" }},
		{name: "issuer", mut: func(c *JWTConfig) { c.Issuer = "" }},
		{name: "audience", mut: func(c *JWTConfig) { c.Audiences = nil }},
		{name: "header", mut: func(c *JWTConfig) { c.Header = "" }},
		{name: "no algorithms", mut: func(c *JWTConfig) { c.Algorithms = nil }},
		{name: "none algorithm", mut: func(c *JWTConfig) { c.Algorithms = []string{"none"} }},
		{name: "pss algorithm", mut: func(c *JWTConfig) { c.Algorithms = []string{"PS256"} }},
		{name: "zero token limit", mut: func(c *JWTConfig) { c.MaxTokenBytes = 0 }},
		{name: "nil keys", mut: func(c *JWTConfig) {}, nilKeys: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testJWTConfig(authkey.Algorithms)
			tc.mut(&cfg)
			var keys KeySource
			if !tc.nilKeys {
				keys = allStatic(t)
			}
			if _, err := NewJWT(cfg, keys, nil); err == nil {
				t.Fatal("NewJWT succeeded, want error")
			}
		})
	}
}

func TestNewStaticKeysErrors(t *testing.T) {
	ks := loadKeyset()
	cases := []struct {
		name string
		keys []Key
	}{
		{name: "no algorithm", keys: []Key{{ID: "a", Material: ks.hmac}}},
		{name: "short secret", keys: []Key{{ID: "a", Algorithm: "HS256", Material: []byte("short")}}},
		{name: "duplicate id", keys: []Key{
			{ID: "a", Algorithm: "HS256", Material: ks.hmac},
			{ID: "a", Algorithm: "HS256", Material: ks.hmac},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewStaticKeys(tc.keys); err == nil {
				t.Fatal("NewStaticKeys succeeded, want error")
			}
		})
	}

	s, err := NewStaticKeys([]Key{
		{Algorithm: "HS256", Material: ks.hmac},
		{Algorithm: "HS256", Material: ks.hmac},
	})
	if err != nil {
		t.Fatalf("kid-less keys: %v", err)
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s.Len())
	}
	var nilStatic *StaticKeys
	if nilStatic.Len() != 0 {
		t.Fatal("nil StaticKeys Len != 0")
	}
}
