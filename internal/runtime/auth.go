package runtime

import (
	"encoding/hex"
	"fmt"
	"log/slog"
	"maps"
	"net/textproto"
	"slices"
	"strings"
	"time"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/auth"
	"github.com/ok0/lychgate/internal/authkey"
	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/state"
)

func buildAuth(cfg *config.Config, tx *state.Tx, f auth.Fetcher, clk clock.Clock, logger *slog.Logger, s *Series) (map[string]auth.Authenticator, []string, error) {
	providers := make(map[string]auth.Authenticator, len(cfg.Auth.JWT)+len(cfg.Auth.APIKeys))
	var strip []string
	addStrip := func(h string) {
		if h == "" {
			return
		}
		n := textproto.CanonicalMIMEHeaderKey(h)
		if !slices.Contains(strip, n) {
			strip = append(strip, n)
		}
	}
	for _, h := range cfg.Global.StripIdentityHeaders {
		addStrip(h)
	}
	for i := range cfg.Auth.JWT {
		p := &cfg.Auth.JWT[i]
		jp, err := buildJWT(p, tx, f, clk, logger, s)
		if err != nil {
			return nil, nil, err
		}
		providers[p.Name] = jp
		for _, claim := range slices.Sorted(maps.Keys(p.ForwardClaims)) {
			addStrip(p.ForwardClaims[claim])
		}
	}
	for i := range cfg.Auth.APIKeys {
		a := &cfg.Auth.APIKeys[i]
		ak, err := buildAPIKey(a)
		if err != nil {
			return nil, nil, err
		}
		providers[a.Name] = ak
		addStrip(a.ConsumerHeader)
		for _, k := range slices.Sorted(maps.Keys(a.MetadataHeaders)) {
			addStrip(a.MetadataHeaders[k])
		}
	}
	return providers, strip, nil
}

func buildJWT(p *config.JWTProvider, tx *state.Tx, f auth.Fetcher, clk clock.Clock, logger *slog.Logger, s *Series) (*auth.JWT, error) {
	var static []auth.Key
	var algs []string
	for i, k := range p.Keys {
		var material any
		if authkey.IsHMAC(k.Algorithm) {
			material = []byte(k.Secret)
		} else {
			pemData := k.PublicKey
			if pemData == "" {
				pemData = k.LoadedPublicKey
			}
			pub, err := authkey.ParsePublicKeyPEM([]byte(pemData))
			if err != nil {
				return nil, fmt.Errorf("auth provider %q key %d: %w", p.Name, i, err)
			}
			material = pub
		}
		static = append(static, auth.Key{ID: k.KID, Algorithm: k.Algorithm, Material: material})
		algs = appendUnique(algs, k.Algorithm)
	}
	staticKeys, err := auth.NewStaticKeys(static)
	if err != nil {
		return nil, err
	}

	var jwks *auth.JWKS
	if p.JWKS != nil {
		for _, a := range p.JWKS.Algorithms {
			algs = appendUnique(algs, a)
		}
		jwks, err = buildJWKS(p.Name, p.JWKS, tx, f, clk, logger, s)
		if err != nil {
			return nil, err
		}
	}

	var src auth.KeySource
	switch {
	case jwks == nil:
		src = staticKeys
	case len(p.Keys) == 0:
		src = jwks
	default:
		src = &auth.Combined{Static: staticKeys, JWKS: jwks}
	}

	return auth.NewJWT(auth.JWTConfig{
		Name:               p.Name,
		Issuer:             p.Issuer,
		Audiences:          p.Audiences,
		Leeway:             durationOr(p.Leeway, 30*time.Second),
		Header:             p.Header,
		MaxTokenBytes:      intOr(p.MaxTokenBytes, 4096),
		Algorithms:         algs,
		ScopeClaim:         p.ScopeClaim,
		RolesClaim:         p.RolesClaim,
		ForwardClaims:      p.ForwardClaims,
		ForwardCredentials: boolOr(p.ForwardCredentials, false),
	}, src, clk)
}

func buildJWKS(provider string, j *config.JWKS, tx *state.Tx, f auth.Fetcher, clk clock.Clock, logger *slog.Logger, s *Series) (*auth.JWKS, error) {
	ttl := durationOr(j.TTL, 10*time.Minute)
	minRefresh := durationOr(j.MinRefreshInterval, 30*time.Second)
	timeout := durationOr(j.Timeout, 5*time.Second)
	return state.Get(tx, auth.KindJWKS, provider, jwksFingerprint(j), func() (*auth.JWKS, error) {
		return auth.NewJWKS(auth.JWKSConfig{
			Provider:           provider,
			URL:                j.URL,
			DisplayURL:         config.RedactURL(j.URL),
			Algorithms:         slices.Clone(j.Algorithms),
			TTL:                ttl,
			MinRefreshInterval: minRefresh,
			Timeout:            timeout,
		}, auth.JWKSDeps{
			Fetcher: f,
			Clock:   clk,
			Logger:  logger,
			OnRefresh: func(ok bool) {
				result := "failure"
				if ok {
					result = "success"
				}
				s.JWKSRefreshes.With(provider, result).Inc()
			},
		})
	})
}

func jwksFingerprint(j *config.JWKS) string {
	return fp(struct {
		URL                string
		Algorithms         []string
		TTL                time.Duration
		MinRefreshInterval time.Duration
		Timeout            time.Duration
	}{
		URL:                j.URL,
		Algorithms:         j.Algorithms,
		TTL:                durationOr(j.TTL, 10*time.Minute),
		MinRefreshInterval: durationOr(j.MinRefreshInterval, 30*time.Second),
		Timeout:            durationOr(j.Timeout, 5*time.Second),
	})
}

func buildAPIKey(a *config.APIKeyProvider) (*auth.APIKey, error) {
	header := "X-API-Key"
	if a.Header != nil {
		header = *a.Header
	}
	var entries []auth.APIKeyEntry
	for _, k := range slices.Concat(a.Keys, a.FileKeys) {
		hash, err := hex.DecodeString(strings.ToLower(strings.TrimPrefix(k.Hash, "sha256:")))
		if err != nil {
			return nil, fmt.Errorf("auth provider %q key %q: invalid hash", a.Name, k.ID)
		}
		var e auth.APIKeyEntry
		if len(hash) != len(e.Hash) {
			return nil, fmt.Errorf("auth provider %q key %q: invalid hash", a.Name, k.ID)
		}
		copy(e.Hash[:], hash)
		e.ID = k.ID
		e.Consumer = k.Consumer
		e.Metadata = k.Metadata
		e.Scopes = k.Scopes
		e.Roles = k.Roles
		entries = append(entries, e)
	}
	return auth.NewAPIKey(auth.APIKeyConfig{
		Name:               a.Name,
		Header:             header,
		Query:              a.Query,
		Keys:               entries,
		ConsumerHeader:     a.ConsumerHeader,
		MetadataHeaders:    a.MetadataHeaders,
		ForwardCredentials: boolOr(a.ForwardCredentials, false),
	}), nil
}

func appendUnique(list []string, v string) []string {
	if slices.Contains(list, v) {
		return list
	}
	return append(list, v)
}

func durationOr(d *config.Duration, def time.Duration) time.Duration {
	if d == nil {
		return def
	}
	return d.Std()
}

func intOr(v *int, def int) int {
	if v == nil {
		return def
	}
	return *v
}

func boolOr(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}
