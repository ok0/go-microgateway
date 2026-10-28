package config

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/ok0/go-microgateway/internal/authkey"
	"github.com/ok0/go-microgateway/internal/yamlx"
)

// maxKeyFileBytes bounds the size of a key file.
const maxKeyFileBytes = 1 << 20

var (
	yamlValueRe = regexp.MustCompile("`[^`]*`")
	yamlTypeRe  = regexp.MustCompile(`column \d+: type \S+: `)
)

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
	for i, aud := range j.Audiences {
		if aud == "" {
			ck.add(fmt.Sprintf("%s.audiences[%d]", p, i), "must not be empty")
		}
	}
	switch {
	case *j.Leeway < 0:
		ck.add(p+".leeway", "must not be negative")
	case *j.Leeway > Duration(5*time.Minute):
		ck.add(p+".leeway", "must not exceed 5m")
	}
	if *j.MaxTokenBytes <= 0 {
		ck.add(p+".max_token_bytes", "must be positive")
	}
	switch {
	case j.Header == "":
		ck.add(p+".header", "is required")
	case !validHeaderName(j.Header):
		ck.add(p+".header", "invalid header name")
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
	hasHMAC, hasAsym := false, false
	for k := range j.Keys {
		key := &j.Keys[k]
		kp := fmt.Sprintf("%s.keys[%d]", p, k)
		switch {
		case key.KID == "" && len(j.Keys) > 1:
			ck.add(kp+".kid", "is required when there is more than one key")
		case key.KID != "" && kids[key.KID]:
			ck.add(kp+".kid", fmt.Sprintf("duplicate kid %q", key.KID))
		}
		if key.KID != "" {
			kids[key.KID] = true
		}
		alg := key.Algorithm
		if !authkey.Supported(alg) {
			ck.add(kp+".algorithm", "must be one of HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384, ES512, EdDSA")
			continue
		}
		n := 0
		for _, set := range []bool{!key.Secret.IsZero(), key.PublicKey != "", key.PublicKeyFile != ""} {
			if set {
				n++
			}
		}
		if n != 1 {
			ck.add(kp, "exactly one of secret, public_key, public_key_file must be set")
		}
		if authkey.IsHMAC(alg) {
			hasHMAC = true
			if key.PublicKey != "" {
				ck.add(kp+".public_key", "is not allowed with HS algorithms")
			}
			if key.PublicKeyFile != "" {
				ck.add(kp+".public_key_file", "is not allowed with HS algorithms")
			}
			if !key.Secret.IsZero() {
				if err := authkey.Check(alg, []byte(key.Secret.Reveal())); err != nil {
					ck.add(kp+".secret", err.Error())
				}
			}
			continue
		}
		hasAsym = true
		if !key.Secret.IsZero() {
			ck.add(kp+".secret", "is allowed only with HS algorithms")
		}
		pemText, path := key.PublicKey, kp+".public_key"
		if key.PublicKeyFile != "" {
			pemText, path = key.LoadedPublicKey, kp+".public_key_file"
		}
		if pemText == "" {
			continue
		}
		pub, err := authkey.ParsePublicKeyPEM([]byte(pemText))
		if err != nil {
			ck.add(path, "invalid public key: "+err.Error())
			continue
		}
		if err := authkey.Check(alg, pub); err != nil {
			ck.add(path, err.Error())
		}
	}
	if hasHMAC && (hasAsym || j.JWKS != nil) {
		ck.add(p+".keys", "HS keys cannot be combined with asymmetric keys or jwks")
	}
	if jw := j.JWKS; jw != nil {
		jp := p + ".jwks"
		u, err := url.Parse(jw.URL.Reveal())
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			ck.add(jp+".url", "must be an http or https URL")
		}
		if len(jw.Algorithms) == 0 {
			ck.add(jp+".algorithms", "at least one algorithm is required")
		}
		seen := map[string]bool{}
		for i, alg := range jw.Algorithms {
			ap := fmt.Sprintf("%s.algorithms[%d]", jp, i)
			if !authkey.Supported(alg) || authkey.IsHMAC(alg) {
				ck.add(ap, "must be one of RS256, RS384, RS512, ES256, ES384, ES512, EdDSA")
				continue
			}
			if seen[alg] {
				ck.add(ap, fmt.Sprintf("duplicate algorithm %q", alg))
			}
			seen[alg] = true
		}
		requirePositive(ck, jp+".ttl", *jw.TTL)
		requirePositive(ck, jp+".min_refresh_interval", *jw.MinRefreshInterval)
		requirePositive(ck, jp+".timeout", *jw.Timeout)
	}
	for _, claim := range slices.Sorted(maps.Keys(j.ForwardClaims)) {
		fp := p + ".forward_claims." + claim
		h := j.ForwardClaims[claim]
		switch {
		case h == "":
			ck.add(fp, "must not be empty")
		case !validHeaderName(h):
			ck.add(fp, "invalid header name")
		}
	}
}

func checkAPIKeys(ck *checker, p string, a *APIKeyProvider) {
	header := *a.Header
	switch {
	case header == "" && a.Query == "":
		ck.add(p+".header", "header or query is required")
	case header != "" && !validHeaderName(header):
		ck.add(p+".header", "invalid header name")
	}
	switch {
	case a.ConsumerHeader == "":
		ck.add(p+".consumer_header", "is required")
	case !validHeaderName(a.ConsumerHeader):
		ck.add(p+".consumer_header", "invalid header name")
	}
	for _, k := range slices.Sorted(maps.Keys(a.MetadataHeaders)) {
		if !validHeaderName(a.MetadataHeaders[k]) {
			ck.add(p+".metadata_headers."+k, "invalid header name")
		}
	}
	if len(a.Keys) == 0 && a.KeyFile == "" {
		ck.add(p, "keys or key_file is required")
	}
	fileIDs := map[string]bool{}
	fileHashes := map[string]bool{}
	for _, fk := range a.FileKeys {
		fileIDs[fk.ID] = true
		fileHashes[strings.ToLower(fk.Hash.Reveal())] = true
	}
	ids := map[string]bool{}
	hashes := map[string]bool{}
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
		if key.ID != "" && fileIDs[key.ID] {
			ck.add(kp+".id", fmt.Sprintf("duplicate id %q (also in key_file)", key.ID))
		}
		if !apiKeyHash.MatchString(key.Hash.Reveal()) {
			ck.add(kp+".hash", "must have the form sha256:<64 hex digits>")
		} else {
			h := strings.ToLower(key.Hash.Reveal())
			switch {
			case hashes[h]:
				ck.add(kp+".hash", "duplicate hash")
			case fileHashes[h]:
				ck.add(kp+".hash", "duplicate hash (also in key_file)")
			}
			hashes[h] = true
		}
		if key.Consumer == "" {
			ck.add(kp+".consumer", "is required")
		}
		for i, s := range key.Scopes {
			if s == "" {
				ck.add(fmt.Sprintf("%s.scopes[%d]", kp, i), "must not be empty")
			}
		}
		for i, r := range key.Roles {
			if r == "" {
				ck.add(fmt.Sprintf("%s.roles[%d]", kp, i), "must not be empty")
			}
		}
	}
}

// checkRouteAuth validates a route's auth block. A route whose mode is not none always names at least one known provider.
func checkRouteAuth(ck *checker, p string, a *RouteAuth, kinds map[string]string) {
	if !slices.Contains([]string{"none", "jwt", "apikey", "any"}, a.Mode) {
		ck.add(p+".mode", "must be one of none, jwt, apikey, any")
	}
	hasRequire := len(a.Require.Scopes) > 0 || len(a.Require.Roles) > 0 || len(a.Require.Claims) > 0
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
	switch {
	case a.Mode == "none" && a.AllowAnonymous:
		ck.add(p+".allow_anonymous", "must not be set when mode is none")
	case a.AllowAnonymous && hasRequire:
		ck.add(p+".allow_anonymous", "cannot be combined with require")
	}
	if a.Mode == "none" && hasRequire {
		ck.add(p+".require", "must be empty when mode is none")
	}
	for i, s := range a.Require.Scopes {
		if s == "" {
			ck.add(fmt.Sprintf("%s.require.scopes[%d]", p, i), "must not be empty")
		}
	}
	for i, r := range a.Require.Roles {
		if r == "" {
			ck.add(fmt.Sprintf("%s.require.roles[%d]", p, i), "must not be empty")
		}
	}
	for _, name := range slices.Sorted(maps.Keys(a.Require.Claims)) {
		if name == "" {
			ck.add(p+".require.claims", "claim name must not be empty")
			continue
		}
		if !validClaimValue(a.Require.Claims[name]) {
			ck.add(p+".require.claims."+name, "must be a string, number or boolean")
		}
	}
}

// validClaimValue reports whether v is a string, boolean, integer or finite float64.
func validClaimValue(v any) bool {
	switch t := v.(type) {
	case string, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return true
	case float64:
		return !math.IsNaN(t) && !math.IsInf(t, 0)
	}
	return false
}

// checkIdentityHeaders rejects identity and forwarded headers that name a provider's credential header,
// because stripping or overwriting that header would disable authentication.
func checkIdentityHeaders(ck *checker, c *Config) {
	type credential struct{ header, provider string }
	var creds []credential
	for i := range c.Auth.JWT {
		creds = append(creds, credential{c.Auth.JWT[i].Header, c.Auth.JWT[i].Name})
	}
	for i := range c.Auth.APIKeys {
		if h := c.Auth.APIKeys[i].Header; h != nil && *h != "" {
			creds = append(creds, credential{*h, c.Auth.APIKeys[i].Name})
		}
	}
	check := func(path, value string) {
		for _, cr := range creds {
			if strings.EqualFold(cr.header, value) {
				ck.add(path, fmt.Sprintf("must not name the credential header %q of provider %q", cr.header, cr.provider))
				return
			}
		}
	}
	for i, h := range c.Global.StripIdentityHeaders {
		path := fmt.Sprintf("global.strip_identity_headers[%d]", i)
		if !validHeaderName(h) {
			ck.add(path, "invalid header name")
			continue
		}
		check(path, h)
	}
	for i := range c.Auth.JWT {
		p := fmt.Sprintf("auth.jwt[%d]", i)
		for _, claim := range slices.Sorted(maps.Keys(c.Auth.JWT[i].ForwardClaims)) {
			check(p+".forward_claims."+claim, c.Auth.JWT[i].ForwardClaims[claim])
		}
	}
	for i := range c.Auth.APIKeys {
		p := fmt.Sprintf("auth.api_keys[%d]", i)
		check(p+".consumer_header", c.Auth.APIKeys[i].ConsumerHeader)
		for _, k := range slices.Sorted(maps.Keys(c.Auth.APIKeys[i].MetadataHeaders)) {
			check(p+".metadata_headers."+k, c.Auth.APIKeys[i].MetadataHeaders[k])
		}
	}
}

// loadKeyMaterial reads every key file referenced by c and stores its content in the loaded fields.
// Relative paths resolve against dir, or against the working directory when dir is empty; resolved
// paths are stored back as absolute paths. Keys read from an API key file replace FileKeys.
func loadKeyMaterial(c *Config, dir string) []FieldError {
	abs := func(p string) string {
		if !filepath.IsAbs(p) && dir != "" {
			p = filepath.Join(dir, p)
		}
		if filepath.IsAbs(p) {
			return p
		}
		if a, err := filepath.Abs(p); err == nil {
			return a
		}
		return p
	}
	var errs []FieldError
	for i := range c.Auth.JWT {
		for k := range c.Auth.JWT[i].Keys {
			key := &c.Auth.JWT[i].Keys[k]
			key.LoadedPublicKey = ""
			if key.PublicKeyFile == "" {
				continue
			}
			key.PublicKeyFile = abs(key.PublicKeyFile)
			path := fmt.Sprintf("auth.jwt[%d].keys[%d].public_key_file", i, k)
			data, err := readSmall(key.PublicKeyFile)
			if err != nil {
				errs = append(errs, FieldError{Path: path, Msg: fmt.Sprintf("cannot read file: %v", err)})
				continue
			}
			if len(data) == 0 {
				errs = append(errs, FieldError{Path: path, Msg: "cannot read file: file is empty"})
				continue
			}
			key.LoadedPublicKey = string(data)
		}
	}
	for i := range c.Auth.APIKeys {
		p := &c.Auth.APIKeys[i]
		p.FileKeys = nil
		if p.KeyFile == "" {
			continue
		}
		p.KeyFile = abs(p.KeyFile)
		path := fmt.Sprintf("auth.api_keys[%d].key_file", i)
		data, err := readSmall(p.KeyFile)
		if err != nil {
			errs = append(errs, FieldError{Path: path, Msg: fmt.Sprintf("%s: %v", p.KeyFile, err)})
			continue
		}
		var kf APIKeyFile
		if err := yamlx.DecodeStrict(data, &kf); err != nil {
			errs = append(errs, FieldError{Path: path, Msg: fmt.Sprintf("%s: %s", p.KeyFile, sanitizeYAML(err.Error()))})
			continue
		}
		var root yaml.Node
		if err := yaml.Unmarshal(data, &root); err != nil {
			errs = append(errs, FieldError{Path: path, Msg: fmt.Sprintf("%s: %s", p.KeyFile, sanitizeYAML(err.Error()))})
			continue
		}
		if len(kf.Keys) == 0 {
			errs = append(errs, FieldError{Path: path, Msg: p.KeyFile + ": contains no keys"})
			continue
		}
		errs = append(errs, checkFileKeys(path, p.KeyFile, kf.Keys, buildIndex(&root))...)
		p.FileKeys = kf.Keys
	}
	return errs
}

// checkFileKeys validates the keys of an API key file. Each problem is reported with the file and line of the offending field.
func checkFileKeys(path, file string, keys []APIKey, idx *lineIndex) []FieldError {
	var errs []FieldError
	report := func(k int, field, msg string) {
		kp := fmt.Sprintf("keys[%d]", k)
		if field != "" {
			kp += "." + field
		}
		errs = append(errs, FieldError{Path: path, Msg: fmt.Sprintf("%s (line %d): %s: %s", file, idx.lineFor(kp), kp, msg)})
	}
	ids := map[string]bool{}
	hashes := map[string]bool{}
	for k := range keys {
		key := &keys[k]
		switch {
		case key.ID == "":
			report(k, "id", "is required")
		case ids[key.ID]:
			report(k, "id", fmt.Sprintf("duplicate id %q", key.ID))
		default:
			ids[key.ID] = true
		}
		if !apiKeyHash.MatchString(key.Hash.Reveal()) {
			report(k, "hash", "must have the form sha256:<64 hex digits>")
		} else {
			h := strings.ToLower(key.Hash.Reveal())
			if hashes[h] {
				report(k, "hash", "duplicate hash")
			}
			hashes[h] = true
		}
		if key.Consumer == "" {
			report(k, "consumer", "is required")
		}
		for i, s := range key.Scopes {
			if s == "" {
				report(k, fmt.Sprintf("scopes[%d]", i), "must not be empty")
			}
		}
		for i, r := range key.Roles {
			if r == "" {
				report(k, fmt.Sprintf("roles[%d]", i), "must not be empty")
			}
		}
	}
	return errs
}

// readSmall reads a regular file of at most 1 MiB.
func readSmall(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxKeyFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxKeyFileBytes {
		return nil, errors.New("file exceeds 1 MiB")
	}
	return data, nil
}

// sanitizeYAML replaces every backquoted value in a YAML error message so that no content of a file is echoed, and removes
// the column and type prefix of custom decoder errors.
func sanitizeYAML(msg string) string {
	return yamlTypeRe.ReplaceAllString(yamlValueRe.ReplaceAllString(msg, "`…`"), "")
}

// KeyFiles returns the absolute key_file and public_key_file paths referenced by c, in configuration order, without duplicates.
func (c *Config) KeyFiles() []string {
	type entry struct {
		path string
		line int
	}
	var entries []entry
	for i := range c.Auth.JWT {
		for k := range c.Auth.JWT[i].Keys {
			if f := c.Auth.JWT[i].Keys[k].PublicKeyFile; f != "" {
				entries = append(entries, entry{f, c.Line(fmt.Sprintf("auth.jwt[%d].keys[%d].public_key_file", i, k))})
			}
		}
	}
	for i := range c.Auth.APIKeys {
		if f := c.Auth.APIKeys[i].KeyFile; f != "" {
			entries = append(entries, entry{f, c.Line(fmt.Sprintf("auth.api_keys[%d].key_file", i))})
		}
	}
	slices.SortStableFunc(entries, func(a, b entry) int {
		return cmp.Compare(a.line, b.line)
	})
	seen := map[string]bool{}
	var out []string
	for _, e := range entries {
		if !seen[e.path] {
			seen[e.path] = true
			out = append(out, e.path)
		}
	}
	return out
}
