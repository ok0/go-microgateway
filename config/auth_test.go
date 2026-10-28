package config

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	testHS32 = "0123456789abcdef0123456789abcdef"
	testHash = "sha256:9771189e33ad49531f98873eb4e4458ce1c96f313fe15c6d753f4c7b6ad4eba5"
)

var testHashUC = "sha256:" + strings.ToUpper(strings.TrimPrefix(testHash, "sha256:"))

const authJWTHead = "auth:\n  jwt:\n    - name: p\n      issuer: https://idp.example\n      audiences: [api]\n"

const authJWTHS = authJWTHead + "      keys:\n        - algorithm: HS256\n          secret: " + testHS32 + "\n"

const authAPIHead = "auth:\n  api_keys:\n    - name: k\n"

func durPtr(d time.Duration) *Duration {
	v := Duration(d)
	return &v
}

func writeTestFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func rsaPublicPEM(t *testing.T, bits int) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func ecPublicPEM(t *testing.T, curve elliptic.Curve) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(curve, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func ed25519PublicPEM(t *testing.T) string {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
}

func findFieldError(err error, path, substr string) bool {
	var ve *ValidationError
	if !errors.As(err, &ve) {
		return false
	}
	for _, fe := range ve.Errors {
		if fe.Path == path && strings.Contains(fe.Msg, substr) {
			return true
		}
	}
	return false
}

func TestAuthValidation(t *testing.T) {
	rsa1024 := strconv.Quote(rsaPublicPEM(t, 1024))
	rsa2048 := strconv.Quote(rsaPublicPEM(t, 2048))
	p384 := strconv.Quote(ecPublicPEM(t, elliptic.P384()))
	p256 := strconv.Quote(ecPublicPEM(t, elliptic.P256()))
	ed := strconv.Quote(ed25519PublicPEM(t))
	jwksOnly := authJWTHead + "      jwks:\n        url: https://idp.example/jwks\n        algorithms: [RS256]\n"

	cases := []struct {
		name    string
		snippet string
		path    string
		msg     string
	}{
		{"PS256 rejected", authJWTHead + "      keys:\n        - algorithm: PS256\n          public_key: " + rsa2048 + "\n", "auth.jwt[0].keys[0].algorithm", "must be one of"},
		{"none rejected", authJWTHead + "      keys:\n        - algorithm: none\n          secret: " + testHS32 + "\n", "auth.jwt[0].keys[0].algorithm", "must be one of"},
		{"HS256 secret 31 bytes", authJWTHead + "      keys:\n        - algorithm: HS256\n          secret: " + testHS32[:31] + "\n", "auth.jwt[0].keys[0].secret", "at least 32 bytes"},
		{"HS256 secret 32 bytes", authJWTHS, "", ""},
		{"HS384 secret 47 bytes", authJWTHead + "      keys:\n        - algorithm: HS384\n          secret: " + strings.Repeat("s", 47) + "\n", "auth.jwt[0].keys[0].secret", "at least 48 bytes"},
		{"RSA 1024 for RS256", authJWTHead + "      keys:\n        - algorithm: RS256\n          public_key: " + rsa1024 + "\n", "auth.jwt[0].keys[0].public_key", "at least 2048 bits"},
		{"RSA 2048 for RS256", authJWTHead + "      keys:\n        - algorithm: RS256\n          public_key: " + rsa2048 + "\n", "", ""},
		{"P-384 for ES256", authJWTHead + "      keys:\n        - algorithm: ES256\n          public_key: " + p384 + "\n", "auth.jwt[0].keys[0].public_key", "requires an ECDSA P-256 public key"},
		{"P-256 for ES256", authJWTHead + "      keys:\n        - algorithm: ES256\n          public_key: " + p256 + "\n", "", ""},
		{"Ed25519 for ES256", authJWTHead + "      keys:\n        - algorithm: ES256\n          public_key: " + ed + "\n", "auth.jwt[0].keys[0].public_key", "requires an ECDSA P-256 public key"},
		{"HS with jwks", authJWTHS + "      jwks:\n        url: https://idp.example/jwks\n        algorithms: [RS256]\n", "auth.jwt[0].keys", "cannot be combined"},
		{"HS with RS", authJWTHS + "        - algorithm: RS256\n          public_key: " + rsa2048 + "\n", "auth.jwt[0].keys", "cannot be combined"},
		{"duplicate kid", authJWTHead + "      keys:\n        - kid: a\n          algorithm: HS256\n          secret: " + testHS32 + "\n        - kid: a\n          algorithm: HS256\n          secret: " + testHS32 + "\n", "auth.jwt[0].keys[1].kid", "duplicate kid"},
		{"missing kid with two keys", authJWTHead + "      keys:\n        - kid: a\n          algorithm: HS256\n          secret: " + testHS32 + "\n        - algorithm: HS256\n          secret: " + testHS32 + "\n", "auth.jwt[0].keys[1].kid", "is required when there is more than one key"},
		{"jwks algorithms empty", authJWTHead + "      jwks:\n        url: https://idp.example/jwks\n        algorithms: []\n", "auth.jwt[0].jwks.algorithms", "at least one algorithm is required"},
		{"jwks algorithms with HS256", authJWTHead + "      jwks:\n        url: https://idp.example/jwks\n        algorithms: [HS256]\n", "auth.jwt[0].jwks.algorithms[0]", "must be one of"},
		{"jwks algorithms duplicate", authJWTHead + "      jwks:\n        url: https://idp.example/jwks\n        algorithms: [RS256, RS256]\n", "auth.jwt[0].jwks.algorithms[1]", "duplicate algorithm"},
		{"jwks ttl zero", jwksOnly + "        ttl: 0s\n", "auth.jwt[0].jwks.ttl", "must be positive"},
		{"jwks min_refresh_interval negative", jwksOnly + "        min_refresh_interval: -1s\n", "auth.jwt[0].jwks.min_refresh_interval", "must be positive"},
		{"jwks timeout zero", jwksOnly + "        timeout: 0s\n", "auth.jwt[0].jwks.timeout", "must be positive"},
		{"max_token_bytes zero", authJWTHS + "      max_token_bytes: 0\n", "auth.jwt[0].max_token_bytes", "must be positive"},
		{"max_token_bytes negative", authJWTHS + "      max_token_bytes: -1\n", "auth.jwt[0].max_token_bytes", "must be positive"},
		{"leeway negative", authJWTHS + "      leeway: -1s\n", "auth.jwt[0].leeway", "must not be negative"},
		{"leeway above 5m", authJWTHS + "      leeway: 6m\n", "auth.jwt[0].leeway", "must not exceed 5m"},
		{"leeway zero accepted", authJWTHS + "      leeway: 0s\n", "", ""},
		{"jwt header invalid", authJWTHS + "      header: bad header\n", "auth.jwt[0].header", "invalid header name"},
		{"audience empty", "auth:\n  jwt:\n    - name: p\n      issuer: https://idp.example\n      audiences: [\"\"]\n      keys:\n        - algorithm: HS256\n          secret: " + testHS32 + "\n", "auth.jwt[0].audiences[0]", "must not be empty"},
		{"forward claim empty", authJWTHS + "      forward_claims:\n        sub: \"\"\n", "auth.jwt[0].forward_claims.sub", "must not be empty"},
		{"forward claim invalid header", authJWTHS + "      forward_claims:\n        sub: bad header\n", "auth.jwt[0].forward_claims.sub", "invalid header name"},
		{"api header empty without query", authAPIHead + "      header: \"\"\n      keys:\n        - id: a\n          hash: " + testHash + "\n          consumer: alice\n", "auth.api_keys[0].header", "header or query is required"},
		{"api header empty with query", authAPIHead + "      header: \"\"\n      query: api_key\n      keys:\n        - id: a\n          hash: " + testHash + "\n          consumer: alice\n", "", ""},
		{"api duplicate inline hash", authAPIHead + "      keys:\n        - id: a\n          hash: " + testHash + "\n          consumer: alice\n        - id: b\n          hash: " + testHashUC + "\n          consumer: bob\n", "auth.api_keys[0].keys[1].hash", "duplicate hash"},
		{"api keys without keys or key_file", authAPIHead, "auth.api_keys[0]", "keys or key_file is required"},
		{"api scope empty", authAPIHead + "      keys:\n        - id: a\n          hash: " + testHash + "\n          consumer: alice\n          scopes: [\"\"]\n", "auth.api_keys[0].keys[0].scopes[0]", "must not be empty"},
		{"api metadata header invalid", authAPIHead + "      metadata_headers:\n        tier: bad header\n      keys:\n        - id: a\n          hash: " + testHash + "\n          consumer: alice\n", "auth.api_keys[0].metadata_headers.tier", "invalid header name"},
		{"strip Authorization", "global:\n  strip_identity_headers:\n    - Authorization\n" + authJWTHS, "global.strip_identity_headers[0]", "must not name the credential header"},
		{"forward claim equal to api key header", authJWTHead + "      forward_claims:\n        sub: X-API-Key\n      keys:\n        - algorithm: HS256\n          secret: " + testHS32 + "\n" + "  api_keys:\n    - name: k\n      keys:\n        - id: a\n          hash: " + testHash + "\n          consumer: alice\n", "auth.jwt[0].forward_claims.sub", "must not name the credential header"},
		{"allow_anonymous with mode none", "    auth:\n      mode: none\n      allow_anonymous: true\n", "routes[0].auth.allow_anonymous", "must not be set when mode is none"},
		{"require with mode none", "    auth:\n      mode: none\n      require:\n        scopes: [read]\n", "routes[0].auth.require", "must be empty when mode is none"},
		{"allow_anonymous with require", "    auth:\n      mode: jwt\n      providers: [p]\n      allow_anonymous: true\n      require:\n        scopes: [read]\n" + authJWTHS, "routes[0].auth.allow_anonymous", "cannot be combined with require"},
		{"require claim list value", "    auth:\n      mode: jwt\n      providers: [p]\n      require:\n        claims:\n          x: [1]\n" + authJWTHS, "routes[0].auth.require.claims.x", "must be a string, number or boolean"},
		{"require claim empty name", "    auth:\n      mode: jwt\n      providers: [p]\n      require:\n        claims:\n          \"\": 1\n" + authJWTHS, "routes[0].auth.require.claims", "claim name must not be empty"},
		{"require scope empty", "    auth:\n      mode: jwt\n      providers: [p]\n      require:\n        scopes: [\"\"]\n" + authJWTHS, "routes[0].auth.require.scopes[0]", "must not be empty"},
		{"explicit pipeline without auth", "    auth:\n      mode: jwt\n      providers: [p]\n    pipeline: [timeout]\n" + authJWTHS, "routes[0].pipeline", `must include "auth"`},
		{"jwt mode without providers", "    auth:\n      mode: jwt\n", "routes[0].auth.providers", "at least one provider is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(minimalYAML + tc.snippet))
			if tc.path == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !findFieldError(err, tc.path, tc.msg) {
				t.Errorf("no error at %s containing %q; got %v", tc.path, tc.msg, err)
			}
		})
	}
}

func TestAuthDefaultsPointer(t *testing.T) {
	c, err := Parse([]byte(minimalYAML + authJWTHead + "      keys:\n        - algorithm: HS256\n          secret: " + testHS32 + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p := c.Auth.JWT[0]; p.Leeway == nil || p.Leeway.Std() != 30*time.Second {
		t.Errorf("absent leeway = %v, want 30s", p.Leeway)
	}
	if p := c.Auth.JWT[0]; p.MaxTokenBytes == nil || *p.MaxTokenBytes != 4096 {
		t.Errorf("absent max_token_bytes = %v, want 4096", p.MaxTokenBytes)
	}
	if p := c.Auth.JWT[0]; p.ForwardCredentials == nil || *p.ForwardCredentials {
		t.Errorf("absent forward_credentials = %v, want false", p.ForwardCredentials)
	}

	c, err = Parse([]byte(minimalYAML + authJWTHS + "      leeway: 0s\n"))
	if err != nil {
		t.Fatal(err)
	}
	if p := c.Auth.JWT[0]; p.Leeway == nil || p.Leeway.Std() != 0 {
		t.Errorf("explicit leeway 0s = %v, want 0", p.Leeway)
	}

	c, err = Parse([]byte(minimalYAML + authJWTHead + "      jwks:\n        url: https://idp.example/jwks\n        algorithms: [RS256]\n"))
	if err != nil {
		t.Fatal(err)
	}
	j := c.Auth.JWT[0].JWKS
	if j.TTL == nil || j.TTL.Std() != 10*time.Minute {
		t.Errorf("absent jwks ttl = %v, want 10m", j.TTL)
	}
	if j.MinRefreshInterval == nil || j.MinRefreshInterval.Std() != 30*time.Second {
		t.Errorf("absent jwks min_refresh_interval = %v, want 30s", j.MinRefreshInterval)
	}
	if j.Timeout == nil || j.Timeout.Std() != 5*time.Second {
		t.Errorf("absent jwks timeout = %v, want 5s", j.Timeout)
	}

	c, err = Parse([]byte(minimalYAML + authAPIHead + "      keys:\n        - id: a\n          hash: " + testHash + "\n          consumer: alice\n"))
	if err != nil {
		t.Fatal(err)
	}
	if h := c.Auth.APIKeys[0].Header; h == nil || *h != "X-API-Key" {
		t.Errorf("absent api header = %v, want X-API-Key", h)
	}
	if f := c.Auth.APIKeys[0].ForwardCredentials; f == nil || *f {
		t.Errorf("absent api forward_credentials = %v, want false", f)
	}

	c, err = Parse([]byte(strings.Replace(minimalYAML, "version: 1\n", "version: 1\nglobal:\n  strip_identity_headers: []\n", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if c.Global.StripIdentityHeaders == nil || len(c.Global.StripIdentityHeaders) != 0 {
		t.Errorf("explicit empty strip list = %#v", c.Global.StripIdentityHeaders)
	}
	c, err = Parse([]byte(minimalYAML))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"X-Consumer-ID", "X-Consumer-Metadata", "X-User-ID", "X-Auth-Subject", "X-Auth-Claims"}
	if strings.Join(c.Global.StripIdentityHeaders, ",") != strings.Join(want, ",") {
		t.Errorf("default strip list = %v", c.Global.StripIdentityHeaders)
	}

	c, err = Parse([]byte(strings.Replace(minimalYAML, "    hosts:\n", "    active_health: {}\n    hosts:\n", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if ah := c.Upstreams[0].ActiveHealth; ah.HealthyThreshold == nil || *ah.HealthyThreshold != 2 || ah.UnhealthyThreshold == nil || *ah.UnhealthyThreshold != 3 {
		t.Errorf("active health thresholds = %v/%v, want 2/3", ah.HealthyThreshold, ah.UnhealthyThreshold)
	}
	_, err = Parse([]byte(strings.Replace(minimalYAML, "    hosts:\n", "    active_health:\n      healthy_threshold: 0\n    hosts:\n", 1)))
	if !findFieldError(err, "upstreams[0].active_health.healthy_threshold", "must be >= 1") {
		t.Errorf("explicit healthy_threshold 0 not rejected: %v", err)
	}

	c, err = Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if rl := c.Routes[0].RateLimit; rl.MaxKeys == nil || *rl.MaxKeys != 100000 {
		t.Errorf("absent max_keys = %v, want 100000", rl.MaxKeys)
	}
	_, err = Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 1\n      max_keys: 0\n"))
	if !findFieldError(err, "routes[0].rate_limit.max_keys", "must be >= 1") {
		t.Errorf("explicit max_keys 0 not rejected: %v", err)
	}
}

func TestSeededAuthErrors(t *testing.T) {
	_, err := LoadFile("testdata/auth_errors.yaml")
	want := []struct {
		path, msg string
	}{
		{"auth.jwt[0].issuer", "is required"},
		{"auth.jwt[1].audiences", "at least one audience"},
		{"auth.jwt[2].keys[0].secret", "at least 32 bytes"},
		{"auth.jwt[2].max_token_bytes", "must be positive"},
		{"auth.jwt[3].keys[0].public_key", "not allowed with HS"},
		{"auth.jwt[4].keys[0].algorithm", "must be one of"},
		{"auth.jwt[5].jwks.ttl", "must be positive"},
		{"auth.api_keys[0].keys[0].hash", "must have the form"},
		{"routes[0].auth.providers[0]", "unknown auth provider"},
		{"routes[1].auth.providers[0]", "is not an apikey provider"},
		{"routes[2].auth.allow_anonymous", "cannot be combined with require"},
		{"routes[3].pipeline", `must include "auth"`},
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	for _, w := range want {
		found := false
		for _, fe := range ve.Errors {
			if fe.Path == w.path && strings.Contains(fe.Msg, w.msg) {
				found = true
				if fe.Line <= 0 {
					t.Errorf("%s has no line number", w.path)
				}
			}
		}
		if !found {
			t.Errorf("no error at %s containing %q; got %v", w.path, w.msg, err)
		}
	}
}

func TestKeyMaterialLoading(t *testing.T) {
	const apiKeysFile = `keys:
  - id: file-key
    hash: "sha256:dff38ba3323b08b8f7a333da8f5b561106e69523bfdd4fa46e89eeb3d1a7e84a"
    consumer: file-consumer
`
	pemA := ed25519PublicPEM(t)
	pemB := ed25519PublicPEM(t)
	config := func(extra string) string {
		return minimalYAML + "auth:\n  jwt:\n    - name: pk\n      issuer: https://auth.example.com\n      audiences: [example-api]\n      keys:\n        - algorithm: EdDSA\n          public_key_file: pub.pem\n  api_keys:\n    - name: file\n      key_file: keys.yaml\n" + extra
	}

	t.Run("LoadFile resolves relative paths and loads content", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "keys.yaml", apiKeysFile)
		writeTestFile(t, dir, "pub.pem", pemA)
		path := writeTestFile(t, dir, "lychgate.yaml", config(""))

		c, err := LoadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		pk := c.Auth.JWT[0].Keys[0]
		if !filepath.IsAbs(pk.PublicKeyFile) || pk.PublicKeyFile != filepath.Join(dir, "pub.pem") {
			t.Errorf("PublicKeyFile = %q", pk.PublicKeyFile)
		}
		if pk.LoadedPublicKey != pemA {
			t.Errorf("LoadedPublicKey not loaded from file")
		}
		ak := c.Auth.APIKeys[0]
		if !filepath.IsAbs(ak.KeyFile) || ak.KeyFile != filepath.Join(dir, "keys.yaml") {
			t.Errorf("KeyFile = %q", ak.KeyFile)
		}
		if len(ak.FileKeys) != 1 || ak.FileKeys[0].ID != "file-key" {
			t.Fatalf("FileKeys = %+v", ak.FileKeys)
		}
		if len(ak.Keys) != 0 {
			t.Errorf("file keys were added to Keys: %+v", ak.Keys)
		}

		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		if n := len(c.Auth.APIKeys[0].FileKeys); n != 1 {
			t.Errorf("Validate duplicated file keys: %d", n)
		}
		if c.Auth.JWT[0].Keys[0].LoadedPublicKey != pemA {
			t.Error("Validate changed the loaded public key")
		}
	})

	t.Run("Parse with WithBaseDir", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "keys.yaml", apiKeysFile)
		writeTestFile(t, dir, "pub.pem", pemA)
		c, err := Parse([]byte(config("")), WithBaseDir(dir))
		if err != nil {
			t.Fatal(err)
		}
		if c.Auth.APIKeys[0].KeyFile != filepath.Join(dir, "keys.yaml") || len(c.Auth.APIKeys[0].FileKeys) != 1 {
			t.Errorf("api key file not resolved against base dir: %+v", c.Auth.APIKeys[0])
		}
		if c.Auth.JWT[0].Keys[0].LoadedPublicKey != pemA {
			t.Error("public key not resolved against base dir")
		}
	})

	t.Run("Parse without base dir uses the working directory", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "keys.yaml", apiKeysFile)
		writeTestFile(t, dir, "pub.pem", pemA)
		t.Chdir(dir)
		c, err := Parse([]byte(config("")))
		if err != nil {
			t.Fatal(err)
		}
		p := c.Auth.JWT[0].Keys[0].PublicKeyFile
		if !filepath.IsAbs(p) || filepath.Base(p) != "pub.pem" {
			t.Fatalf("PublicKeyFile = %q", p)
		}
		if info, err := os.Stat(p); err != nil || !os.SameFile(info, mustStat(t, filepath.Join(dir, "pub.pem"))) {
			t.Errorf("PublicKeyFile %q does not name the working directory file", p)
		}
		if c.Auth.JWT[0].Keys[0].LoadedPublicKey != pemA {
			t.Error("public key not loaded from the working directory")
		}
	})

	t.Run("Go-built config loads KeyFile on Validate and clears stale FileKeys", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "keys.yaml", apiKeysFile)
		t.Chdir(dir)
		c := goConfig(1)
		c.Auth.APIKeys = []APIKeyProvider{{Name: "file", KeyFile: "keys.yaml"}}
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
		if len(c.Auth.APIKeys[0].FileKeys) != 1 || !filepath.IsAbs(c.Auth.APIKeys[0].KeyFile) {
			t.Errorf("Go-built KeyFile not loaded: %+v", c.Auth.APIKeys[0])
		}

		d := goConfig(1)
		d.Auth.APIKeys = []APIKeyProvider{{
			Name:     "inline",
			Keys:     []APIKey{{ID: "inline-1", Hash: testHash, Consumer: "alice"}},
			FileKeys: []APIKey{{ID: "stale", Hash: testHash, Consumer: "stale"}},
		}}
		if err := d.Validate(); err != nil {
			t.Fatal(err)
		}
		if d.Auth.APIKeys[0].FileKeys != nil {
			t.Errorf("caller-set FileKeys without KeyFile were kept: %+v", d.Auth.APIKeys[0].FileKeys)
		}
	})

	t.Run("fingerprint changes with key file and PEM content", func(t *testing.T) {
		dir := t.TempDir()
		keysPath := writeTestFile(t, dir, "keys.yaml", apiKeysFile)
		pemPath := writeTestFile(t, dir, "pub.pem", pemA)
		path := writeTestFile(t, dir, "lychgate.yaml", config(""))
		load := func() string {
			c, err := LoadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			return c.Fingerprint()
		}
		before := load()
		if err := os.WriteFile(keysPath, []byte(strings.Replace(apiKeysFile, "file-key", "other-key", 1)), 0o600); err != nil {
			t.Fatal(err)
		}
		afterKeys := load()
		if afterKeys == before {
			t.Error("fingerprint did not change when the key file changed")
		}
		if err := os.WriteFile(pemPath, []byte(pemB), 0o600); err != nil {
			t.Fatal(err)
		}
		if load() == afterKeys {
			t.Error("fingerprint did not change when the PEM file changed")
		}
	})

	t.Run("bad hash in key file reports line and hides the value", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "keys.yaml", "keys:\n  - id: bad-1\n    hash: \"sha256:not-a-valid-hash-value\"\n    consumer: x\n")
		path := writeTestFile(t, dir, "lychgate.yaml", minimalYAML+"auth:\n  api_keys:\n    - name: file\n      key_file: keys.yaml\n")
		_, err := LoadFile(path)
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("want *ValidationError, got %v", err)
		}
		var found bool
		for _, fe := range ve.Errors {
			if fe.Path != "auth.api_keys[0].key_file" {
				continue
			}
			found = true
			if !strings.Contains(fe.Msg, "(line 3)") || !strings.Contains(fe.Msg, "keys[0].hash") {
				t.Errorf("message lacks line and field: %q", fe.Msg)
			}
			if strings.Contains(err.Error(), "not-a-valid-hash-value") {
				t.Errorf("error echoes the hash value: %v", err)
			}
		}
		if !found {
			t.Errorf("no error at auth.api_keys[0].key_file: %v", err)
		}
	})

	t.Run("malformed YAML key file reports a line", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "keys.yaml", "keys:\n  - id: [\n")
		path := writeTestFile(t, dir, "lychgate.yaml", minimalYAML+"auth:\n  api_keys:\n    - name: file\n      key_file: keys.yaml\n")
		_, err := LoadFile(path)
		if !findFieldError(err, "auth.api_keys[0].key_file", "line") {
			t.Errorf("malformed key file not reported with a line: %v", err)
		}
	})

	t.Run("missing and directory key files are rejected", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
			t.Fatal(err)
		}
		path := writeTestFile(t, dir, "missing.yaml", minimalYAML+"auth:\n  api_keys:\n    - name: file\n      key_file: nope.yaml\n")
		if _, err := LoadFile(path); !findFieldError(err, "auth.api_keys[0].key_file", "") {
			t.Errorf("missing key file not reported: %v", err)
		}
		dirPath := writeTestFile(t, dir, "dir.yaml", minimalYAML+"auth:\n  api_keys:\n    - name: file\n      key_file: sub\n")
		if _, err := LoadFile(dirPath); !findFieldError(err, "auth.api_keys[0].key_file", "not a regular file") {
			t.Errorf("directory key file not rejected: %v", err)
		}
	})

	t.Run("key file id equal to inline id is a duplicate", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "keys.yaml", apiKeysFile)
		src := minimalYAML + "auth:\n  api_keys:\n    - name: file\n      key_file: keys.yaml\n      keys:\n        - id: file-key\n          hash: " + testHash + "\n          consumer: alice\n"
		path := writeTestFile(t, dir, "lychgate.yaml", src)
		_, err := LoadFile(path)
		if !findFieldError(err, "auth.api_keys[0].keys[0].id", `duplicate id "file-key" (also in key_file)`) {
			t.Errorf("duplicate id with key file not reported: %v", err)
		}
	})

	t.Run("empty key file contains no keys", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "keys.yaml", "keys: []\n")
		path := writeTestFile(t, dir, "lychgate.yaml", minimalYAML+"auth:\n  api_keys:\n    - name: file\n      key_file: keys.yaml\n")
		_, err := LoadFile(path)
		if !findFieldError(err, "auth.api_keys[0].key_file", "contains no keys") {
			t.Errorf("empty key file not rejected: %v", err)
		}
	})

	t.Run("oversized public key file is rejected", func(t *testing.T) {
		dir := t.TempDir()
		writeTestFile(t, dir, "pub.pem", strings.Repeat("A", 1<<20+1))
		path := writeTestFile(t, dir, "lychgate.yaml", config(""))
		_, err := LoadFile(path)
		if !findFieldError(err, "auth.jwt[0].keys[0].public_key_file", "file exceeds 1 MiB") {
			t.Errorf("oversized public key file not rejected: %v", err)
		}
	})
}

func mustStat(t *testing.T, path string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func TestKeyFiles(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, dir, "keys.yaml", "keys:\n  - id: f\n    hash: \"sha256:dff38ba3323b08b8f7a333da8f5b561106e69523bfdd4fa46e89eeb3d1a7e84a\"\n    consumer: f\n")
	writeTestFile(t, dir, "pub.pem", ed25519PublicPEM(t))
	src := minimalYAML + fmt.Sprintf(`auth:
  jwt:
    - name: pk
      issuer: https://auth.example.com
      audiences: [example-api]
      keys:
        - kid: a
          algorithm: EdDSA
          public_key_file: pub.pem
        - kid: b
          algorithm: EdDSA
          public_key_file: pub.pem
  api_keys:
    - name: file
      key_file: keys.yaml
`)
	path := writeTestFile(t, dir, "lychgate.yaml", src)
	c, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := c.KeyFiles()
	want := []string{filepath.Join(dir, "pub.pem"), filepath.Join(dir, "keys.yaml")}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("KeyFiles() = %q, want %q", got, want)
	}
}
