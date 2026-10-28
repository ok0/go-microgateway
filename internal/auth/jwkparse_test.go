package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"math/big"
	"slices"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/authkey"
)

func TestParseJWKS(t *testing.T) {
	ks := loadKeyset()
	rsa1 := &ks.rsa1.PublicKey
	p256 := &ks.p256.PublicKey
	p384 := &ks.p384.PublicKey
	p521 := &ks.p521.PublicKey
	algs := authkey.Algorithms

	weak, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}

	withAlg := func(m map[string]any, alg string) map[string]any {
		m["alg"] = alg
		return m
	}
	mutated := func(m map[string]any, field string, value any) map[string]any {
		m[field] = value
		return m
	}

	cases := []struct {
		name    string
		data    []byte
		kids    []string
		wantErr string
		check   func(t *testing.T, keys []Key)
	}{
		{
			name: "rsa without alg is unpinned",
			data: jwksBytes(jwk("r", "", rsa1)),
			kids: []string{"r"},
			check: func(t *testing.T, keys []Key) {
				if keys[0].Algorithm != "" || !slices.Equal(keys[0].Allowed, algs) {
					t.Fatalf("key = %+v, want unpinned with all algorithms", keys[0])
				}
			},
		},
		{
			name: "rsa with alg is pinned",
			data: jwksBytes(jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
			check: func(t *testing.T, keys []Key) {
				if keys[0].Algorithm != "RS256" {
					t.Fatalf("algorithm = %q, want RS256", keys[0].Algorithm)
				}
				if _, ok := keys[0].Material.(*rsa.PublicKey); !ok {
					t.Fatalf("material type %T", keys[0].Material)
				}
			},
		},
		{
			name: "ec curves",
			data: jwksBytes(jwk("e256", "ES256", p256), jwk("e384", "", p384), jwk("e521", "ES512", p521)),
			kids: []string{"e256", "e384", "e521"},
		},
		{
			name: "ed25519",
			data: jwksBytes(jwk("ed", "EdDSA", ks.edPub)),
			kids: []string{"ed"},
		},
		{
			name: "oct skipped",
			data: jwksBytes(map[string]any{"kty": "oct", "kid": "o", "k": "AAAA"}, jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "enc use skipped",
			data: jwksBytes(mutated(jwk("r", "RS256", rsa1), "use", "enc"), jwk("r2", "RS256", rsa1)),
			kids: []string{"r2"},
		},
		{
			name: "sig use kept",
			data: jwksBytes(mutated(jwk("r", "RS256", rsa1), "use", "sig")),
			kids: []string{"r"},
		},
		{
			name: "weak rsa skipped",
			data: jwksBytes(jwk("w", "RS256", &weak.PublicKey), jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "alg not in list skipped",
			data: jwksBytes(withAlg(jwk("ps", "", rsa1), "PS256"), jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "alg incompatible with curve skipped",
			data: jwksBytes(withAlg(jwk("e", "", p256), "ES384"), jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "bad curve point skipped",
			data: jwksBytes(mutated(jwk("e", "ES256", p256), "y", b64([]byte("not-a-point-of-the-right-length-xxxx"))), jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "unknown curve skipped",
			data: jwksBytes(mutated(jwk("e", "ES256", p256), "crv", "P-192"), jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "short ec coordinate skipped",
			data: jwksBytes(mutated(jwk("e", "ES256", p256), "x", b64([]byte{1, 2, 3})), jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "ed25519 wrong curve skipped",
			data: jwksBytes(mutated(jwk("ed", "EdDSA", ks.edPub), "crv", "X25519"), jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "ed25519 wrong length skipped",
			data: jwksBytes(mutated(jwk("ed", "EdDSA", ks.edPub), "x", b64([]byte{1})), jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "unknown key type skipped",
			data: jwksBytes(map[string]any{"kty": "FOO", "kid": "f"}, jwk("r", "RS256", rsa1)),
			kids: []string{"r"},
		},
		{
			name: "bad base64 skipped",
			data: jwksBytes(mutated(jwk("r", "RS256", rsa1), "n", "!!!"), jwk("r2", "RS256", rsa1)),
			kids: []string{"r2"},
		},
		{
			name: "even exponent skipped",
			data: jwksBytes(mutated(jwk("r", "RS256", rsa1), "e", b64([]byte{2})), jwk("r2", "RS256", rsa1)),
			kids: []string{"r2"},
		},
		{
			name: "exponent one skipped",
			data: jwksBytes(mutated(jwk("r", "RS256", rsa1), "e", b64([]byte{1})), jwk("r2", "RS256", rsa1)),
			kids: []string{"r2"},
		},
		{
			name: "exponent too large skipped",
			data: jwksBytes(mutated(jwk("r", "RS256", rsa1), "e", b64(new(big.Int).Lsh(big.NewInt(1), 40).Bytes())), jwk("r2", "RS256", rsa1)),
			kids: []string{"r2"},
		},
		{
			name: "malformed entry skipped",
			data: []byte(`{"keys":[5, {"kty":"RSA","kid":true}, ` + mustJSON(t, jwk("r", "RS256", rsa1)) + `]}`),
			kids: []string{"r"},
		},
		{
			name: "duplicate kid keeps first",
			data: jwksBytes(jwk("d", "RS256", rsa1), jwk("d", "RS256", &ks.rsa2.PublicKey)),
			kids: []string{"d"},
			check: func(t *testing.T, keys []Key) {
				if keys[0].Material.(*rsa.PublicKey).N.Cmp(rsa1.N) != 0 {
					t.Fatal("duplicate kid did not keep the first key")
				}
			},
		},
		{
			name:    "invalid json",
			data:    []byte("{"),
			wantErr: "invalid JWKS document",
		},
		{
			name:    "empty set",
			data:    jwksBytes(),
			wantErr: "no usable keys",
		},
		{
			name:    "only unusable keys",
			data:    jwksBytes(map[string]any{"kty": "oct", "kid": "o"}),
			wantErr: "no usable keys",
		},
		{
			name:    "keys not an array",
			data:    []byte(`{"keys":"x"}`),
			wantErr: "invalid JWKS document",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			keys, err := ParseJWKS(tc.data, algs)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseJWKS: %v", err)
			}
			var got []string
			for _, k := range keys {
				got = append(got, k.ID)
			}
			if !slices.Equal(got, tc.kids) {
				t.Fatalf("kids = %v, want %v", got, tc.kids)
			}
			if tc.check != nil {
				tc.check(t, keys)
			}
		})
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
