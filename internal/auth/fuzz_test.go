package auth

import (
	"net/http"
	"testing"

	"github.com/ok0/go-microgateway/internal/authkey"
	"github.com/ok0/go-microgateway/internal/clock"
)

func FuzzParseJWKS(f *testing.F) {
	ks := loadKeyset()
	f.Add(jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey), jwk("es256", "ES256", &ks.p256.PublicKey), jwk("ed", "", ks.edPub)))
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"keys":[{"kty":"oct"}]}`))
	f.Add([]byte("garbage"))

	f.Fuzz(func(t *testing.T, data []byte) {
		keys, err := ParseJWKS(data, authkey.Algorithms)
		if err != nil {
			return
		}
		for _, k := range keys {
			if k.Algorithm != "" {
				if authkey.Check(k.Algorithm, k.Material) != nil {
					t.Fatalf("pinned key %q fails its own check", k.Algorithm)
				}
				continue
			}
			ok := false
			for _, a := range k.Allowed {
				if authkey.Check(a, k.Material) == nil {
					ok = true
					break
				}
			}
			if !ok {
				t.Fatal("unpinned key is not usable with any allowed algorithm")
			}
		}
	})
}

func FuzzJWTAuthenticate(f *testing.F) {
	ks := loadKeyset()
	valid := signToken(f, "HS256", "hs256", ks.hmac, baseClaims(testNow))
	none := signToken(f, "none", "", nil, baseClaims(testNow))
	seed := jwksBytes(jwk("rs256", "RS256", &ks.rsa1.PublicKey))
	f.Add(valid, seed)
	f.Add(none, seed)
	f.Add("a.b.c", seed)
	f.Add("", seed)

	f.Fuzz(func(t *testing.T, token string, set []byte) {
		static, err := NewStaticKeys([]Key{{ID: "hs256", Algorithm: "HS256", Material: ks.hmac}})
		if err != nil {
			t.Fatal(err)
		}
		hsProvider, err := NewJWT(testJWTConfig(authkey.Algorithms), static, clock.NewFake(testNow))
		if err != nil {
			t.Fatal(err)
		}

		mf := newMemFetcher(set)
		j, err := NewJWKS(testJWKSConfig(), JWKSDeps{Fetcher: mf, Clock: clock.NewFake(testNow)})
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()
		jwksProvider, err := NewJWT(testJWTConfig(authkey.Algorithms), j, clock.NewFake(testNow))
		if err != nil {
			t.Fatal(err)
		}

		for _, p := range []*JWT{hsProvider, jwksProvider} {
			r, err := http.NewRequest(http.MethodGet, "/", nil)
			if err != nil {
				t.Fatal(err)
			}
			r.Header.Set("Authorization", "Bearer "+token)
			res, err := p.Authenticate(r)
			if err == nil && res.Identity.Provider == "" {
				t.Fatal("successful authentication without provider name")
			}
		}
	})
}
