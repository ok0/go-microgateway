package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/go-microgateway/internal/testwait"
)

type keyset struct {
	rsa1, rsa2 *rsa.PrivateKey
	p256       *ecdsa.PrivateKey
	p384       *ecdsa.PrivateKey
	p521       *ecdsa.PrivateKey
	edPub      ed25519.PublicKey
	edPriv     ed25519.PrivateKey
	hmac       []byte
}

var (
	ksOnce sync.Once
	ksVal  *keyset
)

func loadKeyset() *keyset {
	ksOnce.Do(func() {
		var err error
		ks := &keyset{hmac: []byte("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")}
		if ks.rsa1, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
		if ks.rsa2, err = rsa.GenerateKey(rand.Reader, 2048); err != nil {
			panic(err)
		}
		if ks.p256, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			panic(err)
		}
		if ks.p384, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader); err != nil {
			panic(err)
		}
		if ks.p521, err = ecdsa.GenerateKey(elliptic.P521(), rand.Reader); err != nil {
			panic(err)
		}
		if ks.edPub, ks.edPriv, err = ed25519.GenerateKey(rand.Reader); err != nil {
			panic(err)
		}
		ksVal = ks
	})
	return ksVal
}

func signToken(t testing.TB, alg, kid string, key any, claims jwt.MapClaims) string {
	t.Helper()
	method := jwt.GetSigningMethod(alg)
	if method == nil {
		t.Fatalf("unknown signing method %q", alg)
	}
	tok := jwt.NewWithClaims(method, claims)
	if kid != "" {
		tok.Header["kid"] = kid
	}
	signKey := key
	if alg == "none" {
		signKey = jwt.UnsafeAllowNoneSignatureType
	}
	s, err := tok.SignedString(signKey)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func b64(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

func jwk(kid, alg string, pub any) map[string]any {
	m := map[string]any{"kid": kid}
	if alg != "" {
		m["alg"] = alg
	}
	switch k := pub.(type) {
	case *rsa.PublicKey:
		m["kty"] = "RSA"
		m["n"] = b64(k.N.Bytes())
		m["e"] = b64(big.NewInt(int64(k.E)).Bytes())
	case *ecdsa.PublicKey:
		size := (k.Curve.Params().BitSize + 7) / 8
		m["kty"] = "EC"
		m["crv"] = k.Curve.Params().Name
		raw, err := k.Bytes()
		if err != nil {
			panic(err)
		}
		m["x"] = b64(raw[1 : 1+size])
		m["y"] = b64(raw[1+size:])
	case ed25519.PublicKey:
		m["kty"] = "OKP"
		m["crv"] = "Ed25519"
		m["x"] = b64(k)
	}
	return m
}

func jwksBytes(keys ...map[string]any) []byte {
	b, err := json.Marshal(map[string]any{"keys": keys})
	if err != nil {
		panic(err)
	}
	return b
}

type memFetcher struct {
	mu       sync.Mutex
	body     []byte
	err      error
	panics   bool
	block    chan struct{}
	calls    int
	provider string
	url      string
	canceled chan struct{}
	once     sync.Once
}

func newMemFetcher(body []byte) *memFetcher {
	return &memFetcher{body: body, canceled: make(chan struct{})}
}

func (m *memFetcher) set(body []byte, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.body, m.err, m.panics = body, err, false
}

func (m *memFetcher) setPanic() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.panics = true
}

func (m *memFetcher) setBlock(ch chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.block = ch
}

func (m *memFetcher) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func (m *memFetcher) FetchKeySet(ctx context.Context, provider, keySetURL string) ([]byte, error) {
	m.mu.Lock()
	m.calls++
	m.provider = provider
	m.url = keySetURL
	body, err, panics, block := m.body, m.err, m.panics, m.block
	m.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			m.once.Do(func() { close(m.canceled) })
			return nil, ctx.Err()
		}
	}
	if panics {
		panic("fetcher failure")
	}
	return body, err
}

var errTestFetch = errors.New("upstream unavailable")

func waitWaiters(t *testing.T, j *JWKS, n int) {
	t.Helper()
	testwait.Until(t, fmt.Sprintf("%d waiters on the in-flight fetch", n), func() bool {
		j.mu.Lock()
		defer j.mu.Unlock()
		return j.flight != nil && j.flight.waiters >= n
	}, nil)
}

var testNow = time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
