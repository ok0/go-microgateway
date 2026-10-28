package authkey

import (
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

func TestSupported(t *testing.T) {
	for _, alg := range []string{"HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "EdDSA"} {
		if !Supported(alg) {
			t.Errorf("Supported(%q) = false, want true", alg)
		}
	}
	for _, alg := range []string{"none", "PS256", "hs256", ""} {
		if Supported(alg) {
			t.Errorf("Supported(%q) = true, want false", alg)
		}
	}
}

func TestMinSecretBytes(t *testing.T) {
	cases := map[string]int{"HS256": 32, "HS384": 48, "HS512": 64, "RS256": 0}
	for alg, want := range cases {
		if got := MinSecretBytes(alg); got != want {
			t.Errorf("MinSecretBytes(%q) = %d, want %d", alg, got, want)
		}
	}
}

func mustRSA(t *testing.T, bits int) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func mustECDSA(t *testing.T, c elliptic.Curve) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(c, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func mustEd25519(t *testing.T) ed25519.PublicKey {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub
}

func TestParsePublicKeyPEM(t *testing.T) {
	rsaKey := mustRSA(t, 2048)
	ecKey := mustECDSA(t, elliptic.P256())
	edPub := mustEd25519(t)

	pkixPEM := func(pub any) []byte {
		der, err := x509.MarshalPKIXPublicKey(pub)
		if err != nil {
			t.Fatal(err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
	}

	pkcs1 := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&rsaKey.PublicKey)})

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &ecKey.PublicKey, ecKey)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	x25519, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	x25519DER, err := x509.MarshalPKIXPublicKey(x25519.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	x25519PEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: x25519DER})

	privateBlock := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("x")})

	cases := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{"pkix rsa", pkixPEM(&rsaKey.PublicKey), false},
		{"pkix ecdsa", pkixPEM(&ecKey.PublicKey), false},
		{"pkix ed25519", pkixPEM(edPub), false},
		{"pkcs1 rsa", pkcs1, false},
		{"self-signed certificate", certPEM, false},
		{"garbage", []byte("not a pem block"), true},
		{"empty input", []byte{}, true},
		{"private key block", privateBlock, true},
		{"unsupported pkix key type", x25519PEM, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key, err := ParsePublicKeyPEM(tc.data)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParsePublicKeyPEM() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && key == nil {
				t.Fatal("ParsePublicKeyPEM() returned nil key without error")
			}
		})
	}
}

func TestCheck(t *testing.T) {
	rsa2048 := mustRSA(t, 2048)
	rsa1024 := mustRSA(t, 1024)
	p256 := mustECDSA(t, elliptic.P256())
	p384 := mustECDSA(t, elliptic.P384())
	edPub := mustEd25519(t)

	cases := []struct {
		name    string
		alg     string
		key     any
		wantErr bool
	}{
		{"HS256 accepts 32-byte secret", "HS256", make([]byte, 32), false},
		{"HS384 accepts 48-byte secret", "HS384", make([]byte, 48), false},
		{"HS512 accepts 64-byte secret", "HS512", make([]byte, 64), false},
		{"RS256 accepts 2048-bit RSA", "RS256", &rsa2048.PublicKey, false},
		{"RS384 accepts 2048-bit RSA", "RS384", &rsa2048.PublicKey, false},
		{"RS512 accepts 2048-bit RSA", "RS512", &rsa2048.PublicKey, false},
		{"ES256 accepts P-256", "ES256", &p256.PublicKey, false},
		{"ES384 accepts P-384", "ES384", &p384.PublicKey, false},
		{"EdDSA accepts Ed25519", "EdDSA", edPub, false},
		{"RS256 rejects RSA-1024", "RS256", &rsa1024.PublicKey, true},
		{"ES256 rejects P-384", "ES256", &p384.PublicKey, true},
		{"ES256 rejects Ed25519", "ES256", edPub, true},
		{"EdDSA rejects RSA", "EdDSA", &rsa2048.PublicKey, true},
		{"HS256 rejects 31-byte secret", "HS256", make([]byte, 31), true},
		{"HS384 rejects 47-byte secret", "HS384", make([]byte, 47), true},
		{"RS256 rejects []byte", "RS256", []byte("secret"), true},
		{"HS256 rejects RSA key", "HS256", &rsa2048.PublicKey, true},
		{"none rejected", "none", make([]byte, 32), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Check(tc.alg, tc.key)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Check(%q) error = %v, wantErr %v", tc.alg, err, tc.wantErr)
			}
		})
	}
}
