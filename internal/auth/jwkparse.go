package auth

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"slices"
	"strings"

	"github.com/ok0/lychgate/internal/authkey"
)

type jwkEntry struct {
	KTY string `json:"kty"`
	KID string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

func ParseJWKS(data []byte, algorithms []string) ([]Key, error) {
	var doc struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, errors.New("invalid JWKS document")
	}

	seen := make(map[string]bool)
	var out []Key
	for _, raw := range doc.Keys {
		var e jwkEntry
		if err := json.Unmarshal(raw, &e); err != nil {
			continue
		}
		if e.Use != "" && e.Use != "sig" {
			continue
		}
		if e.KID != "" && seen[e.KID] {
			continue
		}
		material, ok := jwkMaterial(e)
		if !ok {
			continue
		}
		key, ok := bindKey(e, material, algorithms)
		if !ok {
			continue
		}
		if e.KID != "" {
			seen[e.KID] = true
		}
		out = append(out, key)
	}
	if len(out) == 0 {
		return nil, errors.New("key set has no usable keys")
	}
	return out, nil
}

func jwkMaterial(e jwkEntry) (any, bool) {
	switch e.KTY {
	case "RSA":
		n, ok := decodeB64(e.N)
		if !ok {
			return nil, false
		}
		eb, ok := decodeB64(e.E)
		if !ok {
			return nil, false
		}
		exp := new(big.Int).SetBytes(eb)
		if !exp.IsInt64() {
			return nil, false
		}
		ev := exp.Int64()
		if ev < 3 || ev > 1<<31-1 || ev%2 == 0 {
			return nil, false
		}
		mod := new(big.Int).SetBytes(n)
		if mod.BitLen() < 2048 {
			return nil, false
		}
		return &rsa.PublicKey{N: mod, E: int(ev)}, true
	case "EC":
		var curve elliptic.Curve
		var size int
		switch e.Crv {
		case "P-256":
			curve, size = elliptic.P256(), 32
		case "P-384":
			curve, size = elliptic.P384(), 48
		case "P-521":
			curve, size = elliptic.P521(), 66
		default:
			return nil, false
		}
		x, ok := decodeB64(e.X)
		if !ok || len(x) != size {
			return nil, false
		}
		y, ok := decodeB64(e.Y)
		if !ok || len(y) != size {
			return nil, false
		}
		point := append([]byte{0x04}, append(x, y...)...)
		pub, err := ecdsa.ParseUncompressedPublicKey(curve, point)
		if err != nil {
			return nil, false
		}
		return pub, true
	case "OKP":
		if e.Crv != "Ed25519" {
			return nil, false
		}
		x, ok := decodeB64(e.X)
		if !ok || len(x) != ed25519.PublicKeySize {
			return nil, false
		}
		return ed25519.PublicKey(x), true
	}
	return nil, false
}

func bindKey(e jwkEntry, material any, algorithms []string) (Key, bool) {
	if e.Alg != "" {
		if !slices.Contains(algorithms, e.Alg) || authkey.Check(e.Alg, material) != nil {
			return Key{}, false
		}
		return Key{ID: e.KID, Algorithm: e.Alg, Material: material}, true
	}
	for _, a := range algorithms {
		if authkey.Check(a, material) == nil {
			return Key{ID: e.KID, Allowed: slices.Clone(algorithms), Material: material}, true
		}
	}
	return Key{}, false
}

func decodeB64(s string) ([]byte, bool) {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
	if err != nil {
		return nil, false
	}
	return b, true
}
