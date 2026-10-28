// Package authkey defines the JWT signing algorithms the gateway accepts and checks verification keys against them.
package authkey

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
)

var Algorithms = []string{"HS256", "HS384", "HS512", "RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "EdDSA"}

func Supported(alg string) bool {
	for _, a := range Algorithms {
		if a == alg {
			return true
		}
	}
	return false
}

func IsHMAC(alg string) bool {
	switch alg {
	case "HS256", "HS384", "HS512":
		return true
	}
	return false
}

func MinSecretBytes(alg string) int {
	switch alg {
	case "HS256":
		return 32
	case "HS384":
		return 48
	case "HS512":
		return 64
	}
	return 0
}

func ParsePublicKeyPEM(data []byte) (crypto.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}

	var (
		key any
		err error
	)
	switch block.Type {
	case "PUBLIC KEY":
		key, err = x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse PKIX public key: %w", err)
		}
	case "RSA PUBLIC KEY":
		key, err = x509.ParsePKCS1PublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse PKCS#1 public key: %w", err)
		}
	case "CERTIFICATE":
		cert, certErr := x509.ParseCertificate(block.Bytes)
		if certErr != nil {
			return nil, fmt.Errorf("parse certificate: %w", certErr)
		}
		key = cert.PublicKey
	default:
		return nil, fmt.Errorf("unsupported PEM block type %q", block.Type)
	}

	switch key.(type) {
	case *rsa.PublicKey, *ecdsa.PublicKey, ed25519.PublicKey:
		return key, nil
	default:
		return nil, fmt.Errorf("unsupported public key type %T", key)
	}
}

func Check(alg string, key any) error {
	if !Supported(alg) {
		return fmt.Errorf("unsupported algorithm %q", alg)
	}

	switch alg {
	case "HS256", "HS384", "HS512":
		secret, ok := key.([]byte)
		if !ok {
			return fmt.Errorf("%s requires a shared secret", alg)
		}
		if min := MinSecretBytes(alg); len(secret) < min {
			return fmt.Errorf("%s secret must be at least %d bytes", alg, min)
		}
		return nil
	case "RS256", "RS384", "RS512":
		pub, ok := key.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("%s requires an RSA public key", alg)
		}
		if pub.N.BitLen() < 2048 {
			return fmt.Errorf("RSA key must be at least 2048 bits (got %d)", pub.N.BitLen())
		}
		return nil
	case "ES256":
		return checkECDSA(alg, key, "P-256")
	case "ES384":
		return checkECDSA(alg, key, "P-384")
	case "ES512":
		return checkECDSA(alg, key, "P-521")
	case "EdDSA":
		pub, ok := key.(ed25519.PublicKey)
		if !ok || len(pub) != ed25519.PublicKeySize {
			return errors.New("EdDSA requires an Ed25519 public key")
		}
		return nil
	}
	return fmt.Errorf("unsupported algorithm %q", alg)
}

func checkECDSA(alg string, key any, curve string) error {
	pub, ok := key.(*ecdsa.PublicKey)
	if !ok || pub.Curve.Params().Name != curve {
		return fmt.Errorf("%s requires an ECDSA %s public key", alg, curve)
	}
	return nil
}
