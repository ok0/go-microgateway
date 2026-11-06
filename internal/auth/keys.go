package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ok0/go-microgateway/internal/authkey"
)

// Key is a verification key: its id, the algorithm it is bound to or the algorithms it allows, and the key material.
type Key struct {
	ID        string
	Algorithm string
	Allowed   []string
	Material  any
}

// KeySource returns the verification key for a key id. An empty kid asks for the only key.
type KeySource interface {
	Lookup(ctx context.Context, kid string) (Key, error)
}

// StaticKeys is a KeySource over keys fixed at configuration time.
type StaticKeys struct {
	keys []Key
}

// NewStaticKeys returns a StaticKeys for keys. It errors when a key has no algorithm or invalid material, or when two keys share an id.
func NewStaticKeys(keys []Key) (*StaticKeys, error) {
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if k.Algorithm == "" {
			return nil, errors.New("static key has no algorithm")
		}
		if err := authkey.Check(k.Algorithm, k.Material); err != nil {
			return nil, fmt.Errorf("static key %q: %w", k.ID, err)
		}
		if k.ID == "" {
			continue
		}
		if seen[k.ID] {
			return nil, fmt.Errorf("duplicate key id %q", k.ID)
		}
		seen[k.ID] = true
	}
	return &StaticKeys{keys: slices.Clone(keys)}, nil
}

// Lookup returns the key with id kid. An empty kid selects the key only when exactly one key is configured. Otherwise it returns ErrUnknownKID.
func (s *StaticKeys) Lookup(_ context.Context, kid string) (Key, error) {
	if kid != "" {
		for _, k := range s.keys {
			if k.ID == kid {
				return k, nil
			}
		}
		return Key{}, ErrUnknownKID
	}
	if len(s.keys) == 1 {
		return s.keys[0], nil
	}
	return Key{}, ErrUnknownKID
}

// Len returns the number of configured keys. A nil StaticKeys has none.
func (s *StaticKeys) Len() int {
	if s == nil {
		return 0
	}
	return len(s.keys)
}

// Combined is a KeySource over static keys and a JWKS, either of which may be nil.
type Combined struct {
	Static *StaticKeys
	JWKS   *JWKS
}

// Lookup returns the key with id kid, trying the static keys first and then the JWKS. An empty kid selects the key only when the static keys and the JWKS hold exactly one key between them.
func (c *Combined) Lookup(ctx context.Context, kid string) (Key, error) {
	if kid != "" {
		if c.Static != nil {
			if k, err := c.Static.Lookup(ctx, kid); err == nil {
				return k, nil
			}
		}
		if c.JWKS == nil {
			return Key{}, ErrUnknownKID
		}
		return c.JWKS.Lookup(ctx, kid)
	}

	var ks []Key
	if c.JWKS != nil {
		var err error
		ks, err = c.JWKS.Keys(ctx)
		if err != nil {
			return Key{}, err
		}
	}
	n := c.Static.Len()
	if n+len(ks) != 1 {
		return Key{}, ErrUnknownKID
	}
	if n == 1 {
		return c.Static.keys[0], nil
	}
	return ks[0], nil
}
