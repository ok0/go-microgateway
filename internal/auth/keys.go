package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/ok0/go-microgateway/internal/authkey"
)

type Key struct {
	ID        string
	Algorithm string
	Allowed   []string
	Material  any
}

type KeySource interface {
	Lookup(ctx context.Context, kid string) (Key, error)
}

type StaticKeys struct {
	keys []Key
}

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

func (s *StaticKeys) Len() int {
	if s == nil {
		return 0
	}
	return len(s.keys)
}

type Combined struct {
	Static *StaticKeys
	JWKS   *JWKS
}

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
