package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/ok0/go-microgateway/internal/reqctx"
)

// APIKeyEntry is one accepted API key: its consumer, the SHA-256 hash of the key, and the metadata, scopes and roles it grants.
type APIKeyEntry struct {
	ID       string
	Consumer string
	Hash     [32]byte
	Metadata map[string]string
	Scopes   []string
	Roles    []string
}

// APIKeyConfig configures an APIKey: where the key is read from, the accepted keys, and which values are forwarded upstream.
type APIKeyConfig struct {
	Name               string
	Header             string
	Query              string
	Keys               []APIKeyEntry
	ConsumerHeader     string
	MetadataHeaders    map[string]string
	ForwardCredentials bool
}

// APIKey is an Authenticator that checks an API key read from a header or a query parameter.
type APIKey struct {
	cfg    APIKeyConfig
	hashes [][32]byte
}

// NewAPIKey returns an APIKey for c.
func NewAPIKey(c APIKeyConfig) *APIKey {
	hashes := make([][32]byte, len(c.Keys))
	for i, k := range c.Keys {
		hashes[i] = k.Hash
	}
	return &APIKey{cfg: c, hashes: hashes}
}

// MatchKey returns the index of the hash equal to the SHA-256 of presented, and whether one matched. Every hash is compared in constant time.
func MatchKey(hashes [][32]byte, presented string) (index int, ok bool) {
	if len(hashes) == 0 || presented == "" {
		return -1, false
	}
	sum := sha256.Sum256([]byte(presented))
	idx, found := -1, 0
	for i := range hashes {
		eq := subtle.ConstantTimeCompare(sum[:], hashes[i][:])
		take := eq & (1 ^ found)
		idx = subtle.ConstantTimeSelect(take, i, idx)
		found |= eq
	}
	return idx, found == 1
}

// Name returns the configured name of the authenticator.
func (k *APIKey) Name() string {
	return k.cfg.Name
}

// Challenge returns the scheme sent in the WWW-Authenticate header when a request is rejected.
func (k *APIKey) Challenge() string {
	return "ApiKey"
}

// Credentials returns the header and the query parameter the API key is read from.
func (k *APIKey) Credentials() (header, query string) {
	return k.cfg.Header, k.cfg.Query
}

// ForwardCredentials reports whether the presented API key is forwarded upstream.
func (k *APIKey) ForwardCredentials() bool {
	return k.cfg.ForwardCredentials
}

// Authenticate checks the API key in the header, or else in the query parameter. It returns ErrNoCredentials when no key is presented, and an error with ReasonInvalidKey when the key matches no entry.
func (k *APIKey) Authenticate(r *http.Request) (Result, error) {
	var v string
	if k.cfg.Header != "" {
		v = r.Header.Get(k.cfg.Header)
	}
	if v == "" && k.cfg.Query != "" {
		v = queryParam(r.URL.RawQuery, k.cfg.Query)
	}
	if v == "" {
		return Result{}, ErrNoCredentials
	}
	idx, ok := MatchKey(k.hashes, v)
	if !ok {
		return Result{}, Fail(ReasonInvalidKey, nil)
	}
	e := k.cfg.Keys[idx]

	values := make(map[string]any, len(e.Metadata))
	for name, val := range e.Metadata {
		values[name] = val
	}

	forward := make(map[string]string)
	if k.cfg.ConsumerHeader != "" {
		if s, ok := forwardValue(e.Consumer); ok {
			forward[k.cfg.ConsumerHeader] = s
		}
	}
	for key, hdr := range k.cfg.MetadataHeaders {
		val, present := e.Metadata[key]
		if !present {
			continue
		}
		if s, ok := forwardValue(val); ok {
			forward[hdr] = s
		}
	}

	return Result{
		Identity: reqctx.Identity{
			Provider: k.cfg.Name,
			Consumer: e.Consumer,
			Metadata: maps.Clone(e.Metadata),
		},
		Scopes:  slices.Clone(e.Scopes),
		Roles:   slices.Clone(e.Roles),
		Values:  values,
		Forward: forward,
	}, nil
}

func queryPart(part string) (key, value string) {
	k, v, _ := strings.Cut(part, "=")
	dk, err := url.QueryUnescape(k)
	if err != nil {
		dk = k
	}
	dv, err := url.QueryUnescape(v)
	if err != nil {
		dv = v
	}
	return dk, dv
}

func queryParam(raw, name string) string {
	for _, part := range strings.Split(raw, "&") {
		k, v := queryPart(part)
		if k == name {
			return v
		}
	}
	return ""
}
