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

type APIKeyEntry struct {
	ID       string
	Consumer string
	Hash     [32]byte
	Metadata map[string]string
	Scopes   []string
	Roles    []string
}

type APIKeyConfig struct {
	Name               string
	Header             string
	Query              string
	Keys               []APIKeyEntry
	ConsumerHeader     string
	MetadataHeaders    map[string]string
	ForwardCredentials bool
}

type APIKey struct {
	cfg    APIKeyConfig
	hashes [][32]byte
}

func NewAPIKey(c APIKeyConfig) *APIKey {
	hashes := make([][32]byte, len(c.Keys))
	for i, k := range c.Keys {
		hashes[i] = k.Hash
	}
	return &APIKey{cfg: c, hashes: hashes}
}

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

func (k *APIKey) Name() string {
	return k.cfg.Name
}

func (k *APIKey) Challenge() string {
	return "ApiKey"
}

func (k *APIKey) Credentials() (header, query string) {
	return k.cfg.Header, k.cfg.Query
}

func (k *APIKey) ForwardCredentials() bool {
	return k.cfg.ForwardCredentials
}

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
