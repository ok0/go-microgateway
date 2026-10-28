package auth

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

func testKeys(n int) ([]string, [][32]byte) {
	rng := rand.New(rand.NewPCG(1, 2))
	keys := make([]string, n)
	hashes := make([][32]byte, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%016x%016x", rng.Uint64(), rng.Uint64())
		hashes[i] = sha256.Sum256([]byte(keys[i]))
	}
	return keys, hashes
}

func TestMatchKey(t *testing.T) {
	keys, hashes := testKeys(1000)
	tests := []struct {
		name      string
		hashes    [][32]byte
		presented string
		wantIdx   int
		wantOK    bool
	}{
		{"first", hashes, keys[0], 0, true},
		{"last", hashes, keys[999], 999, true},
		{"miss", hashes, "not-a-key", -1, false},
		{"empty list", nil, keys[0], -1, false},
		{"empty presented", hashes, "", -1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			idx, ok := MatchKey(tc.hashes, tc.presented)
			if idx != tc.wantIdx || ok != tc.wantOK {
				t.Errorf("MatchKey = (%d, %v), want (%d, %v)", idx, ok, tc.wantIdx, tc.wantOK)
			}
		})
	}
}

var benchSink int

func BenchmarkMatchKey(b *testing.B) {
	keys, hashes := testKeys(1000)
	cases := []struct {
		name      string
		presented string
	}{
		{"first", keys[0]},
		{"last", keys[999]},
		{"miss", "not-a-key"},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				idx, _ := MatchKey(hashes, tc.presented)
				benchSink = idx
			}
		})
	}
}

func testAPIKey(header, query string) *APIKey {
	good := sha256.Sum256([]byte("good-key"))
	other := sha256.Sum256([]byte("other-key"))
	return NewAPIKey(APIKeyConfig{
		Name:   "keys",
		Header: header,
		Query:  query,
		Keys: []APIKeyEntry{
			{
				ID:       "k1",
				Consumer: "svc-a",
				Hash:     good,
				Metadata: map[string]string{"tier": "gold", "bad": "x\r\nInjected: 1"},
				Scopes:   []string{"read"},
				Roles:    []string{"ops"},
			},
			{
				ID:       "k2",
				Consumer: "svc-b",
				Hash:     other,
				Metadata: map[string]string{"tier": "silver"},
			},
		},
		ConsumerHeader:  "X-Consumer",
		MetadataHeaders: map[string]string{"tier": "X-Tier", "bad": "X-Bad"},
	})
}

func TestAPIKeyAuthenticate(t *testing.T) {
	tests := []struct {
		name       string
		header     string
		headerVal  string
		rawQuery   string
		wantOK     bool
		wantReason Reason
		wantNoCred bool
	}{
		{name: "key by header", header: "X-API-Key", headerVal: "good-key", wantOK: true},
		{name: "key by query", rawQuery: "api_key=good-key", wantOK: true},
		{name: "valid header invalid query", header: "X-API-Key", headerVal: "good-key", rawQuery: "api_key=bad", wantOK: true},
		{name: "invalid header valid query", header: "X-API-Key", headerVal: "bad", rawQuery: "api_key=good-key", wantReason: ReasonInvalidKey},
		{name: "empty header valid query", header: "X-API-Key", headerVal: "", rawQuery: "api_key=good-key", wantOK: true},
		{name: "no credentials", wantNoCred: true},
		{name: "unknown key", header: "X-API-Key", headerVal: "nope", wantReason: ReasonInvalidKey},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := testAPIKey("X-API-Key", "api_key")
			r := httptest.NewRequest(http.MethodGet, "/x?"+tc.rawQuery, nil)
			if tc.header != "" {
				r.Header.Set(tc.header, tc.headerVal)
			}
			res, err := a.Authenticate(r)
			switch {
			case tc.wantNoCred:
				if !errors.Is(err, ErrNoCredentials) {
					t.Fatalf("err = %v, want ErrNoCredentials", err)
				}
			case tc.wantOK:
				if err != nil {
					t.Fatalf("err = %v, want nil", err)
				}
				if res.Identity.Provider != "keys" {
					t.Errorf("Provider = %q", res.Identity.Provider)
				}
			default:
				if got := ReasonOf(err); got != tc.wantReason {
					t.Errorf("ReasonOf = %q, want %q (err %v)", got, tc.wantReason, err)
				}
			}
		})
	}
}

func TestAPIKeyIdentityAndForward(t *testing.T) {
	a := testAPIKey("X-API-Key", "api_key")
	r := httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-API-Key", "good-key")
	res, err := a.Authenticate(r)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if res.Identity.Provider != "keys" || res.Identity.Consumer != "svc-a" {
		t.Errorf("identity = %+v", res.Identity)
	}
	if res.Identity.Metadata["tier"] != "gold" {
		t.Errorf("metadata = %v", res.Identity.Metadata)
	}
	if res.Identity.Claims != nil {
		t.Errorf("Claims = %v, want nil", res.Identity.Claims)
	}
	if !slices.Equal(res.Scopes, []string{"read"}) || !slices.Equal(res.Roles, []string{"ops"}) {
		t.Errorf("scopes = %v roles = %v", res.Scopes, res.Roles)
	}
	if res.Values["tier"] != "gold" {
		t.Errorf("Values[tier] = %v", res.Values["tier"])
	}
	if res.Forward["X-Consumer"] != "svc-a" {
		t.Errorf("Forward[X-Consumer] = %q", res.Forward["X-Consumer"])
	}
	if res.Forward["X-Tier"] != "gold" {
		t.Errorf("Forward[X-Tier] = %q", res.Forward["X-Tier"])
	}
	if _, ok := res.Forward["X-Bad"]; ok {
		t.Errorf("Forward contains X-Bad with a value containing CRLF")
	}
}

func TestAPIKeyQueryOnly(t *testing.T) {
	good := sha256.Sum256([]byte("good-key"))
	a := NewAPIKey(APIKeyConfig{
		Name:  "keys",
		Query: "api_key",
		Keys:  []APIKeyEntry{{Consumer: "svc-a", Hash: good}},
	})

	r := httptest.NewRequest(http.MethodGet, "/x?api_key=good-key", nil)
	r.Header.Set("X-API-Key", "good-key")
	if _, err := a.Authenticate(r); err != nil {
		t.Errorf("query credential: err = %v, want nil", err)
	}

	r = httptest.NewRequest(http.MethodGet, "/x", nil)
	r.Header.Set("X-API-Key", "good-key")
	if _, err := a.Authenticate(r); !errors.Is(err, ErrNoCredentials) {
		t.Errorf("header-only request: err = %v, want ErrNoCredentials", err)
	}
}

func TestQueryParam(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		key  string
		want string
	}{
		{"encoded value", "a=1&api_key=abc%2Fdef&b=2", "api_key", "abc/def"},
		{"encoded name", "api%5Fkey=enc&api_key=second", "api_key", "enc"},
		{"first occurrence wins", "x=1&x=2", "x", "1"},
		{"absent", "a=1", "missing", ""},
		{"plus is space", "k=a+b", "k", "a b"},
		{"invalid escape keeps raw", "k=%zz&k2=1", "k", "%zz"},
		{"encoded name with space", "a%20b=1", "a b", "1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := queryParam(tc.raw, tc.key); got != tc.want {
				t.Errorf("queryParam = %q, want %q", got, tc.want)
			}
		})
	}
}
