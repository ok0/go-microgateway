package store

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLimitValidate(t *testing.T) {
	tests := []struct {
		name    string
		limit   Limit
		wantErr bool
	}{
		{name: "valid token bucket", limit: Limit{Algorithm: TokenBucket, Count: 5, Period: time.Second, Burst: 3}},
		{name: "valid sliding window", limit: Limit{Algorithm: SlidingWindow, Count: 10, Period: time.Minute}},
		{name: "boundary valid token bucket emission interval 1µs", limit: Limit{Algorithm: TokenBucket, Count: 1000, Period: time.Millisecond, Burst: 1}},
		{name: "boundary valid token bucket T*Burst equals 24h", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Hour, Burst: 24}},
		{name: "boundary valid sliding window period 24h", limit: Limit{Algorithm: SlidingWindow, Count: 1, Period: 24 * time.Hour}},

		{name: "invalid algorithm zero", limit: Limit{Algorithm: Algorithm(0), Count: 1, Period: time.Second, Burst: 1}, wantErr: true},
		{name: "invalid algorithm three", limit: Limit{Algorithm: Algorithm(3), Count: 1, Period: time.Second, Burst: 1}, wantErr: true},
		{name: "invalid count zero", limit: Limit{Algorithm: TokenBucket, Count: 0, Period: time.Second, Burst: 1}, wantErr: true},
		{name: "invalid count negative", limit: Limit{Algorithm: TokenBucket, Count: -1, Period: time.Second, Burst: 1}, wantErr: true},
		{name: "invalid period zero", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: 0, Burst: 1}, wantErr: true},
		{name: "invalid period negative", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: -time.Second, Burst: 1}, wantErr: true},
		{name: "invalid token bucket burst zero", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Second, Burst: 0}, wantErr: true},
		{name: "invalid token bucket burst negative", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Second, Burst: -1}, wantErr: true},
		{name: "invalid emission interval 999ns", limit: Limit{Algorithm: TokenBucket, Count: 1001, Period: time.Millisecond, Burst: 1}, wantErr: true},
		{name: "invalid sliding window emission interval 999ns", limit: Limit{Algorithm: SlidingWindow, Count: 1001, Period: time.Millisecond}, wantErr: true},
		{name: "invalid token bucket T*Burst exceeds 24h", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Hour, Burst: 25}, wantErr: true},
		{name: "invalid token bucket overflow guard", limit: Limit{Algorithm: TokenBucket, Count: 1, Period: time.Duration(math.MaxInt64), Burst: math.MaxInt64}, wantErr: true},
		{name: "invalid sliding window period exceeds 24h", limit: Limit{Algorithm: SlidingWindow, Count: 1, Period: 24*time.Hour + time.Nanosecond}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.limit.Validate()
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if !errors.Is(err, ErrInvalidLimit) {
				t.Errorf("Validate() error %v does not wrap ErrInvalidLimit", err)
			}
			if errors.Is(err, ErrUnavailable) {
				t.Errorf("Validate() error %v wraps ErrUnavailable", err)
			}
		})
	}
}

func TestAlgorithmString(t *testing.T) {
	tests := []struct {
		alg  Algorithm
		want string
	}{
		{TokenBucket, "token_bucket"},
		{SlidingWindow, "sliding_window"},
		{Algorithm(0), "Algorithm(0)"},
		{Algorithm(9), "Algorithm(9)"},
	}
	for _, tt := range tests {
		if got := tt.alg.String(); got != tt.want {
			t.Errorf("Algorithm(%d).String() = %q, want %q", uint8(tt.alg), got, tt.want)
		}
	}
}

func TestSentinelsDistinct(t *testing.T) {
	sentinels := []error{ErrInvalidEntry, ErrInvalidPurge, ErrInvalidLimit, ErrUnavailable}
	for i := range sentinels {
		for j := i + 1; j < len(sentinels); j++ {
			if sentinels[i] == sentinels[j] {
				t.Errorf("sentinels %d and %d are not distinct", i, j)
			}
		}
	}
	if got := ErrUnavailable.Error(); got != "store: unavailable" {
		t.Errorf("ErrUnavailable.Error() = %q, want %q", got, "store: unavailable")
	}
	if got := ErrInvalidLimit.Error(); got != "store: invalid limit" {
		t.Errorf("ErrInvalidLimit.Error() = %q, want %q", got, "store: invalid limit")
	}
	if got := ErrInvalidEntry.Error(); got != "store: invalid cache entry" {
		t.Errorf("ErrInvalidEntry.Error() = %q, want %q", got, "store: invalid cache entry")
	}
	if got := ErrInvalidPurge.Error(); got != "store: invalid purge" {
		t.Errorf("ErrInvalidPurge.Error() = %q, want %q", got, "store: invalid purge")
	}
	if CapRateLimit != 1 {
		t.Errorf("CapRateLimit = %d, want 1", CapRateLimit)
	}
	if CapCache != 2 {
		t.Errorf("CapCache = %d, want 2", CapCache)
	}
	if CapRateLimit|CapCache != 3 {
		t.Errorf("CapRateLimit|CapCache = %d, want 3", CapRateLimit|CapCache)
	}
}

func TestFillToken(t *testing.T) {
	t.Parallel()
	if !(FillToken{}).IsZero() {
		t.Error("FillToken{}.IsZero() = false, want true")
	}
	if !NewFillToken("").IsZero() {
		t.Error(`NewFillToken("").IsZero() = false, want true`)
	}
	x := NewFillToken("x")
	if x.IsZero() {
		t.Error(`NewFillToken("x").IsZero() = true, want false`)
	}
	if x.Opaque() != "x" {
		t.Errorf(`NewFillToken("x").Opaque() = %q, want "x"`, x.Opaque())
	}
	if x != NewFillToken("x") {
		t.Error(`NewFillToken("x") != NewFillToken("x")`)
	}
}

func TestCanonicalVaryNames(t *testing.T) {
	t.Parallel()
	names16 := make([]string, 0, 16)
	for i := range 16 {
		names16 = append(names16, fmt.Sprintf("h-%02d", i))
	}
	names17 := append(slices.Clone(names16), "h-16")
	dup16 := append(slices.Clone(names16), "H-00")
	tests := []struct {
		name    string
		in      []string
		want    []string
		wantErr bool
	}{
		{name: "nil gives empty", in: nil, want: []string{}},
		{name: "canonical and de-duplicated", in: []string{"Accept-Encoding", "accept-encoding", "X-B"}, want: []string{"accept-encoding", "x-b"}},
		{name: "sixteen distinct with case duplicate", in: dup16, want: names16},
		{name: "seventeen distinct", in: names17, wantErr: true},
		{name: "empty name", in: []string{""}, wantErr: true},
		{name: "space in name", in: []string{"a b"}, wantErr: true},
		{name: "star", in: []string{"*"}, wantErr: true},
		{name: "nul byte", in: []string{"a\x00"}, wantErr: true},
		{name: "non-ascii", in: []string{"é"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			before := slices.Clone(tt.in)
			got, err := CanonicalVaryNames(tt.in)
			if !slices.Equal(before, tt.in) {
				t.Errorf("input changed from %q to %q", before, tt.in)
			}
			if tt.wantErr {
				if err == nil {
					t.Fatal("CanonicalVaryNames() = nil error, want error")
				}
				if !errors.Is(err, ErrInvalidEntry) {
					t.Errorf("error %v does not wrap ErrInvalidEntry", err)
				}
				if errors.Is(err, ErrUnavailable) {
					t.Errorf("error %v wraps ErrUnavailable", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CanonicalVaryNames() error = %v", err)
			}
			if got == nil {
				t.Fatal("CanonicalVaryNames() = nil slice, want non-nil")
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("CanonicalVaryNames() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateFill(t *testing.T) {
	t.Parallel()
	big := make([]byte, MaxBodyBytes+1)
	entry := func(mut func(*Entry)) *Entry {
		e := &Entry{Status: 200}
		if mut != nil {
			mut(e)
		}
		return e
	}
	names := func(n int) []string {
		out := make([]string, 0, n)
		for i := range n {
			out = append(out, fmt.Sprintf("h-%02d", i))
		}
		return out
	}
	good := NewFillToken("t")
	tests := []struct {
		name      string
		tok       FillToken
		primary   string
		secondary string
		names     []string
		entry     *Entry
		ttl       time.Duration
		wantNames []string
		wantErr   bool
	}{
		{name: "valid base", tok: good, primary: "p", secondary: "s", entry: entry(nil), ttl: time.Hour, wantNames: []string{}},
		{name: "invalid zero token", tok: FillToken{}, primary: "p", entry: entry(nil), ttl: time.Hour, wantErr: true},
		{name: "invalid empty primary", tok: good, primary: "", entry: entry(nil), ttl: time.Hour, wantErr: true},
		{name: "invalid primary over MaxKeyBytes", tok: good, primary: strings.Repeat("p", MaxKeyBytes+1), entry: entry(nil), ttl: time.Hour, wantErr: true},
		{name: "invalid secondary over MaxSecondaryBytes", tok: good, primary: "p", secondary: strings.Repeat("s", MaxSecondaryBytes+1), entry: entry(nil), ttl: time.Hour, wantErr: true},
		{name: "invalid nil entry", tok: good, primary: "p", entry: nil, ttl: time.Hour, wantErr: true},
		{name: "invalid status 99", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Status = 99 }), ttl: time.Hour, wantErr: true},
		{name: "invalid status 600", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Status = 600 }), ttl: time.Hour, wantErr: true},
		{name: "invalid body over MaxBodyBytes", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Body = big }), ttl: time.Hour, wantErr: true},
		{name: "invalid negative initial age", tok: good, primary: "p", entry: entry(func(e *Entry) { e.InitialAge = -1 }), ttl: time.Hour, wantErr: true},
		{name: "invalid initial age over MaxInitialAge", tok: good, primary: "p", entry: entry(func(e *Entry) { e.InitialAge = MaxInitialAge + 1 }), ttl: time.Hour, wantErr: true},
		{name: "invalid 65 tags", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Tags = slices.Repeat([]string{"t"}, MaxTags+1) }), ttl: time.Hour, wantErr: true},
		{name: "invalid empty tag", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Tags = []string{""} }), ttl: time.Hour, wantErr: true},
		{name: "invalid tag over MaxTagBytes", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Tags = []string{strings.Repeat("t", MaxTagBytes+1)} }), ttl: time.Hour, wantErr: true},
		{name: "invalid tag with control byte 0x01", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Tags = []string{"a\x01"} }), ttl: time.Hour, wantErr: true},
		{name: "invalid tag 0x7f", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Tags = []string{"\x7f"} }), ttl: time.Hour, wantErr: true},
		{name: "invalid 17 vary names", tok: good, primary: "p", names: names(MaxVaryNames + 1), entry: entry(nil), ttl: time.Hour, wantErr: true},
		{name: "invalid empty vary name", tok: good, primary: "p", names: []string{""}, entry: entry(nil), ttl: time.Hour, wantErr: true},
		{name: "invalid ttl over MaxTTL", tok: good, primary: "p", entry: entry(nil), ttl: MaxTTL + 1, wantErr: true},
		{name: "invalid ttl MaxInt64", tok: good, primary: "p", entry: entry(nil), ttl: time.Duration(math.MaxInt64), wantErr: true},

		{name: "valid primary of MaxKeyBytes", tok: good, primary: strings.Repeat("p", MaxKeyBytes), entry: entry(nil), ttl: time.Hour, wantNames: []string{}},
		{name: "valid secondary of MaxSecondaryBytes", tok: good, primary: "p", secondary: strings.Repeat("s", MaxSecondaryBytes), entry: entry(nil), ttl: time.Hour, wantNames: []string{}},
		{name: "valid status 100", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Status = 100 }), ttl: time.Hour, wantNames: []string{}},
		{name: "valid status 599", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Status = 599 }), ttl: time.Hour, wantNames: []string{}},
		{name: "valid body of MaxBodyBytes", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Body = big[:MaxBodyBytes] }), ttl: time.Hour, wantNames: []string{}},
		{name: "valid initial age of MaxInitialAge", tok: good, primary: "p", entry: entry(func(e *Entry) { e.InitialAge = MaxInitialAge }), ttl: time.Hour, wantNames: []string{}},
		{name: "valid 64 tags", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Tags = slices.Repeat([]string{"t"}, MaxTags) }), ttl: time.Hour, wantNames: []string{}},
		{name: "valid tag of MaxTagBytes", tok: good, primary: "p", entry: entry(func(e *Entry) { e.Tags = []string{strings.Repeat("t", MaxTagBytes)} }), ttl: time.Hour, wantNames: []string{}},
		{name: "valid 16 vary names", tok: good, primary: "p", names: names(MaxVaryNames), entry: entry(nil), ttl: time.Hour, wantNames: names(MaxVaryNames)},
		{name: "valid vary names are canonical", tok: good, primary: "p", names: []string{"Accept-Encoding", "accept-encoding", "X-B"}, entry: entry(nil), ttl: time.Hour, wantNames: []string{"accept-encoding", "x-b"}},
		{name: "valid ttl MaxTTL", tok: good, primary: "p", entry: entry(nil), ttl: MaxTTL, wantNames: []string{}},
		{name: "valid ttl zero", tok: good, primary: "p", entry: entry(nil), ttl: 0, wantNames: []string{}},
		{name: "valid ttl -1ns", tok: good, primary: "p", entry: entry(nil), ttl: -1, wantNames: []string{}},
		{name: "valid ttl MinInt64", tok: good, primary: "p", entry: entry(nil), ttl: time.Duration(math.MinInt64), wantNames: []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			canon, err := ValidateFill(tt.tok, tt.primary, tt.secondary, tt.names, tt.entry, tt.ttl)
			if tt.wantErr {
				if err == nil {
					t.Fatal("ValidateFill() = nil error, want error")
				}
				if !errors.Is(err, ErrInvalidEntry) {
					t.Errorf("error %v does not wrap ErrInvalidEntry", err)
				}
				if errors.Is(err, ErrUnavailable) {
					t.Errorf("error %v wraps ErrUnavailable", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateFill() error = %v", err)
			}
			if !slices.Equal(canon, tt.wantNames) {
				t.Errorf("ValidateFill() names = %q, want %q", canon, tt.wantNames)
			}
		})
	}
}

func TestPurgeValidate(t *testing.T) {
	t.Parallel()
	keys1000 := make([]string, 0, MaxPurgeSelectors)
	for i := range MaxPurgeSelectors {
		keys1000 = append(keys1000, fmt.Sprintf("k%04d", i))
	}
	tests := []struct {
		name    string
		p       Purge
		wantErr bool
	}{
		{name: "valid keys", p: Purge{Kind: PurgeKeys, Keys: []string{"k"}, Retain: time.Hour}},
		{name: "valid tags", p: Purge{Kind: PurgeTags, Tags: []string{"t"}, Retain: time.Hour}},
		{name: "valid all", p: Purge{Kind: PurgeAll, Retain: time.Hour}},
		{name: "valid retain MaxTTL", p: Purge{Kind: PurgeAll, Retain: MaxTTL}},
		{name: "valid 1000 keys", p: Purge{Kind: PurgeKeys, Keys: keys1000, Retain: time.Hour}},

		{name: "invalid kind 0", p: Purge{Kind: 0, Retain: time.Hour}, wantErr: true},
		{name: "invalid kind 4", p: Purge{Kind: 4, Retain: time.Hour}, wantErr: true},
		{name: "invalid retain 0", p: Purge{Kind: PurgeAll, Retain: 0}, wantErr: true},
		{name: "invalid retain -1ns", p: Purge{Kind: PurgeAll, Retain: -1}, wantErr: true},
		{name: "invalid retain over MaxTTL", p: Purge{Kind: PurgeAll, Retain: MaxTTL + 1}, wantErr: true},
		{name: "invalid keys purge without keys", p: Purge{Kind: PurgeKeys, Retain: time.Hour}, wantErr: true},
		{name: "invalid keys purge with tags", p: Purge{Kind: PurgeKeys, Keys: []string{"k"}, Tags: []string{"t"}, Retain: time.Hour}, wantErr: true},
		{name: "invalid tags purge without tags", p: Purge{Kind: PurgeTags, Retain: time.Hour}, wantErr: true},
		{name: "invalid tags purge with keys", p: Purge{Kind: PurgeTags, Keys: []string{"k"}, Tags: []string{"t"}, Retain: time.Hour}, wantErr: true},
		{name: "invalid all purge with keys", p: Purge{Kind: PurgeAll, Keys: []string{"k"}, Retain: time.Hour}, wantErr: true},
		{name: "invalid all purge with tags", p: Purge{Kind: PurgeAll, Tags: []string{"t"}, Retain: time.Hour}, wantErr: true},
		{name: "invalid 1001 keys", p: Purge{Kind: PurgeKeys, Keys: append(slices.Clone(keys1000), "extra"), Retain: time.Hour}, wantErr: true},
		{name: "invalid 1001 tags", p: Purge{Kind: PurgeTags, Tags: slices.Repeat([]string{"t"}, MaxPurgeSelectors+1), Retain: time.Hour}, wantErr: true},
		{name: "invalid empty key", p: Purge{Kind: PurgeKeys, Keys: []string{""}, Retain: time.Hour}, wantErr: true},
		{name: "invalid key over MaxKeyBytes", p: Purge{Kind: PurgeKeys, Keys: []string{strings.Repeat("k", MaxKeyBytes+1)}, Retain: time.Hour}, wantErr: true},
		{name: "invalid empty tag", p: Purge{Kind: PurgeTags, Tags: []string{""}, Retain: time.Hour}, wantErr: true},
		{name: "invalid tag over MaxTagBytes", p: Purge{Kind: PurgeTags, Tags: []string{strings.Repeat("t", MaxTagBytes+1)}, Retain: time.Hour}, wantErr: true},
		{name: "invalid tag with newline", p: Purge{Kind: PurgeTags, Tags: []string{"a\n"}, Retain: time.Hour}, wantErr: true},
		{name: "invalid tag 0x7f", p: Purge{Kind: PurgeTags, Tags: []string{"\x7f"}, Retain: time.Hour}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.p.Validate()
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Validate() = nil, want error")
			}
			if !errors.Is(err, ErrInvalidPurge) {
				t.Errorf("Validate() error %v does not wrap ErrInvalidPurge", err)
			}
			if errors.Is(err, ErrUnavailable) {
				t.Errorf("Validate() error %v wraps ErrUnavailable", err)
			}
		})
	}
}
