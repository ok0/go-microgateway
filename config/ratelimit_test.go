package config

import (
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/store"
)

func rlDur(d time.Duration) *Duration {
	v := Duration(d)
	return &v
}

func lineOfKey(t *testing.T, lines []string, route, anchor string) int {
	t.Helper()
	start := -1
	for i, l := range lines {
		if strings.TrimSpace(l) == "- name: "+route {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("route %s not found in fixture", route)
	}
	for i := start; i < len(lines); i++ {
		if strings.HasPrefix(strings.TrimSpace(lines[i]), anchor) {
			return i + 1
		}
	}
	t.Fatalf("anchor %q not found in route %s", anchor, route)
	return 0
}

func TestRateLimitSeededErrors(t *testing.T) {
	const file = "testdata/ratelimit_invalid.yaml"
	raw, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(raw), "\n")

	_, err = LoadFile(file)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("LoadFile error = %v, want *ValidationError", err)
	}

	seeded := []struct {
		route, path, anchor, msg string
	}{
		{"r1", "routes[0].rate_limit.idle_ttl", "idle_ttl:", "unknown field"},
		{"r2", "routes[1].rate_limit.rate", "rate:", "must be >= 1"},
		{"r3", "routes[2].rate_limit.rate", "rate_limit:", "is required for token_bucket"},
		{"r4", "routes[3].rate_limit.per", "per:", "must be positive"},
		{"r5", "routes[4].rate_limit.burst", "burst:", "must be >= 1"},
		{"r6", "routes[5].rate_limit.max_keys", "max_keys:", "must be >= 1"},
		{"r7", "routes[6].rate_limit.window", "window:", "must be positive"},
		{"r8", "routes[7].rate_limit.limit", "limit:", "is only valid with algorithm sliding_window"},
		{"r9", "routes[8].rate_limit.rate", "rate:", "is only valid with algorithm token_bucket"},
		{"r10", "routes[9].rate_limit.key.name", "key:", "is required when key source is header"},
		{"r11", "routes[10].rate_limit.key.name", "key:", "is only valid with key source header"},
		{"r12", "routes[11].rate_limit.algorithm", "algorithm:", "must be one of"},
		{"r13", "routes[12].rate_limit.key.source", "key:", "must be one of"},
		{"r14", "routes[13].rate_limit.rate", "rate:", "at least 1µs"},
		{"r15", "routes[14].rate_limit.burst", "burst:", "24h"},
		{"r16", "routes[15].rate_limit.key.source", "key:", "consumer requires route authentication"},
		{"r17", "routes[16].pipeline", "pipeline:", `must include "rate_limit"`},
	}
	if len(ve.Errors) != len(seeded) {
		t.Fatalf("got %d errors, want %d: %v", len(ve.Errors), len(seeded), ve)
	}
	for _, s := range seeded {
		want := lineOfKey(t, lines, s.route, s.anchor)
		var hits []FieldError
		for _, fe := range ve.Errors {
			if fe.Path == s.path && strings.Contains(fe.Msg, s.msg) {
				hits = append(hits, fe)
			}
		}
		if len(hits) != 1 {
			t.Errorf("%s containing %q: %d matches, want 1", s.path, s.msg, len(hits))
			continue
		}
		if hits[0].Line != want {
			t.Errorf("%s: line %d, want %d", s.path, hits[0].Line, want)
		}
	}
}

func TestRateLimitDefaults(t *testing.T) {
	c, err := Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 7\n"))
	if err != nil {
		t.Fatal(err)
	}
	rl := c.Routes[0].RateLimit
	if rl.Per == nil || *rl.Per != Duration(time.Second) {
		t.Errorf("Per = %v, want 1s", rl.Per)
	}
	if rl.Burst == nil || *rl.Burst != 7 {
		t.Errorf("Burst = %v, want 7", rl.Burst)
	}
	if rl.Window != nil {
		t.Errorf("Window = %v, want nil", rl.Window)
	}
	if rl.Limit != nil {
		t.Errorf("Limit = %v, want nil", rl.Limit)
	}
	if rl.MaxKeys == nil || *rl.MaxKeys != 100000 {
		t.Errorf("MaxKeys = %v, want 100000", rl.MaxKeys)
	}
	if rl.Key.Source != "ip" {
		t.Errorf("Key.Source = %q, want ip", rl.Key.Source)
	}

	c, err = Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 7\n      per: 2s\n      burst: 3\n      max_keys: 9\n"))
	if err != nil {
		t.Fatal(err)
	}
	rl = c.Routes[0].RateLimit
	if rl.Per == nil || *rl.Per != Duration(2*time.Second) {
		t.Errorf("Per = %v, want 2s", rl.Per)
	}
	if rl.Burst == nil || *rl.Burst != 3 {
		t.Errorf("Burst = %v, want 3", rl.Burst)
	}
	if rl.MaxKeys == nil || *rl.MaxKeys != 9 {
		t.Errorf("MaxKeys = %v, want 9", rl.MaxKeys)
	}

	c, err = Parse([]byte(minimalYAML + "    rate_limit:\n      algorithm: sliding_window\n      limit: 4\n"))
	if err != nil {
		t.Fatal(err)
	}
	rl = c.Routes[0].RateLimit
	if rl.Window == nil || *rl.Window != Duration(time.Minute) {
		t.Errorf("Window = %v, want 1m", rl.Window)
	}
	if rl.Per != nil || rl.Burst != nil || rl.Rate != nil {
		t.Errorf("token_bucket fields set on sliding_window: per=%v burst=%v rate=%v", rl.Per, rl.Burst, rl.Rate)
	}

	c, err = Parse([]byte(minimalYAML + "    rate_limit:\n      algorithm: sliding_window\n      limit: 4\n      window: 10s\n"))
	if err != nil {
		t.Fatal(err)
	}
	if w := c.Routes[0].RateLimit.Window; w == nil || *w != Duration(10*time.Second) {
		t.Errorf("Window = %v, want 10s", w)
	}

	c, err = Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 7\n"))
	if err != nil {
		t.Fatal(err)
	}
	before := c.Clone()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before.Routes[0].RateLimit, c.Routes[0].RateLimit) {
		t.Errorf("repeated Validate changed rate_limit: before %+v, after %+v", before.Routes[0].RateLimit, c.Routes[0].RateLimit)
	}
}

func TestRateLimitExplicitZeroNotDefaulted(t *testing.T) {
	zero := Duration(0)
	c := &Config{Routes: []Route{{RateLimit: &RateLimit{Algorithm: "token_bucket", Rate: int64Ptr(5), Per: &zero, Burst: int64Ptr(0)}}}}
	c.applyDefaults()
	rl := c.Routes[0].RateLimit
	if rl.Per == nil || *rl.Per != 0 {
		t.Errorf("Per = %v, want explicit 0", rl.Per)
	}
	if rl.Burst == nil || *rl.Burst != 0 {
		t.Errorf("Burst = %v, want explicit 0", rl.Burst)
	}

	wz := Duration(0)
	sw := &Config{Routes: []Route{{RateLimit: &RateLimit{Algorithm: "sliding_window", Limit: int64Ptr(4), Window: &wz}}}}
	sw.applyDefaults()
	if w := sw.Routes[0].RateLimit.Window; w == nil || *w != 0 {
		t.Errorf("Window = %v, want explicit 0", w)
	}

	_, err := Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 5\n      per: 0s\n      burst: 0\n"))
	if !findFieldError(err, "routes[0].rate_limit.per", "must be positive") {
		t.Errorf("per: 0s error missing: %v", err)
	}
	if !findFieldError(err, "routes[0].rate_limit.burst", "must be >= 1") {
		t.Errorf("burst: 0 error missing: %v", err)
	}

	_, err = Parse([]byte(minimalYAML + "    rate_limit:\n      algorithm: sliding_window\n      limit: 4\n      window: 0s\n"))
	if !findFieldError(err, "routes[0].rate_limit.window", "must be positive") {
		t.Errorf("window: 0s error missing: %v", err)
	}
}

func TestRateLimitRateAbsentVsZero(t *testing.T) {
	_, absent := Parse([]byte(minimalYAML + "    rate_limit:\n      algorithm: token_bucket\n"))
	_, zero := Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 0\n"))
	if !findFieldError(absent, "routes[0].rate_limit.rate", "is required for token_bucket") {
		t.Errorf("absent rate error missing: %v", absent)
	}
	if findFieldError(absent, "routes[0].rate_limit.rate", "must be >= 1") {
		t.Errorf("absent rate reported as zero: %v", absent)
	}
	if !findFieldError(zero, "routes[0].rate_limit.rate", "must be >= 1") {
		t.Errorf("zero rate error missing: %v", zero)
	}
	if findFieldError(zero, "routes[0].rate_limit.rate", "is required") {
		t.Errorf("zero rate reported as absent: %v", zero)
	}
	for _, err := range []error{absent, zero} {
		var ve *ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("error = %v, want *ValidationError", err)
		}
		for _, fe := range ve.Errors {
			if strings.HasSuffix(fe.Path, ".burst") {
				t.Errorf("unexpected burst error %v", fe)
			}
		}
	}
}

func TestRateLimitFractionalRate(t *testing.T) {
	tests := []struct {
		name, block, path, value, typ, key string
	}{
		{"token bucket rate 2.5", "    rate_limit:\n      rate: 2.5\n", "routes[0].rate_limit.rate", "2.5", "int64", "rate: 2.5"},
		{"token bucket rate 1.0", "    rate_limit:\n      rate: 1.0\n", "routes[0].rate_limit.rate", "1.0", "int64", "rate: 1.0"},
		{"token bucket rate 1e3", "    rate_limit:\n      rate: 1e3\n", "routes[0].rate_limit.rate", "1e3", "int64", "rate: 1e3"},
		{"token bucket burst 2.5", "    rate_limit:\n      rate: 5\n      burst: 2.5\n", "routes[0].rate_limit.burst", "2.5", "int64", "burst: 2.5"},
		{"token bucket max_keys 2.5", "    rate_limit:\n      rate: 5\n      max_keys: 2.5\n", "routes[0].rate_limit.max_keys", "2.5", "int", "max_keys: 2.5"},
		{"sliding window limit 2.5", "    rate_limit:\n      algorithm: sliding_window\n      limit: 2.5\n", "routes[0].rate_limit.limit", "2.5", "int64", "limit: 2.5"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := minimalYAML + tc.block
			_, err := Parse([]byte(src))
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error = %v, want *ValidationError", err)
			}
			var hits []FieldError
			for _, fe := range ve.Errors {
				if fe.Path == tc.path {
					hits = append(hits, fe)
				}
			}
			if len(hits) != 1 {
				t.Fatalf("%d errors at %s, want 1: %v", len(hits), tc.path, err)
			}
			want := "cannot unmarshal !!float `" + tc.value + "` into " + tc.typ
			if hits[0].Msg != want {
				t.Errorf("Msg = %q, want %q", hits[0].Msg, want)
			}
			if line := lineOf(src, tc.key); hits[0].Line != line {
				t.Errorf("Line = %d, want %d", hits[0].Line, line)
			}
		})
	}
}

func TestStoreLimit(t *testing.T) {
	c, err := Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 5\n      per: 1s\n      burst: 3\n"))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := c.Routes[0].RateLimit.StoreLimit()
	want := store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3}
	if !ok || got != want {
		t.Errorf("StoreLimit() = %+v, %v; want %+v, true", got, ok, want)
	}

	c, err = Parse([]byte(minimalYAML + "    rate_limit:\n      algorithm: sliding_window\n      limit: 10\n      window: 1m\n"))
	if err != nil {
		t.Fatal(err)
	}
	got, ok = c.Routes[0].RateLimit.StoreLimit()
	want = store.Limit{Algorithm: store.SlidingWindow, Count: 10, Period: time.Minute}
	if !ok || got != want {
		t.Errorf("StoreLimit() = %+v, %v; want %+v, true", got, ok, want)
	}

	unknown := &RateLimit{Algorithm: "leaky", Rate: int64Ptr(1), Per: rlDur(time.Second), Burst: int64Ptr(1)}
	if _, ok := unknown.StoreLimit(); ok {
		t.Error("StoreLimit() ok = true for unknown algorithm")
	}

	cases := []struct {
		name  string
		rl    RateLimit
		valid bool
	}{
		{"token bucket 1000 per 1ms", RateLimit{Algorithm: "token_bucket", Rate: int64Ptr(1000), Per: rlDur(time.Millisecond)}, true},
		{"token bucket 1 per 1h burst 24", RateLimit{Algorithm: "token_bucket", Rate: int64Ptr(1), Per: rlDur(time.Hour), Burst: int64Ptr(24)}, true},
		{"sliding window 1 per 24h", RateLimit{Algorithm: "sliding_window", Limit: int64Ptr(1), Window: rlDur(24 * time.Hour)}, true},
		{"token bucket 1001 per 1ms", RateLimit{Algorithm: "token_bucket", Rate: int64Ptr(1001), Per: rlDur(time.Millisecond)}, false},
		{"token bucket 1 per 1h burst 25", RateLimit{Algorithm: "token_bucket", Rate: int64Ptr(1), Per: rlDur(time.Hour), Burst: int64Ptr(25)}, false},
		{"sliding window 1 per 24h+1ns", RateLimit{Algorithm: "sliding_window", Limit: int64Ptr(1), Window: rlDur(24*time.Hour + time.Nanosecond)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rl := tc.rl
			cfg := &Config{Routes: []Route{{RateLimit: &rl}}}
			cfg.applyDefaults()
			ck := &checker{}
			checkRateLimit(ck, "routes[0].rate_limit", &cfg.Routes[0])
			if (len(ck.errs) == 0) != tc.valid {
				t.Fatalf("config errors = %v, want valid = %v", ck.errs, tc.valid)
			}
			l, ok := cfg.Routes[0].RateLimit.StoreLimit()
			if !ok {
				t.Fatal("StoreLimit() ok = false after defaults")
			}
			if err := l.Validate(); (err == nil) != tc.valid {
				t.Fatalf("store Validate() = %v, want valid = %v", err, tc.valid)
			}
		})
	}
}

func TestRateLimitFingerprint(t *testing.T) {
	parse := func(body string) *Config {
		t.Helper()
		c, err := Parse([]byte(minimalYAML + "    rate_limit:\n" + body))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}

	if parse("      rate: 7\n").Fingerprint() != parse("      rate: 7\n      per: 1s\n").Fingerprint() {
		t.Error("absent per and per: 1s have different fingerprints")
	}
	if parse("      rate: 7\n").Fingerprint() != parse("      rate: 7\n      burst: 7\n").Fingerprint() {
		t.Error("absent burst and burst equal to rate have different fingerprints")
	}

	base := parse("      rate: 7\n      per: 2s\n      burst: 3\n      max_keys: 9\n")
	mutations := map[string]func(rl *RateLimit){
		"algorithm":  func(rl *RateLimit) { rl.Algorithm = "sliding_window" },
		"rate":       func(rl *RateLimit) { rl.Rate = int64Ptr(8) },
		"per":        func(rl *RateLimit) { rl.Per = rlDur(3 * time.Second) },
		"burst":      func(rl *RateLimit) { rl.Burst = int64Ptr(4) },
		"limit":      func(rl *RateLimit) { rl.Limit = int64Ptr(3) },
		"window":     func(rl *RateLimit) { rl.Window = rlDur(30 * time.Second) },
		"key.source": func(rl *RateLimit) { rl.Key.Source = "route" },
		"key.name":   func(rl *RateLimit) { rl.Key.Name = "X-B" },
		"max_keys":   func(rl *RateLimit) { rl.MaxKeys = intPtr(10) },
	}
	for name, mutate := range mutations {
		c := base.Clone()
		mutate(c.Routes[0].RateLimit)
		if c.Fingerprint() == base.Fingerprint() {
			t.Errorf("changing %s did not change Fingerprint", name)
		}
	}
}

func TestRateLimitClone(t *testing.T) {
	c, err := Parse([]byte(minimalYAML + "    rate_limit:\n      rate: 7\n      per: 2s\n      burst: 3\n      max_keys: 9\n      key:\n        source: header\n        name: X-A\n"))
	if err != nil {
		t.Fatal(err)
	}
	rl := c.Routes[0].RateLimit
	rl.Limit = int64Ptr(3)
	rl.Window = rlDur(time.Minute)
	before := c.Fingerprint()

	clone := c.Clone()
	crl := clone.Routes[0].RateLimit
	if crl.Rate == rl.Rate || crl.Per == rl.Per || crl.Burst == rl.Burst || crl.Limit == rl.Limit || crl.Window == rl.Window || crl.MaxKeys == rl.MaxKeys {
		t.Fatal("clone shares a pointer field with the original")
	}

	*crl.Rate = 99
	*crl.Per = Duration(9 * time.Second)
	*crl.Burst = 99
	*crl.Limit = 99
	*crl.Window = Duration(9 * time.Minute)
	*crl.MaxKeys = 99
	crl.Key.Source = "route"
	crl.Key.Name = "other"

	if c.Fingerprint() != before {
		t.Error("mutating the clone changed the original")
	}
}

func TestRateLimitRedacted(t *testing.T) {
	c := parseFull(t)
	r := c.Redacted()
	for i := range c.Routes {
		if !reflect.DeepEqual(c.Routes[i].RateLimit, r.Routes[i].RateLimit) {
			t.Errorf("routes[%d].rate_limit changed by Redacted", i)
		}
	}
}

func TestExamplesValidate(t *testing.T) {
	for _, path := range []string{"../examples/basic.yaml", "testdata/full.yaml"} {
		if _, err := LoadFile(path); err != nil {
			t.Errorf("LoadFile(%s): %v", path, err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "idle_ttl") {
			t.Errorf("%s still contains idle_ttl", path)
		}
	}
}
