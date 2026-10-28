package runtime

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/leakcheck"
)

func TestScrubber(t *testing.T) {
	const secret = "a+b c/d&e?f=\"g\"<h>é"
	variants := map[string]string{
		"raw":            secret,
		"query escape":   url.QueryEscape(secret),
		"path escape":    url.PathEscape(secret),
		"go quote":       stripQuotes(strconv.Quote(secret)),
		"json":           jsonBody(t, secret),
		"base64 std":     base64.StdEncoding.EncodeToString([]byte(secret)),
		"base64 raw":     base64.RawStdEncoding.EncodeToString([]byte(secret)),
		"base64 url":     base64.URLEncoding.EncodeToString([]byte(secret)),
		"base64 raw url": base64.RawURLEncoding.EncodeToString([]byte(secret)),
		"hex":            hex.EncodeToString([]byte(secret)),
	}
	e := &config.Store{Name: "s", Type: "test", Redis: &config.RedisStore{Password: config.NewSecret(secret)}}
	sc := newScrubber(e)
	for name, v := range variants {
		if got, want := sc.text("x"+v+"y"), "xREDACTEDy"; got != want {
			t.Errorf("%s: scrubbed = %q, want %q", name, got, want)
		}
	}
}

func TestScrubberOneByteSecret(t *testing.T) {
	e := &config.Store{Name: "s", Type: "test", Params: config.NewSecretParams(map[string]any{"k": "Z"})}
	if got, want := newScrubber(e).text("aZb"), "aREDACTEDb"; got != want {
		t.Errorf("scrubbed = %q, want %q", got, want)
	}
}

func TestScrubberLongestFirst(t *testing.T) {
	e := &config.Store{Name: "s", Type: "test", Params: config.NewSecretParams(map[string]any{"short": "abc", "long": "abcdef"})}
	if got, want := newScrubber(e).text("abcdef"), "REDACTED"; got != want {
		t.Errorf("scrubbed = %q, want %q", got, want)
	}
}

func TestScrubberIgnoresEmptyValues(t *testing.T) {
	e := &config.Store{Name: "s", Type: "test", Params: config.NewSecretParams(map[string]any{"empty": ""}), Redis: &config.RedisStore{}}
	if got, want := newScrubber(e).text("hello world"), "hello world"; got != want {
		t.Errorf("scrubbed = %q, want %q", got, want)
	}
}

func TestScrubberNestedAndSentinel(t *testing.T) {
	e := &config.Store{
		Name:   "s",
		Type:   "redis",
		Redis:  &config.RedisStore{SentinelPassword: config.NewSecret("sentinel-secret")},
		Params: config.SecretParams{},
	}
	nested := &config.Store{
		Name:   "s",
		Type:   "test",
		Params: config.NewSecretParams(map[string]any{"n": map[string]any{"deep": []any{"deep-secret"}}}),
	}
	if got, want := newScrubber(e).text("sentinel-secret"), "REDACTED"; got != want {
		t.Errorf("sentinel scrubbed = %q, want %q", got, want)
	}
	if got, want := newScrubber(nested).text("deep-secret"), "REDACTED"; got != want {
		t.Errorf("nested scrubbed = %q, want %q", got, want)
	}
}

func TestScrubText(t *testing.T) {
	const secret = "sv-LEAK-1"
	text := "raw " + secret +
		" b64 " + base64.StdEncoding.EncodeToString([]byte(secret)) +
		" hex " + hex.EncodeToString([]byte(secret))
	got := ScrubText(text, []string{secret})
	if !strings.Contains(got, "REDACTED") {
		t.Errorf("scrubbed = %q, want REDACTED", got)
	}
	for _, form := range leakcheck.Forms(secret) {
		if strings.Contains(got, form) {
			t.Errorf("scrubbed text still contains %q: %q", form, got)
		}
	}
	const plain = "nothing to hide"
	if got, want := ScrubText(plain, nil), plain; got != want {
		t.Errorf("no values: scrubbed = %q, want %q", got, want)
	}
	if got, want := ScrubText(plain, []string{""}), plain; got != want {
		t.Errorf("empty value: scrubbed = %q, want %q", got, want)
	}
}

func jsonBody(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return stripQuotes(string(b))
}
