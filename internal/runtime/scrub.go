package runtime

import (
	"cmp"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/ok0/lychgate/config"
)

// scrubber replaces the secret values of one store, and their standard encodings, in text.
type scrubber struct{ olds []string }

func newScrubber(e *config.Store) *scrubber {
	var values []string
	if r := e.Redis; r != nil {
		values = append(values, r.Password.Reveal(), r.SentinelPassword.Reveal())
	}
	values = append(values, e.Params.StringLeaves()...)
	return newValueScrubber(values)
}

// ScrubText returns text with every value in values, and its standard encodings, replaced by config.Redacted.
func ScrubText(text string, values []string) string {
	return newValueScrubber(values).text(text)
}

func newValueScrubber(values []string) *scrubber {
	seen := make(map[string]bool)
	var olds []string
	add := func(s string) {
		if s == "" || seen[s] {
			return
		}
		seen[s] = true
		olds = append(olds, s)
	}
	for _, v := range values {
		add(v)
		add(url.QueryEscape(v))
		add(url.PathEscape(v))
		add(stripQuotes(strconv.Quote(v)))
		if b, err := json.Marshal(v); err == nil {
			add(stripQuotes(string(b)))
		}
		add(base64.StdEncoding.EncodeToString([]byte(v)))
		add(base64.RawStdEncoding.EncodeToString([]byte(v)))
		add(base64.URLEncoding.EncodeToString([]byte(v)))
		add(base64.RawURLEncoding.EncodeToString([]byte(v)))
		add(hex.EncodeToString([]byte(v)))
	}
	slices.SortFunc(olds, func(a, b string) int {
		if c := cmp.Compare(len(b), len(a)); c != 0 {
			return c
		}
		return strings.Compare(a, b)
	})
	return &scrubber{olds: olds}
}

func stripQuotes(s string) string {
	return s[1 : len(s)-1]
}

func (s *scrubber) text(t string) string {
	for _, old := range s.olds {
		t = strings.ReplaceAll(t, old, config.Redacted)
	}
	return t
}
