package config

import (
	"errors"
	"os"
	"testing"
)

func FuzzParse(f *testing.F) {
	for _, name := range []string{"testdata/full.yaml", "testdata/invalid_multi.yaml"} {
		data, err := os.ReadFile(name)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	for _, s := range []string{
		"",
		"version: 1",
		"---\n---\n",
		"routes: [{match: {path: '/:a/*b', path_type: pattern}}]",
		"a: &x [*x]",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		cfg, err := Parse(data, WithEnvLookup(func(string) (string, bool) { return "v", true }))
		if err != nil {
			var ve *ValidationError
			if !errors.As(err, &ve) || len(ve.Errors) == 0 || err.Error() == "" || cfg != nil {
				t.Fatalf("invalid result: cfg=%v err=%T %v", cfg, err, err)
			}
			return
		}
		if cfg == nil {
			t.Fatal("nil config without error")
		}
		if cfg.Fingerprint() != cfg.Fingerprint() {
			t.Fatal("fingerprint is not stable")
		}
		if cfg.Redacted() == nil {
			t.Fatal("Redacted returned nil")
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("parsed config fails Validate: %v", err)
		}
	})
}
