package config

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	adminHexA = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	adminHexB = "FEDCBA9876543210FEDCBA9876543210FEDCBA9876543210FEDCBA9876543210"
	adminHexC = "1111111111111111111111111111111111111111111111111111111111111111"
)

// adminSeedError is an unrelated invalid field that every invalid admin case also carries.
const adminSeedError = "global:\n  drain_timeout: -1s\n"

type adminSpec struct{ id, hash string }

func adminKeysYAML(specs []adminSpec) string {
	var b strings.Builder
	for _, s := range specs {
		b.WriteString("      - id: " + strconv.Quote(s.id) + "\n")
		b.WriteString("        hash: " + strconv.Quote(s.hash) + "\n")
	}
	return b.String()
}

func adminKeysGo(specs []adminSpec) []AdminKey {
	keys := make([]AdminKey, 0, len(specs))
	for _, s := range specs {
		keys = append(keys, AdminKey{ID: s.id, Hash: NewSecret(s.hash)})
	}
	return keys
}

func adminBlock(specs []adminSpec) string {
	return "admin:\n  auth:\n    keys:\n" + adminKeysYAML(specs)
}

func seededAdminDoc(specs []adminSpec) string {
	return "version: 1\n" + adminSeedError + minimalBody + adminBlock(specs)
}

func checkAdminErr(t *testing.T, err error, path, msg string, specs []adminSpec, parsed bool) {
	t.Helper()
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	found, seed := false, false
	for _, fe := range ve.Errors {
		if fe.Path == path && strings.Contains(fe.Msg, msg) {
			found = true
			if parsed && fe.Line <= 0 {
				t.Errorf("%s reported with line %d, want a line", path, fe.Line)
			}
			if !parsed && fe.Line != 0 {
				t.Errorf("%s reported with line %d, want 0 for a Go-built config", path, fe.Line)
			}
		}
		if fe.Path == "global.drain_timeout" {
			seed = true
		}
	}
	if !found {
		t.Errorf("no error at %s containing %q: %v", path, msg, err)
	}
	if !seed {
		t.Errorf("seed error at global.drain_timeout missing: %v", err)
	}
	for _, s := range specs {
		if s.hash != "" && strings.Contains(err.Error(), s.hash) {
			t.Errorf("error text carries a hash: %v", err)
		}
	}
}

func TestAdminAuthValid(t *testing.T) {
	specs := []adminSpec{{"ops", "sha256:" + adminHexA}, {"ci", "sha256:" + adminHexB}}
	c, err := Parse([]byte(minimalYAML + adminBlock(specs)))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Admin.Auth.Keys) != 2 {
		t.Fatalf("got %d admin keys, want 2", len(c.Admin.Auth.Keys))
	}
	if got := c.Admin.Auth.Keys[1].Hash.Reveal(); got != "sha256:"+adminHexB {
		t.Errorf("Hash = %q, want the input", got)
	}

	if c, err := Parse([]byte(minimalYAML)); err != nil {
		t.Errorf("absent admin.auth rejected: %v", err)
	} else if len(c.Admin.Auth.Keys) != 0 {
		t.Errorf("absent admin.auth gave %d keys", len(c.Admin.Auth.Keys))
	}
	if _, err := Parse([]byte(minimalYAML + "admin:\n  auth:\n    keys: []\n")); err != nil {
		t.Errorf("empty keys rejected: %v", err)
	}
	if _, err := Parse([]byte(minimalYAML + "admin:\n  enabled: false\n  auth:\n    keys:\n" + adminKeysYAML(specs))); err != nil {
		t.Errorf("keys with admin.enabled false rejected: %v", err)
	}
}

func TestAdminAuthInvalid(t *testing.T) {
	cases := []struct {
		name  string
		specs []adminSpec
		path  string
		msg   string
		env   bool
	}{
		{"empty id", []adminSpec{{"", "sha256:" + adminHexA}}, "admin.auth.keys[0].id", "is required", false},
		{"duplicate id", []adminSpec{{"ops", "sha256:" + adminHexA}, {"ops", "sha256:" + adminHexB}}, "admin.auth.keys[1].id", `duplicate id "ops"`, false},
		{"duplicate hash", []adminSpec{{"ops", "sha256:" + adminHexA}, {"ci", "sha256:" + strings.ToUpper(adminHexA)}}, "admin.auth.keys[1].hash", "duplicate hash", false},
		{"wrong prefix", []adminSpec{{"ops", "md5:" + adminHexA}}, "admin.auth.keys[0].hash", "must have the form sha256:<64 hex digits>", false},
		{"short", []adminSpec{{"ops", "sha256:" + adminHexA[:63]}}, "admin.auth.keys[0].hash", "must have the form sha256:<64 hex digits>", false},
		{"non-hex", []adminSpec{{"ops", "sha256:" + adminHexA[:63] + "g"}}, "admin.auth.keys[0].hash", "must have the form sha256:<64 hex digits>", false},
		{"unset variable", []adminSpec{{"ops", "${UNSET}"}}, "admin.auth.keys[0].hash", "UNSET", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			noEnv := func(string) (string, bool) { return "", false }
			_, err := Parse([]byte(seededAdminDoc(tc.specs)), WithEnvLookup(noEnv))
			checkAdminErr(t, err, tc.path, tc.msg, tc.specs, true)
			if tc.env {
				return
			}
			c := goConfig(1)
			c.Global.DrainTimeout = Duration(-time.Second)
			c.Admin.Auth.Keys = adminKeysGo(tc.specs)
			checkAdminErr(t, c.Validate(), tc.path, tc.msg, tc.specs, false)
		})
	}
}

func TestAdminAuthEnvHash(t *testing.T) {
	doc := minimalYAML + adminBlock([]adminSpec{{"ops", "${ADMIN_HASH}"}})
	lookup := func(name string) (string, bool) {
		if name == "ADMIN_HASH" {
			return "sha256:" + adminHexA, true
		}
		return "", false
	}
	c, err := Parse([]byte(doc), WithEnvLookup(lookup))
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Admin.Auth.Keys[0].Hash.Reveal(); got != "sha256:"+adminHexA {
		t.Errorf("Hash = %q, want the looked-up value", got)
	}
}

func TestAdminAuthRedacted(t *testing.T) {
	specs := []adminSpec{{"ops", "sha256:" + adminHexA}, {"ci", "sha256:" + adminHexB}}
	c, err := Parse([]byte(minimalYAML + adminBlock(specs)))
	if err != nil {
		t.Fatal(err)
	}
	r := c.Redacted()
	if r.Admin.Auth.Keys[0].ID != "ops" {
		t.Errorf("ID = %q, want ops", r.Admin.Auth.Keys[0].ID)
	}
	if got := r.Admin.Auth.Keys[0].Hash.Reveal(); got != Redacted {
		t.Errorf("Hash = %q, want %q", got, Redacted)
	}
	if got := c.Admin.Auth.Keys[0].Hash.Reveal(); got != "sha256:"+adminHexA {
		t.Errorf("original hash changed to %q", got)
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, `"id":"ops"`) {
		t.Errorf("redacted JSON lacks the key id: %s", s)
	}
	if strings.Contains(s, adminHexA) || strings.Contains(s, adminHexB) {
		t.Errorf("redacted JSON carries a hash: %s", s)
	}
}

func TestAdminAuthFingerprint(t *testing.T) {
	fp := func(hash string) string {
		c, err := Parse([]byte(minimalYAML + adminBlock([]adminSpec{{"ops", hash}})))
		if err != nil {
			t.Fatal(err)
		}
		return c.Fingerprint()
	}
	if fp("sha256:"+adminHexA) == fp("sha256:"+adminHexB) {
		t.Error("changing the admin key hash did not change the fingerprint")
	}
	if fp("sha256:"+adminHexA) != fp("sha256:"+adminHexA) {
		t.Error("same admin key hash gave different fingerprints")
	}
}
