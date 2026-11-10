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
	first := fp("sha256:" + adminHexA)
	if first != fp("sha256:"+adminHexA) {
		t.Error("same admin key hash gave different fingerprints")
	}
}

func TestAdminProbeDefaults(t *testing.T) {
	c, err := Parse([]byte(minimalYAML))
	if err != nil {
		t.Fatal(err)
	}
	checkProbeDefaults(t, c)

	c, err = Parse([]byte(minimalYAML + "admin:\n  probes:\n    metrics: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !*c.Admin.Probes.Enabled || c.Admin.Probes.Address != ":9902" || !*c.Admin.Probes.Metrics || c.Admin.Auth.Management != "auto" {
		t.Errorf("partial probes block: got enabled=%v address=%q metrics=%v management=%q", *c.Admin.Probes.Enabled, c.Admin.Probes.Address, *c.Admin.Probes.Metrics, c.Admin.Auth.Management)
	}

	c, err = Parse([]byte(minimalYAML + "admin:\n  probes:\n    enabled: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if *c.Admin.Probes.Enabled {
		t.Error("explicit probes.enabled false was replaced")
	}

	c, err = Parse([]byte(minimalYAML + "admin:\n  probes:\n    address: \"\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if c.Admin.Probes.Address != ":9902" {
		t.Errorf("empty probes.address = %q, want the default :9902", c.Admin.Probes.Address)
	}

	g := goConfig(1)
	if err := g.Validate(); err != nil {
		t.Fatal(err)
	}
	checkProbeDefaults(t, g)

	g = goConfig(1)
	g.Admin.Probes.Enabled = boolPtr(false)
	if err := g.Validate(); err != nil {
		t.Fatal(err)
	}
	if *g.Admin.Probes.Enabled {
		t.Error("Go-built probes.enabled false was replaced")
	}
}

func checkProbeDefaults(t *testing.T, c *Config) {
	t.Helper()
	if c.Admin.Address != "127.0.0.1:9901" {
		t.Errorf("Admin.Address = %q, want 127.0.0.1:9901", c.Admin.Address)
	}
	if c.Admin.Probes.Enabled == nil || !*c.Admin.Probes.Enabled {
		t.Error("Probes.Enabled is not true by default")
	}
	if c.Admin.Probes.Address != ":9902" {
		t.Errorf("Probes.Address = %q, want :9902", c.Admin.Probes.Address)
	}
	if c.Admin.Probes.Metrics == nil || *c.Admin.Probes.Metrics {
		t.Error("Probes.Metrics is not false by default")
	}
	if c.Admin.Auth.Management != "auto" {
		t.Errorf("Auth.Management = %q, want auto", c.Admin.Auth.Management)
	}
}

func TestIsLoopbackAddress(t *testing.T) {
	cases := []struct {
		addr string
		want bool
	}{
		{"127.0.0.1:1", true},
		{"[::1]:1", true},
		{"localhost:1", true},
		{"LOCALHOST:1", true},
		{"127.0.0.2:1", true},
		{"127.0.0.1:0", true},
		{":1", false},
		{"0.0.0.0:1", false},
		{"[::]:1", false},
		{"host:1", false},
		{"10.0.0.1:1", false},
		{"127.0.0.1", false},
		{"localhost", false},
		{"[::1]", false},
		{"", false},
		{"127.0.0.1:x", false},
		{"127.0.0.1:65536", false},
		{"[::1%lo0]:1", false},
	}
	for _, tc := range cases {
		if got := IsLoopbackAddress(tc.addr); got != tc.want {
			t.Errorf("IsLoopbackAddress(%q) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

func TestManagementAuthRequired(t *testing.T) {
	for _, addr := range []string{"127.0.0.1:9901", "0.0.0.0:9901"} {
		loopback := IsLoopbackAddress(addr)
		for _, mode := range []string{"auto", "", "required", "none", "open"} {
			a := Admin{Address: addr, Auth: AdminAuth{Management: mode}}
			var want bool
			switch mode {
			case "auto", "":
				want = !loopback
			case "required", "open":
				want = true
			case "none":
				want = false
			}
			if got := a.ManagementAuthRequired(); got != want {
				t.Errorf("address %s mode %q: ManagementAuthRequired() = %v, want %v", addr, mode, got, want)
			}
		}
	}
}

func TestAdminExposureInvalid(t *testing.T) {
	cases := []struct {
		name    string
		admin   string
		goAdmin func(*Admin)
		path    string
		msg     string
	}{
		{
			"auto non-loopback without keys",
			"admin:\n  address: \"0.0.0.0:9901\"\n",
			func(a *Admin) { a.Address = "0.0.0.0:9901" },
			"admin.auth.keys", `non-loopback address "0.0.0.0:9901"`,
		},
		{
			"empty host without keys",
			"admin:\n  address: \":9901\"\n",
			func(a *Admin) { a.Address = ":9901" },
			"admin.auth.keys", `non-loopback address ":9901"`,
		},
		{
			"explicit auto unspecified IPv6 without keys",
			"admin:\n  address: \"[::]:9901\"\n  auth:\n    management: auto\n",
			func(a *Admin) { a.Address = "[::]:9901"; a.Auth.Management = "auto" },
			"admin.auth.keys", "non-loopback",
		},
		{
			"required on loopback without keys",
			"admin:\n  auth:\n    management: required\n",
			func(a *Admin) { a.Auth.Management = "required" },
			"admin.auth.keys", "admin.auth.management is required",
		},
		{
			"unknown management mode",
			"admin:\n  auth:\n    management: open\n",
			func(a *Admin) { a.Auth.Management = "open" },
			"admin.auth.management", "must be one of auto, required, none",
		},
		{
			"invalid probe address",
			"admin:\n  probes:\n    address: \"nowhere\"\n",
			func(a *Admin) { a.Probes.Address = "nowhere" },
			"admin.probes.address", `invalid address "nowhere"`,
		},
		{
			"probe port out of range",
			"admin:\n  probes:\n    address: \":99999\"\n",
			func(a *Admin) { a.Probes.Address = ":99999" },
			"admin.probes.address", "invalid address",
		},
		{
			"whitespace probe address",
			"admin:\n  probes:\n    address: \" \"\n",
			func(a *Admin) { a.Probes.Address = " " },
			"admin.probes.address", "invalid address",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte("version: 1\n" + adminSeedError + minimalBody + tc.admin))
			checkAdminErr(t, err, tc.path, tc.msg, nil, true)
			c := goConfig(1)
			c.Global.DrainTimeout = Duration(-time.Second)
			tc.goAdmin(&c.Admin)
			checkAdminErr(t, c.Validate(), tc.path, tc.msg, nil, false)
		})
	}
}

func TestAdminExposureOnePass(t *testing.T) {
	doc := "version: 1\n" + adminSeedError + minimalBody + "admin:\n  address: \"0.0.0.0:9901\"\n  probes:\n    address: \"nowhere\"\n"
	_, err := Parse([]byte(doc))
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	want := map[string]bool{"global.drain_timeout": false, "admin.auth.keys": false, "admin.probes.address": false}
	for _, fe := range ve.Errors {
		if _, ok := want[fe.Path]; ok {
			want[fe.Path] = true
			if fe.Line <= 0 {
				t.Errorf("%s reported with line %d, want a line", fe.Path, fe.Line)
			}
		}
	}
	for path, found := range want {
		if !found {
			t.Errorf("no error at %s: %v", path, err)
		}
	}
	if strings.Contains(err.Error(), "sha256:") {
		t.Errorf("error text carries a hash: %v", err)
	}
}

func TestAdminExposureValid(t *testing.T) {
	keyA := []adminSpec{{"ops", "sha256:" + adminHexA}}
	cases := []struct {
		name    string
		admin   string
		goAdmin func(*Admin)
	}{
		{
			"none on unspecified address without keys",
			"admin:\n  address: \"0.0.0.0:9901\"\n  auth:\n    management: none\n",
			func(a *Admin) { a.Address = "0.0.0.0:9901"; a.Auth.Management = "none" },
		},
		{
			"auto on unspecified address with one key",
			"admin:\n  address: \"0.0.0.0:9901\"\n  auth:\n    keys:\n" + adminKeysYAML(keyA),
			func(a *Admin) { a.Address = "0.0.0.0:9901"; a.Auth.Keys = adminKeysGo(keyA) },
		},
		{
			"required on loopback with one key",
			"admin:\n  auth:\n    management: required\n    keys:\n" + adminKeysYAML(keyA),
			func(a *Admin) { a.Auth.Management = "required"; a.Auth.Keys = adminKeysGo(keyA) },
		},
		{
			"disabled admin on unspecified address without keys",
			"admin:\n  enabled: false\n  address: \"0.0.0.0:9901\"\n",
			func(a *Admin) { a.Enabled = boolPtr(false); a.Address = "0.0.0.0:9901" },
		},
		{
			"disabled admin with required and no keys",
			"admin:\n  enabled: false\n  auth:\n    management: required\n",
			func(a *Admin) { a.Enabled = boolPtr(false); a.Auth.Management = "required" },
		},
		{
			"disabled probes with invalid address",
			"admin:\n  probes:\n    enabled: false\n    address: \"nowhere\"\n",
			func(a *Admin) { a.Probes.Enabled = boolPtr(false); a.Probes.Address = "nowhere" },
		},
		{
			"localhost on auto without keys",
			"admin:\n  address: \"localhost:9901\"\n",
			func(a *Admin) { a.Address = "localhost:9901" },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse([]byte(minimalYAML + tc.admin)); err != nil {
				t.Errorf("Parse: %v", err)
			}
			c := goConfig(1)
			tc.goAdmin(&c.Admin)
			if err := c.Validate(); err != nil {
				t.Errorf("Validate: %v", err)
			}
		})
	}
}

func TestAdminExposureInvalidAddressNoDerived(t *testing.T) {
	_, err := Parse([]byte(minimalYAML + "admin:\n  address: \"nowhere\"\n"))
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("want *ValidationError, got %T: %v", err, err)
	}
	atAddress := false
	for _, fe := range ve.Errors {
		if fe.Path == "admin.address" {
			atAddress = true
		}
		if fe.Path == "admin.auth.keys" {
			t.Errorf("derived error at admin.auth.keys: %v", err)
		}
	}
	if !atAddress {
		t.Errorf("no error at admin.address: %v", err)
	}
}
