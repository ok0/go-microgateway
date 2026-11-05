package config_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/doccheck"
	"go.yaml.in/yaml/v3"
)

type docKey struct {
	Path    string
	Default string
	Line    int
}

const defaultsFixture = `version: 1
listeners:
  - name: public
    address: ":8443"
    tls:
      cert_file: "{{cert}}"
      key_file: "{{key}}"
  - name: plain
    address: ":8080"
upstreams:
  - name: hosts-backend
    hosts:
      - url: "http://127.0.0.1:9001"
    load_balancer: {}
    timeouts: {}
    pool: {}
    passive_health: {}
    active_health: {}
    circuit_breaker: {}
  - name: dns-backend
    dns:
      hostname: backend.example.com
      port: 8080
    load_balancer: {}
    timeouts: {}
    pool: {}
    passive_health: {}
    active_health: {}
    circuit_breaker: {}
stores:
  - name: shared
    type: redis
    guard: {}
    redis:
      addresses: ["127.0.0.1:6379"]
      tls: {}
cache:
  memory: {}
  coalescing: {}
auth:
  jwt:
    - name: jwt-main
      issuer: "https://auth.example.com"
      audiences: [example-api]
      jwks:
        url: "https://auth.example.com/.well-known/jwks.json"
        algorithms: [RS256]
  api_keys:
    - name: keys-main
      keys:
        - id: alice
          hash: "sha256:9771189e33ad49531f98873eb4e4458ce1c96f313fe15c6d753f4c7b6ad4eba5"
          consumer: alice
middlewares:
  - name: custom-mw
    type: custom
routes:
  - name: orders
    upstream: hosts-backend
    auth: {}
    rate_limit:
      rate: 10
    cache: {}
    circuit_breaker: {}
    retry:
      idempotency_key: {}
      budget: {}
`

func docKeys(t *testing.T) []docKey {
	t.Helper()
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := doccheck.Load(filepath.Join(root, "docs", "configuration.md"))
	if err != nil {
		t.Fatal(err)
	}
	header := []string{"Key", "Type", "Default", "Constraints", "Meaning"}
	var keys []docKey
	for _, tbl := range doc.Tables {
		if !reflect.DeepEqual(tbl.Header, header) {
			continue
		}
		for i, row := range tbl.Rows {
			line := tbl.Line + 2 + i
			if len(row) != len(header) {
				t.Fatalf("docs/configuration.md:%d: row has %d cells, want %d", line, len(row), len(header))
			}
			path, ok := codeSpan(row[0])
			if !ok {
				t.Fatalf("docs/configuration.md:%d: key cell %q is not one code span", line, row[0])
			}
			keys = append(keys, docKey{Path: path, Default: row[2], Line: line})
		}
	}
	return keys
}

func codeSpan(cell string) (string, bool) {
	if len(cell) < 3 || cell[0] != '`' || cell[len(cell)-1] != '`' || strings.Count(cell, "`") != 2 {
		return "", false
	}
	return cell[1 : len(cell)-1], true
}

func documentedPaths(keys []docKey) []string {
	paths := make([]string, 0, len(keys))
	for _, k := range keys {
		paths = append(paths, k.Path)
	}
	return paths
}

func schemaSet(t *testing.T) map[string]bool {
	t.Helper()
	set := map[string]bool{}
	for _, p := range doccheck.SchemaPaths(reflect.TypeOf(config.Config{})) {
		set[p] = true
	}
	return set
}

func setPaths(n *yaml.Node, prefix string, set map[string]bool) {
	switch n.Kind {
	case yaml.DocumentNode:
		for _, c := range n.Content {
			setPaths(c, prefix, set)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			path := n.Content[i].Value
			if prefix != "" {
				path = prefix + "." + path
			}
			setValue(n.Content[i+1], path, set)
		}
	}
}

func setValue(v *yaml.Node, path string, set map[string]bool) {
	switch v.Kind {
	case yaml.MappingNode:
		setPaths(v, path, set)
	case yaml.SequenceNode:
		set[path] = true
		for _, item := range v.Content {
			if item.Kind == yaml.MappingNode {
				setPaths(item, path+"[]", set)
			}
		}
	case yaml.ScalarNode:
		set[path] = true
	}
}

func writeSelfSigned(t *testing.T, certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "example.com"},
		DNSNames:     []string{"example.com"},
		NotBefore:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:     time.Date(2036, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func fixtureYAML(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	writeSelfSigned(t, certPath, keyPath)
	return strings.NewReplacer("{{cert}}", certPath, "{{key}}", keyPath).Replace(defaultsFixture)
}

func fixtureConfig(t *testing.T) (*config.Config, map[string]bool) {
	t.Helper()
	src := fixtureYAML(t)
	cfg, err := config.Parse([]byte(src), config.WithEnvLookup(doccheck.ExampleLookup(nil)))
	if err != nil {
		t.Fatal(err)
	}
	var root yaml.Node
	if err := yaml.Unmarshal([]byte(src), &root); err != nil {
		t.Fatal(err)
	}
	set := map[string]bool{}
	setPaths(&root, "", set)
	return cfg, set
}

func isCodeOrNone(d string) bool {
	_, ok := codeSpan(d)
	return ok || d == "none"
}

func defaultProblems(cfg *config.Config, keys []docKey, set map[string]bool) []string {
	var problems []string
	root := reflect.ValueOf(cfg)
	for _, k := range keys {
		where := fmt.Sprintf("docs/configuration.md:%d: %s", k.Line, k.Path)
		if !isCodeOrNone(k.Default) {
			if k.Default != "required" && !strings.HasPrefix(k.Default, "depends on") {
				problems = append(problems, fmt.Sprintf("%s: unrecognised default %q", where, k.Default))
			}
			continue
		}
		if set[k.Path] {
			problems = append(problems, where+": fixture sets the key")
			continue
		}
		vals := doccheck.Values(root, k.Path)
		if len(vals) == 0 {
			problems = append(problems, where+": fixture does not reach the key")
			continue
		}
		if k.Default == "none" {
			for _, v := range vals {
				if !v.IsZero() {
					problems = append(problems, fmt.Sprintf("%s: default is none but the fixture yields %v", where, v.Interface()))
					break
				}
			}
			continue
		}
		code, _ := codeSpan(k.Default)
		leaf := vals[0].Type()
		if leaf.Kind() == reflect.Pointer {
			leaf = leaf.Elem()
		}
		ptr := reflect.New(leaf)
		if err := yaml.Unmarshal([]byte(code), ptr.Interface()); err != nil {
			problems = append(problems, fmt.Sprintf("%s: default %s does not parse: %v", where, code, err))
			continue
		}
		want := ptr.Elem().Interface()
		for _, v := range vals {
			got := v
			if got.Kind() == reflect.Pointer {
				if got.IsNil() {
					problems = append(problems, fmt.Sprintf("%s: default is %s but the fixture yields nil", where, code))
					break
				}
				got = got.Elem()
			}
			if !reflect.DeepEqual(got.Interface(), want) {
				problems = append(problems, fmt.Sprintf("%s: default is %s but the fixture yields %v", where, code, got.Interface()))
				break
			}
		}
	}
	return problems
}

func containsProblem(problems []string, want string) bool {
	for _, p := range problems {
		if strings.Contains(p, want) {
			return true
		}
	}
	return false
}

func TestConfigurationReferenceKeys(t *testing.T) {
	schema := doccheck.SchemaPaths(reflect.TypeOf(config.Config{}))
	for _, p := range doccheck.CompareKeys(schema, documentedPaths(docKeys(t))) {
		t.Error(p)
	}
	in := schemaSet(t)
	for _, want := range []string{"stores[].redis.key_prefix", "routes[].cache.max_ttl", "admin.auth.keys[].hash", "routes[].rate_limit.on_store_error", "stores[].params"} {
		if !in[want] {
			t.Errorf("schema lacks %s; the comparison would be vacuous", want)
		}
	}
}

func TestConfigurationReferenceDefaults(t *testing.T) {
	cfg, set := fixtureConfig(t)
	keys := docKeys(t)
	for _, p := range defaultProblems(cfg, keys, set) {
		t.Error(p)
	}
	compared := 0
	for _, k := range keys {
		if isCodeOrNone(k.Default) {
			compared++
		}
	}
	if compared < 60 {
		t.Errorf("only %d keys have a literal or none default, want at least 60", compared)
	}
}

func TestConfigurationReferenceCheckFails(t *testing.T) {
	schema := doccheck.SchemaPaths(reflect.TypeOf(config.Config{}))

	keys := docKeys(t)
	var dropped []string
	found := false
	for _, k := range keys {
		if k.Path == "routes[].cache.max_ttl" {
			found = true
			continue
		}
		dropped = append(dropped, k.Path)
	}
	if !found {
		t.Fatal("docs/configuration.md lacks routes[].cache.max_ttl")
	}
	if !containsProblem(doccheck.CompareKeys(schema, dropped), "undocumented key routes[].cache.max_ttl") {
		t.Error("dropping routes[].cache.max_ttl is not reported as undocumented")
	}

	added := append(documentedPaths(docKeys(t)), "routes[].cache.not_a_key")
	if !containsProblem(doccheck.CompareKeys(schema, added), "routes[].cache.not_a_key") {
		t.Error("an extra documented key is not reported")
	}

	cfg, set := fixtureConfig(t)
	changed := docKeys(t)
	found = false
	for i := range changed {
		if changed[i].Path == "cache.store_timeout" {
			changed[i].Default = "`7s`"
			found = true
		}
	}
	if !found {
		t.Fatal("docs/configuration.md lacks cache.store_timeout")
	}
	if !containsProblem(defaultProblems(cfg, changed, set), "cache.store_timeout") {
		t.Error("a wrong default for cache.store_timeout is not reported")
	}
}
