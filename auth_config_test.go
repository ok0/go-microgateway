package microgateway

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/ok0/go-microgateway/config"
)

func TestAuthSeededErrorsNew(t *testing.T) {
	data, err := os.ReadFile("config/testdata/auth_errors.yaml")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewFromYAML(data)
	var ve *config.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("NewFromYAML error = %v, want *config.ValidationError", err)
	}
	if len(ve.Errors) < 12 {
		t.Fatalf("got %d errors, want at least 12:\n%v", len(ve.Errors), err)
	}
	paths := []string{
		"auth.jwt[0].issuer",
		"auth.jwt[1].audiences",
		"auth.jwt[2].keys[0].secret",
		"auth.jwt[2].max_token_bytes",
		"auth.jwt[3].keys[0].public_key",
		"auth.jwt[4].keys[0].algorithm",
		"auth.jwt[5].jwks.ttl",
		"auth.api_keys[0].keys[0].hash",
		"routes[0].auth.providers[0]",
		"routes[1].auth.providers[0]",
		"routes[2].auth.allow_anonymous",
		"routes[3].pipeline",
	}
	for _, p := range paths {
		found := false
		for _, fe := range ve.Errors {
			if fe.Path == p {
				found = true
				if fe.Line <= 0 {
					t.Errorf("%s: line = %d, want > 0", p, fe.Line)
				}
			}
		}
		if !found {
			t.Errorf("no error at %s:\n%v", p, err)
		}
	}
}

func TestAuthExplicitZeroAndEmpty(t *testing.T) {
	t.Run("leeway zero is honored", func(t *testing.T) {
		e := newAuEnv(t)
		be, rec := auBackend(e)
		secret := e.secret(auHMAC("HS256"))
		provider := func(name, leeway string) string {
			extra := ""
			if leeway != "" {
				extra = "      leeway: " + leeway + "\n"
			}
			return fmt.Sprintf(`    - name: %s
      issuer: https://idp.example
      audiences: [api]
%s      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
`, name, extra, secret)
		}
		authYAML := "auth:\n  jwt:\n" + provider("zero", "0s") + provider("default", "")
		routes := aidRoute("zero", "/zero", aidAuthBlock("jwt", "zero", "")) +
			aidRoute("default", "/default", aidAuthBlock("jwt", "default", ""))
		g := e.gateway(auDoc(be.URL, authYAML, routes))

		tok := e.mint("HS256", "h1", []byte(secret), e.claims(jwt.MapClaims{"exp": e.clk.Now().Add(-time.Second).Unix()}))
		hdr := http.Header{"Authorization": {"Bearer " + tok}}
		auAssert401(t, e.get(g, "/zero", hdr), "Bearer")
		if got := auFailures(t, g, "zero", "expired"); got != 1 {
			t.Fatalf("expired failures on zero = %v, want 1", got)
		}
		if res := e.get(g, "/default", hdr); res.Code != http.StatusOK {
			t.Fatalf("default leeway: status = %d, want 200", res.Code)
		}
		if rec.Count() != 1 {
			t.Fatalf("backend requests = %d, want 1", rec.Count())
		}
	})

	t.Run("empty strip list keeps identity headers", func(t *testing.T) {
		e := newAuEnv(t)
		be, rec := auBackend(e)
		routes := aidRoute("none", "/none", "")
		doc := auDoc(be.URL, "", routes)
		empty := strings.Replace(doc, "version: 1\n", "version: 1\nglobal:\n  strip_identity_headers: []\n", 1)

		kept := e.gateway(empty)
		if res := e.get(kept, "/none", http.Header{"X-User-Id": {"spoofed"}}); res.Code != http.StatusOK {
			t.Fatalf("empty strip list: status = %d, want 200", res.Code)
		}
		h, _ := rec.Last()
		if got := h.Get("X-User-Id"); got != "spoofed" {
			t.Errorf("empty strip list: X-User-Id = %q, want the forwarded value", got)
		}

		stripped := e.gateway(doc)
		if res := e.get(stripped, "/none", http.Header{"X-User-Id": {"spoofed"}}); res.Code != http.StatusOK {
			t.Fatalf("default strip list: status = %d, want 200", res.Code)
		}
		h, _ = rec.Last()
		if _, ok := h["X-User-Id"]; ok {
			t.Error("default strip list: X-User-Id reached the backend")
		}
	})
}

func TestAuthAdminConfigRedacted(t *testing.T) {
	e := newAuEnv(t)
	be, _ := auBackend(e)
	dir := t.TempDir()
	secret := e.secret(auHMAC("HS256"))
	inline := e.secret(auHash("act-inline-key-4f5e6d7c8b9a0f1e"))
	fileHash := e.secret(auHash("act-file-key-1e2d3c4b5a697881"))
	query := e.secret("q-s3cret-5b")
	keysPath := filepath.Join(dir, "keys.yaml")
	apkWrite(t, keysPath, apkKeysFile(fileHash))
	doc := auDoc(be.URL, fmt.Sprintf(`auth:
  jwt:
    - name: jwt1
      issuer: https://idp.example
      audiences: [api]
      keys:
        - kid: h1
          algorithm: HS256
          secret: %q
    - name: jwks1
      issuer: https://idp.example
      audiences: [api]
      jwks:
        url: "https://idp.example/jwks?token=%s"
        algorithms: [RS256]
  api_keys:
    - name: keys1
      keys:
        - id: k1
          hash: %q
          consumer: alice
    - name: keys2
      key_file: %q
`, secret, query, inline, keysPath), aidRoute("jwt", "/jwt", aidAuthBlock("jwt", "jwt1", "")))
	cfgPath := writeConfig(t, dir, doc)
	f := &auFetcher{}
	g := apkFromFile(t, e, cfgPath, withClock(e.clk), WithKeySetFetcher(f))

	body := adminBody(t, g, "/config")
	if !strings.Contains(body, "REDACTED") {
		t.Error("/config contains no REDACTED marker")
	}
	for _, s := range []string{secret, inline, fileHash, query, "file_keys"} {
		if strings.Contains(body, s) {
			t.Error("/config exposes a registered value or the file_keys field")
		}
	}
	if f.count() != 0 {
		t.Errorf("the JWKS fetcher was called %d times by the admin handler", f.count())
	}
}
