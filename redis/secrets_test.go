package redis

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/leakcheck"
)

func uniqueSecret(t *testing.T, prefix string) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return prefix + hex.EncodeToString(b[:])
}

func containsSecretForm(text string, secrets ...string) bool {
	for _, s := range secrets {
		for _, f := range leakcheck.Forms(s) {
			if strings.Contains(text, f) {
				return true
			}
		}
	}
	return false
}

func TestSecretsNeverLeak(t *testing.T) {
	pw := uniqueSecret(t, "pw-")
	spw := uniqueSecret(t, "spw-")
	wpw := uniqueSecret(t, "wpw-")
	secrets := []string{pw, spw, wpw}

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var observed []string
	var errs []string
	remote := "192.0.2.10:4000"
	deny := routeYAML("rl", "rate: 1\nper: 1h\nburst: 2\nstore: shared\non_store_error: deny\nstore_timeout: 2s\n")

	authMini := newMini(t)
	authMini.RequireAuth(pw)
	g1 := mustGateway(t, gwDoc(redisStoreYAML("shared", []string{authMini.Addr()}, "password: "+pw+"\n", ""), deny, upstream(t)),
		clock.NewFake(miniStart), logger)
	for _, want := range []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests} {
		rec := doGet(g1, "/rl", remote)
		if rec.Code != want {
			t.Fatalf("correct password: status %d, want %d", rec.Code, want)
		}
		observed = append(observed, rec.Body.String(), fmt.Sprint(rec.Header()))
	}

	wrongMini := newMini(t)
	wrongMini.RequireAuth(pw)
	g2 := mustGateway(t, gwDoc(redisStoreYAML("shared", []string{wrongMini.Addr()}, "password: "+wpw+"\n", ""), deny, upstream(t)),
		clock.NewFake(miniStart), logger)
	for range 4 {
		rec := doGet(g2, "/rl", remote)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("wrong password: status %d, want 503", rec.Code)
		}
		observed = append(observed, rec.Body.String(), fmt.Sprint(rec.Header()))
	}

	sentinelMini := newMini(t)
	sentinelExtra := "mode: sentinel\nmaster_name: mymaster\npassword: " + pw + "\nsentinel_password: " + spw + "\n"
	g3 := mustGateway(t, gwDoc(redisStoreYAML("shared", []string{sentinelMini.Addr()}, sentinelExtra, ""), deny, upstream(t)),
		clock.NewFake(miniStart), logger)
	for range 4 {
		rec := doGet(g3, "/rl", remote)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("sentinel: status %d, want 503", rec.Code)
		}
		observed = append(observed, rec.Body.String(), fmt.Sprint(rec.Header()))
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := ln.Addr().String()
	_ = ln.Close()
	g4 := mustGateway(t, gwDoc(redisStoreYAML("shared", []string{closedAddr}, "password: "+pw+"\n", ""), deny, upstream(t)),
		clock.NewFake(miniStart), logger)
	for range 4 {
		rec := doGet(g4, "/rl", remote)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("unreachable: status %d, want 503", rec.Code)
		}
		observed = append(observed, rec.Body.String(), fmt.Sprint(rec.Header()))
	}

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caPath, []byte("not a certificate "+spw), 0o600); err != nil {
		t.Fatal(err)
	}
	tlsExtra := "password: " + pw + "\ntls:\n  enabled: true\n  ca_file: " + strconv.Quote(caPath) + "\n"
	_, err = newGateway(t, gwDoc(redisStoreYAML("shared", []string{closedAddr}, tlsExtra, ""), deny, upstream(t)),
		true, clock.NewFake(miniStart), logger)
	if err == nil {
		t.Fatal("invalid ca_file: NewFromYAML returned no error")
	}
	errs = append(errs, err.Error())

	for _, g := range []*microgateway.Gateway{g1, g2, g3, g4} {
		observed = append(observed, metricsText(t, g))
		rec := httptest.NewRecorder()
		g.AdminHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/config", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /config: status %d, want 200", rec.Code)
		}
		observed = append(observed, rec.Body.String(), fmt.Sprint(rec.Header()))
	}
	observed = append(observed, logs.String())

	if !strings.Contains(logs.String(), "rate limit store unavailable") {
		t.Error("log has no guard transition record for the store becoming unavailable")
	}
	for i, text := range append(observed, errs...) {
		if containsSecretForm(text, secrets...) {
			t.Errorf("observable output %d contains a form of a secret", i)
		}
	}
}

func TestStoreFormatting(t *testing.T) {
	pw := uniqueSecret(t, "pw-")
	spw := uniqueSecret(t, "spw-")
	addr, _ := blackhole(t)
	s, err := NewFromConfig(&config.RedisStore{
		Mode:             "sentinel",
		MasterName:       "mymaster",
		Addresses:        []string{addr},
		Password:         config.NewSecret(pw),
		SentinelPassword: config.NewSecret(spw),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })

	opts := Options{
		KeyPrefix: "p",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)).With("password", pw, "sentinel_password", spw),
	}
	formats := []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X"}
	for _, f := range formats {
		for _, v := range []any{s, opts, &opts} {
			out := fmt.Sprintf(f, v)
			if containsSecretForm(out, pw, spw) {
				t.Errorf("%s of %T reveals a secret", f, v)
			}
		}
	}
	leakcheck.Check(t, "Store", s, pw, spw)
	leakcheck.Check(t, "Options", opts, pw, spw)
	if leakcheck.Reaches(reflect.TypeFor[Options](), reflect.TypeFor[config.Secret]()) {
		t.Error("Options can hold a config.Secret")
	}
}
