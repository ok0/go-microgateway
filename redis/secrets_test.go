package redis

import (
	"bytes"
	"context"
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
	"time"

	microgateway "github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/httpcache"
	"github.com/ok0/go-microgateway/internal/leakcheck"
	"github.com/ok0/go-microgateway/store"
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

func gcMalformed(t *testing.T, secret string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_, _ = io.WriteString(c, "%malformed "+secret+"\r\n")
			}(c)
		}
	}()
	return ln.Addr().String()
}

func gcSecretBypass(t *testing.T, label string, rec *httptest.ResponseRecorder) {
	t.Helper()
	if st := gcStatus(t, rec); rec.Code != http.StatusOK || st.Fwd != httpcache.FwdBypass {
		t.Fatalf("%s: status %d %+v, want 200 with fwd=bypass", label, rec.Code, st)
	}
}

func TestCacheSecretsNeverLeak(t *testing.T) {
	pw := uniqueSecret(t, "pw-")
	spw := uniqueSecret(t, "spw-")
	wpw := uniqueSecret(t, "wpw-")
	qv := uniqueSecret(t, "qv-")
	hv := uniqueSecret(t, "hv-")
	tg := uniqueSecret(t, "tg-")
	tok := uniqueSecret(t, "tok-")
	secrets := []string{pw, spw, wpw, qv, hv, tg, tok}

	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	var observed []string
	var errs []string
	seen := func(rec *httptest.ResponseRecorder) {
		h := rec.Header().Clone()
		h.Del("Surrogate-Key")
		observed = append(observed, rec.Body.String(), fmt.Sprint(h))
	}

	o := newGCOrigin(t)
	o.set(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Vary": {"X-Session"}}, "v1")
	o.tag("/q", tg)
	cacheTop := "  store: shared\n  store_timeout: 2s\n"
	routes := gcRoute("r")
	qURL := "http://example.com/q?token=" + qv
	hdr := func() http.Header {
		return http.Header{"X-Session": {hv}}
	}
	purgeBody := `{"tags":["` + tg + `"]}`

	authMini := newMini(t)
	authMini.RequireAuth(pw)
	g1 := mustGateway(t, gcDoc(redisStoreYAML("shared", []string{authMini.Addr()}, "password: "+pw+"\n", ""), cacheTop, gcAdminYAML(tok), routes, o.url),
		clock.NewFake(miniStart), logger)
	rec := gcRequest(g1, http.MethodGet, qURL, hdr())
	if st := gcStatus(t, rec); rec.Code != http.StatusOK || !st.Stored {
		t.Fatalf("fill with the correct password: status %d %+v, want 200 stored", rec.Code, st)
	}
	seen(rec)
	if rec = gcRequest(g1, http.MethodGet, qURL, hdr()); !gcStatus(t, rec).Hit {
		t.Fatal("repeat with the correct password: want hit")
	}
	seen(rec)
	if rep := gcPurge(t, g1, tok, purgeBody); rep.Code != http.StatusOK {
		t.Fatalf("tag purge: status %d, want 200", rep.Code)
	} else {
		seen(rep)
	}
	rejected := []struct {
		rec    *httptest.ResponseRecorder
		status int
	}{
		{gcPurge(t, g1, tok, `{"urls":["ftp://example.com/?token=`+qv+`"]}`), http.StatusBadRequest},
		{gcPurge(t, g1, tok, `{"tags":["`+tg+`\u0001"]}`), http.StatusBadRequest},
		{gcPurge(t, g1, "", purgeBody), http.StatusUnauthorized},
		{gcPurge(t, g1, tok+"x", purgeBody), http.StatusForbidden},
	}
	for i, r := range rejected {
		if r.rec.Code != r.status {
			t.Fatalf("rejected purge %d: status %d, want %d", i, r.rec.Code, r.status)
		}
		seen(r.rec)
	}

	wrongMini := newMini(t)
	wrongMini.RequireAuth(pw)
	g2 := mustGateway(t, gcDoc(redisStoreYAML("shared", []string{wrongMini.Addr()}, "password: "+wpw+"\n", ""), cacheTop, gcAdminYAML(tok), routes, o.url),
		clock.NewFake(miniStart), logger)
	rec = gcRequest(g2, http.MethodGet, qURL, hdr())
	gcSecretBypass(t, "auth failure", rec)
	seen(rec)
	if rep := gcPurge(t, g2, tok, purgeBody); rep.Code != http.StatusServiceUnavailable {
		t.Fatalf("purge with the wrong password: status %d, want 503", rep.Code)
	} else {
		seen(rep)
	}

	typeMini := newMini(t)
	g3 := mustGateway(t, gcDoc(redisStoreYAML("shared", []string{typeMini.Addr()}, "", ""), cacheTop, gcAdminYAML(tok), routes, o.url),
		clock.NewFake(miniStart), logger)
	if rec = gcRequest(g3, http.MethodGet, qURL, hdr()); !gcStatus(t, rec).Stored {
		t.Fatal("fill before the wrong-type key: want stored")
	}
	seen(rec)
	c3 := newTestClient(t, typeMini.Addr(), 0)
	ctx := context.Background()
	keys, err := c3.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatal(err)
	}
	replaced := 0
	for _, k := range keys {
		if !strings.Contains(k, ":i:") {
			continue
		}
		if err := c3.Del(ctx, k).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c3.Set(ctx, k, "x", 0).Err(); err != nil {
			t.Fatal(err)
		}
		replaced++
	}
	if replaced == 0 {
		t.Fatal("no index key to replace with a string")
	}
	rec = gcRequest(g3, http.MethodGet, qURL, hdr())
	gcSecretBypass(t, "wrong-type key", rec)
	seen(rec)

	malformedAddr := gcMalformed(t, hv)
	g4 := mustGateway(t, gcDoc(redisStoreYAML("shared", []string{malformedAddr}, "password: "+pw+"\n", ""), cacheTop, gcAdminYAML(tok), routes, o.url),
		clock.NewFake(miniStart), logger)
	rec = gcRequest(g4, http.MethodGet, qURL, hdr())
	gcSecretBypass(t, "malformed reply", rec)
	seen(rec)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := ln.Addr().String()
	_ = ln.Close()
	g5 := mustGateway(t, gcDoc(redisStoreYAML("shared", []string{closedAddr}, "password: "+pw+"\n", ""), cacheTop, gcAdminYAML(tok), routes, o.url),
		clock.NewFake(miniStart), logger)
	rec = gcRequest(g5, http.MethodGet, qURL, hdr())
	gcSecretBypass(t, "outage", rec)
	seen(rec)
	if rep := gcPurge(t, g5, tok, purgeBody); rep.Code != http.StatusServiceUnavailable {
		t.Fatalf("purge during the outage: status %d, want 503", rep.Code)
	} else {
		seen(rep)
	}

	sentinelExtra := "mode: sentinel\nmaster_name: mymaster\npassword: " + pw + "\nsentinel_password: " + spw + "\n"
	g6 := mustGateway(t, gcDoc(redisStoreYAML("shared", []string{closedAddr}, sentinelExtra, ""), cacheTop, gcAdminYAML(tok), routes, o.url),
		clock.NewFake(miniStart), logger)
	rec = gcRequest(g6, http.MethodGet, qURL, hdr())
	gcSecretBypass(t, "sentinel outage", rec)
	seen(rec)

	for _, g := range []*microgateway.Gateway{g1, g2, g3, g4, g5, g6} {
		observed = append(observed, metricsText(t, g))
		cfg := httptest.NewRecorder()
		g.AdminHandler().ServeHTTP(cfg, httptest.NewRequest(http.MethodGet, "/config", nil))
		if cfg.Code != http.StatusOK {
			t.Fatalf("GET /config: status %d, want 200", cfg.Code)
		}
		seen(cfg)
	}

	prefix := "p"
	s, err := NewFromConfig(&config.RedisStore{
		Mode:      "standalone",
		Addresses: []string{wrongMini.Addr()},
		Password:  config.NewSecret(wpw),
		KeyPrefix: &prefix,
	}, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Get(ctx, qURL, func([]string) (string, bool) { return hv, true }); err == nil {
		t.Error("Get with the wrong password succeeded")
	} else {
		errs = append(errs, err.Error())
	}
	if _, err := s.Put(ctx, store.FillToken{}, qURL, hv, []string{"X-Session"}, &store.Entry{Status: 200, Header: http.Header{"X-Session": {hv}}, Body: []byte("v1"), Tags: []string{tg}}, time.Minute); err != nil {
		errs = append(errs, err.Error())
	}
	if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeTags, Tags: []string{tg}, Retain: time.Minute}); err == nil {
		t.Error("Purge with the wrong password succeeded")
	} else {
		errs = append(errs, err.Error())
	}
	if _, err := s.Put(ctx, store.FillToken{}, "", hv, nil, &store.Entry{Status: 42, Header: http.Header{"X-Session": {hv}}}, time.Minute); err == nil {
		t.Error("Put of an invalid entry succeeded")
	} else {
		errs = append(errs, err.Error())
	}
	if _, err := s.Purge(ctx, store.Purge{Kind: store.PurgeTags, Tags: []string{tg + "\x01"}, Retain: time.Minute}); err == nil {
		t.Error("Purge of an invalid tag succeeded")
	} else {
		errs = append(errs, err.Error())
	}

	opts := Options{
		KeyPrefix: "p",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)).With("password", pw, "sentinel_password", spw, "token", tok),
	}
	for _, f := range []string{"%v", "%+v", "%#v"} {
		for _, v := range []any{s, opts, &opts} {
			if out := fmt.Sprintf(f, v); containsSecretForm(out, secrets...) {
				t.Errorf("%s of %T reveals a secret", f, v)
			}
		}
	}
	leakcheck.Check(t, "Store", s, secrets...)

	if !strings.Contains(logs.String(), "unavailable") {
		t.Error("log has no record of the store becoming unavailable")
	}
	observed = append(observed, errs...)
	observed = append(observed, logs.String())
	for i, text := range observed {
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
