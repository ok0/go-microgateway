package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ok0/go-microgateway"
	"github.com/ok0/go-microgateway/config"
)

var testUpstreams = upstreams{A: "http://127.0.0.1:1001", B: "http://127.0.0.1:1002"}

func TestRenderValid(t *testing.T) {
	for _, step := range []string{stepBaseline, stepRoute, stepWeight} {
		for _, opts := range []renderOpts{{}, {signalOnly: true}} {
			cfg, err := config.Parse(render(step, testUpstreams, opts))
			if err != nil {
				t.Fatalf("%s %+v: parse: %v", step, opts, err)
			}
			if err := microgateway.Validate(cfg); err != nil {
				t.Fatalf("%s %+v: validate: %v", step, opts, err)
			}
			if got := cfg.Global.DrainTimeout.Std(); got != 5*time.Second {
				t.Fatalf("%s: drain_timeout = %v, want 5s", step, got)
			}
			if got := cfg.Global.ShutdownDelay.Std(); got != time.Second {
				t.Fatalf("%s: shutdown_delay = %v, want 1s", step, got)
			}
			if opts.signalOnly {
				if got := cfg.Global.Reload.Debounce.Std(); got != time.Hour {
					t.Fatalf("%s: debounce = %v, want 1h", step, got)
				}
				if got := cfg.Global.Reload.PollInterval.Std(); got != time.Hour {
					t.Fatalf("%s: poll_interval = %v, want 1h", step, got)
				}
			}
		}
	}
}

func TestRenderInvalid(t *testing.T) {
	_, err := config.Parse(invalidDoc(testUpstreams, renderOpts{}))
	var ve *config.ValidationError
	if err == nil || !errors.As(err, &ve) {
		t.Fatalf("error = %v, want a *config.ValidationError", err)
	}
	var routeErr, drainErr bool
	for _, fe := range ve.Errors {
		if strings.HasPrefix(fe.Path, "routes[") && strings.HasSuffix(fe.Path, ".upstream") && strings.Contains(fe.Msg, `unknown upstream "missing"`) {
			routeErr = true
		}
		if fe.Path == "global.drain_timeout" {
			drainErr = true
		}
	}
	if !routeErr || !drainErr {
		t.Fatalf("errors = %+v, want the unknown upstream and the drain_timeout problems", ve.Errors)
	}
}

func TestRenderLoopbackOnly(t *testing.T) {
	re := regexp.MustCompile(`(?m)^\s*(?:-\s*)?(address|url):\s*"?([^"\s]+)"?`)
	for _, step := range stepOrder {
		for _, opts := range []renderOpts{{}, {signalOnly: true}} {
			body := render(step, testUpstreams, opts)
			matches := re.FindAllStringSubmatch(string(body), -1)
			if len(matches) < 4 {
				t.Fatalf("%s %+v: found %d address or url values, want at least 4", step, opts, len(matches))
			}
			for _, m := range matches {
				switch m[1] {
				case "address":
					host, _, err := net.SplitHostPort(m[2])
					if err != nil || host != "127.0.0.1" {
						t.Fatalf("%s: address %q is not loopback", step, m[2])
					}
				case "url":
					u, err := url.Parse(m[2])
					if err != nil || u.Hostname() != "127.0.0.1" {
						t.Fatalf("%s: url %q is not loopback", step, m[2])
					}
				}
			}
		}
	}
}

func TestParseSettingsBounds(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "microgateway")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	withBin := func(extra ...string) []string {
		return append([]string{"-binary", bin}, extra...)
	}

	passing := map[string][]string{
		"duration minimum":    withBin("-duration", "30s"),
		"duration maximum":    withBin("-duration", "10m"),
		"rate minimum":        withBin("-rate", "1"),
		"rate maximum":        withBin("-rate", "1000"),
		"concurrency minimum": withBin("-concurrency", "1"),
		"concurrency maximum": withBin("-concurrency", "32"),
		"instances minimum":   withBin("-instances", "1"),
		"instances maximum":   withBin("-instances", "4"),
		"defaults":            withBin(),
	}
	for name, args := range passing {
		t.Run(name, func(t *testing.T) {
			if _, err := parseSettings(args, io.Discard); err != nil {
				t.Fatalf("parseSettings(%v) error: %v", args, err)
			}
		})
	}

	failing := map[string]struct {
		args []string
		flag string
	}{
		"duration below":     {withBin("-duration", "29s"), "-duration"},
		"duration above":     {withBin("-duration", "10m1s"), "-duration"},
		"rate zero":          {withBin("-rate", "0"), "-rate"},
		"rate above":         {withBin("-rate", "1001"), "-rate"},
		"concurrency zero":   {withBin("-concurrency", "0"), "-concurrency"},
		"concurrency above":  {withBin("-concurrency", "33"), "-concurrency"},
		"instances zero":     {withBin("-instances", "0"), "-instances"},
		"instances above":    {withBin("-instances", "5"), "-instances"},
		"missing binary":     {[]string{"-duration", "60s"}, "-binary"},
		"nonexistent binary": {[]string{"-binary", filepath.Join(dir, "missing")}, "-binary"},
		"unknown flag":       {withBin("-no-such-flag", "1"), "-no-such-flag"},
		"extra argument":     {withBin("extra"), "extra"},
	}
	for name, c := range failing {
		t.Run(name, func(t *testing.T) {
			_, err := parseSettings(c.args, io.Discard)
			if err == nil {
				t.Fatalf("parseSettings(%v) error = nil, want an error", c.args)
			}
			if !strings.Contains(err.Error(), c.flag) {
				t.Fatalf("error %q does not name %q", err, c.flag)
			}
		})
	}
}

func TestRunRejectsBounds(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "microgateway")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"-duration":    "-duration 29s",
		"-rate":        "-rate 1001",
		"-concurrency": "-concurrency 33",
		"-instances":   "-instances 5",
	}
	for flagName, bad := range cases {
		t.Run(flagName, func(t *testing.T) {
			args := append([]string{"-binary", bin}, strings.Fields(bad)...)
			var stdout, stderr bytes.Buffer
			if code := run(context.Background(), args, &stdout, &stderr); code != 2 {
				t.Fatalf("exit code = %d, want 2", code)
			}
			if !strings.Contains(stderr.String(), flagName) {
				t.Fatalf("stderr %q does not name %s", stderr.String(), flagName)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
		})
	}
}
