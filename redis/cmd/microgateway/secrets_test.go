package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/ok0/go-microgateway/internal/cli"
	"github.com/ok0/go-microgateway/internal/leakcheck"
)

func TestSecretsNeverPrinted(t *testing.T) {
	pw := uniqueSecret(t, "pw-")
	wpw := uniqueSecret(t, "wpw-")
	outputs := map[string]string{}
	envPW := lookupWith(map[string]string{"MG_TEST_REDIS_PASSWORD": pw})
	backendURL := backend(t)
	dir := t.TempDir()

	literal := func(o docOpts, password string) string {
		return strings.ReplaceAll(instanceDoc(o), "${MG_TEST_REDIS_PASSWORD}", password)
	}
	validDoc := func(addr string) docOpts {
		return docOpts{Backend: backendURL, RedisAddr: addr, Prefix: uniquePrefix(t), Password: true, UseStore: true}
	}

	t.Run("validate_env_and_literal", func(t *testing.T) {
		envPath := writeDoc(t, instanceDoc(validDoc("127.0.0.1:1")))
		var out, errb bytes.Buffer
		code := gateway(context.Background(), cli.Env{
			Args: []string{"validate", "-config", envPath}, Stdout: &out, Stderr: &errb, LookupEnv: envPW,
		})
		if code != 0 {
			t.Fatalf("env password: exit %d, want 0: %s", code, errb.String())
		}
		outputs["validate env password"] = out.String() + errb.String()

		litPath := writeDoc(t, literal(validDoc("127.0.0.1:1"), pw))
		out.Reset()
		errb.Reset()
		code = gateway(context.Background(), cli.Env{
			Args: []string{"validate", "-config", litPath}, Stdout: &out, Stderr: &errb,
		})
		if code != 0 {
			t.Fatalf("literal password: exit %d, want 0: %s", code, errb.String())
		}
		outputs["validate literal password"] = out.String() + errb.String()
	})

	t.Run("validate_literal_with_error", func(t *testing.T) {
		o := validDoc("127.0.0.1:1")
		src := literal(o, pw)
		src = strings.Replace(src, "      db: 0\n", "      db: -1\n", 1)
		path := writeDoc(t, src)
		var out, errb bytes.Buffer
		code := gateway(context.Background(), cli.Env{
			Args: []string{"validate", "-config", path}, Stdout: &out, Stderr: &errb,
		})
		if code != 1 {
			t.Fatalf("exit %d, want 1: %s", code, errb.String())
		}
		outputs["validate literal password with error stdout"] = out.String()
		outputs["validate literal password with error stderr"] = errb.String()
	})

	t.Run("run_correct_password", func(t *testing.T) {
		m := miniredis.RunT(t)
		m.RequireAuth(pw)
		o := validDoc(m.Addr())
		path := writeDoc(t, instanceDoc(o))
		inst := startInstance(t, t.Context(), path, envPW, "-log-level", "debug")
		base := "http://" + inst.addrs.Listeners["public"].String()
		for range 3 {
			get(t, base+"/rl")
		}
		_, cfgBody := get(t, "http://"+inst.addrs.Admin.String()+"/config")
		outputs["run correct password admin config"] = string(cfgBody)
		if code := inst.stop(t); code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
		outputs["run correct password stderr"] = inst.errb.String()
	})

	t.Run("run_wrong_password", func(t *testing.T) {
		m := miniredis.RunT(t)
		m.RequireAuth(pw)
		o := validDoc(m.Addr())
		path := writeDoc(t, instanceDoc(o))
		inst := startInstance(t, t.Context(), path, lookupWith(map[string]string{"MG_TEST_REDIS_PASSWORD": wpw}), "-log-level", "debug")
		base := "http://" + inst.addrs.Listeners["public"].String()
		for range 3 {
			resp, _ := get(t, base+"/rl")
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status %d with wrong password, want 503", resp.StatusCode)
			}
		}
		_, cfgBody := get(t, "http://"+inst.addrs.Admin.String()+"/config")
		outputs["run wrong password admin config"] = string(cfgBody)
		if code := inst.stop(t); code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
		outputs["run wrong password stderr"] = inst.errb.String()
	})

	t.Run("run_unreachable_address", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		path := writeDoc(t, instanceDoc(validDoc(addr)))
		inst := startInstance(t, t.Context(), path, envPW, "-log-level", "debug")
		base := "http://" + inst.addrs.Listeners["public"].String()
		for range 3 {
			get(t, base+"/rl")
		}
		if code := inst.stop(t); code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
		outputs["run unreachable address stderr"] = inst.errb.String()
	})

	t.Run("run_tls_build_error", func(t *testing.T) {
		caPath := filepath.Join(dir, "ca.pem")
		if err := os.WriteFile(caPath, []byte("not a PEM certificate\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		src := literal(validDoc("127.0.0.1:1"), pw)
		src = strings.Replace(src, "key_prefix:", `tls: {enabled: true, ca_file: "`+caPath+`"}`+"\n        key_prefix:", 1)
		path := writeDoc(t, src)
		var out, errb bytes.Buffer
		code := gateway(context.Background(), cli.Env{
			Args: []string{"run", "-config", path}, Stdout: &out, Stderr: &errb, LookupEnv: envPW,
		})
		if code != 1 {
			t.Fatalf("exit %d, want 1: %s", code, errb.String())
		}
		outputs["run tls build error stdout"] = out.String()
		outputs["run tls build error stderr"] = errb.String()
	})

	secrets := []struct{ name, value string }{{"password", pw}, {"wrong password", wpw}}
	for name, out := range outputs {
		for _, s := range secrets {
			for _, form := range leakcheck.Forms(s.value) {
				if strings.Contains(out, form) {
					t.Errorf("output of case %q contains a form of the %s", name, s.name)
				}
			}
		}
	}
}
