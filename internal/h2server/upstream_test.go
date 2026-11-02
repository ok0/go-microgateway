package h2server

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

const upstreamBase = "go1.27.2"

type upstreamFile struct {
	copy   string
	base   string
	sha256 string
}

func upstreamFiles() []upstreamFile {
	return []upstreamFile{
		{copy: "api.go", base: "net/http/internal/http2/api.go", sha256: "37118d08b835bc4324cb0963a4135c7af0af17fd6e526c687ea2b138d4863d06"},
		{copy: "ascii.go", base: "net/http/internal/http2/ascii.go", sha256: "d8ee0e0a6b2c41a17a18e263b7f9b5701174dbe10e59496daadc9db6a647f708"},
		{copy: "ciphers.go", base: "net/http/internal/http2/ciphers.go", sha256: "623262af28491cd81b3570936cd06757f629d2ae4fb6ef087af6762f30d7a5eb"},
		{copy: "config.go", base: "net/http/internal/http2/config.go", sha256: "b94f003c337459a06408b30c77b52861133c4f6cbf88da2ca1aa32415b4242cf"},
		{copy: "databuffer.go", base: "net/http/internal/http2/databuffer.go", sha256: "ea04623f1b6e444d91a11faaed9e91a207f057d4108d52fdcef602c5effa039d"},
		{copy: "errors.go", base: "net/http/internal/http2/errors.go", sha256: "2652c558a999e8104cb1f64ec32b3eec00c4ccda76c29708c54908840fd90aa6"},
		{copy: "flow.go", base: "net/http/internal/http2/flow.go", sha256: "fedf89792079bcd134e4c41da64f5ac99a0881a681cf5b642c9e086d4e655250"},
		{copy: "frame.go", base: "net/http/internal/http2/frame.go", sha256: "9d9414d6cc7bc482210f9bb7751226c2d864883e93abad22e0e585de669094cf"},
		{copy: "gotrack.go", base: "net/http/internal/http2/gotrack.go", sha256: "239e8ae03982809675189ab7755003bbcc1fe29c7b5e66b577593919b998fb14"},
		{copy: "http2.go", base: "net/http/internal/http2/http2.go", sha256: "b5fbef76881003cb941b30a85bc45b3535ea4cbb78dcc57de274d3a17398a0ff"},
		{copy: "pipe.go", base: "net/http/internal/http2/pipe.go", sha256: "3fbe00025d441802c154549f5bd7562b9937ff43f1ce64c987bda75fcaf7f518"},
		{copy: "server.go", base: "net/http/internal/http2/server.go", sha256: "95ec3beab3acaf294bf214113a6a1b668befbcade25a73a70779ce2d975893d7"},
		{copy: "write.go", base: "net/http/internal/http2/write.go", sha256: "810ece152a598a451ddda5557769221e1d1069a037db2fefbcaf452bf2cd88ff"},
		{copy: "writesched.go", base: "net/http/internal/http2/writesched.go", sha256: "fe9249d675dc0bcc1cec480f53fbfff69e87a6a490ab780660e1b434a9f4b9a7"},
		{copy: "writesched_priority_rfc9218.go", base: "net/http/internal/http2/writesched_priority_rfc9218.go", sha256: "2f9b42f46fbbed54c4fd8e060d34f0526ab055453468432d0beea37cea761213"},
		{copy: "writesched_roundrobin.go", base: "net/http/internal/http2/writesched_roundrobin.go", sha256: "15f3db4cefc4b3179b59a2df67c79407cdc573b6008612d8a58d1ffc0c15d758"},
		{copy: "nethttp.go", base: "net/http/http2.go", sha256: "62166149f89e1a1b85f587777ac28c0500f45c5dc465fc0ce97e134eccfedbc4"},
		{copy: "hpack/encode.go", base: "vendor/golang.org/x/net/http2/hpack/encode.go", sha256: "a9c57bec32cdd84c8372aa9622c6d6c99a25d47133952b88a3f391f3e411bd4c"},
		{copy: "hpack/hpack.go", base: "vendor/golang.org/x/net/http2/hpack/hpack.go", sha256: "468bb8569beb1271228967ef4873307b86824754b6e69aaa7e9ed4849307a519"},
		{copy: "hpack/huffman.go", base: "vendor/golang.org/x/net/http2/hpack/huffman.go", sha256: "3d41693bd77da34093c1d334842d8cbeffb3a2f3e0fa5cf0af5e6e9e65b19be1"},
		{copy: "hpack/static_table.go", base: "vendor/golang.org/x/net/http2/hpack/static_table.go", sha256: "c3d863a43ea29e6f03db930eb2556664eeeda22a4ac0b8bcbd9e31bde499eed1"},
		{copy: "hpack/tables.go", base: "vendor/golang.org/x/net/http2/hpack/tables.go", sha256: "6e8ce01dfb4d5e9e02a982581988621d2bc84b5e90708e1ffd3ef988cf8b2d5c"},
		{copy: "httpguts/guts.go", base: "vendor/golang.org/x/net/http/httpguts/guts.go", sha256: "3da54270a625540809f2f767e54e18acf27199aca64b6cf16d6d3fa3999de959"},
		{copy: "httpguts/httplex.go", base: "vendor/golang.org/x/net/http/httpguts/httplex.go", sha256: "8cfd5819dc821d45f82fde10afff4b2e652535b0091e0559fc8ad7766051a2c2"},
		{copy: "httpcommon/httpcommon.go", base: "net/http/internal/httpcommon/httpcommon.go", sha256: "875db0f0226dd2405d7d3e03242d272cda42a6687196d1c6bf5c85caeeecf1ea"},
		{copy: "httpsfv/httpsfv.go", base: "net/http/internal/httpsfv/httpsfv.go", sha256: "1826dc95ab20909265c03ec7ef1c05251c4fb56351c9016038210b629dd8bd5c"},
		{copy: "ascii_test.go", base: "net/http/internal/http2/ascii_test.go", sha256: "173d0b6efabef296973a370a3d9d8b827f3fc8b7d01bb486dcc1f763fcb2a0e1"},
		{copy: "ciphers_test.go", base: "net/http/internal/http2/ciphers_test.go", sha256: "2faeece82713cc2f456482c48af6937a57aea63c9fbc9e5bf45f4afe238e3831"},
		{copy: "config_test.go", base: "net/http/internal/http2/config_test.go", sha256: "9f09869a4c0983a44d7ebea0ef862441add48ab6b9813d5b39efcc72ec75bb78"},
		{copy: "connframes_test.go", base: "net/http/internal/http2/connframes_test.go", sha256: "1830452172a44bb81d777feb5e0a71a19a4a32c0e146cdb778cb0508155ac9a0"},
		{copy: "databuffer_test.go", base: "net/http/internal/http2/databuffer_test.go", sha256: "6eaad282763b9df79a45612a2251ec64ab25d11217170d7c31645cb56f4539fa"},
		{copy: "errors_test.go", base: "net/http/internal/http2/errors_test.go", sha256: "0b1da045d5c0874d3987e9bb07b92ba0834355ab2c6ccbbfc8fd0f5213a6b9c3"},
		{copy: "export_test.go", base: "net/http/internal/http2/export_test.go", sha256: "c64b057a7e19ea094e3ed058a20e485e9fa5469d34b2de850c9a4314f47177d0"},
		{copy: "flow_test.go", base: "net/http/internal/http2/flow_test.go", sha256: "3210e9d2f9fb2cb2286eebe350304e4374b22e7258af62ccccefd02a740a202a"},
		{copy: "frame_test.go", base: "net/http/internal/http2/frame_test.go", sha256: "3ec51195599f319d80870f82ef43ce3603640178df0c840d1c42bcaab395b9d7"},
		{copy: "gotrack_test.go", base: "net/http/internal/http2/gotrack_test.go", sha256: "e9a90b0b314e9ac7a755b186a0eb894521b8853400cad69baa6dc73dbded0b3e"},
		{copy: "http2_test.go", base: "net/http/internal/http2/http2_test.go", sha256: "d4dd28c1959752fbc02f2fbda8dac3074a4f2b6a237eb372d992b8345613b83f"},
		{copy: "netconn_test.go", base: "net/http/internal/http2/netconn_test.go", sha256: "1a578610526cbdf00c9ece0f0792aa383dd862f6fcb1ec6c585bcc2fb40eb7e1"},
		{copy: "pipe_test.go", base: "net/http/internal/http2/pipe_test.go", sha256: "25bd987f53749b8557e79bee34b3e6f548d1bc006726c14a5cc04ae5fb4b97e4"},
		{copy: "server_internal_test.go", base: "net/http/internal/http2/server_internal_test.go", sha256: "16ffa5b8bda2994a24738d6ec20248d5c01e2bbfa82fd99013841b9586457062"},
		{copy: "server_push_test.go", base: "net/http/internal/http2/server_push_test.go", sha256: "9d5046b91313b09da64f8e19c3bf77317e9c398b9090efc9c17eba62eff9e8d0"},
		{copy: "server_test.go", base: "net/http/internal/http2/server_test.go", sha256: "a120ca3c4c398dcfff235542f46e8998351021e7b0a2281fd0a9480b52cf08a1"},
		{copy: "synctest_test.go", base: "net/http/internal/http2/synctest_test.go", sha256: "0115d3cebfbe08b0d1bc7b0e5a9ae73ab6312186759659e6722a94a3fd26f9d7"},
		{copy: "writesched_test.go", base: "net/http/internal/http2/writesched_test.go", sha256: "f65197ed4c40ca82de3c3b6aebce100c3b26279f31c3bb88d6276e05b90a70df"},
		{copy: "writesched_roundrobin_test.go", base: "net/http/internal/http2/writesched_roundrobin_test.go", sha256: "903fbd4cfc422969a76f362479d5df91b7e1120d90f33d9e6f9aad25ddb6e119"},
		{copy: "writesched_priority_rfc9218_test.go", base: "net/http/internal/http2/writesched_priority_rfc9218_test.go", sha256: "027fffafad63fb4c856bc7a86760ccb31c9e43e9057d1fac0737bd6d0982e3f0"},
		{copy: "writesched_benchmarks_test.go", base: "net/http/internal/http2/writesched_benchmarks_test.go", sha256: "0f0a0ccfc5f336c30aecf31ced818d0b7deb97d5bec8051e10997272e3675d6d"},
		{copy: "testcert_test.go", base: "net/http/internal/testcert/testcert.go", sha256: "812c69f79fc71384de1d93fb1058ab20e5b58ab2860dea72167681048d14a12e"},
	}
}

func fileSHA256(b []byte) string {
	h := sha256.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))
}

func compareBase(root string, files []upstreamFile) (changed, missing []string) {
	for _, f := range files {
		b, err := os.ReadFile(filepath.Join(root, f.base))
		if err != nil {
			if os.IsNotExist(err) {
				missing = append(missing, f.base)
			} else {
				missing = append(missing, f.base+": "+err.Error())
			}
			continue
		}
		if fileSHA256(b) != f.sha256 {
			changed = append(changed, f.base)
		}
	}
	slices.Sort(changed)
	slices.Sort(missing)
	return changed, missing
}

func goSrcRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOROOT").Output()
	if err != nil {
		t.Fatalf("go env GOROOT: %v", err)
	}
	return filepath.Join(strings.TrimSpace(string(out)), "src")
}

func headLines(t *testing.T, path string, n int) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(b), "\n")
	if len(lines) < n {
		t.Fatalf("%s has %d lines, want at least %d", path, len(lines), n)
	}
	return strings.Join(lines[:n], "")
}

func TestUpstreamUnchanged(t *testing.T) {
	root := goSrcRoot(t)
	changed, missing := compareBase(root, upstreamFiles())
	if len(changed) > 0 || len(missing) > 0 {
		t.Fatalf("upstream base files under %s differ from %s (test toolchain %s)\nchanged: %v\nmissing: %v\nresync: copy the D1 files from the new toolchain, reapply the local changes, update the hashes in upstream_test.go, run this package's tests and internal/server's comparison tests",
			root, upstreamBase, runtime.Version(), changed, missing)
	}
}

func TestCompareBase(t *testing.T) {
	src := goSrcRoot(t)
	files := []string{"net/http/internal/http2/flow.go", "net/http/internal/http2/pipe.go"}
	root := t.TempDir()
	var entries []upstreamFile
	for _, rel := range files {
		b, err := os.ReadFile(filepath.Join(src, rel))
		if err != nil {
			t.Fatal(err)
		}
		dst := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, b, 0o644); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, upstreamFile{copy: filepath.Base(rel), base: rel, sha256: fileSHA256(b)})
	}

	changed, missing := compareBase(root, entries)
	if len(changed) != 0 || len(missing) != 0 {
		t.Fatalf("unmodified files: changed %v, missing %v; want none", changed, missing)
	}

	if err := os.WriteFile(filepath.Join(root, files[0]), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, files[1])); err != nil {
		t.Fatal(err)
	}
	changed, missing = compareBase(root, entries)
	if !slices.Equal(changed, []string{files[0]}) {
		t.Errorf("changed = %v, want [%s]", changed, files[0])
	}
	if !slices.Equal(missing, []string{files[1]}) {
		t.Errorf("missing = %v, want [%s]", missing, files[1])
	}
}

func TestCopiedFilesKeepHeaders(t *testing.T) {
	src := goSrcRoot(t)
	for _, f := range upstreamFiles() {
		want := headLines(t, filepath.Join(src, f.base), 3)
		got := headLines(t, f.copy, 3)
		if got != want {
			t.Errorf("%s: header differs from %s", f.copy, f.base)
		}
	}
	lic, err := os.ReadFile("LICENSE")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(lic), "Copyright 2009 The Go Authors.") {
		t.Errorf("LICENSE does not start with the Go Authors copyright line")
	}
	pat, err := os.ReadFile("PATENTS")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(pat), "Additional IP Rights Grant (Patents)") {
		t.Errorf("PATENTS does not start with the patent grant heading")
	}
}
