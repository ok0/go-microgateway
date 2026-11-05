package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ok0/go-microgateway/internal/doccheck"
)

func usageSpec(t *testing.T) map[string][]string {
	t.Helper()
	spec := map[string][]string{}
	inCommands := false
	for _, line := range strings.Split(usage, "\n") {
		if line == "commands:" {
			inCommands = true
			continue
		}
		fields := strings.Fields(line)
		if !inCommands || len(fields) == 0 {
			continue
		}
		var flags []string
		for _, f := range fields[1:] {
			f = strings.Trim(f, "[]")
			if strings.HasPrefix(f, "-") {
				flags = append(flags, f)
			}
		}
		spec[fields[0]] = flags
	}
	if len(spec) == 0 {
		t.Fatal("usage lists no commands")
	}
	return spec
}

func helpFlags(t *testing.T, cmd string) []string {
	t.Helper()
	var b bytes.Buffer
	if code := Main(context.Background(), Env{Args: []string{cmd, "-h"}, Stderr: &b}); code != 0 {
		t.Fatalf("%s -h exited with %d", cmd, code)
	}
	var flags []string
	for _, line := range strings.Split(b.String(), "\n") {
		if strings.HasPrefix(line, "  -") {
			flags = append(flags, strings.Fields(line)[0])
		}
	}
	return flags
}

func documentedCommands(t *testing.T) map[string][]string {
	t.Helper()
	root, err := doccheck.Root()
	if err != nil {
		t.Fatal(err)
	}
	doc, err := doccheck.Load(filepath.Join(root, "docs", "operations.md"))
	if err != nil {
		t.Fatal(err)
	}
	header := []string{"Command", "Flags", "Meaning"}
	docs := map[string][]string{}
	found := false
	for _, tbl := range doc.Tables {
		if tbl.Heading != "Commands" || !reflect.DeepEqual(tbl.Header, header) {
			continue
		}
		found = true
		for i, row := range tbl.Rows {
			line := tbl.Line + 2 + i
			if len(row) != len(header) {
				t.Fatalf("docs/operations.md:%d: row has %d cells, want %d", line, len(row), len(header))
			}
			cmd, ok := codeSpan(row[0])
			if !ok {
				t.Fatalf("docs/operations.md:%d: command cell %q is not one code span", line, row[0])
			}
			if _, dup := docs[cmd]; dup {
				t.Errorf("docs/operations.md:%d: command %s documented twice", line, cmd)
			}
			docs[cmd] = nil
			if row[1] == "none" {
				continue
			}
			for _, cell := range strings.Split(row[1], ", ") {
				flag, ok := codeSpan(cell)
				if !ok {
					t.Fatalf("docs/operations.md:%d: flag %q is not a code span", line, cell)
				}
				docs[cmd] = append(docs[cmd], flag)
			}
		}
	}
	if !found {
		t.Fatal("docs/operations.md has no Commands table with header | Command | Flags | Meaning |")
	}
	return docs
}

func codeSpan(cell string) (string, bool) {
	if len(cell) < 3 || cell[0] != '`' || cell[len(cell)-1] != '`' || strings.Count(cell, "`") != 2 {
		return "", false
	}
	return cell[1 : len(cell)-1], true
}

func compareFlags(t *testing.T, cmd, source string, got, want []string) {
	t.Helper()
	g := map[string]bool{}
	for _, f := range got {
		g[f] = true
	}
	w := map[string]bool{}
	for _, f := range want {
		w[f] = true
	}
	for _, f := range want {
		if !g[f] {
			t.Errorf("command %s: %s lacks flag %s", cmd, source, f)
		}
	}
	for _, f := range got {
		if !w[f] {
			t.Errorf("command %s: %s lists flag %s that the usage text does not", cmd, source, f)
		}
	}
}

func TestDocumentedCommandsAndFlags(t *testing.T) {
	usageCmds := usageSpec(t)
	docCmds := documentedCommands(t)
	for cmd, flags := range usageCmds {
		docFlags, ok := docCmds[cmd]
		if !ok {
			t.Errorf("docs/operations.md does not document command %s", cmd)
			continue
		}
		compareFlags(t, cmd, "docs/operations.md", docFlags, flags)
	}
	for cmd := range docCmds {
		if _, ok := usageCmds[cmd]; !ok {
			t.Errorf("docs/operations.md documents command %s, which the usage text does not list", cmd)
		}
	}
	for _, cmd := range []string{"run", "validate"} {
		compareFlags(t, cmd, "-h output", helpFlags(t, cmd), usageCmds[cmd])
	}
}
