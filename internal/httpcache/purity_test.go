package httpcache

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type purityFile struct {
	path string
	src  string
	file *ast.File
}

func purityFiles(t *testing.T, mode parser.Mode) (*token.FileSet, []purityFile) {
	t.Helper()
	fset := token.NewFileSet()
	var files []purityFile
	err := filepath.WalkDir(".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || filepath.Ext(p) != ".go" || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(fset, p, src, mode)
		if err != nil {
			return err
		}
		files = append(files, purityFile{path: p, src: string(src), file: f})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no non-test source files found")
	}
	return fset, files
}

func TestStandardLibraryOnly(t *testing.T) {
	t.Parallel()
	forbidden := map[string]bool{
		"os":           true,
		"os/exec":      true,
		"io/fs":        true,
		"log":          true,
		"log/slog":     true,
		"math/rand":    true,
		"math/rand/v2": true,
		"sync":         true,
		"syscall":      true,
		"unsafe":       true,
	}
	fset, files := purityFiles(t, parser.ImportsOnly)
	for _, pf := range files {
		for _, imp := range pf.file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Errorf("%s: %v", fset.Position(imp.Pos()), err)
				continue
			}
			first, _, _ := strings.Cut(path, "/")
			if strings.Contains(first, ".") {
				t.Errorf("%s: import %q is outside the standard library", fset.Position(imp.Pos()), path)
			}
			if forbidden[path] {
				t.Errorf("%s: import %q is forbidden", fset.Position(imp.Pos()), path)
			}
		}
	}
}

func TestNoClockOrGoroutines(t *testing.T) {
	t.Parallel()
	clockCalls := []string{
		"time.Now(",
		"time.Since(",
		"time.Until(",
		"time.After(",
		"time.AfterFunc(",
		"time.Sleep(",
		"time.NewTimer(",
		"time.NewTicker(",
		"time.Tick(",
	}
	fset, files := purityFiles(t, 0)
	for _, pf := range files {
		for _, call := range clockCalls {
			if strings.Contains(pf.src, call) {
				t.Errorf("%s: contains %s", pf.path, call)
			}
		}
		ast.Inspect(pf.file, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.GoStmt:
				t.Errorf("%s: goroutine statement", fset.Position(n.Pos()))
			case *ast.FuncDecl:
				if n.Recv == nil && n.Name.Name == "init" {
					t.Errorf("%s: func init", fset.Position(n.Pos()))
				}
			}
			return true
		})
	}
}

func TestNoPackageMutableState(t *testing.T) {
	t.Parallel()
	fset, files := purityFiles(t, 0)
	for _, pf := range files {
		for _, decl := range pf.file.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}
			for _, spec := range gd.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					sentinel := len(vs.Values) == len(vs.Names) &&
						strings.HasPrefix(name.Name, "Err") &&
						isErrorsNew(vs.Values[i])
					if !sentinel {
						t.Errorf("%s: package variable %s is not an errors.New sentinel", fset.Position(name.Pos()), name.Name)
					}
				}
			}
		}
	}
}

func isErrorsNew(e ast.Expr) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "errors" && sel.Sel.Name == "New"
}

type concurrentInputs struct {
	req         *http.Request
	respHdr     http.Header
	notModified http.Header
	noDate      http.Header
	entry       *Entry
	staleEntry  *Entry
	meta        ResponseMeta
	opts        Options
	rawURL      string
	status      string
	badStatus   string
	members     []Status
	vary        []string
}

func headerOf(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Add(kv[i], kv[i+1])
	}
	return h
}

func newConcurrentInputs(t0 time.Time) *concurrentInputs {
	date := t0.Format(http.TimeFormat)
	respHdr := headerOf(
		"Cache-Control", "max-age=60, stale-while-revalidate=30, s-maxage=120",
		"Date", date,
		"ETag", `"v1"`,
		"Last-Modified", t0.Add(-time.Hour).Format(http.TimeFormat),
		"Vary", "Accept-Encoding",
		"Age", "5",
		"Surrogate-Key", "a b c",
	)
	req := httptest.NewRequest(http.MethodGet, "http://example.com/a?b=1", nil)
	req.Header.Set("Cache-Control", "max-age=30, min-fresh=5")
	req.Header.Set("Accept-Encoding", "gzip")
	req.Header.Set("X-Api-Key", "key-1")
	return &concurrentInputs{
		req:         req,
		respHdr:     respHdr,
		notModified: headerOf("Date", date, "ETag", `"v1"`, "Cache-Control", "max-age=120"),
		noDate:      headerOf("Cache-Control", "max-age=60", "Expires", t0.Add(time.Hour).Format(http.TimeFormat)),
		entry:       &Entry{Status: 200, Header: respHdr.Clone(), Body: []byte("ok"), InitialAge: 5 * time.Second},
		staleEntry:  &Entry{Status: 200, Header: respHdr.Clone(), Body: []byte("ok"), InitialAge: 5 * time.Second, Stale: true},
		meta:        ResponseMeta{Status: 200, Header: respHdr, Complete: true, ResponseTime: t0},
		opts: Options{
			Statuses:             []int{200, 404},
			DefaultTTL:           time.Minute,
			MaxTTL:               time.Hour,
			Keep:                 time.Hour,
			StaleWhileRevalidate: 10 * time.Second,
			StaleIfError:         time.Minute,
			TagHeader:            "Surrogate-Key",
			TagDelimiter:         DelimSpace,
			Credential: func(r *http.Request) bool {
				return r.Header.Get("X-Api-Key") != ""
			},
		},
		rawURL:    "http://Example.com:80/a?b=1#f",
		status:    `c; hit; fwd=uri-miss; ttl=30, d; stored; key="k"`,
		badStatus: `c; (a b)`,
		members: []Status{
			{Name: "microgateway", Hit: true, TTL: 30, HasTTL: true},
			{Name: "edge", Fwd: FwdURIMiss, Stored: true, Key: "k"},
		},
		vary: []string{"Accept", "accept-encoding"},
	}
}

func cloneEntry(e *Entry) *Entry {
	if e == nil {
		return nil
	}
	c := *e
	c.Header = e.Header.Clone()
	c.Body = slices.Clone(e.Body)
	return &c
}

func requestValue(r *http.Request) any {
	if r == nil {
		return nil
	}
	return []any{r.Method, r.URL.String(), r.Header}
}

func entryValue(e *Entry) any {
	if e == nil {
		return nil
	}
	return *e
}

func snapshotInputs(in *concurrentInputs) []any {
	return []any{
		in.req.Method, in.req.URL.String(), in.req.Header.Clone(),
		in.respHdr.Clone(), in.notModified.Clone(), in.noDate.Clone(),
		cloneEntry(in.entry), cloneEntry(in.staleEntry),
		in.meta.Status, in.meta.Complete, in.meta.ResponseTime, in.meta.Header.Clone(),
		slices.Clone(in.opts.Statuses), in.opts.DefaultTTL, in.opts.MaxTTL, in.opts.Keep,
		in.opts.StaleWhileRevalidate, in.opts.StaleIfError, in.opts.TagHeader, in.opts.TagDelimiter,
		in.opts.Credential != nil,
		in.rawURL, in.status, in.badStatus, slices.Clone(in.members), slices.Clone(in.vary),
	}
}

type concurrentCall struct {
	name string
	run  func() []any
}

func concurrentCalls(in *concurrentInputs, t0 time.Time) []concurrentCall {
	age := 90 * time.Second
	return []concurrentCall{
		{"Action.String", func() []any {
			return []any{Fresh.String(), ServeStale.String(), Action(0).String()}
		}},
		{"Decide", func() []any {
			return []any{
				Decide(in.req, in.entry, age, in.opts),
				Decide(in.req, in.staleEntry, age, in.opts),
				Decide(in.req, nil, age, in.opts),
			}
		}},
		{"StaleOnError", func() []any {
			return []any{StaleOnError(in.entry, age, in.opts), StaleOnError(in.staleEntry, age, in.opts)}
		}},
		{"ParseCacheControl", func() []any {
			return []any{ParseCacheControl(in.respHdr)}
		}},
		{"RequestDirectives", func() []any {
			return []any{RequestDirectives(in.req.Header)}
		}},
		{"HasValidator", func() []any {
			return []any{HasValidator(in.respHdr)}
		}},
		{"FreshnessLifetime", func() []any {
			return []any{FreshnessLifetime(in.respHdr, t0, in.opts)}
		}},
		{"DateToAdd", func() []any {
			s, ok := DateToAdd(in.noDate, t0)
			return []any{s, ok}
		}},
		{"InitialAge", func() []any {
			return []any{InitialAge(in.respHdr, t0.Add(-time.Second), t0)}
		}},
		{"CurrentAge", func() []any {
			return []any{CurrentAge(5*time.Second, t0, t0.Add(time.Minute))}
		}},
		{"Remaining", func() []any {
			return []any{Remaining(time.Minute, age)}
		}},
		{"IsFresh", func() []any {
			return []any{IsFresh(time.Minute, age)}
		}},
		{"StaleWindows", func() []any {
			swr, sie := StaleWindows(in.respHdr, in.opts)
			return []any{swr, sie}
		}},
		{"StorageTTL", func() []any {
			return []any{StorageTTL(time.Minute, 10*time.Second, time.Minute, time.Hour, true)}
		}},
		{"Usable", func() []any {
			return []any{Usable(t0, t0.Add(time.Second), time.Minute)}
		}},
		{"TTLSeconds", func() []any {
			return []any{TTLSeconds(1500 * time.Millisecond)}
		}},
		{"AgeSeconds", func() []any {
			return []any{AgeSeconds(age)}
		}},
		{"PrimaryKey", func() []any {
			return []any{PrimaryKey(in.req)}
		}},
		{"URLKeys", func() []any {
			keys, err := URLKeys(in.rawURL)
			return []any{keys, err}
		}},
		{"URLError", func() []any {
			e := &URLError{Reason: "not absolute"}
			return []any{e.Error(), e.Unwrap()}
		}},
		{"SecondaryKey", func() []any {
			k, ok := SecondaryKey(in.vary, in.req)
			return []any{k, ok}
		}},
		{"Conditional", func() []any {
			return []any{requestValue(Conditional(in.req, in.entry)), requestValue(Conditional(in.req, nil))}
		}},
		{"Merge304", func() []any {
			e, ok := Merge304(in.entry, in.notModified)
			return []any{entryValue(e), ok}
		}},
		{"VaryNames", func() []any {
			names, star := VaryNames(in.respHdr)
			return []any{names, star}
		}},
		{"Storable", func() []any {
			ok, reason := Storable(in.req, in.meta, in.opts)
			return []any{ok, reason}
		}},
		{"Status.String", func() []any {
			return []any{in.members[0].String(), in.members[1].String()}
		}},
		{"FormatStatus", func() []any {
			return []any{FormatStatus(in.members...)}
		}},
		{"ParseStatus", func() []any {
			list, err := ParseStatus(in.status)
			bad, badErr := ParseStatus(in.badStatus)
			return []any{list, err, bad, badErr}
		}},
		{"ParseTags", func() []any {
			tags, ok := ParseTags(in.respHdr, "Surrogate-Key", DelimSpace)
			return []any{tags, ok}
		}},
	}
}

func TestConcurrentUse(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	in := newConcurrentInputs(t0)
	calls := concurrentCalls(in, t0)
	before := snapshotInputs(in)

	want := make([][]any, len(calls))
	for i, c := range calls {
		want[i] = c.run()
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 100 {
				for i, c := range calls {
					if got := c.run(); !reflect.DeepEqual(got, want[i]) {
						t.Errorf("%s: concurrent result differs from serial result", c.name)
					}
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	if !reflect.DeepEqual(snapshotInputs(in), before) {
		t.Error("shared inputs changed during concurrent use")
	}
}
