package h2server

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	catError        = "error value"
	catTable        = "read-only table"
	catLazy         = "lazily built read-only table"
	catCache        = "allocation cache"
	catContextKey   = "context key"
	catMessage      = "message identity"
	lazyHuffmanNode = catLazy + "|buildRootHuffmanNode"
	lazyCommonMaps  = catLazy + "|buildCommonHeaderMaps"
)

var packageVarAllowlist = map[string]string{
	"ErrAbortHandler":                     catError,
	"ErrBodyNotAllowed":                   catError,
	"ErrNotSupported":                     catError,
	"ErrSkipAltProtocol":                  catError,
	"dataChunkPools":                      catCache,
	"errReadEmpty":                        catError,
	"errCodeName":                         catTable,
	"errFromPeer":                         catError,
	"errMixPseudoHeaderTypes":             catError,
	"errPseudoAfterRegular":               catError,
	"errDepStreamID":                      catError,
	"ErrFrameTooLarge":                    catError,
	"errPadBytes":                         catError,
	"errPadLength":                        catError,
	"errStreamID":                         catError,
	"fhBytes":                             catCache,
	"flagName":                            catTable,
	"frameNames":                          catTable,
	"frameParsers":                        catTable,
	"padZeros":                            catTable,
	"goroutineSpace":                      catTable,
	"littleBuf":                           catCache,
	"bufWriterPool":                       catCache,
	"clientPreface":                       catTable,
	"errTimeout":                          catError,
	"settingName":                         catTable,
	"sorterPool":                          catCache,
	"stateName":                           catTable,
	"errClosedPipeWrite":                  catError,
	"errUninitializedPipeWrite":           catError,
	"ConnectionStateContextKey":           catContextKey,
	"NewConnContextKey":                   catContextKey,
	"connHeaders":                         catTable,
	"errChanPool":                         catCache,
	"errClientDisconnected":               catError,
	"errClosedBody":                       catError,
	"errHandlerComplete":                  catError,
	"errHandlerPanicked":                  catError,
	"errPrefaceTimeout":                   catError,
	"ErrPushLimitReached":                 catError,
	"ErrRecursivePush":                    catError,
	"errStreamClosed":                     catError,
	"gracefulShutdownMsg":                 catMessage,
	"handlerDoneMsg":                      catMessage,
	"idleTimerMsg":                        catMessage,
	"readIdleTimerMsg":                    catMessage,
	"settingsTimerMsg":                    catMessage,
	"shutdownTimerMsg":                    catMessage,
	"responseWriterStatePool":             catCache,
	"writeDataPool":                       catCache,
	"hpack/bufPool":                       catCache,
	"hpack/buildRootOnce":                 lazyHuffmanNode,
	"hpack/lazyRootHuffmanNode":           lazyHuffmanNode,
	"hpack/ErrInvalidHuffman":             catError,
	"hpack/ErrStringLength":               catError,
	"hpack/errNeedMore":                   catError,
	"hpack/errVarintOverflow":             catError,
	"hpack/staticTable":                   catTable,
	"hpack/huffmanCodeLen":                catTable,
	"hpack/huffmanCodes":                  catTable,
	"httpcommon/ErrRequestHeaderListSize": catError,
	"httpcommon/commonBuildOnce":          lazyCommonMaps,
	"httpcommon/commonCanonHeader":        lazyCommonMaps,
	"httpcommon/commonLowerHeader":        lazyCommonMaps,
	"httpguts/badTrailer":                 catTable,
	"httpguts/isTokenTable":               catTable,
	"httpguts/validHostByte":              catTable,
}

type sourceFile struct {
	dir  string
	file *ast.File
}

func packageKey(dir, name string) string {
	return filepath.ToSlash(filepath.Join(dir, name))
}

func TestNoPackageMutableState(t *testing.T) {
	fset := token.NewFileSet()
	var files []sourceFile
	err := filepath.WalkDir(".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, 0)
		if err != nil {
			return err
		}
		files = append(files, sourceFile{dir: filepath.Dir(p), file: f})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	declared := map[string]bool{}
	for _, sf := range files {
		for _, decl := range sf.file.Decls {
			if fd, ok := decl.(*ast.FuncDecl); ok && fd.Recv == nil && fd.Name.Name == "init" {
				t.Errorf("%s: func init is not allowed", fset.Position(fd.Pos()))
			}
		}
		for name, obj := range sf.file.Scope.Objects {
			if obj.Kind == ast.Var && name != "_" {
				declared[packageKey(sf.dir, name)] = true
			}
		}
	}

	var declaredKeys, allowKeys []string
	for k := range declared {
		declaredKeys = append(declaredKeys, k)
	}
	for k := range packageVarAllowlist {
		allowKeys = append(allowKeys, k)
	}
	slices.Sort(declaredKeys)
	slices.Sort(allowKeys)
	if missing := setDiff(declaredKeys, allowKeys); len(missing) > 0 {
		t.Errorf("package-level vars without a category: %v", missing)
	}
	if extra := setDiff(allowKeys, declaredKeys); len(extra) > 0 {
		t.Errorf("allowlisted names that are not package-level vars: %v", extra)
	}

	for _, sf := range files {
		isPackageVar := func(id *ast.Ident) bool {
			if id.Obj == nil {
				return declared[packageKey(sf.dir, id.Name)]
			}
			return id.Obj.Kind == ast.Var && sf.file.Scope.Lookup(id.Name) == id.Obj
		}
		check := func(expr ast.Expr, fn string) {
			id := rootIdent(expr)
			if id == nil || id.Name == "_" || !isPackageVar(id) {
				return
			}
			category, writer, _ := strings.Cut(packageVarAllowlist[packageKey(sf.dir, id.Name)], "|")
			if writer != "" && fn == writer {
				return
			}
			t.Errorf("%s: %s writes package-level var %s (category %q, writer %q)", fset.Position(id.Pos()), fn, id.Name, category, writer)
		}
		for _, decl := range sf.file.Decls {
			fn := ""
			for n := range ast.Preorder(decl) {
				switch n := n.(type) {
				case *ast.FuncDecl:
					fn = n.Name.Name
				case *ast.AssignStmt:
					for _, lhs := range n.Lhs {
						check(lhs, fn)
					}
				case *ast.IncDecStmt:
					check(n.X, fn)
				case *ast.RangeStmt:
					if n.Tok == token.ASSIGN {
						if n.Key != nil {
							check(n.Key, fn)
						}
						if n.Value != nil {
							check(n.Value, fn)
						}
					}
				case *ast.UnaryExpr:
					if n.Op == token.AND {
						check(n.X, fn)
					}
				}
			}
		}
	}
}

func rootIdent(expr ast.Expr) *ast.Ident {
	for {
		switch e := expr.(type) {
		case *ast.Ident:
			return e
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		case *ast.SelectorExpr:
			expr = e.X
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		default:
			return nil
		}
	}
}

func setDiff(a, b []string) []string {
	var out []string
	for _, x := range a {
		if _, found := slices.BinarySearch(b, x); !found {
			out = append(out, x)
		}
	}
	return out
}
