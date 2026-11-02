# Third-party notices

## Go (The Go Authors)

The directory `internal/h2server` and its subdirectories `hpack`, `httpguts`, `httpcommon` and `httpsfv` contain code copied from the Go standard library at go1.27.2. The copied code comes from the `net/http/internal/http2` server files, the server half of `net/http/http2.go`, `net/http/internal/httpcommon`, `net/http/internal/httpsfv`, and from `golang.org/x/net/http2/hpack` and `golang.org/x/net/http/httpguts` as vendored by Go. The copies are modified as described in `internal/h2server/doc.go`.

License: BSD-3-Clause. The full license text is in `internal/h2server/LICENSE`, and the patent grant is in `internal/h2server/PATENTS`.

The BSD-3-Clause license is compatible with this project's MIT or Apache-2.0 license. No module dependency is added.
