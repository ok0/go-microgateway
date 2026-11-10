// Package admin serves the gateway's operational endpoints: the management handler (liveness, readiness, configuration, upstream state, metrics, authenticated cache purge) and the probe handler (liveness, readiness, opt-in metrics).
package admin

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/auth"
	"github.com/ok0/go-microgateway/internal/cache"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/metrics"
	"github.com/ok0/go-microgateway/internal/upstream"
)

// Source provides the state reported by the admin endpoints.
type Source interface {
	Ready() bool
	RedactedConfig() *config.Config
	Upstreams() []upstream.Status
	Metrics() *metrics.Registry
	// PurgeAccess reports the admin keys and whether a cache is configured, from the active configuration.
	PurgeAccess() PurgeAccess
	// ManagementAuth reports whether /config, /upstreams and /metrics need a bearer token, and the accepted keys, from the active configuration. hashes[i] is the sha256 digest of the token of key keyIDs[i].
	ManagementAuth() (required bool, keyIDs []string, hashes [][32]byte)
	// Purge runs req on behalf of the admin key keyID.
	Purge(ctx context.Context, req cache.PurgeRequest, keyID string) (cache.PurgeResult, error)
}

// PurgeAccess is the purge configuration of the active generation. Hashes[i] is the sha256 digest of the token of key KeyIDs[i].
type PurgeAccess struct {
	KeyIDs []string
	Hashes [][32]byte
	Cache  bool
}

type statusBody struct {
	Status string `json:"status"`
}

// gated lists the management paths that need a bearer token while the management auth is required.
var gated = map[string]bool{"/config": true, "/upstreams": true, "/metrics": true}

// Handler returns the management handler that serves /livez, /readyz, /config, /upstreams, /metrics and POST /cache/purge from src.
// /livez and /readyz never need a credential; /config, /upstreams and /metrics need a bearer token from the admin keys while src.ManagementAuth reports required.
// A nil logger discards log output.
func Handler(src Source, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	routes := map[string]http.HandlerFunc{
		"/livez":  serveLivez,
		"/readyz": serveReadyz(src.Ready),
		"/config": func(w http.ResponseWriter, r *http.Request) {
			writeValue(w, r, http.StatusOK, src.RedactedConfig())
		},
		"/upstreams": func(w http.ResponseWriter, r *http.Request) {
			list := src.Upstreams()
			if list == nil {
				list = []upstream.Status{}
			}
			writeValue(w, r, http.StatusOK, list)
		},
		"/metrics": serveMetrics(src.Metrics),
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == purgePath {
			servePurge(src, logger, w, r)
			return
		}
		serve, ok := routes[r.URL.Path]
		if !ok {
			notFound(w, r)
			return
		}
		if !readMethod(w, r) {
			return
		}
		if gated[r.URL.Path] && !authorizeManagement(src, logger, w, r) {
			return
		}
		serve(w, r)
	})
}

// authorizeManagement checks the bearer token of r against the management keys of src before any state is read. It writes 401 (with WWW-Authenticate: Bearer) or 403 and returns false when the request is rejected.
func authorizeManagement(src Source, logger *slog.Logger, w http.ResponseWriter, r *http.Request) bool {
	required, _, hashes := src.ManagementAuth()
	if !required {
		return true
	}
	token, ok := bearer(r)
	if !ok {
		w.Header().Set("WWW-Authenticate", "Bearer")
		httperr.Write(w, r, http.StatusUnauthorized, httperr.CodeUnauthorized, httperr.Message(httperr.CodeUnauthorized))
		logger.Warn("admin request rejected", "path", r.URL.Path, "status", http.StatusUnauthorized, "reason", "missing or malformed bearer token")
		return false
	}
	if _, ok := auth.MatchKey(hashes, token); !ok {
		httperr.Write(w, r, http.StatusForbidden, httperr.CodeForbidden, httperr.Message(httperr.CodeForbidden))
		logger.Warn("admin request rejected", "path", r.URL.Path, "status", http.StatusForbidden, "reason", "unknown key")
		return false
	}
	return true
}

func serveLivez(w http.ResponseWriter, _ *http.Request) {
	writeStatus(w, http.StatusOK, "ok")
}

func serveReadyz(ready func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ready() {
			writeStatus(w, http.StatusOK, "ready")
			return
		}
		writeStatus(w, http.StatusServiceUnavailable, "not_ready")
	}
}

func serveMetrics(reg func() *metrics.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reg().Handler().ServeHTTP(w, r)
	}
}

// readMethod reports whether r is GET or HEAD. Any other method gets 405 with Allow: GET, HEAD and false.
func readMethod(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return true
	}
	w.Header().Set("Allow", "GET, HEAD")
	httperr.Write(w, r, http.StatusMethodNotAllowed, httperr.CodeMethodNotAllowed, httperr.Message(httperr.CodeMethodNotAllowed))
	return false
}

func notFound(w http.ResponseWriter, r *http.Request) {
	httperr.Write(w, r, http.StatusNotFound, httperr.CodeNotFound, httperr.Message(httperr.CodeNotFound))
}

func writeStatus(w http.ResponseWriter, code int, s string) {
	b, _ := json.Marshal(statusBody{Status: s})
	writeJSON(w, code, b)
}

func writeValue(w http.ResponseWriter, r *http.Request, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		httperr.Write(w, r, http.StatusInternalServerError, httperr.CodeInternal, httperr.Message(httperr.CodeInternal))
		return
	}
	writeJSON(w, code, b)
}

func writeJSON(w http.ResponseWriter, code int, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(append(b, '\n'))
}
