// Package admin serves the gateway's operational endpoints: liveness, readiness, configuration, upstream state and metrics.
package admin

import (
	"encoding/json"
	"net/http"

	"github.com/ok0/lychgate/config"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/upstream"
)

// Source provides the state reported by the admin endpoints.
type Source interface {
	Ready() bool
	RedactedConfig() *config.Config
	Upstreams() []upstream.Status
	Metrics() *metrics.Registry
}

type statusBody struct {
	Status string `json:"status"`
}

// Handler returns the admin handler that serves /livez, /readyz, /config, /upstreams and /metrics from src.
func Handler(src Source) http.Handler {
	routes := map[string]http.HandlerFunc{
		"/livez": func(w http.ResponseWriter, r *http.Request) {
			writeStatus(w, http.StatusOK, "ok")
		},
		"/readyz": func(w http.ResponseWriter, r *http.Request) {
			if src.Ready() {
				writeStatus(w, http.StatusOK, "ready")
				return
			}
			writeStatus(w, http.StatusServiceUnavailable, "not_ready")
		},
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
		"/metrics": func(w http.ResponseWriter, r *http.Request) {
			src.Metrics().Handler().ServeHTTP(w, r)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		serve, ok := routes[r.URL.Path]
		if !ok {
			httperr.Write(w, r, http.StatusNotFound, httperr.CodeNotFound, httperr.Message(httperr.CodeNotFound))
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			httperr.Write(w, r, http.StatusMethodNotAllowed, httperr.CodeMethodNotAllowed, httperr.Message(httperr.CodeMethodNotAllowed))
			return
		}
		serve(w, r)
	})
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
