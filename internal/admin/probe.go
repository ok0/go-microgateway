package admin

import (
	"net/http"

	"github.com/ok0/go-microgateway/internal/metrics"
)

// ProbeSource provides the state served by the probe handler.
type ProbeSource interface {
	Ready() bool
	// ProbeMetrics reports whether the probe handler serves /metrics, from the active configuration.
	ProbeMetrics() bool
	Metrics() *metrics.Registry
}

// ProbeHandler returns the probe handler. It serves GET and HEAD /livez and /readyz, and /metrics while src.ProbeMetrics
// reports true, without a credential, and answers 404 for every other path. It never serves configuration, upstream state or cache purge.
func ProbeHandler(src ProbeSource) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		var serve http.HandlerFunc
		switch r.URL.Path {
		case "/livez":
			serve = serveLivez
		case "/readyz":
			serve = serveReadyz(src.Ready)
		case "/metrics":
			if src.ProbeMetrics() {
				serve = serveMetrics(src.Metrics)
			}
		}
		if serve == nil {
			notFound(w, r)
			return
		}
		if !readMethod(w, r) {
			return
		}
		serve(w, r)
	})
}
