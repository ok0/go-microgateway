package middleware

import (
	"bufio"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/httperr"
	"github.com/ok0/lychgate/internal/metrics"
	"github.com/ok0/lychgate/internal/reqctx"
)

// RequestMetrics holds the request series recorded by Observe.
type RequestMetrics struct {
	// Requests counts completed requests by route, method and status code.
	Requests *metrics.CounterVec
	// Duration observes request latency in seconds by route.
	Duration *metrics.HistogramVec
}

// Observe writes one access log line and records request metrics for every request that reaches it, including requests whose handler panics. It returns nil when neither logging nor metrics is enabled.
func Observe(logger *slog.Logger, clk clock.Clock, accessLog bool, m *RequestMetrics) Middleware {
	logEnabled := accessLog && logger != nil
	if !logEnabled && m == nil {
		return nil
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := &reqctx.Info{}
			r = r.WithContext(reqctx.WithInfo(r.Context(), info))
			sw := &statusWriter{ResponseWriter: w}
			start := clk.Now()
			defer func() {
				p := recover()
				status := sw.status
				if status == 0 {
					switch {
					case p != nil:
						status = http.StatusInternalServerError
					case r.Context().Err() != nil:
						status = httperr.StatusClientClosed
					default:
						status = http.StatusOK
					}
				}
				elapsed := clk.Since(start)
				ctx := r.Context()
				if m != nil {
					label := info.Route
					if label == "" {
						label = "unmatched"
					}
					m.Requests.With(label, MethodLabel(r.Method), strconv.Itoa(status)).Inc()
					m.Duration.With(label).Observe(elapsed.Seconds())
				}
				if logEnabled {
					attrs := []slog.Attr{
						slog.String("request_id", reqctx.RequestID(ctx)),
						slog.String("method", r.Method),
						slog.String("path", r.URL.Path),
						slog.String("route", info.Route),
					}
					if info.Upstream != "" {
						attrs = append(attrs, slog.String("upstream", info.Upstream))
					}
					attrs = append(attrs,
						slog.Int("status", status),
						slog.Int64("bytes", sw.bytes),
						slog.Float64("latency_ms", float64(elapsed)/float64(time.Millisecond)),
						slog.String("client_ip", reqctx.ClientIP(ctx).String()),
					)
					logger.LogAttrs(ctx, slog.LevelInfo, "request", attrs...)
				}
				if p != nil {
					panic(p)
				}
			}()
			next.ServeHTTP(sw, r)
		})
	}
}

// MethodLabel maps a request method to a bounded metric label: standard methods are kept and any other method becomes "OTHER".
func MethodLabel(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodDelete,
		http.MethodConnect, http.MethodOptions, http.MethodTrace, http.MethodPatch:
		return method
	}
	return "OTHER"
}

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (sw *statusWriter) WriteHeader(code int) {
	informational := code >= 100 && code < 200 && code != http.StatusSwitchingProtocols
	if sw.status == 0 && !informational {
		sw.status = code
	}
	sw.ResponseWriter.WriteHeader(code)
}

func (sw *statusWriter) Write(p []byte) (int, error) {
	if sw.status == 0 {
		sw.status = http.StatusOK
	}
	n, err := sw.ResponseWriter.Write(p)
	sw.bytes += int64(n)
	return n, err
}

func (sw *statusWriter) Flush() {
	_ = http.NewResponseController(sw.ResponseWriter).Flush()
}

func (sw *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(sw.ResponseWriter).Hijack()
	if err == nil && sw.status == 0 {
		sw.status = http.StatusSwitchingProtocols
	}
	return conn, rw, err
}

func (sw *statusWriter) Unwrap() http.ResponseWriter {
	return sw.ResponseWriter
}
