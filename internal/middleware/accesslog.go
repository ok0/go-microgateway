package middleware

import (
	"bufio"
	"log/slog"
	"net"
	"net/http"

	"github.com/ok0/lychgate/internal/clock"
	"github.com/ok0/lychgate/internal/reqctx"
)

func AccessLog(logger *slog.Logger, clk clock.Clock, enabled bool) Middleware {
	if !enabled || logger == nil {
		return nil
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sw := &statusWriter{ResponseWriter: w}
			start := clk.Now()
			next.ServeHTTP(sw, r)
			status := sw.status
			if status == 0 {
				status = http.StatusOK
			}
			ctx := r.Context()
			logger.LogAttrs(ctx, slog.LevelInfo, "request",
				slog.String("request_id", reqctx.RequestID(ctx)),
				slog.String("route", reqctx.Route(ctx)),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", status),
				slog.Int64("bytes", sw.bytes),
				slog.Float64("latency_ms", float64(clk.Since(start))/1e6),
				slog.String("client_ip", reqctx.ClientIP(ctx).String()),
			)
		})
	}
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
