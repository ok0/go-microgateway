package proxy

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"sync"

	"github.com/ok0/go-microgateway/internal/breaker"
	"github.com/ok0/go-microgateway/internal/httperr"
	"github.com/ok0/go-microgateway/internal/upstream"
)

type stageKey struct{}

// BreakerStage returns middleware that admits each request through the aggregate circuit breaker of up. A request the breaker refuses is answered with 503 and the circuit-open error code. Otherwise the breaker outcome is taken from the response status: 5xx is a failure, 4xx is ignored, and any other response is a success.
func BreakerStage(up *upstream.Upstream) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok, err := up.AllowAggregate()
			if err != nil {
				httperr.Write(w, r, http.StatusServiceUnavailable, httperr.CodeCircuitOpen, httperr.Message(httperr.CodeCircuitOpen))
				return
			}
			bw := &breakerWriter{ResponseWriter: w, tok: tok}
			ctx := context.WithValue(r.Context(), stageKey{}, bw)
			completed := false
			defer func() { bw.finish(r.Context(), completed) }()
			next.ServeHTTP(bw, r.WithContext(ctx))
			completed = true
		})
	}
}

type breakerWriter struct {
	http.ResponseWriter
	tok  breaker.Token
	once sync.Once
	skip bool
}

func (bw *breakerWriter) record(o breaker.Outcome) {
	bw.once.Do(func() { bw.tok.Done(o) })
}

func (bw *breakerWriter) outcomeFor(code int) breaker.Outcome {
	switch {
	case bw.skip:
		return breaker.Ignored
	case code >= 500:
		return breaker.Failure
	case code >= 400:
		return breaker.Ignored
	default:
		return breaker.Success
	}
}

func (bw *breakerWriter) WriteHeader(code int) {
	if code >= 100 && code < 200 && code != http.StatusSwitchingProtocols {
		bw.ResponseWriter.WriteHeader(code)
		return
	}
	bw.record(bw.outcomeFor(code))
	bw.ResponseWriter.WriteHeader(code)
}

func (bw *breakerWriter) Write(p []byte) (int, error) {
	bw.record(breaker.Success)
	return bw.ResponseWriter.Write(p)
}

func (bw *breakerWriter) Flush() {
	bw.record(breaker.Success)
	_ = http.NewResponseController(bw.ResponseWriter).Flush()
}

func (bw *breakerWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	bw.record(breaker.Success)
	return http.NewResponseController(bw.ResponseWriter).Hijack()
}

func (bw *breakerWriter) Unwrap() http.ResponseWriter {
	return bw.ResponseWriter
}

func (bw *breakerWriter) finish(ctx context.Context, completed bool) {
	if !completed || ctx.Err() == context.Canceled {
		bw.record(breaker.Ignored)
		return
	}
	bw.record(breaker.Success)
}

func markCircuitOpen(ctx context.Context) {
	if bw, ok := ctx.Value(stageKey{}).(*breakerWriter); ok {
		bw.skip = true
	}
}
