package cache

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ok0/go-microgateway/internal/reqctx"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// sink is a response writer that discards the body and keeps the status.
type sink struct {
	h    http.Header
	code int
}

func (s *sink) Header() http.Header { return s.h }

func (s *sink) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
}

func (s *sink) Write(p []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return len(p), nil
}

func (o *origin) update(f func(o *origin)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	f(o)
}

func (o *origin) setHandle(f func(w http.ResponseWriter, r *http.Request)) {
	o.update(func(o *origin) { o.handle = f })
}

func TestStreamingFlushes(t *testing.T) {
	rg := newRig(t, rigOpts{})
	release := make(chan struct{})
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseOnce)
	rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
		ex := reqctx.ExchangeFrom(r.Context())
		ex.Begin()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("event: one\n\n"))
		http.NewResponseController(w).Flush()
		<-release
		_, _ = w.Write([]byte("event: two\n\n"))
		ex.Finish()
	})
	srv := httptest.NewServer(rg.h)
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/sse")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	first := make(chan string, 1)
	go func() {
		line, _ := reader.ReadString('\n')
		first <- line
	}()
	line, ok := testwait.Recv(t, "first event before the origin finishes", first, nil)
	if !ok || line != "event: one\n" {
		t.Fatalf("first line %q", line)
	}
	releaseOnce()
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rest), "event: two") {
		t.Fatalf("rest of stream %q", rest)
	}
	if n := len(rg.spy.callsOf(opPut)); n != 1 {
		t.Fatalf("put calls %d, want 1", n)
	}
}

func TestTooLarge(t *testing.T) {
	body := strings.Repeat("0123456789", 4)
	t.Run("declared length", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "max_body_bytes: 16"})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"40"}}, body)
		rec := rg.get("/big", nil)
		if rec.Body.String() != body {
			t.Fatalf("client body %q", rec.Body.String())
		}
		if rg.stores(storeTooLarge) != 1 || len(rg.spy.callsOf(opPut)) != 0 {
			t.Fatalf("too_large=%v puts=%d", rg.stores(storeTooLarge), len(rg.spy.callsOf(opPut)))
		}
	})
	t.Run("chunked", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "max_body_bytes: 16"})
		rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
			ex := reqctx.ExchangeFrom(r.Context())
			ex.Begin()
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body[:20]))
			_, _ = w.Write([]byte(body[20:]))
			ex.Finish()
		})
		rec := rg.get("/big", nil)
		if rec.Body.String() != body {
			t.Fatalf("client body %q", rec.Body.String())
		}
		if rg.stores(storeTooLarge) != 1 || len(rg.spy.callsOf(opPut)) != 0 {
			t.Fatalf("too_large=%v puts=%d", rg.stores(storeTooLarge), len(rg.spy.callsOf(opPut)))
		}
	})
}

func TestBufferBound(t *testing.T) {
	const maxBody = 1 << 16
	chunk := make([]byte, maxBody/8)
	for i := range chunk {
		chunk[i] = byte('a' + i%26)
	}
	setup := func(t *testing.T, chunks int) *rig {
		rg := newRig(t, rigOpts{global: "max_body_bytes: 65536"})
		rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
			ex := reqctx.ExchangeFrom(r.Context())
			ex.Begin()
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			fw, _ := w.(*fillWriter)
			for i := 0; i < chunks; i++ {
				_, _ = w.Write(chunk)
				if fw == nil {
					continue
				}
				if cap(fw.buf) > fw.maxBody {
					t.Errorf("buffer capacity %d exceeds max_body_bytes %d", cap(fw.buf), fw.maxBody)
				}
				if (i+1)*len(chunk) > fw.maxBody && !(fw.buf == nil && !fw.buffering && fw.tooLarge) {
					t.Errorf("after %d bytes the copy is not abandoned: buf=%v buffering=%v tooLarge=%v", (i+1)*len(chunk), fw.buf != nil, fw.buffering, fw.tooLarge)
				}
			}
			ex.Finish()
		})
		return rg
	}
	t.Run("within limit is stored", func(t *testing.T) {
		rg := setup(t, 7)
		rg.serveSink(t)
		if rg.stores(storeStored) != 1 || len(rg.spy.callsOf(opPut)) != 1 {
			t.Fatalf("stored=%v puts=%d", rg.stores(storeStored), len(rg.spy.callsOf(opPut)))
		}
	})
	t.Run("over limit is too large with bounded memory", func(t *testing.T) {
		rg := setup(t, 64)
		rg.serveSink(t)
		if rg.stores(storeTooLarge) != 1 || len(rg.spy.callsOf(opPut)) != 0 {
			t.Fatalf("too_large=%v puts=%d", rg.stores(storeTooLarge), len(rg.spy.callsOf(opPut)))
		}
	})
}

// serveSink runs one GET through the rig with a discarding client, so that memory measurements only see the stage.
func (r *rig) serveSink(t *testing.T) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "http://example.com/stream", nil)
	s := &sink{h: http.Header{}}
	r.h.ServeHTTP(s, req)
	if s.code != http.StatusOK {
		t.Fatalf("status %d", s.code)
	}
}

func TestAborted(t *testing.T) {
	t.Run("origin resets without finishing", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.update(func(o *origin) {
			o.header = http.Header{"Cache-Control": {"max-age=60"}}
			o.body = []byte("x")
			o.omitFinish = true
		})
		rg.get("/a", nil)
		if rg.stores(storeAborted) != 1 || len(rg.spy.callsOf(opPut)) != 0 {
			t.Fatalf("aborted=%v puts=%d", rg.stores(storeAborted), len(rg.spy.callsOf(opPut)))
		}
		rg.orig.update(func(o *origin) { o.omitFinish = false })
		if hit(t, rg.get("/a", nil)) {
			t.Fatal("aborted fill was served")
		}
	})
	t.Run("content-length mismatch", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}, "Content-Length": {"100"}}, "0123456789")
		rg.get("/m", nil)
		if rg.stores(storeAborted) != 1 || len(rg.spy.callsOf(opPut)) != 0 {
			t.Fatalf("aborted=%v puts=%d", rg.stores(storeAborted), len(rg.spy.callsOf(opPut)))
		}
	})
	t.Run("handler panic re-panics and stores nothing", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("partial"))
			panic(http.ErrAbortHandler)
		})
		func() {
			defer func() {
				if v := recover(); v != http.ErrAbortHandler {
					t.Fatalf("recovered %v, want ErrAbortHandler", v)
				}
			}()
			rg.serve(http.MethodGet, "/p", nil)
		}()
		if rg.stores(storeAborted) != 1 || len(rg.spy.callsOf(opPut)) != 0 {
			t.Fatalf("aborted=%v puts=%d", rg.stores(storeAborted), len(rg.spy.callsOf(opPut)))
		}
	})
	t.Run("client cancelled mid-body", func(t *testing.T) {
		rg := newRig(t, rigOpts{global: "coalescing:\n  enabled: false"})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
			ex := reqctx.ExchangeFrom(r.Context())
			ex.Begin()
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("first"))
			cancel()
			_, _ = w.Write([]byte("second"))
			ex.Finish()
		})
		req := httptest.NewRequest(http.MethodGet, "http://example.com/c", nil).WithContext(ctx)
		do(rg.h, req)
		if rg.stores(storeAborted) != 1 || len(rg.spy.callsOf(opPut)) != 0 {
			t.Fatalf("aborted=%v puts=%d", rg.stores(storeAborted), len(rg.spy.callsOf(opPut)))
		}
		rg.orig.setHandle(nil)
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		if hit(t, rg.get("/c", nil)) {
			t.Fatal("cancelled fill was served")
		}
	})
	t.Run("client cancelled mid-body while leading still stores", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		rg.orig.setHandle(func(w http.ResponseWriter, r *http.Request) {
			ex := reqctx.ExchangeFrom(r.Context())
			ex.Begin()
			w.Header().Set("Cache-Control", "max-age=60")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("first"))
			cancel()
			_, _ = w.Write([]byte("second"))
			ex.Finish()
		})
		req := httptest.NewRequest(http.MethodGet, "http://example.com/c", nil).WithContext(ctx)
		do(rg.h, req)
		if rg.stores(storeStored) != 1 || len(rg.spy.callsOf(opPut)) != 1 {
			t.Fatalf("stored=%v puts=%d", rg.stores(storeStored), len(rg.spy.callsOf(opPut)))
		}
		rg.orig.setHandle(nil)
		rec := rg.get("/c", nil)
		if !hit(t, rec) || rec.Body.String() != "firstsecond" {
			t.Fatalf("next GET body %q", rec.Body.String())
		}
	})
}

func TestNoStoreOutcomes(t *testing.T) {
	t.Run("stored", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rg.get("/s", nil)
		if rg.stores(storeStored) != 1 {
			t.Fatalf("stored=%v", rg.stores(storeStored))
		}
	})
	t.Run("refused", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.spy.setRefuse(true)
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rec := rg.get("/r", nil)
		if rec.Code != http.StatusOK || rec.Body.String() != "x" {
			t.Fatalf("client response %d %q", rec.Code, rec.Body.String())
		}
		if rg.stores(storeRefused) != 1 {
			t.Fatalf("refused=%v", rg.stores(storeRefused))
		}
	})
	t.Run("put error leaves the response unchanged", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.spy.setErr(opPut, errors.New("disk full"))
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"max-age=60"}}, "x")
		rec := rg.get("/e", nil)
		if rec.Code != http.StatusOK || rec.Body.String() != "x" {
			t.Fatalf("client response %d %q", rec.Code, rec.Body.String())
		}
		if rg.stores(storeError) != 1 || rg.storeErrs(opPut) != 1 {
			t.Fatalf("error=%v put errors=%v", rg.stores(storeError), rg.storeErrs(opPut))
		}
	})
	t.Run("not storable", func(t *testing.T) {
		rg := newRig(t, rigOpts{})
		rg.orig.script(http.StatusOK, http.Header{"Cache-Control": {"no-store"}}, "x")
		rg.get("/n", nil)
		if rg.stores(storeNotStorable) != 1 || len(rg.spy.callsOf(opPut)) != 0 {
			t.Fatalf("not_storable=%v puts=%d", rg.stores(storeNotStorable), len(rg.spy.callsOf(opPut)))
		}
	})
}
