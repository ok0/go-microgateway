package server

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/testcert"
	"github.com/ok0/go-microgateway/internal/testwait"
)

// h2Transport is one way a test reaches the gateway over HTTP/2: TLS with ALPN h2, or prior-knowledge h2c.
type h2Transport struct {
	name     string
	tls      bool
	certFile string
	keyFile  string
	pool     *x509.CertPool
}

func tlsTransport(t *testing.T) h2Transport {
	t.Helper()
	certFile, keyFile, pool := testcert.Write(t, t.TempDir())
	return h2Transport{name: "tls", tls: true, certFile: certFile, keyFile: keyFile, pool: pool}
}

func h2cTransport() h2Transport {
	return h2Transport{name: "h2c"}
}

// h2Transports returns the TLS transport followed by the h2c transport.
func h2Transports(t *testing.T) []h2Transport {
	t.Helper()
	return []h2Transport{tlsTransport(t), h2cTransport()}
}

// config returns the test configuration for tr with muts applied.
func (tr h2Transport) config(t *testing.T, muts ...func(*config.Listener)) *config.Config {
	t.Helper()
	if tr.tls {
		muts = append(muts, tlsOn(tr.certFile, tr.keyFile, true))
	} else {
		muts = append(muts, h2cOn)
	}
	return limitCfg(t, muts...)
}

// start serves h on a server for tr that runs its limits on fake.
func (tr h2Transport) start(t *testing.T, fake *clock.Fake, h http.Handler, muts ...func(*config.Listener)) *Server {
	t.Helper()
	return startServer(t, tr.config(t, muts...), h, nil, nil, WithClock(fake))
}

// dial connects an HTTP/2 peer to addr over the transport of tr.
func (tr h2Transport) dial(t *testing.T, addr string) *h2peer {
	t.Helper()
	return dialH2(t, addr, tr.pool, 0)
}

// untilDataBytes collects frames until stream id has delivered n DATA bytes.
func untilDataBytes(p *h2peer, id uint32, n int) []h2frame {
	total := 0
	return p.until(fmt.Sprintf("%d DATA bytes on stream %d", n, id), func(f h2frame) bool {
		if f.Stream == id && f.Type == h2Data {
			total += f.DataLen
		}
		return total >= n
	})
}

// requireOKStream fails the test unless the frames of stream id are a 200 HEADERS and a two-byte DATA that ends the stream.
func requireOKStream(t *testing.T, fs []h2frame, id uint32) {
	t.Helper()
	got := onStream(fs, id)
	if len(got) != 2 || got[0].Type != h2Headers || statusOf(got[0]) != "200" || got[0].EndStream ||
		got[1].Type != h2Data || got[1].DataLen != 2 || !got[1].EndStream {
		t.Fatalf("stream %d frames = %+v, want HEADERS 200 then 2 DATA bytes with END_STREAM", id, got)
	}
}

// requireRSTCode fails the test unless the last frame of fs is RST_STREAM on stream id with code.
func requireRSTCode(t *testing.T, fs []h2frame, id uint32, code h2ErrCode) {
	t.Helper()
	if len(fs) == 0 {
		t.Fatalf("no frames, want RST_STREAM %v on stream %d", code, id)
	}
	last := fs[len(fs)-1]
	if last.Type != h2RSTStream || last.Stream != id || last.Code != code {
		t.Fatalf("last frame = type %v stream %d code %v, want RST_STREAM stream %d code %v", last.Type, last.Stream, last.Code, id, code)
	}
}

func TestHTTP2StreamReadLimit(t *testing.T) {
	for _, tr := range h2Transports(t) {
		t.Run(tr.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			errc := make(chan error, 1)
			srv := tr.start(t, fake, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/upload" {
					_, err := io.ReadAll(r.Body)
					errc <- err
					return
				}
				io.WriteString(w, "ok")
			}), limits(time.Hour, 3*time.Second, 0, time.Hour))
			p := tr.dial(t, srv.Addrs()["public"].String())
			p.handshake(true)
			p.headers(1, http.MethodPost, "/upload", false)
			p.data(1, []byte("01234"), false)
			p.headers(3, http.MethodGet, "/ok", true)
			seen := p.until("stream 3 completed", streamEnded(3))
			requireOKStream(t, seen, 3)
			p.barrier("stream 3 closed on the server")
			awaitDue(t, fake, 1, 3*time.Second, "stream 1 read timer")
			if n := fake.Waiters(); n != 1 {
				t.Fatalf("armed timers = %d, want 1 (stream 1 read timer only; the read limit arms no send-ping timer)", n)
			}
			fake.Advance(3*time.Second - time.Nanosecond)
			if n := fake.Due(time.Nanosecond); n != 1 {
				t.Fatalf("limit timers due within 1ns = %d, want 1", n)
			}
			select {
			case err := <-errc:
				t.Fatalf("handler read ended before the limit: %v", err)
			default:
			}
			fake.Advance(time.Nanosecond)
			err, _ := testwait.Recv(t, "stream 1 read error", errc, nil)
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatalf("handler read error = %v, want deadline exceeded", err)
			}
			reset := p.until("stream 1 reset", streamRST(1))
			for _, f := range append(seen, reset...) {
				if pingFrame(f) {
					t.Fatalf("server sent a PING under a read limit: %+v", f)
				}
			}
			s1 := onStream(reset, 1)
			n := len(s1)
			if n < 2 || s1[n-2].Type != h2Headers || statusOf(s1[n-2]) != "200" || !s1[n-2].EndStream ||
				s1[n-1].Type != h2RSTStream || s1[n-1].Code != h2ErrNo {
				t.Fatalf("stream 1 frames = %+v, want HEADERS 200 END_STREAM then RST_STREAM NO_ERROR", s1)
			}
		})
	}
}

func TestHTTP2StreamWriteLimit(t *testing.T) {
	for _, tr := range h2Transports(t) {
		t.Run(tr.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			srv := tr.start(t, fake, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/big" {
					w.Write(make([]byte, 1<<20))
					return
				}
				io.WriteString(w, "ok")
			}), limits(time.Hour, 0, 3*time.Second, time.Hour))
			p := tr.dial(t, srv.Addrs()["public"].String())
			p.handshake(true)
			p.headers(1, http.MethodGet, "/big", true)
			seen := untilDataBytes(p, 1, 65535)
			p.windowUpdate(0, 1<<20)
			p.headers(3, http.MethodGet, "/ok", true)
			seen = append(seen, p.until("stream 3 completed", streamEnded(3))...)
			requireOKStream(t, seen, 3)
			p.barrier("stream 3 closed on the server")
			if n := fake.Due(3 * time.Second); n != 1 {
				t.Fatalf("timers due within 3s = %d, want 1 (stream 1 write timer)", n)
			}
			fake.Advance(3*time.Second - time.Nanosecond)
			p.open()
			fake.Advance(time.Nanosecond)
			seen = append(seen, p.until("RST_STREAM on stream 1", streamRST(1))...)
			requireRSTCode(t, seen, 1, h2ErrInternal)
			if got := dataBytes(seen, 1); got != 65535 {
				t.Fatalf("stream 1 DATA bytes = %d, want 65535", got)
			}
		})
	}
}

func TestHTTP2WriteLimitAfterHandlerReturn(t *testing.T) {
	for _, tr := range h2Transports(t) {
		t.Run(tr.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			returned := make(chan struct{})
			srv := tr.start(t, fake, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write(make([]byte, 65535))
				w.Write(make([]byte, 100))
				close(returned)
			}), limits(time.Hour, 0, 3*time.Second, time.Hour))
			p := tr.dial(t, srv.Addrs()["public"].String())
			p.handshake(true)
			p.headers(1, http.MethodGet, "/", true)
			seen := untilDataBytes(p, 1, 65535)
			testwait.Done(t, "handler returned", returned, nil)
			awaitDue(t, fake, 1, 3*time.Second, "stream 1 write timer")
			fake.Advance(3*time.Second - time.Nanosecond)
			p.open()
			fake.Advance(time.Nanosecond)
			seen = append(seen, p.until("RST_STREAM on stream 1", streamRST(1))...)
			requireRSTCode(t, seen, 1, h2ErrInternal)
			for _, f := range onStream(seen, 1) {
				if f.EndStream {
					t.Fatalf("stream 1 frame %+v carries END_STREAM, want none", f)
				}
			}
		})
	}
}

func TestHTTP2ResponseControllerDeadlines(t *testing.T) {
	for _, tr := range h2Transports(t) {
		t.Run(tr.name, func(t *testing.T) {
			t.Run("write", func(t *testing.T) {
				fake := clock.NewFake(serverStart)
				armed := make(chan struct{})
				srv := tr.start(t, fake, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if err := http.NewResponseController(w).SetWriteDeadline(fake.Now().Add(2 * time.Second)); err != nil {
						t.Errorf("SetWriteDeadline: %v", err)
					}
					close(armed)
					w.Write(make([]byte, 1<<20))
				}), limits(time.Hour, 0, 0, time.Hour))
				p := tr.dial(t, srv.Addrs()["public"].String())
				p.handshake(true)
				p.headers(1, http.MethodGet, "/", true)
				waitSignal(t, armed, "write deadline set")
				awaitDue(t, fake, 1, 2*time.Second, "response write deadline")
				fake.Advance(2*time.Second - time.Nanosecond)
				p.open()
				fake.Advance(time.Nanosecond)
				requireRSTCode(t, p.until("RST_STREAM on stream 1", streamRST(1)), 1, h2ErrInternal)
			})
			t.Run("read", func(t *testing.T) {
				fake := clock.NewFake(serverStart)
				armed := make(chan struct{})
				errc := make(chan error, 1)
				srv := tr.start(t, fake, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if err := http.NewResponseController(w).SetReadDeadline(fake.Now().Add(2 * time.Second)); err != nil {
						t.Errorf("SetReadDeadline: %v", err)
					}
					close(armed)
					_, err := io.ReadAll(r.Body)
					errc <- err
				}), limits(time.Hour, 0, 0, time.Hour))
				p := tr.dial(t, srv.Addrs()["public"].String())
				p.handshake(true)
				p.headers(1, http.MethodPost, "/", false)
				waitSignal(t, armed, "read deadline set")
				awaitDue(t, fake, 1, 2*time.Second, "request read deadline")
				fake.Advance(2*time.Second - time.Nanosecond)
				select {
				case err := <-errc:
					t.Fatalf("body read ended before the deadline: %v", err)
				default:
				}
				fake.Advance(time.Nanosecond)
				err, _ := testwait.Recv(t, "body read error", errc, nil)
				if !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Fatalf("body read error = %v, want deadline exceeded", err)
				}
			})
		})
	}
}

func TestHTTP2IdleGoAway(t *testing.T) {
	for _, tr := range h2Transports(t) {
		t.Run(tr.name, func(t *testing.T) {
			t.Run("idle", func(t *testing.T) {
				fake := clock.NewFake(serverStart)
				srv := tr.start(t, fake, okHandler(), limits(time.Hour, 0, 0, 5*time.Second))
				p := tr.dial(t, srv.Addrs()["public"].String())
				p.handshake(true)
				awaitIdle(t, fake, 1)
				if fake.Waiters() != 0 || fake.Due(24*time.Hour) != 0 {
					t.Fatalf("non-housekeeping timers: waiters = %d, due within 24h = %d; want 0, 0", fake.Waiters(), fake.Due(24*time.Hour))
				}
				fake.Advance(5*time.Second - time.Nanosecond)
				if n := fake.HousekeepingWaiters(); n != 1 {
					t.Fatalf("housekeeping timers pending = %d, want 1", n)
				}
				p.open()
				fake.Advance(time.Nanosecond)
				fs := p.until("GOAWAY", goAwayFrame)
				last := fs[len(fs)-1]
				if last.Code != h2ErrNo || last.LastStream != 0 {
					t.Fatalf("GOAWAY code = %v last stream = %d, want NO_ERROR, 0", last.Code, last.LastStream)
				}
				awaitDue(t, fake, 1, time.Second, "GOAWAY linger timer")
				fake.Advance(time.Second - time.Nanosecond)
				if n := fake.Due(time.Nanosecond); n != 1 {
					t.Fatalf("linger timers due within 1ns = %d, want 1", n)
				}
				p.open()
				fake.Advance(time.Nanosecond)
				p.awaitEnd("connection closed after the GOAWAY linger")
			})
			t.Run("active-stream", func(t *testing.T) {
				fake := clock.NewFake(serverStart)
				entered := make(chan struct{}, 1)
				release := make(chan struct{})
				srv := tr.start(t, fake, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					entered <- struct{}{}
					<-release
					io.WriteString(w, "ok")
				}), limits(time.Hour, 0, 0, 5*time.Second))
				p := tr.dial(t, srv.Addrs()["public"].String())
				p.handshake(true)
				p.headers(1, http.MethodGet, "/", true)
				waitSignal(t, entered, "handler entered")
				testwait.Until(t, "new stream stopped the idle timer", func() bool { return fake.HousekeepingWaiters() == 0 }, nil)
				fake.Advance(time.Hour)
				if fake.HousekeepingWaiters() != 0 || fake.Waiters() != 0 {
					t.Fatalf("timers pending with an open stream: housekeeping = %d, waiters = %d; want 0, 0", fake.HousekeepingWaiters(), fake.Waiters())
				}
				p.open()
				close(release)
				p.until("stream 1 completed", streamEnded(1))
				awaitIdle(t, fake, 1)
				fake.Advance(5*time.Second - time.Nanosecond)
				p.open()
				fake.Advance(time.Nanosecond)
				fs := p.until("GOAWAY with last stream 1", func(f h2frame) bool {
					return f.Type == h2GoAway && f.LastStream == 1
				})
				if last := fs[len(fs)-1]; last.Code != h2ErrNo {
					t.Fatalf("GOAWAY code = %v, want NO_ERROR", last.Code)
				}
			})
		})
	}
}

func TestHTTP2ReadLimitSendsNoPing(t *testing.T) {
	for _, tr := range h2Transports(t) {
		t.Run(tr.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			srv := tr.start(t, fake, okHandler(), limits(time.Hour, 4*time.Second, 0, time.Hour))
			p := tr.dial(t, srv.Addrs()["public"].String())
			p.handshake(false)
			p.headers(1, http.MethodGet, "/ok", true)
			seen := p.until("stream 1 completed", streamEnded(1))
			requireOKStream(t, seen, 1)
			p.barrier("stream 1 closed on the server")
			awaitIdle(t, fake, 1)
			if n := fake.Waiters(); n != 0 {
				t.Fatalf("armed timers after stream 1 = %d, want 0 (no send-ping timer)", n)
			}
			fake.Advance(30 * time.Minute)
			p.open()
			p.headers(3, http.MethodGet, "/ok", true)
			seen = append(seen, p.until("stream 3 completed", streamEnded(3))...)
			requireOKStream(t, onStream(seen, 3), 3)
			p.barrier("stream 3 closed on the server")
			awaitIdle(t, fake, 1)
			if n := fake.Waiters(); n != 0 {
				t.Fatalf("armed timers after stream 3 = %d, want 0 (no send-ping timer)", n)
			}
			fake.Advance(time.Hour)
			seen = append(seen, p.until("GOAWAY with last stream 3", func(f h2frame) bool {
				return f.Type == h2GoAway && f.LastStream == 3
			})...)
			if last := seen[len(seen)-1]; last.Code != h2ErrNo {
				t.Fatalf("GOAWAY code = %v, want NO_ERROR", last.Code)
			}
			for _, f := range seen {
				if pingFrame(f) {
					t.Fatalf("server sent a PING under a read limit: %+v", f)
				}
			}
		})
	}
}

func TestHTTP2PrefaceTimeout(t *testing.T) {
	tr := tlsTransport(t)
	fake := clock.NewFake(serverStart)
	srv := tr.start(t, fake, okHandler(), limits(time.Hour, 0, 0, time.Hour))
	p := tr.dial(t, srv.Addrs()["public"].String())
	p.start(false, 0)
	awaitDue(t, fake, 1, 10*time.Second, "preface timer")
	fake.Advance(10*time.Second - time.Nanosecond)
	p.open()
	fake.Advance(time.Nanosecond)
	p.awaitEnd("connection closed after the preface timeout")
}

func TestHTTP2FirstSettingsTimeout(t *testing.T) {
	for _, tr := range h2Transports(t) {
		t.Run(tr.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			srv := tr.start(t, fake, okHandler(), limits(time.Hour, 0, 0, time.Hour))
			p := tr.dial(t, srv.Addrs()["public"].String())
			// A SETTINGS ACK is a frame and would stop the first-SETTINGS timer; the peer must stay silent after the preface.
			p.holdSettingsAck = true
			p.mustSend(func() error {
				_, err := p.conn.Write([]byte(h2Preface))
				return err
			})
			p.start(false, 0)
			awaitDue(t, fake, 1, 2*time.Second, "first SETTINGS timer")
			fake.Advance(2*time.Second - time.Nanosecond)
			p.open()
			fake.Advance(time.Nanosecond)
			p.awaitEnd("connection closed after the first SETTINGS timeout")
		})
	}
}

func TestHTTP2InitialSettings(t *testing.T) {
	for _, tr := range h2Transports(t) {
		t.Run(tr.name, func(t *testing.T) {
			fake := clock.NewFake(serverStart)
			srv := tr.start(t, fake, okHandler())
			p := tr.dial(t, srv.Addrs()["public"].String())
			fs := p.handshake(true)
			want := map[h2SettingID]uint32{
				h2SettingMaxFrameSize:         1048576,
				h2SettingMaxConcurrentStreams: 250,
				h2SettingMaxHeaderListSize:    1048896,
				h2SettingHeaderTableSize:      4096,
				h2SettingInitialWindowSize:    1048576,
				h2SettingNoRFC7540Priorities:  1,
			}
			if !reflect.DeepEqual(fs.Settings, want) {
				t.Fatalf("server SETTINGS = %v, want %v", fs.Settings, want)
			}
			wu := p.until("connection WINDOW_UPDATE", func(f h2frame) bool {
				return f.Type == h2WindowUpdate && f.Stream == 0
			})
			if got := wu[len(wu)-1].Incr; got != 983041 {
				t.Fatalf("connection WINDOW_UPDATE increment = %d, want 983041", got)
			}
		})
	}
}

func TestHTTP2TwoServersOwnClocks(t *testing.T) {
	tr := tlsTransport(t)
	big := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(make([]byte, 1<<20))
	})
	f1 := clock.NewFake(serverStart)
	f2 := clock.NewFake(serverStart)
	s1 := tr.start(t, f1, big, limits(time.Hour, 0, 3*time.Second, time.Hour))
	s2 := tr.start(t, f2, big, limits(time.Hour, 0, 3*time.Second, time.Hour))
	p1 := tr.dial(t, s1.Addrs()["public"].String())
	p1.handshake(true)
	p1.headers(1, http.MethodGet, "/big", true)
	p2 := tr.dial(t, s2.Addrs()["public"].String())
	p2.handshake(true)
	p2.headers(1, http.MethodGet, "/big", true)
	untilDataBytes(p1, 1, 65535)
	untilDataBytes(p2, 1, 65535)
	awaitDue(t, f1, 1, 3*time.Second, "server one write timer")
	awaitDue(t, f2, 1, 3*time.Second, "server two write timer")
	f1.Advance(3 * time.Second)
	requireRSTCode(t, p1.until("RST_STREAM on server one", streamRST(1)), 1, h2ErrInternal)
	if n := f2.Due(3 * time.Second); n != 1 {
		t.Fatalf("server two timers due = %d, want 1", n)
	}
	p2.open()
	f2.Advance(3 * time.Second)
	requireRSTCode(t, p2.until("RST_STREAM on server two", streamRST(1)), 1, h2ErrInternal)
}

func TestHTTP2ShutdownStopsTimers(t *testing.T) {
	lim := limits(time.Hour, time.Hour, time.Hour, time.Hour)
	tlsTr := tlsTransport(t)
	h2cTr := h2cTransport()
	t.Run("tls-active-stream", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		// Baseline inside the subtest: the subtest runs on its own goroutine while its parent waits.
		base := runtime.NumGoroutine()
		entered := make(chan struct{}, 1)
		srv := tlsTr.start(t, fake, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- struct{}{}
			<-r.Context().Done()
		}), lim)
		p := tlsTr.dial(t, srv.Addrs()["public"].String())
		p.handshake(true)
		p.headers(1, http.MethodGet, "/", true)
		waitSignal(t, entered, "handler entered")
		shutdownForced(t, srv, fake, base)
	})
	t.Run("tls-no-preface", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		base := runtime.NumGoroutine()
		srv := tlsTr.start(t, fake, okHandler(), lim)
		tlsTr.dial(t, srv.Addrs()["public"].String())
		awaitDue(t, fake, 1, 10*time.Second, "preface timer")
		shutdownForced(t, srv, fake, base)
	})
	t.Run("h2c-idle", func(t *testing.T) {
		fake := clock.NewFake(serverStart)
		base := runtime.NumGoroutine()
		srv := h2cTr.start(t, fake, okHandler(), lim)
		p := h2cTr.dial(t, srv.Addrs()["public"].String())
		p.handshake(true)
		awaitIdle(t, fake, 1)
		shutdownForced(t, srv, fake, base)
	})
}

// shutdownForced runs Shutdown with a canceled context, which must force-close the connections, and then requires
// every timer of fake to be stopped and the goroutines to be back at base.
func shutdownForced(t *testing.T, srv *Server, fake *clock.Fake, base int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := srv.Shutdown(ctx); !errors.Is(err, ErrForcedClose) {
		t.Fatalf("Shutdown = %v, want ErrForcedClose", err)
	}
	testwait.Until(t, "fake timers stopped", func() bool {
		return fake.Waiters() == 0 && fake.HousekeepingWaiters() == 0
	}, func() string {
		return fmt.Sprintf("waiters=%d housekeeping=%d", fake.Waiters(), fake.HousekeepingWaiters())
	})
	testwait.Until(t, "goroutines back to baseline", func() bool { return runtime.NumGoroutine() <= base }, nil)
}
