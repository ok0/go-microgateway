package h2server_test

import (
	"crypto/tls"
	"io"
	"math"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/ok0/go-microgateway/internal/h2server"

	"github.com/ok0/go-microgateway/internal/clock"
)

func TestWriteLimitBehindBlockedSocketWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		bReturned := make(chan struct{})
		st := newServerTester(t, func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/a":
				http.NewResponseController(w).SetWriteDeadline(time.Time{})
				w.Write(make([]byte, 64<<10))
			case "/b":
				defer close(bReturned)
				w.Header().Set("Trailer", "X-T")
				w.Write([]byte("b"))
				w.Header().Set("X-T", "v")
			}
		}, fake, func(s *http.Server) {
			s.WriteTimeout = 3 * time.Second
		})
		adv := func(d time.Duration) {
			fake.Advance(d)
			synctest.Wait()
		}
		conn := st.cc.(*tls.Conn).NetConn().(*synctestNetConn)

		st.greet()
		st.writeWindowUpdate(0, 1<<30)
		conn.SetReadBufferSize(0)
		st.writeHeaders(HeadersFrameParam{
			StreamID:      1,
			BlockFragment: st.encodeHeader(":method", "GET", ":scheme", "https", ":path", "/a", ":authority", st.authority()),
			EndStream:     true,
			EndHeaders:    true,
		})
		st.writeWindowUpdate(1, 1<<30)
		synctest.Wait()
		st.writeHeaders(HeadersFrameParam{
			StreamID:      3,
			BlockFragment: st.encodeHeader(":method", "GET", ":scheme", "https", ":path", "/b", ":authority", st.authority()),
			EndStream:     true,
			EndHeaders:    true,
		})
		<-bReturned
		synctest.Wait()
		time.Sleep(time.Hour)
		synctest.Wait()

		if n := fake.Due(3 * time.Second); n != 1 {
			t.Fatalf("Due(3s) = %d, want 1", n)
		}
		adv(3*time.Second - time.Nanosecond)
		if n := fake.Due(time.Nanosecond); n != 1 {
			t.Fatalf("Due(1ns) = %d, want 1", n)
		}
		adv(time.Nanosecond)
		conn.SetReadBufferSize(math.MaxInt)

		var streamOneBytes int
		var streamOneEnded bool
		var rstCount int
		for {
			fr := st.readFrame()
			if fr == nil {
				break
			}
			switch fr := fr.(type) {
			case *HeadersFrame:
				if fr.StreamID != 1 {
					t.Fatalf("got HEADERS on stream %d, want stream 1 only", fr.StreamID)
				}
			case *DataFrame:
				if fr.StreamID != 1 {
					t.Fatalf("got DATA on stream %d, want stream 1 only", fr.StreamID)
				}
				streamOneBytes += len(fr.Data())
				streamOneEnded = fr.StreamEnded()
			case *RSTStreamFrame:
				if fr.StreamID != 3 || fr.ErrCode != ErrCodeInternal {
					t.Fatalf("got %v, want RST_STREAM stream 3 code INTERNAL_ERROR", SummarizeFrame(fr))
				}
				rstCount++
			default:
				t.Fatalf("got unexpected frame %v", SummarizeFrame(fr))
			}
		}
		if !streamOneEnded || streamOneBytes != 64<<10 {
			t.Fatalf("stream 1: %d bytes, ended=%v; want 65536 bytes ended", streamOneBytes, streamOneEnded)
		}
		if rstCount != 1 {
			t.Fatalf("got %d RST_STREAM frames for stream 3, want 1", rstCount)
		}
	})
}

func TestStreamTimersIgnoreRealStall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		const size = 256 << 10
		st := newServerTester(t, func(w http.ResponseWriter, req *http.Request) {
			switch req.URL.Path {
			case "/post":
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Errorf("reading body: %v", err)
					return
				}
				w.Write(body)
			case "/get":
				w.Write(make([]byte, size))
			}
		}, fake, func(s *http.Server) {
			s.ReadTimeout = time.Millisecond
			s.WriteTimeout = time.Millisecond
			s.IdleTimeout = time.Millisecond
		})

		st.greet()
		st.writeWindowUpdate(0, 1<<30)

		st.writeHeaders(HeadersFrameParam{
			StreamID:      1,
			BlockFragment: st.encodeHeader(":method", "POST", ":scheme", "https", ":path", "/post", ":authority", st.authority()),
			EndHeaders:    true,
		})
		st.writeData(1, false, []byte("first"))
		synctest.Wait()
		if n := fake.Waiters(); n < 2 {
			t.Fatalf("fake clock has %d waiters with stream 1 open, want the stream timers counted", n)
		}
		time.Sleep(24 * time.Hour)
		st.writeData(1, true, []byte("second"))
		st.wantHeaders(wantHeader{streamID: 1, header: http.Header{":status": []string{"200"}}})
		st.wantData(wantData{streamID: 1, endStream: true, multiple: true, data: []byte("firstsecond")})

		st.writeHeaders(HeadersFrameParam{
			StreamID:      3,
			BlockFragment: st.encodeHeader(":method", "GET", ":scheme", "https", ":path", "/get", ":authority", st.authority()),
			EndStream:     true,
			EndHeaders:    true,
		})
		st.writeWindowUpdate(3, 1<<30)
		st.wantHeaders(wantHeader{streamID: 3, header: http.Header{":status": []string{"200"}}})
		var got int
		for ended := false; !ended; {
			time.Sleep(time.Hour)
			df := readFrame[*DataFrame](t, st)
			if df.StreamID != 3 {
				t.Fatalf("got DATA on stream %d, want stream 3", df.StreamID)
			}
			got += len(df.Data())
			ended = df.StreamEnded()
		}
		if got != size {
			t.Fatalf("got %d bytes on stream 3, want %d", got, size)
		}
		st.wantIdle()
	})
}

func TestWriteByteTimeoutOnClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		st := newServerTester(t, func(w http.ResponseWriter, req *http.Request) {
			w.Write(make([]byte, 64<<10))
		}, fake, func(c *http.HTTP2Config) {
			c.WriteByteTimeout = 2 * time.Second
		})
		adv := func(d time.Duration) {
			fake.Advance(d)
			synctest.Wait()
		}
		conn := st.cc.(*tls.Conn).NetConn().(*synctestNetConn)

		st.greet()
		st.writeWindowUpdate(0, 1<<30)
		conn.SetReadBufferSize(0)
		st.writeHeaders(HeadersFrameParam{
			StreamID:      1,
			BlockFragment: st.encodeHeader(":method", "GET", ":scheme", "https", ":path", "/", ":authority", st.authority()),
			EndStream:     true,
			EndHeaders:    true,
		})
		st.writeWindowUpdate(1, 1<<30)
		synctest.Wait()
		time.Sleep(time.Hour)
		synctest.Wait()

		st.wantIdle()
		if n := fake.Due(2 * time.Second); n < 1 {
			t.Fatalf("Due(2s) = %d, want at least 1", n)
		}
		adv(2*time.Second - time.Nanosecond)
		st.wantIdle()
		adv(time.Nanosecond)
		conn.SetReadBufferSize(math.MaxInt)
		st.wantClosed()
	})
}

func TestServeStopsEveryTimer(t *testing.T) {
	t.Run("open-streams", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fake := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			st := newServerTester(t, func(w http.ResponseWriter, req *http.Request) {
				io.ReadAll(req.Body)
			}, fake, func(s *http.Server) {
				s.ReadTimeout = time.Hour
				s.WriteTimeout = time.Hour
				s.IdleTimeout = time.Hour
			}, func(c *http.HTTP2Config) {
				c.WriteByteTimeout = time.Hour
			})

			st.greet()
			st.writeHeaders(HeadersFrameParam{
				StreamID:      1,
				BlockFragment: st.encodeHeader(":method", "POST", ":scheme", "https", ":path", "/", ":authority", st.authority()),
				EndHeaders:    true,
			})
			st.writeData(1, false, []byte("x"))
			st.writeHeaders(HeadersFrameParam{
				StreamID:      3,
				BlockFragment: st.encodeHeader(":method", "POST", ":scheme", "https", ":path", "/", ":authority", st.authority()),
				EndHeaders:    true,
			})
			synctest.Wait()
			if hw := fake.HousekeepingWaiters(); hw != 0 {
				t.Fatalf("before close: %d housekeeping waiters, want 0", hw)
			}

			st.cc.Close()
			synctest.Wait()
			if w, hw := fake.Waiters(), fake.HousekeepingWaiters(); w != 0 || hw != 0 {
				t.Fatalf("after close: %d waiters and %d housekeeping waiters, want 0 and 0", w, hw)
			}
		})
	})
	t.Run("no-streams", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fake := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			st := newServerTester(t, func(w http.ResponseWriter, req *http.Request) {
				io.ReadAll(req.Body)
			}, fake, func(s *http.Server) {
				s.ReadTimeout = time.Hour
				s.WriteTimeout = time.Hour
				s.IdleTimeout = time.Hour
			}, func(c *http.HTTP2Config) {
				c.WriteByteTimeout = time.Hour
			})

			st.greet()
			synctest.Wait()
			if hw := fake.HousekeepingWaiters(); hw != 1 {
				t.Fatalf("before close: %d housekeeping waiters, want 1", hw)
			}

			st.cc.Close()
			synctest.Wait()
			if w, hw := fake.Waiters(), fake.HousekeepingWaiters(); w != 0 || hw != 0 {
				t.Fatalf("after close: %d waiters and %d housekeeping waiters, want 0 and 0", w, hw)
			}
		})
	})
}

func TestPingAckTimerOnClock(t *testing.T) {
	for _, name := range []string{"unacked", "acked"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fake := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
				st := newServerTester(t, func(w http.ResponseWriter, req *http.Request) {}, fake, func(h2 *http.HTTP2Config) {
					h2.SendPingTimeout = 4 * time.Second
				})
				adv := func(d time.Duration) {
					fake.Advance(d)
					synctest.Wait()
				}

				st.greet()
				synctest.Wait()
				if n := fake.Waiters(); n != 1 {
					t.Fatalf("armed timers = %d, want 1", n)
				}
				if n := fake.Due(4 * time.Second); n != 1 {
					t.Fatalf("Due(4s) = %d, want 1", n)
				}
				time.Sleep(time.Hour)
				synctest.Wait()
				st.wantIdle()
				adv(4*time.Second - time.Nanosecond)
				st.wantIdle()
				if n := fake.Due(time.Nanosecond); n != 1 {
					t.Fatalf("Due(1ns) = %d, want 1", n)
				}
				adv(time.Nanosecond)
				pf := readFrame[*PingFrame](t, st)
				if pf.Flags.Has(FlagPingAck) {
					t.Fatalf("got PING ACK, want a non-ACK PING")
				}
				if n := fake.Waiters(); n != 1 {
					t.Fatalf("after PING: armed timers = %d, want 1 (ack timer)", n)
				}
				if n := fake.Due(15*time.Second - time.Nanosecond); n != 0 {
					t.Fatalf("Due(15s-1ns) = %d, want 0", n)
				}
				if n := fake.Due(15 * time.Second); n != 1 {
					t.Fatalf("after PING: Due(15s) = %d, want 1", n)
				}

				if name == "unacked" {
					time.Sleep(time.Hour)
					synctest.Wait()
					st.wantIdle()
					adv(15*time.Second - time.Nanosecond)
					st.wantIdle()
					if n := fake.Due(time.Nanosecond); n != 1 {
						t.Fatalf("Due(1ns) = %d, want 1", n)
					}
					adv(time.Nanosecond)
					st.wantClosed()
					synctest.Wait()
					if w, hw := fake.Waiters(), fake.HousekeepingWaiters(); w != 0 || hw != 0 {
						t.Fatalf("after close: %d waiters and %d housekeeping waiters, want 0 and 0", w, hw)
					}
					return
				}

				adv(14 * time.Second)
				st.wantIdle()
				if n := fake.Due(time.Second); n != 1 {
					t.Fatalf("Due(1s) = %d, want 1", n)
				}
				st.writePing(true, pf.Data)
				synctest.Wait()
				if n := fake.Due(time.Second); n != 0 {
					t.Fatalf("after ACK: Due(1s) = %d, want 0", n)
				}
				if n := fake.Waiters(); n != 1 {
					t.Fatalf("after ACK: armed timers = %d, want 1", n)
				}
				if n := fake.Due(4*time.Second - time.Nanosecond); n != 0 {
					t.Fatalf("after ACK: Due(4s-1ns) = %d, want 0", n)
				}
				if n := fake.Due(4 * time.Second); n != 1 {
					t.Fatalf("after ACK: Due(4s) = %d, want 1", n)
				}
				adv(time.Second)
				st.wantIdle()
				adv(3*time.Second - time.Nanosecond)
				st.wantIdle()
				adv(time.Nanosecond)
				pf = readFrame[*PingFrame](t, st)
				if pf.Flags.Has(FlagPingAck) {
					t.Fatalf("got PING ACK after resume, want a non-ACK PING")
				}
			})
		})
	}
}

func TestReadTimeoutSendsNoPing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fake := clock.NewFake(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		st := newServerTester(t, func(w http.ResponseWriter, req *http.Request) {}, fake, func(s *http.Server) {
			s.ReadTimeout = 4 * time.Second
			s.IdleTimeout = time.Hour
		})

		st.greet()
		synctest.Wait()
		if n := fake.Waiters(); n != 0 {
			t.Fatalf("armed timers = %d, want 0 (no send-ping timer)", n)
		}
		if n := fake.HousekeepingWaiters(); n != 1 {
			t.Fatalf("housekeeping timers = %d, want 1 (idle)", n)
		}
		fake.Advance(30 * time.Minute)
		synctest.Wait()
		st.wantIdle()
	})
}
