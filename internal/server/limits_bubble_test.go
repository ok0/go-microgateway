package server

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ok0/go-microgateway/internal/clock"
)

// wallStall is one stalled HTTP/1 exchange of TestHTTP1IgnoresWallTimeInBubble and the limit that must cut it.
type wallStall struct {
	name  string
	limit time.Duration
}

func wallStalls() []wallStall {
	return []wallStall{
		{"header", seqH},
		{"body", seqR},
		{"write", seqW},
		{"idle", seqI},
		{"handshake", seqSmallest(seqH, seqR, seqW)},
	}
}

// wallHandler serves the stalled exchanges: body reads the request body, write writes 8 MiB, and every other request
// gets "ok". The outcome of body and write is sent on res.
func wallHandler(name string, res chan<- error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch name {
		case "body":
			_, err := io.ReadAll(r.Body)
			res <- err
			return
		case "write":
			chunk := make([]byte, 64<<10)
			for range 128 {
				if _, err := w.Write(chunk); err != nil {
					res <- err
					return
				}
			}
			res <- nil
			return
		}
		io.WriteString(w, "ok")
	})
}

// wallStart sends the bytes that begin the stalled exchange name on conn.
func wallStart(t *testing.T, conn net.Conn, name string) {
	t.Helper()
	switch name {
	case "header":
		writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n")
	case "body":
		writeString(t, conn, "POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 10\r\n\r\n01234")
	case "write":
		writeString(t, conn, "GET /big HTTP/1.1\r\nHost: x\r\n\r\n")
	case "idle":
		writeString(t, conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	}
}

// wallStallRun stalls one exchange on a gateway whose clock is frozen. Forty seconds of bubble time must not cut it;
// the limit, advanced on the frozen clock, must. The cut is seen where it happens, on the raw conn below the wrapper.
func wallStallRun(t *testing.T, kind string, st wallStall) {
	t.Helper()
	fake := clock.NewFake(serverStart)
	res := make(chan error, 1)
	addr, ln, m := seqGateway(t, kind, seqLimits{h: true, r: true, w: true, i: true}, wallHandler(st.name, res), fake)
	raw, err := m.dial(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	conn := raw
	if kind == "tls" && st.name != "handshake" {
		if conn, err = seqClient(raw, kind, ln.pool, "http/1.1"); err != nil {
			t.Fatal(err)
		}
	}
	var c *capture
	if st.name != "write" {
		c = capturing(raw)
	}
	wallStart(t, conn, st.name)
	synctest.Wait()
	base := len(ln.last().list())
	cut := func() bool {
		if c != nil {
			select {
			case <-c.done:
				return true
			default:
			}
		}
		for _, tok := range ln.last().list()[base:] {
			if strings.HasSuffix(tok, "-") {
				return true
			}
		}
		return len(res) > 0
	}
	time.Sleep(40 * time.Second)
	synctest.Wait()
	if cut() {
		t.Fatal("stall cut after 40s of bubble time, before any limit")
	}
	fake.Advance(st.limit - time.Nanosecond)
	synctest.Wait()
	if cut() {
		t.Fatalf("stall cut before its %v limit", st.limit)
	}
	fake.Advance(time.Nanosecond)
	synctest.Wait()
	if !cut() {
		t.Fatalf("stall not cut at its %v limit", st.limit)
	}
	if c == nil {
		c = capturing(raw)
	}
	if st.name == "body" || st.name == "write" {
		err, _ := bubbleRecv(t, "handler result", res, nil)
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("handler got %v, want os.ErrDeadlineExceeded", err)
		}
	}
	bubbleDone(t, "server closes the connection", c.done, nil)
}

func TestHTTP1IgnoresWallTimeInBubble(t *testing.T) {
	for _, kind := range []string{"plain", "h2c", "tls"} {
		for _, st := range wallStalls() {
			if st.name == "handshake" && kind != "tls" {
				continue
			}
			t.Run(kind+"/"+st.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					wallStallRun(t, kind, st)
				})
			})
		}
	}
}
