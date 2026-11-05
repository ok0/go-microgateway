package memnet

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

type ioResult struct {
	n   int
	err error
}

type dialResult struct {
	c   net.Conn
	err error
}

func assertDeadline(t *testing.T, err error) {
	t.Helper()
	var oe *net.OpError
	if !errors.As(err, &oe) || !oe.Timeout() || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("error = %v, want a timeout *net.OpError wrapping os.ErrDeadlineExceeded", err)
	}
}

func TestPipeBufferedWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := Pipe(16)
		defer client.Close()
		defer server.Close()
		n, err := client.Write([]byte("0123456789"))
		if n != 10 || err != nil {
			t.Fatalf("Write = %d, %v; want 10, nil", n, err)
		}
		buf := make([]byte, 10)
		if _, err := io.ReadFull(server, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != "0123456789" {
			t.Fatalf("read %q", buf)
		}
	})
}

func TestPipeUnbufferedWriteWaitsForRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := Pipe(0)
		defer client.Close()
		defer server.Close()
		res := make(chan ioResult, 1)
		go func() {
			n, err := client.Write([]byte("hello"))
			res <- ioResult{n, err}
		}()
		synctest.Wait()
		select {
		case <-res:
			t.Fatal("write returned before the peer read")
		default:
		}
		buf := make([]byte, 3)
		if _, err := io.ReadFull(server, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != "hel" {
			t.Fatalf("read %q, want %q", buf, "hel")
		}
		synctest.Wait()
		select {
		case <-res:
			t.Fatal("write returned with bytes still unread")
		default:
		}
		rest := make([]byte, 2)
		if _, err := io.ReadFull(server, rest); err != nil {
			t.Fatal(err)
		}
		if string(rest) != "lo" {
			t.Fatalf("read %q, want %q", rest, "lo")
		}
		r := <-res
		if r.n != 5 || r.err != nil {
			t.Fatalf("Write = %d, %v; want 5, nil", r.n, r.err)
		}
	})
}

func TestPipeWriteBeyondBufferBlocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := Pipe(4)
		defer client.Close()
		defer server.Close()
		res := make(chan ioResult, 1)
		go func() {
			n, err := client.Write([]byte("0123456789"))
			res <- ioResult{n, err}
		}()
		synctest.Wait()
		select {
		case <-res:
			t.Fatal("write returned beyond the buffer without a read")
		default:
		}
		for _, size := range []int{4, 4, 2} {
			buf := make([]byte, size)
			if _, err := io.ReadFull(server, buf); err != nil {
				t.Fatal(err)
			}
		}
		r := <-res
		if r.n != 10 || r.err != nil {
			t.Fatalf("Write = %d, %v; want 10, nil", r.n, r.err)
		}
	})
}

func TestPipeLoweredReadBufferStallsPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := Pipe(64)
		defer client.Close()
		defer server.Close()
		if err := client.SetReadBuffer(4); err != nil {
			t.Fatal(err)
		}
		res := make(chan ioResult, 1)
		go func() {
			n, err := server.Write([]byte("0123456789"))
			res <- ioResult{n, err}
		}()
		synctest.Wait()
		select {
		case <-res:
			t.Fatal("write returned past the lowered read buffer")
		default:
		}
		buf := make([]byte, 10)
		if _, err := io.ReadFull(client, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != "0123456789" {
			t.Fatalf("read %q", buf)
		}
		r := <-res
		if r.n != 10 || r.err != nil {
			t.Fatalf("Write = %d, %v; want 10, nil", r.n, r.err)
		}
	})
}

func TestPipeDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := Pipe(0)
		defer client.Close()
		defer server.Close()

		start := time.Now()
		if err := server.SetReadDeadline(start.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		readRes := make(chan ioResult, 1)
		go func() {
			n, err := server.Read(make([]byte, 1))
			readRes <- ioResult{n, err}
		}()
		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()
		select {
		case <-readRes:
			t.Fatal("read returned before its deadline")
		default:
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		var r ioResult
		select {
		case r = <-readRes:
		default:
			t.Fatal("read still pending at its deadline")
		}
		assertDeadline(t, r.err)
		if got := time.Since(start); got != time.Second {
			t.Fatalf("read returned after %v, want exactly 1s", got)
		}

		if err := server.SetReadDeadline(time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err := server.Read(make([]byte, 1)); err == nil {
			t.Fatal("read at a past deadline succeeded")
		} else {
			assertDeadline(t, err)
		}

		if err := server.SetReadDeadline(time.Time{}); err != nil {
			t.Fatal(err)
		}
		readRes = make(chan ioResult, 1)
		go func() {
			n, err := server.Read(make([]byte, 1))
			readRes <- ioResult{n, err}
		}()
		synctest.Wait()
		select {
		case <-readRes:
			t.Fatal("read returned after the deadline was cleared, without data")
		default:
		}
		if _, err := client.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case r = <-readRes:
		default:
			t.Fatal("read still pending after the peer wrote")
		}
		if r.n != 1 || r.err != nil {
			t.Fatalf("Read = %d, %v; want 1, nil", r.n, r.err)
		}

		wstart := time.Now()
		if err := client.SetWriteDeadline(wstart.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		writeRes := make(chan ioResult, 1)
		go func() {
			n, err := client.Write([]byte("abc"))
			writeRes <- ioResult{n, err}
		}()
		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()
		select {
		case <-writeRes:
			t.Fatal("write returned before its deadline")
		default:
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		var w ioResult
		select {
		case w = <-writeRes:
		default:
			t.Fatal("write still pending at its deadline")
		}
		assertDeadline(t, w.err)
		if got := time.Since(wstart); got != time.Second {
			t.Fatalf("write returned after %v, want exactly 1s", got)
		}

		if err := client.SetWriteDeadline(time.Time{}); err != nil {
			t.Fatal(err)
		}
		writeRes = make(chan ioResult, 1)
		go func() {
			n, err := client.Write([]byte("abc"))
			writeRes <- ioResult{n, err}
		}()
		synctest.Wait()
		select {
		case <-writeRes:
			t.Fatal("write returned after the deadline was cleared, without a read")
		default:
		}
		buf := make([]byte, 3)
		if _, err := io.ReadFull(server, buf); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		select {
		case w = <-writeRes:
		default:
			t.Fatal("write still pending after the peer read")
		}
		if w.n != 3 || w.err != nil {
			t.Fatalf("Write = %d, %v; want 3, nil", w.n, w.err)
		}

		pc, ps := Pipe(0)
		defer pc.Close()
		defer ps.Close()
		if err := pc.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		partialRes := make(chan ioResult, 1)
		go func() {
			n, err := pc.Write([]byte("abcde"))
			partialRes <- ioResult{n, err}
		}()
		got3 := make([]byte, 3)
		if _, err := io.ReadFull(ps, got3); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		var p ioResult
		select {
		case p = <-partialRes:
		default:
			t.Fatal("partial write still pending at its deadline")
		}
		if p.n != 3 {
			t.Fatalf("partial Write n = %d, want 3", p.n)
		}
		assertDeadline(t, p.err)
		if err := ps.SetReadDeadline(time.Now()); err != nil {
			t.Fatal(err)
		}
		n, err := ps.Read(make([]byte, 2))
		if n != 0 {
			t.Fatalf("Read after the withdrawn bytes = %d, want 0", n)
		}
		assertDeadline(t, err)
	})
}

func TestPipeClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a, b := Pipe(16)
		if _, err := a.Write([]byte("abc")); err != nil {
			t.Fatal(err)
		}
		if err := a.Close(); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 3)
		if _, err := io.ReadFull(b, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != "abc" {
			t.Fatalf("read %q, want %q", buf, "abc")
		}
		if _, err := b.Read(buf); err != io.EOF {
			t.Fatalf("Read after the peer closed = %v, want io.EOF", err)
		}
		if _, err := b.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Write to a closed peer = %v, want net.ErrClosed", err)
		}
		if _, err := a.Read(buf); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Read on a closed end = %v, want net.ErrClosed", err)
		}
		if _, err := a.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Write on a closed end = %v, want net.ErrClosed", err)
		}
		b.Close()

		pc, ps := Pipe(0)
		partialRes := make(chan ioResult, 1)
		go func() {
			n, err := pc.Write([]byte("abcde"))
			partialRes <- ioResult{n, err}
		}()
		two := make([]byte, 2)
		if _, err := io.ReadFull(ps, two); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := ps.Close(); err != nil {
			t.Fatal(err)
		}
		p := <-partialRes
		if p.n != 2 || !errors.Is(p.err, net.ErrClosed) {
			t.Fatalf("partial Write = %d, %v; want 2, net.ErrClosed", p.n, p.err)
		}
		pc.Close()

		uc, us := Pipe(0)
		unreadRes := make(chan ioResult, 1)
		go func() {
			n, err := uc.Write([]byte("abcde"))
			unreadRes <- ioResult{n, err}
		}()
		synctest.Wait()
		if err := us.Close(); err != nil {
			t.Fatal(err)
		}
		u := <-unreadRes
		if u.n != 0 || !errors.Is(u.err, net.ErrClosed) {
			t.Fatalf("unread Write = %d, %v; want 0, net.ErrClosed", u.n, u.err)
		}
		uc.Close()
	})
}

func TestPipeWritePendingWhenReaderCloses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := Pipe(0)
		defer client.Close()
		res := make(chan ioResult, 1)
		go func() {
			n, err := client.Write([]byte("abc"))
			res <- ioResult{n, err}
		}()
		synctest.Wait()
		if err := server.Close(); err != nil {
			t.Fatal(err)
		}
		r := <-res
		if !errors.Is(r.err, net.ErrClosed) {
			t.Fatalf("pending Write = %d, %v; want net.ErrClosed", r.n, r.err)
		}
	})
}

func TestListenerAcceptAndDial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ln := Listen("127.0.0.1:1", 64)
		defer ln.Close()
		res := make(chan dialResult, 1)
		go func() {
			c, err := ln.DialContext(context.Background(), "tcp", "ignored:0")
			res <- dialResult{c, err}
		}()
		srv, err := ln.Accept()
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		d := <-res
		if d.err != nil {
			t.Fatal(d.err)
		}
		client := d.c
		defer client.Close()

		if _, err := client.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		buf := make([]byte, 4)
		if _, err := io.ReadFull(srv, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != "ping" {
			t.Fatalf("server read %q", buf)
		}
		if _, err := srv.Write([]byte("pong")); err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadFull(client, buf); err != nil {
			t.Fatal(err)
		}
		if string(buf) != "pong" {
			t.Fatalf("client read %q", buf)
		}
		if got := ln.Addr().String(); got != "127.0.0.1:1" {
			t.Fatalf("Addr = %q", got)
		}
		if got := srv.LocalAddr().String(); got != "127.0.0.1:1" {
			t.Fatalf("server LocalAddr = %q", got)
		}
	})
}

func TestListenerClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ln1 := Listen("127.0.0.1:2", 64)
		accRes := make(chan error, 1)
		go func() {
			_, err := ln1.Accept()
			accRes <- err
		}()
		synctest.Wait()
		if err := ln1.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-accRes; !errors.Is(err, net.ErrClosed) {
			t.Fatalf("pending Accept = %v, want net.ErrClosed", err)
		}

		ln2 := Listen("127.0.0.1:3", 64)
		dialRes := make(chan dialResult, 1)
		go func() {
			c, err := ln2.DialContext(context.Background(), "memnet", "x")
			dialRes <- dialResult{c, err}
		}()
		synctest.Wait()
		if err := ln2.Close(); err != nil {
			t.Fatal(err)
		}
		if d := <-dialRes; !errors.Is(d.err, net.ErrClosed) || d.c != nil {
			t.Fatalf("pending DialContext = %v, %v; want nil, net.ErrClosed", d.c, d.err)
		}

		if _, err := ln2.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept after Close = %v, want net.ErrClosed", err)
		}
		if _, err := ln2.DialContext(context.Background(), "memnet", "x"); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("DialContext after Close = %v, want net.ErrClosed", err)
		}
	})
}

func TestDialContextCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ln := Listen("127.0.0.1:4", 64)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if c, err := ln.DialContext(ctx, "memnet", "x"); !errors.Is(err, context.Canceled) || c != nil {
			t.Fatalf("DialContext with a canceled context = %v, %v; want nil, context.Canceled", c, err)
		}

		ctx2, cancel2 := context.WithCancel(context.Background())
		res := make(chan dialResult, 1)
		go func() {
			c, err := ln.DialContext(ctx2, "memnet", "x")
			res <- dialResult{c, err}
		}()
		synctest.Wait()
		cancel2()
		if d := <-res; !errors.Is(d.err, context.Canceled) || d.c != nil {
			t.Fatalf("canceled DialContext = %v, %v; want nil, context.Canceled", d.c, d.err)
		}

		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept after the canceled dial = %v, want net.ErrClosed", err)
		}
	})
}

func TestConcurrentUse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := Pipe(7)
		defer client.Close()
		defer server.Close()
		errs := make(chan error, 2)
		for range 2 {
			go func() {
				chunk := []byte("0123456789abc")
				for range 1000 {
					if _, err := client.Write(chunk); err != nil {
						errs <- err
						return
					}
				}
				errs <- nil
			}()
		}
		buf := make([]byte, 26000)
		if _, err := io.ReadFull(server, buf); err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
	})
}
