package redis

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/config"
	"github.com/ok0/go-microgateway/internal/clock"
	"github.com/ok0/go-microgateway/internal/memstore"
	"github.com/ok0/go-microgateway/store"
)

var miniStart = time.Unix(1_700_000_000, 0).UTC()

func newMini(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	m := miniredis.RunT(t)
	m.SetTime(miniStart)
	return m
}

type miniClock struct {
	m   *miniredis.Miniredis
	now time.Time
}

func (c *miniClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
	c.m.SetTime(c.now)
	c.m.FastForward(d)
}

func (c *miniClock) Set(at time.Time) {
	d := at.Sub(c.now)
	if d < 0 {
		panic("miniClock: time moves forward only")
	}
	c.Advance(d)
}

func newTestStore(t *testing.T, addr string, db int, prefix string) *Store {
	t.Helper()
	s, err := NewFromConfig(&config.RedisStore{
		Mode:      "standalone",
		Addresses: []string{addr},
		DB:        db,
		KeyPrefix: &prefix,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newTestClient(t *testing.T, addr string, db int) *goredis.Client {
	t.Helper()
	c := goredis.NewClient(&goredis.Options{
		Addr:            addr,
		DB:              db,
		Protocol:        2,
		DisableIdentity: true,
		MaxRetries:      -1,
	})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

const luaClockInjected = "local SEC = tonumber(ARGV[#ARGV - 1])\nlocal USEC = tonumber(ARGV[#ARGV])\n"

func withInjectedClock(s *Store, now func() time.Time) *Store {
	s.sc = newScriptSet(luaClockInjected)
	s.clockArgs = func() []any {
		n := now()
		return []any{strconv.FormatInt(n.Unix(), 10), strconv.FormatInt(int64(n.Nanosecond()/1000), 10)}
	}
	return s
}

func injectedStart() time.Time {
	return time.Now().Add(72 * time.Hour).Truncate(time.Second)
}

func uniquePrefix(t *testing.T) string {
	t.Helper()
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "mgtest-" + hex.EncodeToString(b[:])
}

func deletePrefix(t *testing.T, c goredis.UniversalClient, prefix string) {
	t.Helper()
	ctx := context.Background()
	var cursor uint64
	for {
		keys, next, err := c.Scan(ctx, cursor, prefix+":*", 1000).Result()
		if err != nil {
			t.Errorf("scanning keys for cleanup: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := c.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("deleting keys for cleanup: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}

type timedStore struct {
	rl      store.RateLimiter
	advance func(time.Duration)
}

type step struct {
	advance time.Duration
	key     string
	limit   store.Limit
}

func compareSteps(t *testing.T, name string, steps []step, got, ref timedStore) {
	t.Helper()
	ctx := context.Background()
	var offset time.Duration
	for i, st := range steps {
		got.advance(st.advance)
		ref.advance(st.advance)
		offset += st.advance
		gd, err := got.rl.Take(ctx, st.key, st.limit)
		if err != nil {
			t.Fatalf("%s: step %d (offset %s): backend take: %v", name, i, offset, err)
		}
		rd, err := ref.rl.Take(ctx, st.key, st.limit)
		if err != nil {
			t.Fatalf("%s: step %d (offset %s): reference take: %v", name, i, offset, err)
		}
		if gd != rd {
			t.Fatalf("%s: step %d (offset %s): limit %+v: backend %+v, reference %+v", name, i, offset, st.limit, gd, rd)
		}
	}
}

func memBackend(t *testing.T, start time.Time) timedStore {
	t.Helper()
	fake := clock.NewFake(start)
	rl := memstore.NewRateLimiter(memstore.RateLimiterOptions{MaxKeys: 1 << 20, Clock: fake})
	t.Cleanup(func() { _ = rl.Close() })
	return timedStore{rl: rl, advance: fake.Advance}
}

func miniBackend(t *testing.T) (timedStore, time.Time) {
	t.Helper()
	m := newMini(t)
	s := newTestStore(t, m.Addr(), 0, "mgtest")
	mc := &miniClock{m: m, now: miniStart}
	return timedStore{rl: s, advance: mc.Advance}, miniStart
}

func injectedBackend(t *testing.T, addr string, db int) (timedStore, time.Time) {
	t.Helper()
	prefix := uniquePrefix(t)
	fake := clock.NewFake(injectedStart())
	s := withInjectedClock(newTestStore(t, addr, db, prefix), fake.Now)
	t.Cleanup(func() { deletePrefix(t, s.c, prefix) })
	return timedStore{rl: s, advance: fake.Advance}, fake.Now()
}

type proxyMode int

const (
	proxyForward proxyMode = iota
	proxyDropReply
)

// setupCommands are sent by go-redis while initializing a connection.
var setupCommands = map[string]bool{"HELLO": true, "AUTH": true, "SELECT": true, "CLIENT": true, "READONLY": true}

// respProxy relays RESP2 traffic to a target address. Setup commands are
// relayed transparently in every mode and recorded separately. In
// proxyDropReply mode each data-plane request is sent to the target, its reply
// is discarded and the client connection is closed.
type respProxy struct {
	ln       net.Listener
	target   string
	mu       sync.Mutex
	mode     proxyMode
	commands []string
	requests [][]string
	setup    []string
}

func newRespProxy(t *testing.T, target string) *respProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	p := &respProxy{ln: ln, target: target}
	go p.serve()
	return p
}

func (p *respProxy) Addr() string {
	return p.ln.Addr().String()
}

func (p *respProxy) SetMode(m proxyMode) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mode = m
}

func (p *respProxy) Commands() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.commands...)
}

func (p *respProxy) Requests() [][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([][]string, len(p.requests))
	for i, args := range p.requests {
		out[i] = append([]string(nil), args...)
	}
	return out
}

func (p *respProxy) Setup() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.setup...)
}

func (p *respProxy) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commands = nil
	p.requests = nil
	p.setup = nil
}

func (p *respProxy) currentMode() proxyMode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode
}

func (p *respProxy) record(name string, args []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commands = append(p.commands, name)
	p.requests = append(p.requests, args)
}

func (p *respProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *respProxy) handle(client net.Conn) {
	defer client.Close()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer up.Close()
	cr := bufio.NewReader(client)
	ur := bufio.NewReader(up)
	for {
		raw, args, err := readRequest(cr)
		if err != nil {
			return
		}
		name := strings.ToUpper(args[0])
		if setupCommands[name] {
			p.mu.Lock()
			p.setup = append(p.setup, name)
			p.mu.Unlock()
			if _, err := up.Write(raw); err != nil {
				return
			}
			reply, err := readReply(ur)
			if err != nil {
				return
			}
			if _, err := client.Write(reply); err != nil {
				return
			}
			continue
		}
		p.record(name, args)
		mode := p.currentMode()
		if _, err := up.Write(raw); err != nil {
			return
		}
		reply, err := readReply(ur)
		if err != nil {
			return
		}
		if mode == proxyDropReply {
			return
		}
		if _, err := client.Write(reply); err != nil {
			return
		}
	}
}

// readRequest reads one RESP2 array of bulk strings and returns its raw bytes
// and its arguments.
func readRequest(r *bufio.Reader) ([]byte, []string, error) {
	header, err := r.ReadBytes('\n')
	if err != nil {
		return nil, nil, err
	}
	n, err := parseHeader(header, '*')
	if err != nil {
		return nil, nil, err
	}
	if n < 1 {
		return nil, nil, errors.New("empty request")
	}
	raw := append([]byte(nil), header...)
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lenLine, err := r.ReadBytes('\n')
		if err != nil {
			return nil, nil, err
		}
		size, err := parseHeader(lenLine, '$')
		if err != nil {
			return nil, nil, err
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, nil, err
		}
		raw = append(raw, lenLine...)
		raw = append(raw, body...)
		args = append(args, string(body[:size]))
	}
	return raw, args, nil
}

func parseHeader(line []byte, prefix byte) (int, error) {
	if len(line) < 3 || line[0] != prefix {
		return 0, fmt.Errorf("unexpected RESP header %q", line)
	}
	return strconv.Atoi(string(line[1 : len(line)-2]))
}

// readReply reads one complete RESP2 reply and returns its raw bytes.
func readReply(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, errors.New("short RESP reply")
	}
	out := line
	switch line[0] {
	case '+', '-', ':':
		return out, nil
	case '$':
		n, err := parseHeader(line, '$')
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return out, nil
		}
		body := make([]byte, n+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		return append(out, body...), nil
	case '*':
		n, err := parseHeader(line, '*')
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return out, nil
		}
		for i := 0; i < n; i++ {
			sub, err := readReply(r)
			if err != nil {
				return nil, err
			}
			out = append(out, sub...)
		}
		return out, nil
	}
	return nil, fmt.Errorf("unsupported RESP reply type %q", line[0])
}

// blackhole listens on a loopback address, signals each accepted connection
// and reads and discards everything it receives.
func blackhole(t *testing.T) (string, <-chan struct{}) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	accepted := make(chan struct{}, 64)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- struct{}{}:
			default:
			}
			go func() {
				_, _ = io.Copy(io.Discard, c)
				_ = c.Close()
			}()
		}
	}()
	return ln.Addr().String(), accepted
}

// crc16 computes the CRC-16/XMODEM checksum used by Redis Cluster.
func crc16(b []byte) uint16 {
	var crc uint16
	for _, x := range b {
		crc ^= uint16(x) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// keySlot returns the Redis Cluster hash slot of key, honoring hash tags.
func keySlot(key string) int {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		if j := strings.IndexByte(key[i+1:], '}'); j > 0 {
			key = key[i+1 : i+1+j]
		}
	}
	return int(crc16([]byte(key))) % 16384
}
