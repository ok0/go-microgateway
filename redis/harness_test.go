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
	"net/http"
	"sort"
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
	"github.com/ok0/go-microgateway/store/storetest"
)

var miniStart = time.Unix(1_700_000_000, 0).UTC()

func newMini(t *testing.T) *miniredis.Miniredis {
	t.Helper()
	return newMiniAt(t, miniStart)
}

func newMiniAt(t *testing.T, start time.Time) *miniredis.Miniredis {
	t.Helper()
	m := miniredis.RunT(t)
	m.SetTime(start)
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

// Back steps the server clock back by d. Remaining TTLs are kept, so absolute expiries move back too (D13).
func (c *miniClock) Back(d time.Duration) {
	c.now = c.now.Add(-d)
	c.m.SetTime(c.now)
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
	// The client's limits run on a fake clock of its own that nothing advances, so no limit ends a call to a healthy server.
	s, err := newFromConfig(&config.RedisStore{
		Mode:      "standalone",
		Addresses: []string{addr},
		DB:        db,
		KeyPrefix: &prefix,
	}, nil, clock.NewFake(miniStart))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func newTestClient(t *testing.T, addr string, db int) *goredis.Client {
	t.Helper()
	// The options of a store's client, on a fake clock of its own that nothing advances.
	opts, err := clientOptions(&config.RedisStore{Mode: "standalone", Addresses: []string{addr}, DB: db}, nil, clock.NewFake(miniStart))
	if err != nil {
		t.Fatal(err)
	}
	c := goredis.NewClient(opts.(*goredis.Options))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// luaKeyGuard shadows redis with a table whose call and pcall reject, before the command runs, any key argument that
// is not in KEYS, and any command that cannot be proven single-slot. It assigns no globals.
const luaKeyGuard = `local redis = setmetatable({}, {__index = redis})
local declared = {}
for _, k in ipairs(KEYS) do declared[tostring(k)] = true end
local forbidden = {KEYS = true, SCAN = true, MGET = true, MSET = true, RENAMENX = true, COPY = true, SORT = true, BITOP = true, SMOVE = true, LMOVE = true, RPOPLPUSH = true, PFMERGE = true}
local function requireDeclared(k)
  if not declared[tostring(k)] then error('microgateway-test: undeclared key ' .. tostring(k), 0) end
end
local function guard(...)
  local args = {...}
  local name = string.upper(tostring(args[1]))
  if forbidden[name] or string.sub(name, -5) == 'STORE' then error('microgateway-test: forbidden command ' .. name, 0) end
  if name == 'TIME' then return end
  if name == 'DEL' or name == 'UNLINK' or name == 'EXISTS' or name == 'RENAME' then
    for i = 2, #args do requireDeclared(args[i]) end
  elseif args[2] ~= nil then
    requireDeclared(args[2])
  end
end
local rawcall, rawpcall = redis.call, redis.pcall
redis.call = function(...) guard(...); return rawcall(...) end
redis.pcall = function(...) guard(...); return rawpcall(...) end
`

const luaClockInjected = "local SEC = tonumber(ARGV[#ARGV - 1])\nlocal USEC = tonumber(ARGV[#ARGV])\n"

// cacheStore builds a store whose scripts run the key guard (D12a) on the server clock.
func cacheStore(t *testing.T, addr string, db int, prefix string) *Store {
	t.Helper()
	s := newTestStore(t, addr, db, prefix)
	s.sc = newScriptSet(luaKeyGuard + luaClockServer)
	return s
}

func withInjectedClock(s *Store, now func() time.Time) *Store {
	s.sc = newScriptSet(luaKeyGuard + luaClockInjected)
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
	addr   string
	target string

	mu         sync.Mutex
	ln         net.Listener // nil while the proxy is down
	served     chan struct{}
	conns      map[net.Conn]struct{}
	mode       proxyMode
	commands   []string
	requests   [][]string
	setup      []string
	pager      *pagerConfig
	cursors    map[uint64]string
	nextCursor uint64
	violations []string
}

// pagerConfig makes the proxy answer HSCAN itself (D11). Page > 0 caps the pairs per page at min(COUNT, Page).
// Duplicate re-emits the previous page's last field first.
type pagerConfig struct {
	Page      int
	Duplicate bool
}

// serverError is an error reply from the target, kept as its raw RESP line so it can be relayed unchanged.
type serverError string

func (e serverError) Error() string {
	return string(e)
}

func newRespProxy(t *testing.T, target string) *respProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &respProxy{addr: ln.Addr().String(), target: target, conns: make(map[net.Conn]struct{})}
	p.start(ln)
	t.Cleanup(p.shutdown)
	return p
}

// start serves ln. The caller holds no lock.
func (p *respProxy) start(ln net.Listener) {
	served := make(chan struct{})
	p.mu.Lock()
	p.ln, p.served = ln, served
	p.mu.Unlock()
	go p.serve(ln, served)
}

// Down stops the proxy: it closes the listener, so a later dial is refused, and every client connection. It returns
// once the accept loop has ended, so no connection accepted before Down is left open and none is accepted after it.
func (p *respProxy) Down() {
	p.mu.Lock()
	ln, served := p.ln, p.served
	p.ln = nil
	conns := p.conns
	p.conns = make(map[net.Conn]struct{})
	p.mu.Unlock()
	if ln == nil {
		return
	}
	_ = ln.Close()
	<-served
	p.mu.Lock()
	for c := range p.conns {
		conns[c] = struct{}{}
	}
	p.conns = make(map[net.Conn]struct{})
	p.mu.Unlock()
	for c := range conns {
		_ = c.Close()
	}
}

// Up makes the proxy accept connections again on the address it had before Down.
func (p *respProxy) Up() error {
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		return err
	}
	p.start(ln)
	return nil
}

func (p *respProxy) shutdown() {
	p.Down()
}

func (p *respProxy) Addr() string {
	return p.addr
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

// SetHscanPager makes HSCAN be answered by the proxy from HGETALL of the target (D11). nil forwards HSCAN unchanged.
func (p *respProxy) SetHscanPager(cfg *pagerConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pager = cfg
	p.cursors = make(map[uint64]string)
	p.nextCursor = 0
}

// Violations returns the slot co-location violations seen so far (D12b).
func (p *respProxy) Violations() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.violations...)
}

func (p *respProxy) currentMode() proxyMode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode
}

func (p *respProxy) currentPager() *pagerConfig {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pager
}

func (p *respProxy) record(name string, args []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.commands = append(p.commands, name)
	p.requests = append(p.requests, args)
	if v, bad := slotViolation(name, keysOf(name, args)); bad {
		p.violations = append(p.violations, v)
	}
}

// keysOf returns the key arguments whose slots must agree for a data-plane request.
func keysOf(name string, args []string) []string {
	switch name {
	case "EVAL", "EVALSHA":
		if len(args) < 3 {
			return nil
		}
		n, err := strconv.Atoi(args[2])
		if err != nil || n < 0 || 3+n > len(args) {
			return nil
		}
		return args[3 : 3+n]
	case "DEL", "UNLINK", "EXISTS", "RENAME":
		if len(args) < 2 {
			return nil
		}
		return args[1:]
	}
	return nil
}

// slotViolation reports the first key whose slot differs from the first key's slot.
func slotViolation(name string, keys []string) (string, bool) {
	if len(keys) < 2 {
		return "", false
	}
	first := keySlot(keys[0])
	for _, k := range keys[1:] {
		if s := keySlot(k); s != first {
			return fmt.Sprintf("%s: slots %d %d", name, first, s), true
		}
	}
	return "", false
}

func (p *respProxy) serve(ln net.Listener, served chan struct{}) {
	defer close(served)
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.conns[c] = struct{}{}
		p.mu.Unlock()
		go p.handle(c)
	}
}

func (p *respProxy) handle(client net.Conn) {
	defer func() {
		_ = client.Close()
		p.mu.Lock()
		delete(p.conns, client)
		p.mu.Unlock()
	}()
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }()
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
		var reply []byte
		if cfg := p.currentPager(); cfg != nil && name == "HSCAN" {
			reply, err = p.pagedHscan(cfg, args, up, ur)
			if err != nil {
				return
			}
		} else {
			if _, err := up.Write(raw); err != nil {
				return
			}
			reply, err = readReply(ur)
			if err != nil {
				return
			}
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

// pagedHscan answers one HSCAN from HGETALL of the target on the same connection. Fields are sorted bytewise and each
// page resumes strictly after the last field of the previous page (D11). A server-style error is returned as the
// reply; a non-nil error means the connection is broken.
func (p *respProxy) pagedHscan(cfg *pagerConfig, args []string, up io.Writer, ur *bufio.Reader) ([]byte, error) {
	if len(args) < 3 {
		return errorLine("ERR wrong number of arguments for 'hscan' command"), nil
	}
	cursor, err := strconv.ParseUint(args[2], 10, 64)
	if err != nil {
		return errorLine("ERR invalid cursor"), nil
	}
	count, err := parseHscanOptions(args[3:])
	if err != nil {
		return errorLine(err.Error()), nil
	}
	var last string
	if cursor != 0 {
		p.mu.Lock()
		resume, ok := p.cursors[cursor]
		p.mu.Unlock()
		if !ok {
			return errorLine("ERR pager: unknown cursor"), nil
		}
		last = resume
	}
	if _, err := fmt.Fprintf(up, "*2\r\n$7\r\nHGETALL\r\n$%d\r\n%s\r\n", len(args[1]), args[1]); err != nil {
		return nil, err
	}
	fields, err := readFlatArray(ur)
	if err != nil {
		var se serverError
		if errors.As(err, &se) {
			return []byte(se), nil
		}
		return nil, err
	}
	vals := make(map[string]string, len(fields)/2)
	keys := make([]string, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		keys = append(keys, fields[i])
		vals[fields[i]] = fields[i+1]
	}
	sort.Strings(keys)
	start := 0
	var page []string
	if cursor != 0 {
		start = sort.Search(len(keys), func(i int) bool { return keys[i] > last })
		if cfg.Duplicate {
			if i := sort.SearchStrings(keys, last); i < len(keys) && keys[i] == last {
				page = append(page, last)
			}
		}
	}
	size := count
	if cfg.Page > 0 && cfg.Page < size {
		size = cfg.Page
	}
	end := min(start+size, len(keys))
	page = append(page, keys[start:end]...)
	var next uint64
	if end < len(keys) {
		p.mu.Lock()
		p.nextCursor++
		next = p.nextCursor
		p.cursors[next] = keys[end-1]
		p.mu.Unlock()
	}
	cur := strconv.FormatUint(next, 10)
	var b strings.Builder
	fmt.Fprintf(&b, "*2\r\n$%d\r\n%s\r\n*%d\r\n", len(cur), cur, 2*len(page))
	for _, k := range page {
		v := vals[k]
		fmt.Fprintf(&b, "$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
	}
	return []byte(b.String()), nil
}

func parseHscanOptions(opts []string) (int, error) {
	count := 10
	match := ""
	hasMatch := false
	for i := 0; i < len(opts); i += 2 {
		if i+1 >= len(opts) {
			return 0, errors.New("ERR syntax error")
		}
		switch strings.ToUpper(opts[i]) {
		case "MATCH":
			match, hasMatch = opts[i+1], true
		case "COUNT":
			n, err := strconv.Atoi(opts[i+1])
			if err != nil || n < 1 {
				return 0, errors.New("ERR value is not an integer or out of range")
			}
			count = n
		default:
			return 0, errors.New("ERR syntax error")
		}
	}
	if hasMatch && match != "*" {
		return 0, errors.New("ERR pager: unsupported MATCH")
	}
	return count, nil
}

func errorLine(msg string) []byte {
	return []byte("-" + msg + "\r\n")
}

// readFlatArray reads one RESP2 array of bulk strings. An error reply is returned as a serverError holding its raw line.
func readFlatArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, errors.New("short RESP reply")
	}
	if line[0] == '-' {
		return nil, serverError(line)
	}
	n, err := parseHeader(line, '*')
	if err != nil {
		return nil, err
	}
	if n < 0 {
		return nil, errors.New("nil array reply")
	}
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lenLine, err := r.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		size, err := parseHeader(lenLine, '$')
		if err != nil {
			return nil, err
		}
		if size < 0 {
			return nil, errors.New("nil bulk string in array reply")
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(r, body); err != nil {
			return nil, err
		}
		out = append(out, string(body[:size]))
	}
	return out, nil
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

var cacheHorizon = time.Date(2250, 1, 1, 0, 0, 0, 0, time.UTC)

func miniCacheHarness() storetest.Harness[store.Cache] {
	var mc *miniClock
	newAt := func(t *testing.T, start time.Time) (store.Cache, store.Cache) {
		t.Helper()
		m := newMiniAt(t, start)
		mc = &miniClock{m: m, now: start}
		return cacheStore(t, m.Addr(), 0, "mgtest"), cacheStore(t, m.Addr(), 0, "mgtest")
	}
	return storetest.Harness[store.Cache]{
		New: func(t *testing.T) (store.Cache, store.Cache) {
			t.Helper()
			return newAt(t, miniStart)
		},
		NewAt: newAt,
		Advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			mc.Advance(d)
		},
		Resolution: time.Microsecond,
		Horizon:    cacheHorizon,
	}
}

func injectedCacheHarness(addr func(t *testing.T) (string, int)) storetest.Harness[store.Cache] {
	var fake *clock.Fake
	build := func(t *testing.T, start time.Time) (store.Cache, store.Cache) {
		t.Helper()
		a, db := addr(t)
		prefix := uniquePrefix(t)
		raw := newTestClient(t, a, db)
		fake = clock.NewFake(start)
		sa := withInjectedClock(cacheStore(t, a, db, prefix), fake.Now)
		sb := withInjectedClock(cacheStore(t, a, db, prefix), fake.Now)
		t.Cleanup(func() { deletePrefix(t, raw, prefix) })
		return sa, sb
	}
	return storetest.Harness[store.Cache]{
		New: func(t *testing.T) (store.Cache, store.Cache) {
			t.Helper()
			return build(t, injectedStart())
		},
		NewAt: func(t *testing.T, start time.Time) (store.Cache, store.Cache) {
			t.Helper()
			if start.Before(injectedStart()) {
				t.Fatalf("injected cache clock starts at %v, before %v", start, injectedStart())
			}
			return build(t, start)
		},
		Advance: func(t *testing.T, d time.Duration) {
			t.Helper()
			fake.Advance(d)
		},
		Resolution: time.Microsecond,
		Horizon:    cacheHorizon,
	}
}

func ckEntry(body string, tags ...string) *store.Entry {
	return &store.Entry{
		Status: 200,
		Header: http.Header{"Content-Type": {"text/plain"}, "X-Body": {body}},
		Body:   []byte(body),
		Tags:   tags,
	}
}

func ckGet(t *testing.T, s store.Cache, primary, sec string) store.Lookup {
	t.Helper()
	l, err := s.Get(context.Background(), primary, func([]string) (string, bool) { return sec, true })
	if err != nil {
		t.Fatalf("Get(%q): %v", primary, err)
	}
	return l
}

func ckFill(t *testing.T, s store.Cache, primary, sec string, names []string, e *store.Entry, ttl time.Duration) bool {
	t.Helper()
	l := ckGet(t, s, primary, sec)
	stored, err := s.Put(context.Background(), l.Token, primary, sec, names, e, ttl)
	if err != nil {
		t.Fatalf("Put(%q, %q): %v", primary, sec, err)
	}
	return stored
}

// ckKeys returns the sorted keys under prefix+":*".
func ckKeys(t *testing.T, c goredis.UniversalClient, prefix string) []string {
	t.Helper()
	ctx := context.Background()
	seen := make(map[string]bool)
	var cursor uint64
	for {
		keys, next, err := c.Scan(ctx, cursor, prefix+":*", 1000).Result()
		if err != nil {
			t.Fatalf("scanning keys: %v", err)
		}
		for _, k := range keys {
			seen[k] = true
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// ckExpiryMs returns the absolute expiry of key in unix milliseconds (PEXPIRETIME), or -1 with no expiry.
// PEXPIRETIME needs Redis 7.0: use it on miniredis; real-server callers use serverExpiryMs.
func ckExpiryMs(t *testing.T, c goredis.UniversalClient, key string) int64 {
	t.Helper()
	ms, err := c.Do(context.Background(), "PEXPIRETIME", key).Int64()
	if err != nil {
		t.Fatalf("PEXPIRETIME %s: %v", key, err)
	}
	return ms
}

func expiryMsIfLive(t *testing.T, c goredis.UniversalClient, key string) (int64, time.Duration, bool) {
	t.Helper()
	ctx := context.Background()
	for attempt := 0; attempt < 100; attempt++ {
		pipe := c.Pipeline()
		t0 := pipe.Time(ctx)
		pttl := pipe.PTTL(ctx, key)
		t1 := pipe.Time(ctx)
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("PTTL %q: %v", key, err)
		}
		if t0.Val().UnixMilli() == t1.Val().UnixMilli() {
			if pttl.Val() < 0 {
				return 0, pttl.Val(), false
			}
			return t0.Val().UnixMilli() + pttl.Val().Milliseconds(), pttl.Val(), true
		}
	}
	t.Fatal("no PTTL bracketed by TIME readings within one millisecond in 100 attempts")
	return 0, 0, false
}

func realExpiryMs(t *testing.T, c goredis.UniversalClient, key string) int64 {
	t.Helper()
	e, pttl, live := expiryMsIfLive(t, c, key)
	if !live {
		t.Fatalf("PTTL %q = %s, want a live expiry", key, pttl)
	}
	return e
}

// serverExpiryMs returns the absolute expiry of key in unix milliseconds on a Redis server, or -1 with no expiry,
// without PEXPIRETIME: it reads PTTL between two TIME readings in the same millisecond, which is exact on Redis 6.2
// and later. It fails the test when key does not exist.
func serverExpiryMs(t *testing.T, c goredis.UniversalClient, key string) int64 {
	t.Helper()
	e, pttl, live := expiryMsIfLive(t, c, key)
	switch {
	case live:
		return e
	case pttl == -1:
		return -1
	}
	t.Fatalf("PTTL %q = %d, want an existing key", key, pttl)
	return 0
}

// ckProxied builds a store on m through a respProxy. Slot co-location violations recorded by the proxy fail the test
// at cleanup (D12b).
func ckProxied(t *testing.T, m *miniredis.Miniredis, prefix string) (*Store, *respProxy) {
	t.Helper()
	p := newRespProxy(t, m.Addr())
	s := cacheStore(t, p.Addr(), 0, prefix)
	t.Cleanup(func() {
		if v := p.Violations(); len(v) > 0 {
			t.Errorf("slot co-location violations: %q", v)
		}
	})
	return s, p
}
