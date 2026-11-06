package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/store"
)

// DefaultKeyPrefix is the key prefix used when none is configured.
const DefaultKeyPrefix = "microgateway"

// Options configures a Store built with New.
type Options struct {
	// KeyPrefix namespaces every key the store writes. An empty value selects
	// DefaultKeyPrefix. The value must not contain '{' or '}'.
	KeyPrefix string
	// Logger receives the store's diagnostics. A nil Logger discards them.
	Logger *slog.Logger
}

// Store is a Redis backed store.RateLimiter and store.Cache. It is safe for concurrent use.
// A Store holds no secret: credentials stay inside the client.
type Store struct {
	c         goredis.UniversalClient
	owned     bool
	prefix    string
	logger    *slog.Logger
	sc        scriptSet
	clockArgs func() []any
	allowUs   int64
	hook      func(hookPoint)
	closed    atomic.Bool
}

var _ store.RateLimiter = (*Store)(nil)
var _ store.Cache = (*Store)(nil)

// clusterSkewUs is the allowance, in microseconds, for clock skew between the nodes of a cluster. It is added to the
// lifetimes of keys whose slot may sit on another node's clock.
const clusterSkewUs = 1_000_000

// New returns a Store that uses c. New panics if c is nil, if the key prefix
// contains '{' or '}', or if the prefix is empty after defaulting. New never
// closes c; the caller keeps ownership of the client.
func New(c goredis.UniversalClient, o Options) *Store {
	if c == nil {
		panic("redis: New requires a client")
	}
	prefix := o.KeyPrefix
	if prefix == "" {
		prefix = DefaultKeyPrefix
	}
	if strings.ContainsAny(prefix, "{}") {
		panic("redis: key prefix must not contain { or }")
	}
	logger := o.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return newStore(c, false, prefix, logger)
}

func newStore(c goredis.UniversalClient, owned bool, prefix string, logger *slog.Logger) *Store {
	s := &Store{
		c:      c,
		owned:  owned,
		prefix: prefix,
		logger: logger,
		sc:     serverScripts,
	}
	if _, ok := c.(*goredis.ClusterClient); ok {
		s.allowUs = clusterSkewUs
	}
	return s
}

func (s *Store) keyFor(key string, a store.Algorithm) string {
	k := s.prefix + ":rl:{" + key + "}"
	if a == store.SlidingWindow {
		k += ":sw"
	}
	return k
}

// Take consumes one request for key under l if it is admitted and reports the
// decision. Take follows the order defined by the store package: it returns a
// context error unwrapped when ctx is already done, validates l, fails with an
// error wrapping store.ErrUnavailable when the store is closed or the script
// call fails, and otherwise decides and persists atomically. Take returns as
// soon as ctx ends; the script call it abandons goes on until a connection
// limit of the store ends it.
func (s *Store) Take(ctx context.Context, key string, l store.Limit) (store.Decision, error) {
	if err := ctx.Err(); err != nil {
		return store.Decision{}, err
	}
	if err := l.Validate(); err != nil {
		return store.Decision{}, err
	}
	if s.closed.Load() {
		return store.Decision{}, fmt.Errorf("redis: store is closed: %w", store.ErrUnavailable)
	}
	var (
		script *goredis.Script
		args   []any
		limit  int64
	)
	switch l.Algorithm {
	case store.TokenBucket:
		t := int64(l.Period) / l.Count
		script = s.sc.tb
		args = []any{strconv.FormatInt(t, 10), strconv.FormatInt(t*l.Burst, 10)}
		limit = l.Burst
	default:
		script = s.sc.sw
		args = []any{strconv.FormatInt(int64(l.Period), 10), strconv.FormatInt(l.Count, 10)}
		limit = l.Count
	}
	if s.clockArgs != nil {
		args = append(args, s.clockArgs()...)
	}
	res, err := s.run(ctx, script, []string{s.keyFor(key, l.Algorithm)}, args)
	if err != nil {
		return store.Decision{}, s.fail(ctx, "take", err)
	}
	d, ok := decode(res, limit)
	if !ok {
		return store.Decision{}, fmt.Errorf("redis: take: unexpected script reply: %w", store.ErrUnavailable)
	}
	return d, nil
}

// run runs script on keys with args and returns its reply.
func (s *Store) run(ctx context.Context, script *goredis.Script, keys []string, args []any) (any, error) {
	var res any
	err := s.await(ctx, func(ctx context.Context) error {
		var err error
		res, err = script.Run(ctx, s.c, keys, args...).Result()
		return err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// await runs f under ctx. go-redis does not watch a context during socket I/O, so when ctx can end, await waits for
// f or for the end of ctx, whichever comes first. A call it abandons holds its connection until the read or write
// limit of the store ends it. When ctx cannot end, await runs f on the calling goroutine.
func (s *Store) await(ctx context.Context, f func(context.Context) error) error {
	if ctx.Done() == nil {
		return f(ctx)
	}
	ch := make(chan error, 1)
	go func() {
		ch <- f(ctx)
	}()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// decode converts the reply of a rate-limit script into a Decision. It reports
// false when the reply does not have the expected shape.
func decode(res any, limit int64) (store.Decision, bool) {
	vals, ok := res.([]any)
	if !ok || len(vals) != 4 {
		return store.Decision{}, false
	}
	var n [4]int64
	for i, v := range vals {
		x, ok := v.(int64)
		if !ok {
			return store.Decision{}, false
		}
		n[i] = x
	}
	allowed, remaining, retry, reset := n[0], n[1], n[2], n[3]
	if allowed != 0 && allowed != 1 {
		return store.Decision{}, false
	}
	if remaining < 0 || retry < 0 || reset < 0 {
		return store.Decision{}, false
	}
	if allowed == 1 && retry != 0 {
		return store.Decision{}, false
	}
	return store.Decision{
		Allowed:    allowed == 1,
		Limit:      limit,
		Remaining:  remaining,
		RetryAfter: time.Duration(retry),
		ResetAfter: time.Duration(reset),
	}, true
}

// fail maps an error from the script call of operation op to the error returned
// to the caller. A context that ended for any reason other than its deadline is
// reported as its own error. Deadline expiry and every other timeout wrap
// store.ErrUnavailable with the category "timeout". Every other failure wraps
// store.ErrUnavailable and carries only a category produced by describe.
func (s *Store) fail(ctx context.Context, op string, err error) error {
	cerr := ctx.Err()
	if cerr != nil && !errors.Is(cerr, context.DeadlineExceeded) {
		return cerr
	}
	if cerr != nil {
		return fmt.Errorf("redis: %s: timeout: %w", op, store.ErrUnavailable)
	}
	return fmt.Errorf("redis: %s: %s: %w", op, describe(err), store.ErrUnavailable)
}

// describe returns a fixed category for err. The category never includes the
// text of err, so no server reply, address or credential reaches the caller.
func describe(err error) string {
	var (
		rerr goredis.Error
		cve  *tls.CertificateVerificationError
		rhe  tls.RecordHeaderError
		nerr net.Error
		oerr *net.OpError
	)
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, goredis.ErrClosed):
		return "client closed"
	case errors.As(err, &rerr):
		return "server error " + serverErrorCode(rerr.Error())
	case errors.As(err, &cve), errors.As(err, &rhe):
		return "tls handshake failed"
	case errors.As(err, &nerr) && nerr.Timeout():
		return "timeout"
	case errors.As(err, &oerr):
		switch oerr.Op {
		case "dial", "read", "write":
			return "connection failed during " + oerr.Op
		}
		return "connection failed during network operation"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "connection closed"
	}
	return "request failed"
}

// errorCode returns the first token of a server error message when it is one
// to 32 upper-case ASCII letters, and "ERR" otherwise.
func errorCode(msg string) string {
	code, _, _ := strings.Cut(msg, " ")
	if len(code) < 1 || len(code) > 32 {
		return "ERR"
	}
	for i := 0; i < len(code); i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return "ERR"
		}
	}
	return code
}

// serverErrorCode returns the Redis error code of msg. A script error wrapper
// (Redis 6.2 and miniredis) is unwrapped to the code of the innermost error. The
// result is always one to 32 upper-case ASCII letters and never other message
// text.
func serverErrorCode(msg string) string {
	code := errorCode(msg)
	if code != "ERR" || (!strings.HasPrefix(msg, "ERR Error running script") && !strings.HasPrefix(msg, "ERR Error compiling script")) {
		return code
	}
	rest := msg
	for {
		_, after, ok := strings.Cut(rest, ": ")
		if !ok {
			return "ERR"
		}
		if c := errorCode(after); c != "ERR" {
			return c
		}
		rest = after
	}
}

// Close releases the store. It is idempotent. Close closes the client only
// when the Store created it; a client supplied to New stays open.
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	if s.owned {
		if err := s.c.Close(); err != nil && !errors.Is(err, goredis.ErrClosed) {
			return fmt.Errorf("redis: closing client: %s", describe(err))
		}
	}
	return nil
}

// String returns a description of s that shows only the key prefix.
func (s *Store) String() string {
	return fmt.Sprintf("redis.Store{KeyPrefix:%q}", s.prefix)
}

// GoString returns the same text as String.
func (s *Store) GoString() string {
	return s.String()
}

// Format writes the same text as String for every verb.
func (s *Store) Format(f fmt.State, _ rune) {
	_, _ = io.WriteString(f, s.String())
}

// LogValue returns the key prefix as a log group.
func (s *Store) LogValue() slog.Value {
	return slog.GroupValue(slog.String("key_prefix", s.prefix))
}
