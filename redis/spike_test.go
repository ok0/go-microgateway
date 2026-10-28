package redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/ok0/go-microgateway/store"
)

type fidelityEnv struct {
	c       goredis.UniversalClient
	setTime func(time.Time)
}

func probeKey(t *testing.T, c goredis.UniversalClient, name string) string {
	t.Helper()
	ctx := context.Background()
	key := "spike:" + name
	if err := c.Del(ctx, key).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Del(context.Background(), key).Err() })
	return key
}

func serverAtLeast(t *testing.T, c goredis.UniversalClient, major, minor int) bool {
	t.Helper()
	info, err := c.Info(context.Background(), "server").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(info, "\n") {
		version, ok := strings.CutPrefix(strings.TrimSpace(line), "redis_version:")
		if !ok {
			continue
		}
		parts := strings.Split(version, ".")
		if len(parts) < 2 {
			t.Fatalf("redis_version %q: cannot parse", version)
		}
		gotMajor, err := strconv.Atoi(parts[0])
		if err != nil {
			t.Fatalf("redis_version %q: cannot parse: %v", version, err)
		}
		gotMinor, err := strconv.Atoi(parts[1])
		if err != nil {
			t.Fatalf("redis_version %q: cannot parse: %v", version, err)
		}
		return gotMajor > major || (gotMajor == major && gotMinor >= minor)
	}
	t.Fatal("INFO server has no redis_version line")
	return false
}

const luaNowMs = "local t = redis.call('TIME')\nlocal nowMs = math.floor((tonumber(t[1]) * 1000000 + tonumber(t[2])) / 1000)\n"

func runFidelityProbes(t *testing.T, env fidelityEnv) {
	ctx := context.Background()
	c := env.c

	t.Run("Time", func(t *testing.T) {
		if env.setTime != nil {
			env.setTime(miniStart.Add(123456789))
			got, err := c.Time(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			if got.Unix() != 1_700_000_000 || got.Nanosecond()/1000 != 123456 {
				t.Fatalf("TIME = unix %d usec %d, want unix 1700000000 usec 123456", got.Unix(), got.Nanosecond()/1000)
			}
			return
		}
		before := time.Now()
		got, err := c.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		after := time.Now()
		if got.Before(before.Add(-time.Second)) || got.After(after.Add(time.Second)) {
			t.Fatalf("TIME = %s outside [%s, %s] ± 1s", got, before, after)
		}
	})

	t.Run("GetMissingIsFalse", func(t *testing.T) {
		key := probeKey(t, c, "GetMissingIsFalse")
		script := goredis.NewScript(`if redis.call('GET', KEYS[1]) == false then return 1 end return 0`)
		got, err := script.Run(ctx, c, []string{key}).Int64()
		if err != nil {
			t.Fatal(err)
		}
		if got != 1 {
			t.Fatalf("got %d, want 1", got)
		}
	})

	t.Run("EvalshaNoscript", func(t *testing.T) {
		key := probeKey(t, c, "EvalshaNoscript")
		script := goredis.NewScript("return 1")
		if err := c.ScriptFlush(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		err := c.EvalSha(ctx, script.Hash(), []string{key}).Err()
		if err == nil || !goredis.HasErrorPrefix(err, "NOSCRIPT") {
			t.Fatalf("EVALSHA after SCRIPT FLUSH: got %v, want NOSCRIPT", err)
		}
		if err := script.Run(ctx, c, []string{key}).Err(); err != nil {
			t.Fatalf("Script.Run: %v", err)
		}
		if err := c.EvalSha(ctx, script.Hash(), []string{key}).Err(); err != nil {
			t.Fatalf("EVALSHA after Script.Run: %v", err)
		}
	})

	t.Run("PexpireatAbsolute", func(t *testing.T) {
		key := probeKey(t, c, "PexpireatAbsolute")
		if env.setTime != nil {
			env.setTime(miniStart.Add(300 * time.Microsecond))
		} else if !serverAtLeast(t, c, 7, 0) {
			t.Skip("PEXPIRETIME needs Redis 7.0; absolute expiry on this server is checked exactly by PttlRounding")
		}
		script := goredis.NewScript(luaNowMs + "local w = nowMs + 201; redis.call('SET', KEYS[1], 'x'); redis.call('PEXPIREAT', KEYS[1], string.format('%.0f', w)); return {string.format('%.0f', w), redis.call('PEXPIRETIME', KEYS[1])}")
		reply, err := script.Run(ctx, c, []string{key}).Slice()
		if err != nil {
			t.Fatal(err)
		}
		if len(reply) != 2 {
			t.Fatalf("reply %v, want two elements", reply)
		}
		wText, ok := reply[0].(string)
		if !ok {
			t.Fatalf("reply[0] %v (%T), want string", reply[0], reply[0])
		}
		w, err := strconv.ParseInt(wText, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		back, ok := reply[1].(int64)
		if !ok {
			t.Fatalf("reply[1] %v (%T), want int64", reply[1], reply[1])
		}
		if back != w {
			t.Fatalf("PEXPIRETIME = %d, want %d", back, w)
		}
		if env.setTime != nil && w != miniStart.UnixMilli()+201 {
			t.Fatalf("expiry = %d, want %d", w, miniStart.UnixMilli()+201)
		}
	})

	t.Run("PttlRounding", func(t *testing.T) {
		key := probeKey(t, c, "PttlRounding")
		if env.setTime != nil {
			env.setTime(miniStart.Add(300 * time.Microsecond))
		}
		src := luaNowMs + "local w = nowMs + 201; redis.call('SET', KEYS[1], 'x'); redis.call('PEXPIREAT', KEYS[1], string.format('%.0f', w)); return {string.format('%.0f', w), redis.call('PTTL', KEYS[1])}"
		for attempt := 0; attempt < 100; attempt++ {
			var t0, t1 *goredis.TimeCmd
			var ev *goredis.Cmd
			_, err := c.Pipelined(ctx, func(p goredis.Pipeliner) error {
				t0 = p.Time(ctx)
				ev = p.Eval(ctx, src, []string{key})
				t1 = p.Time(ctx)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			before, after := t0.Val(), t1.Val()
			if before.UnixMilli() != after.UnixMilli() || before.UnixMicro()%1000 == 0 {
				continue
			}
			reply, err := ev.Slice()
			if err != nil {
				t.Fatal(err)
			}
			if len(reply) != 2 {
				t.Fatalf("reply %v, want two elements", reply)
			}
			wText, ok := reply[0].(string)
			if !ok {
				t.Fatalf("reply[0] %v (%T), want string", reply[0], reply[0])
			}
			w, err := strconv.ParseInt(wText, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			pttl, ok := reply[1].(int64)
			if !ok {
				t.Fatalf("reply[1] %v (%T), want int64", reply[1], reply[1])
			}
			if w != before.UnixMilli()+201 {
				t.Fatalf("expiry = %d, want %d", w, before.UnixMilli()+201)
			}
			if env.setTime != nil {
				if pttl != 200 {
					t.Fatal("miniredis PTTL semantics changed (documented divergence: sub-ms clock); update redis/doc.go # Test server fidelity")
				}
				return
			}
			if pttl != 201 {
				t.Fatalf("PTTL = %d, want 201", pttl)
			}
			return
		}
		t.Fatal("no attempt kept the server clock inside one millisecond for the whole script in 100 attempts")
	})

	t.Run("PexpireatPast", func(t *testing.T) {
		key := probeKey(t, c, "PexpireatPast")
		if env.setTime != nil {
			env.setTime(miniStart)
			script := goredis.NewScript("redis.call('SET', KEYS[1], 'x'); redis.call('PEXPIREAT', KEYS[1], ARGV[1]); return redis.call('EXISTS', KEYS[1])")
			got, err := script.Run(ctx, c, []string{key}, strconv.FormatInt(miniStart.UnixMilli()-1, 10)).Int64()
			if err != nil {
				t.Fatal(err)
			}
			if got != 0 {
				t.Fatalf("EXISTS = %d, want 0", got)
			}
			return
		}
		script := goredis.NewScript(luaNowMs + "redis.call('SET', KEYS[1], 'x'); redis.call('PEXPIREAT', KEYS[1], string.format('%.0f', nowMs - 1)); return redis.call('EXISTS', KEYS[1])")
		got, err := script.Run(ctx, c, []string{key}).Int64()
		if err != nil {
			t.Fatal(err)
		}
		if got != 0 {
			t.Fatalf("EXISTS = %d, want 0", got)
		}
	})

	t.Run("ReplyConversion", func(t *testing.T) {
		key := probeKey(t, c, "ReplyConversion")
		script := goredis.NewScript("return {3.7, -3.7, 172800000000000, 9007199254740993}")
		got, err := script.Run(ctx, c, []string{key}).Int64Slice()
		if err != nil {
			t.Fatal(err)
		}
		want := []int64{3, -3, 172800000000000, 9007199254740992}
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	})

	t.Run("NumberFormatting", func(t *testing.T) {
		key := probeKey(t, c, "NumberFormatting")
		script := goredis.NewScript("return {string.format('%.0f', tonumber(ARGV[1])), string.format('%.0f', 1786000000123456), string.format('%.0f', tonumber('1786000000123456') + 1)}")
		got, err := script.Run(ctx, c, []string{key}, "1700000000123456789").StringSlice()
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"1700000000123456768", "1786000000123456", "1786000000123457"}
		if len(got) != len(want) {
			t.Fatalf("got %q, want %q", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %q, want %q", got, want)
			}
		}
	})

	t.Run("FloatOrder", func(t *testing.T) {
		key := probeKey(t, c, "FloatOrder")
		script := goredis.NewScript(`
local P = tonumber(ARGV[1])
local function estimate(pv, cu, el)
  local e = el / P
  local w = 1 - e
  local pw = pv * w
  return pw + cu
end
return string.format('%.17g', estimate(tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])))
`)
		cases := []struct{ prev, elapsed, p int64 }{
			{2, 111000000, 333000000},
			{7, 1, 7000000000},
			{3, 142857142, 1000000000},
			{99, 86399999999000, 86400000000000},
		}
		for _, cs := range cases {
			for _, curr := range []int64{0, 1} {
				out, err := script.Run(ctx, c, []string{key},
					strconv.FormatInt(cs.p, 10),
					strconv.FormatInt(cs.prev, 10),
					strconv.FormatInt(curr, 10),
					strconv.FormatInt(cs.elapsed, 10),
				).Text()
				if err != nil {
					t.Fatal(err)
				}
				got, err := strconv.ParseFloat(out, 64)
				if err != nil {
					t.Fatal(err)
				}
				e := float64(cs.elapsed) / float64(cs.p)
				w := 1 - e
				pw := float64(float64(cs.prev) * w)
				want := pw + float64(curr)
				if got != want {
					t.Fatalf("prev %d elapsed %d P %d curr %d: script %s (%v), Go %v", cs.prev, cs.elapsed, cs.p, curr, out, got, want)
				}
			}
		}
	})

	t.Run("PatternMatch", func(t *testing.T) {
		key := probeKey(t, c, "PatternMatch")
		split := goredis.NewScript("local a, b = string.match('1700000000123456789', '^(%d+)(%d%d%d%d%d%d)$'); return {a, b}")
		got, err := split.Run(ctx, c, []string{key}).StringSlice()
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0] != "1700000000123" || got[1] != "456789" {
			t.Fatalf("got %q, want [1700000000123 456789]", got)
		}
		triple := goredis.NewScript("local a, b, c = string.match('1966 3 4', '^(%d+) (%d+) (%d+)$'); return {a, b, c}")
		got, err = triple.Run(ctx, c, []string{key}).StringSlice()
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 3 || got[0] != "1966" || got[1] != "3" || got[2] != "4" {
			t.Fatalf("got %q, want [1966 3 4]", got)
		}
		none := goredis.NewScript("if string.match('12x', '^(%d+)(%d%d%d%d%d%d)$') == nil then return 1 end return 0")
		matched, err := none.Run(ctx, c, []string{key}).Int64()
		if err != nil {
			t.Fatal(err)
		}
		if matched != 1 {
			t.Fatalf("pattern matched '12x'")
		}
	})

	t.Run("CallOrdering", func(t *testing.T) {
		key := probeKey(t, c, "CallOrdering")
		script := goredis.NewScript("redis.call('SET', KEYS[1], 'a'); redis.call('SET', KEYS[1], 'b'); return redis.call('GET', KEYS[1])")
		got, err := script.Run(ctx, c, []string{key}).Text()
		if err != nil {
			t.Fatal(err)
		}
		if got != "b" {
			t.Fatalf("got %q, want %q", got, "b")
		}
	})

	t.Run("ErrorPropagation", func(t *testing.T) {
		key := probeKey(t, c, "ErrorPropagation")
		_, err := goredis.NewScript("return redis.error_reply('ERR microgateway: probe')").Run(ctx, c, []string{key}).Result()
		if err == nil {
			t.Fatal("error_reply: got nil error")
		}
		var rerr goredis.Error
		if !errors.As(err, &rerr) {
			t.Fatalf("error_reply: %v is not a goredis.Error", err)
		}
		if !strings.HasPrefix(err.Error(), "ERR microgateway: probe") {
			t.Fatalf("error_reply: text %q", err.Error())
		}

		hashKey := probeKey(t, c, "ErrorPropagation/hash")
		if err := c.HSet(ctx, hashKey, "f", "v").Err(); err != nil {
			t.Fatal(err)
		}
		_, err = goredis.NewScript("return redis.call('GET', KEYS[1])").Run(ctx, c, []string{hashKey}).Result()
		if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
			t.Fatalf("GET on hash: got %v, want WRONGTYPE", err)
		}
		if !errors.As(err, &rerr) {
			t.Fatalf("GET on hash: %v is not a goredis.Error", err)
		}
		if got := serverErrorCode(err.Error()); got != "WRONGTYPE" {
			t.Fatalf("GET on hash: serverErrorCode = %q, want WRONGTYPE", got)
		}

		_, err = goredis.NewScript("local x = nil + 1; return 1").Run(ctx, c, []string{key}).Result()
		if err == nil {
			t.Fatal("arithmetic on nil: got nil error")
		}

		_, err = goredis.NewScript("redis.call('SET', KEYS[1], 'x'); return redis.call('INCR', KEYS[1])").Run(ctx, c, []string{key}).Result()
		if err == nil {
			t.Fatal("INCR on non-integer: got nil error")
		}
		value, err := c.Get(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if value != "x" {
			t.Fatalf("after failed script GET = %q, want %q (no rollback)", value, "x")
		}
	})
}

type spikeSeq struct {
	name   string
	limit  store.Limit
	offset time.Duration
	adv    []time.Duration
}

var spikeSequences = []spikeSeq{
	{
		name:   "tb_burst_refill",
		limit:  store.Limit{Algorithm: store.TokenBucket, Count: 5, Period: time.Second, Burst: 3},
		offset: 0,
		adv: []time.Duration{
			0, 0, 0, 0, 199 * time.Millisecond, time.Millisecond, 600 * time.Millisecond,
			0, 0, 0, 0, time.Microsecond, 999 * time.Microsecond,
		},
	},
	{
		name:   "tb_non_whole_interval",
		limit:  store.Limit{Algorithm: store.TokenBucket, Count: 7, Period: time.Second, Burst: 4},
		offset: 456789 * time.Microsecond,
		adv: []time.Duration{
			0, 0, 0, 0, 0, time.Microsecond, 142856 * time.Microsecond, time.Microsecond,
			142857 * time.Microsecond, 143 * time.Millisecond, 0, 0, 571428 * time.Microsecond, time.Millisecond,
		},
	},
	{
		name:   "tb_min_interval_max_burst",
		limit:  store.Limit{Algorithm: store.TokenBucket, Count: 1000000, Period: time.Second, Burst: 86400000000},
		offset: 0,
		adv:    []time.Duration{0, 0, 0, time.Microsecond, 0, 24 * time.Hour, 0},
	},
	{
		name:   "tb_day",
		limit:  store.Limit{Algorithm: store.TokenBucket, Count: 1, Period: 24 * time.Hour, Burst: 1},
		offset: 0,
		adv:    []time.Duration{0, 0, 24*time.Hour - time.Microsecond, time.Microsecond, 0, 48 * time.Hour},
	},
	{
		name:   "sw_roll",
		limit:  store.Limit{Algorithm: store.SlidingWindow, Count: 5, Period: 2 * time.Second},
		offset: 0,
		adv: []time.Duration{
			0, 0, 0, 0, 0, 0, 1999999 * time.Microsecond, time.Microsecond, 0, 0, 0, 0,
			time.Second, 0, 0, 2 * time.Second, 0, 3999999 * time.Microsecond, 0,
		},
	},
	{
		name:   "sw_non_representable",
		limit:  store.Limit{Algorithm: store.SlidingWindow, Count: 3, Period: 333 * time.Millisecond},
		offset: 111111 * time.Microsecond,
		adv: []time.Duration{
			0, 0, 0, 0, 111 * time.Microsecond, 222222 * time.Microsecond, 0, 0, 0,
			333 * time.Millisecond, time.Microsecond, 0, 0, 0, 665999 * time.Microsecond, 0,
		},
	},
	{
		name:   "sw_retry_crosses_roll",
		limit:  store.Limit{Algorithm: store.SlidingWindow, Count: 2, Period: 333 * time.Millisecond},
		offset: 0,
		adv:    []time.Duration{0, 0, 0, 300 * time.Millisecond, 0, 0, 33 * time.Millisecond, 0, 0},
	},
	{
		name:   "sw_day",
		limit:  store.Limit{Algorithm: store.SlidingWindow, Count: 10, Period: 24 * time.Hour},
		offset: 0,
		adv: []time.Duration{
			0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			12 * time.Hour,
			0, 0, 0, 0, 0, 0,
			12 * time.Hour,
			0, 0, 0,
			24*time.Hour - time.Microsecond, 0, 48 * time.Hour, 0,
		},
	},
	{
		name:   "sw_max_count",
		limit:  store.Limit{Algorithm: store.SlidingWindow, Count: 86400000000, Period: 24 * time.Hour},
		offset: 0,
		adv:    []time.Duration{0, 0, time.Microsecond},
	},
}

func runSpikeSequences(t *testing.T, newBackend func(t *testing.T) (timedStore, time.Time)) {
	for _, sq := range spikeSequences {
		t.Run(sq.name, func(t *testing.T) {
			got, start := newBackend(t)
			ref := memBackend(t, start)
			got.advance(sq.offset)
			ref.advance(sq.offset)
			steps := make([]step, len(sq.adv))
			for i, d := range sq.adv {
				steps[i] = step{advance: d, key: "spike:" + sq.name, limit: sq.limit}
			}
			compareSteps(t, sq.name, steps, got, ref)
		})
	}
}

func TestSpike(t *testing.T) {
	t.Run("Fidelity", func(t *testing.T) {
		m := newMini(t)
		c := newTestClient(t, m.Addr(), 0)
		runFidelityProbes(t, fidelityEnv{c: c, setTime: m.SetTime})
	})
	t.Run("ServerTime", func(t *testing.T) {
		runSpikeSequences(t, miniBackend)
	})
	t.Run("InjectedTime", func(t *testing.T) {
		runSpikeSequences(t, func(t *testing.T) (timedStore, time.Time) {
			m := newMini(t)
			return injectedBackend(t, m.Addr(), 0)
		})
	})
}
