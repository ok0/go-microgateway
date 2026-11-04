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
	addr    string
	setTime func(time.Time)
	advance func(time.Duration)
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

func runPexpireatAbsolute(t *testing.T, env fidelityEnv, name, write string) {
	ctx := context.Background()
	c := env.c
	key := probeKey(t, c, name)
	if env.setTime != nil {
		env.setTime(miniStart.Add(300 * time.Microsecond))
	} else if !serverAtLeast(t, c, 7, 0) {
		t.Skip("PEXPIRETIME needs Redis 7.0; absolute expiry on this server is checked exactly by PttlRounding")
	}
	script := goredis.NewScript(luaNowMs + "local w = nowMs + 201; " + write + "; redis.call('PEXPIREAT', KEYS[1], string.format('%.0f', w)); return {string.format('%.0f', w), redis.call('PEXPIRETIME', KEYS[1])}")
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
}

// hscanThroughPager runs HSCAN with COUNT 100 to cursor 0 through a respProxy that answers HSCAN itself (D11) and
// returns each page's flat reply. The client uses the database of env.c.
func hscanThroughPager(t *testing.T, env fidelityEnv, key string, cfg *pagerConfig) [][]string {
	t.Helper()
	ctx := context.Background()
	gc, ok := env.c.(*goredis.Client)
	if !ok {
		t.Fatalf("fidelityEnv.c is %T, want *goredis.Client", env.c)
	}
	p := newRespProxy(t, env.addr)
	p.SetHscanPager(cfg)
	pc := newTestClient(t, p.Addr(), gc.Options().DB)
	var pages [][]string
	var cursor uint64
	for {
		if len(pages) >= 10000 {
			t.Fatal("pager HSCAN cursor did not return to 0 within 10000 calls")
		}
		page, next, err := pc.HScan(ctx, key, cursor, "", 100).Result()
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, page)
		cursor = next
		if cursor == 0 {
			return pages
		}
	}
}

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
		runPexpireatAbsolute(t, env, "PexpireatAbsolute", "redis.call('SET', KEYS[1], 'x')")
	})

	t.Run("PexpireatAbsoluteHash", func(t *testing.T) {
		runPexpireatAbsolute(t, env, "PexpireatAbsoluteHash", "redis.call('HSET', KEYS[1], 'f', 'x')")
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

	t.Run("RenameKeepsTTL", func(t *testing.T) {
		a := probeKey(t, c, "RenameKeepsTTL/a")
		b := probeKey(t, c, "RenameKeepsTTL/b")
		if env.setTime != nil {
			env.setTime(miniStart)
		} else if !serverAtLeast(t, c, 7, 0) {
			t.Skip("PEXPIRETIME needs Redis 7.0; the renamed deadline is checked exactly by PexpireatAbsolute")
		}
		script := goredis.NewScript(luaNowMs + "redis.call('SET', KEYS[1], 'a'); redis.call('PEXPIREAT', KEYS[1], string.format('%.0f', nowMs + 200000)); redis.call('SET', KEYS[2], 'b'); redis.call('PEXPIREAT', KEYS[2], string.format('%.0f', nowMs + 60000)); redis.call('RENAME', KEYS[1], KEYS[2]); return {redis.call('EXISTS', KEYS[1]), redis.call('PEXPIRETIME', KEYS[2]), string.format('%.0f', nowMs + 200000)}")
		reply, err := script.Run(ctx, c, []string{a, b}).Slice()
		if err != nil {
			t.Fatal(err)
		}
		if len(reply) != 3 {
			t.Fatalf("reply %v, want three elements", reply)
		}
		if reply[0] != int64(0) {
			t.Fatalf("EXISTS source after RENAME = %v, want 0", reply[0])
		}
		back, ok := reply[1].(int64)
		if !ok {
			t.Fatalf("reply[1] %v (%T), want int64", reply[1], reply[1])
		}
		wText, ok := reply[2].(string)
		if !ok {
			t.Fatalf("reply[2] %v (%T), want string", reply[2], reply[2])
		}
		want, err := strconv.ParseInt(wText, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if back != want {
			t.Fatalf("PEXPIRETIME destination = %d, want the source deadline %d (the overwritten 60000 ms deadline must not survive)", back, want)
		}
	})

	t.Run("ExpiryInstant", func(t *testing.T) {
		key := probeKey(t, c, "ExpiryInstant")
		if env.setTime != nil {
			env.setTime(miniStart)
			if err := c.Set(ctx, key, "x", 0).Err(); err != nil {
				t.Fatal(err)
			}
			if err := c.PExpireAt(ctx, key, miniStart.Add(time.Second)).Err(); err != nil {
				t.Fatal(err)
			}
			env.advance(time.Second - time.Microsecond)
			if n, err := c.Exists(ctx, key).Result(); err != nil || n != 1 {
				t.Fatalf("EXISTS 1 µs before the deadline = %d, %v; want 1", n, err)
			}
			env.advance(time.Millisecond + time.Microsecond)
			if n, err := c.Exists(ctx, key).Result(); err != nil || n != 0 {
				t.Fatalf("EXISTS 1 ms after the deadline = %d, %v; want 0", n, err)
			}
			return
		}
		set := goredis.NewScript(luaNowMs + "local w = nowMs + 300; redis.call('SET', KEYS[1], 'x'); redis.call('PEXPIREAT', KEYS[1], string.format('%.0f', w)); return string.format('%.0f', w)")
		deadlineText, err := set.Run(ctx, c, []string{key}).Text()
		if err != nil {
			t.Fatal(err)
		}
		deadlineMs, err := strconv.ParseInt(deadlineText, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		deadlineUs := deadlineMs * 1000
		sample := goredis.NewScript("local t = redis.call('TIME'); return {t[1], t[2], redis.call('EXISTS', KEYS[1])}")
		limit := time.Now().Add(10 * time.Second)
		for {
			reply, err := sample.Run(ctx, c, []string{key}).Slice()
			if err != nil {
				t.Fatal(err)
			}
			if len(reply) != 3 {
				t.Fatalf("reply %v, want three elements", reply)
			}
			secText, ok1 := reply[0].(string)
			usecText, ok2 := reply[1].(string)
			exists, ok3 := reply[2].(int64)
			if !ok1 || !ok2 || !ok3 {
				t.Fatalf("reply %v has unexpected types", reply)
			}
			sec, err := strconv.ParseInt(secText, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			usec, err := strconv.ParseInt(usecText, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			nowUs := sec*1_000_000 + usec
			if nowUs < deadlineUs && exists != 1 {
				t.Fatalf("key absent at server time %d µs, before its deadline %d µs", nowUs, deadlineUs)
			}
			if nowUs >= deadlineUs+2000 {
				if exists != 0 {
					t.Fatalf("key present at server time %d µs, 2 ms after its deadline %d µs", nowUs, deadlineUs)
				}
				return
			}
			if time.Now().After(limit) {
				t.Fatal("server clock did not pass the deadline in 10 s")
			}
			wait := time.NewTimer(time.Millisecond)
			<-wait.C
		}
	})

	t.Run("HdelLastFieldDeletesKey", func(t *testing.T) {
		key := probeKey(t, c, "HdelLastFieldDeletesKey")
		if err := c.HSet(ctx, key, "f", "v").Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.HDel(ctx, key, "f").Err(); err != nil {
			t.Fatal(err)
		}
		if n, err := c.Exists(ctx, key).Result(); err != nil || n != 0 {
			t.Fatalf("EXISTS after HDEL of the last field = %d, %v; want 0", n, err)
		}
	})

	t.Run("HscanCoversAll", func(t *testing.T) {
		key := probeKey(t, c, "HscanCoversAll")
		const n = 1050
		want := make(map[string]string, n)
		for i := 0; i < n; i++ {
			want["f"+strconv.Itoa(i)] = "v" + strconv.Itoa(i)
		}
		fields := make([]any, 0, 2*n)
		for k, v := range want {
			fields = append(fields, k, v)
		}
		if err := c.HSet(ctx, key, fields...).Err(); err != nil {
			t.Fatal(err)
		}
		seen := make(map[string]string, n)
		calls := 0
		var cursor uint64
		for {
			page, next, err := c.HScan(ctx, key, cursor, "", 100).Result()
			if err != nil {
				t.Fatal(err)
			}
			calls++
			if calls > 10000 {
				t.Fatal("HSCAN cursor did not return to 0 within 10000 calls")
			}
			for i := 0; i+1 < len(page); i += 2 {
				seen[page[i]] = page[i+1]
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if len(seen) != n {
			t.Fatalf("HSCAN covered %d fields, want %d", len(seen), n)
		}
		for k, v := range want {
			if seen[k] != v {
				t.Fatalf("HSCAN field %q = %q, want %q", k, seen[k], v)
			}
		}
		if env.setTime != nil {
			if calls != 1 {
				t.Fatalf("miniredis HSCAN returned %d pages, want 1 (documented divergence D11: COUNT is ignored)", calls)
			}
		} else if calls < 2 {
			t.Fatalf("HSCAN with COUNT 100 returned %d page(s) for %d fields, want more than one", calls, n)
		}
	})

	t.Run("HscanPagedCoversAll", func(t *testing.T) {
		if env.addr == "" {
			t.Fatal("fidelityEnv.addr is not set: the pager probe proxies HSCAN to the server at that address")
		}
		key := probeKey(t, c, "HscanPagedCoversAll")
		const n = 1050
		want := make(map[string]string, n)
		fields := make([]any, 0, 2*n)
		for i := 0; i < n; i++ {
			k, v := "f"+strconv.Itoa(i), "v"+strconv.Itoa(i)
			want[k] = v
			fields = append(fields, k, v)
		}
		if err := c.HSet(ctx, key, fields...).Err(); err != nil {
			t.Fatal(err)
		}

		pages := hscanThroughPager(t, env, key, &pagerConfig{})
		if len(pages) != 11 {
			t.Fatalf("pager with COUNT 100 returned %d pages for %d fields, want 11", len(pages), n)
		}
		counts := make(map[string]int, n)
		got := make(map[string]string, n)
		for i, page := range pages {
			if len(page) > 2*100 {
				t.Fatalf("page %d has %d pairs, want at most 100", i, len(page)/2)
			}
			for j := 0; j+1 < len(page); j += 2 {
				counts[page[j]]++
				got[page[j]] = page[j+1]
			}
		}
		if len(got) != n {
			t.Fatalf("pager covered %d fields, want %d", len(got), n)
		}
		for k, v := range want {
			if got[k] != v || counts[k] != 1 {
				t.Fatalf("pager field %q = %q (returned %d times), want %q once", k, got[k], counts[k], v)
			}
		}

		pages = hscanThroughPager(t, env, key, &pagerConfig{Duplicate: true})
		if len(pages) < 2 {
			t.Fatalf("pager with Duplicate returned %d page(s), want more than one", len(pages))
		}
		counts = make(map[string]int, n)
		got = make(map[string]string, n)
		for _, page := range pages {
			for j := 0; j+1 < len(page); j += 2 {
				counts[page[j]]++
				got[page[j]] = page[j+1]
			}
		}
		if len(got) != n {
			t.Fatalf("pager with Duplicate covered %d fields, want %d", len(got), n)
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("pager with Duplicate field %q = %q, want %q", k, got[k], v)
			}
		}
		duplicated := false
		for _, cnt := range counts {
			if cnt > 1 {
				duplicated = true
			}
		}
		if !duplicated {
			t.Fatal("pager with Duplicate returned no field twice")
		}
	})

	t.Run("ScriptKeyGuard", func(t *testing.T) {
		key := probeKey(t, c, "ScriptKeyGuard")
		other := probeKey(t, c, "ScriptKeyGuard/other")
		got, err := goredis.NewScript(luaKeyGuard+"redis.call('HSET', KEYS[1], 'f', 'v'); return redis.call('HGET', KEYS[1], 'f')").Run(ctx, c, []string{key}).Text()
		if err != nil {
			t.Fatalf("declared key HSET: %v", err)
		}
		if got != "v" {
			t.Fatalf("declared key HGET = %q, want %q", got, "v")
		}
		_, err = goredis.NewScript(luaKeyGuard+"return redis.error_reply('ERR microgateway: probe')").Run(ctx, c, []string{key}).Result()
		var rerr goredis.Error
		if err == nil || !errors.As(err, &rerr) || !strings.HasPrefix(err.Error(), "ERR microgateway: probe") {
			t.Fatalf("redis.error_reply through the guard: %v, want the error reply", err)
		}
		cases := []struct{ name, body, want string }{
			{"HSET on an undeclared key", "redis.call('HSET', ARGV[1], 'f', 'v')", "microgateway-test: undeclared key"},
			{"RENAME to an undeclared key", "redis.call('RENAME', KEYS[1], ARGV[1])", "microgateway-test: undeclared key"},
			{"KEYS *", "return redis.call('KEYS', '*')", "microgateway-test: forbidden command"},
		}
		for _, cs := range cases {
			_, err := goredis.NewScript(luaKeyGuard+cs.body).Run(ctx, c, []string{key}, other).Result()
			if err == nil || !strings.Contains(err.Error(), cs.want) {
				t.Fatalf("%s: got %v, want the guard text %q", cs.name, err, cs.want)
			}
		}
		if n, err := c.Exists(ctx, other).Result(); err != nil || n != 0 {
			t.Fatalf("EXISTS undeclared key after the guard = %d, %v; want 0", n, err)
		}
		if n, err := c.HLen(ctx, key).Result(); err != nil || n != 1 {
			t.Fatalf("HLEN declared key after the guard = %d, %v; want 1", n, err)
		}
	})

	t.Run("HashBinaryFields", func(t *testing.T) {
		key := probeKey(t, c, "HashBinaryFields")
		f1, v1 := "a\x00b\nc\xff", "\x00\n\xff\x01"
		f2, v2 := "\xff", "x\x00"
		script := goredis.NewScript("redis.call('HSET', KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4]); return redis.call('HGET', KEYS[1], ARGV[1])")
		got, err := script.Run(ctx, c, []string{key}, f1, v1, f2, v2).Text()
		if err != nil {
			t.Fatal(err)
		}
		if got != v1 {
			t.Fatalf("script HGET = %q, want %q", got, v1)
		}
		all, err := c.HGetAll(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]string{f1: v1, f2: v2}
		if len(all) != len(want) {
			t.Fatalf("HGETALL has %d fields, want %d", len(all), len(want))
		}
		for k, v := range want {
			if all[k] != v {
				t.Fatalf("HGETALL field %q = %q, want %q", k, all[k], v)
			}
		}
	})

	t.Run("ScriptManyKeys", func(t *testing.T) {
		const n = 37
		keys := make([]string, n)
		args := make([]any, n)
		for i := range keys {
			keys[i] = probeKey(t, c, "ScriptManyKeys/"+strconv.Itoa(i))
			args[i] = strconv.Itoa(i)
		}
		got, err := c.Eval(ctx, "for i = 1, #KEYS do redis.call('SET', KEYS[i], ARGV[i]) end return #KEYS", keys, args...).Int()
		if err != nil {
			t.Fatal(err)
		}
		if got != n {
			t.Fatalf("#KEYS = %d, want %d", got, n)
		}
		for i, k := range keys {
			v, err := c.Get(ctx, k).Result()
			if err != nil {
				t.Fatal(err)
			}
			if v != strconv.Itoa(i) {
				t.Fatalf("key %d = %q, want %q", i, v, strconv.Itoa(i))
			}
		}
	})

	t.Run("LargeArgv", func(t *testing.T) {
		key := probeKey(t, c, "LargeArgv")
		value := make([]byte, 0, 1<<20)
		for i := 0; i < 4096; i++ {
			for b := 0; b < 256; b++ {
				value = append(value, byte(b))
			}
		}
		script := goredis.NewScript("redis.call('HSET', KEYS[1], 'v', ARGV[1]); return 1")
		if err := script.Run(ctx, c, []string{key}, string(value)).Err(); err != nil {
			t.Fatal(err)
		}
		got, err := c.HGet(ctx, key, "v").Result()
		if err != nil {
			t.Fatal(err)
		}
		if got != string(value) {
			t.Fatalf("1 MiB value round-trip: got %d bytes, want %d equal bytes", len(got), len(value))
		}
	})

	t.Run("LuaExactIntegers", func(t *testing.T) {
		key := probeKey(t, c, "LuaExactIntegers")
		script := goredis.NewScript("return {string.format('%.0f', 8912591253740990 + 94608000000000 + 1000000), string.format('%.0f', 8912591253740990 + 94608000000000 + 1000000 + 1), string.format('%.0f', tonumber('9007199254740991')), string.format('%.0f', tonumber('1786000000123456') + 1)}")
		got, err := script.Run(ctx, c, []string{key}).StringSlice()
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"9007199254740990", "9007199254740991", "9007199254740991", "1786000000123457"}
		if len(got) != len(want) {
			t.Fatalf("got %q, want %q", got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("got %q, want %q", got, want)
			}
		}
	})

	t.Run("TimeMicro", func(t *testing.T) {
		key := probeKey(t, c, "TimeMicro")
		script := goredis.NewScript("local t = redis.call('TIME'); return {t[1], t[2]}")
		if env.setTime != nil {
			env.setTime(miniStart.Add(123456789))
			got, err := script.Run(ctx, c, []string{key}).StringSlice()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 2 || got[0] != "1700000000" || got[1] != "123456" {
				t.Fatalf("script TIME = %q, want [1700000000 123456]", got)
			}
			return
		}
		before := time.Now()
		got, err := script.Run(ctx, c, []string{key}).StringSlice()
		if err != nil {
			t.Fatal(err)
		}
		after := time.Now()
		if len(got) != 2 {
			t.Fatalf("script TIME = %q, want two elements", got)
		}
		sec, err := strconv.ParseInt(got[0], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		usec, err := strconv.ParseInt(got[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if nowUs := sec*1_000_000 + usec; nowUs < before.Add(-time.Second).UnixMicro() || nowUs > after.Add(time.Second).UnixMicro() {
			t.Fatalf("script TIME = %d µs outside [%s, %s] ± 1s", nowUs, before, after)
		}
	})

	t.Run("PipelinedNoscript", func(t *testing.T) {
		key := probeKey(t, c, "PipelinedNoscript")
		if err := c.Set(ctx, key, "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if err := c.ScriptFlush(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		script := goredis.NewScript("return 1")
		var ev *goredis.Cmd
		var get *goredis.StringCmd
		_, err := c.Pipelined(ctx, func(p goredis.Pipeliner) error {
			ev = p.EvalSha(ctx, script.Hash(), []string{key})
			get = p.Get(ctx, key)
			return nil
		})
		if err != nil && !goredis.HasErrorPrefix(err, "NOSCRIPT") {
			t.Fatal(err)
		}
		if !goredis.HasErrorPrefix(ev.Err(), "NOSCRIPT") {
			t.Fatalf("pipelined EVALSHA after SCRIPT FLUSH: %v, want NOSCRIPT", ev.Err())
		}
		if get.Err() != nil || get.Val() != "v" {
			t.Fatalf("pipelined GET after the NOSCRIPT reply: %q, %v; want v", get.Val(), get.Err())
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
		now := miniStart
		runFidelityProbes(t, fidelityEnv{
			c:    c,
			addr: m.Addr(),
			setTime: func(at time.Time) {
				now = at
				m.SetTime(at)
			},
			advance: func(d time.Duration) {
				now = now.Add(d)
				m.SetTime(now)
				m.FastForward(d)
			},
		})
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
