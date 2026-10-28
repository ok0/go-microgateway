package redis

import goredis "github.com/redis/go-redis/v9"

const luaClockServer = `
local t = redis.call('TIME')
local SEC = tonumber(t[1])
local USEC = tonumber(t[2])
`

const luaLib = `
local function idiv(a, b)
  local q = math.floor(a / b)
  local r = a - q * b
  if r < 0 then q = q - 1; r = r + b elseif r >= b then q = q + 1; r = r - b end
  return q, r
end
local function fmtint(x) return string.format('%.0f', x) end
local function pad6(x) local s = fmtint(x); return string.rep('0', 6 - #s) .. s end
local U = SEC * 1000000 + USEC
local NOW_MS, NOW_REM = idiv(U, 1000)
local NOW_SUB = NOW_REM * 1000
local function expire_at(key, rel)
  local carry, sub = idiv(NOW_SUB + rel, 1000000)
  local ms = NOW_MS + carry
  if sub > 0 then ms = ms + 1 end
  redis.call('PEXPIREAT', key, fmtint(ms))
end
`

const luaTokenBucket = `
local T = tonumber(ARGV[1])
local TAU = tonumber(ARGV[2])
local key = KEYS[1]
local d = 0
local raw = redis.call('GET', key)
if raw then
  local ms, sub = string.match(raw, '^(%d+)(%d%d%d%d%d%d)$')
  if not ms then return redis.error_reply('ERR microgateway: malformed token bucket state') end
  d = (tonumber(ms) - NOW_MS) * 1000000 + (tonumber(sub) - NOW_SUB)
end
local base = d
if base < 0 then base = 0 end
local new = base + T
local allowed = new <= TAU
local stored = d
if allowed then
  stored = new
  local carry, sub = idiv(NOW_SUB + new, 1000000)
  redis.call('SET', key, fmtint(NOW_MS + carry) .. pad6(sub))
  expire_at(key, new)
end
local live = stored
if live < 0 then live = 0 end
local x = TAU - live
local remaining = 0
if x >= 0 then remaining = idiv(x, T) end
local retry = 0
if not allowed then retry = new - TAU end
return {allowed and 1 or 0, remaining, retry, live}
`

const luaSlidingWindow = `
local P = tonumber(ARGV[1])
local COUNT = tonumber(ARGV[2])
local key = KEYS[1]
local MS = 1000000
local idx, elapsed = idiv(U, P)
for _ = 1, 3 do
  local digit
  digit, elapsed = idiv(elapsed * 10, P)
  idx = idx * 10 + digit
end
local win = nil
local sprev, scurr = 0, 0
local raw = redis.call('GET', key)
if raw then
  local a, b, c = string.match(raw, '^(%d+) (%d+) (%d+)$')
  if not a then return redis.error_reply('ERR microgateway: malformed sliding window state') end
  win = tonumber(a); sprev = tonumber(b); scurr = tonumber(c)
end
local prev, curr = 0, 0
if win == idx then prev = sprev; curr = scurr elseif win == idx - 1 then prev = scurr end
local function estimate(pv, cu, el)
  local e = el / P
  local w = 1 - e
  local pw = pv * w
  return pw + cu
end
local est = estimate(prev, curr, elapsed)
local allowed = est + 1 <= COUNT
if allowed then
  curr = curr + 1
  est = estimate(prev, curr, elapsed)
  redis.call('SET', key, fmtint(idx) .. ' ' .. fmtint(prev) .. ' ' .. fmtint(curr))
  expire_at(key, 2 * P - elapsed)
end
local remaining = math.floor(COUNT - est)
if remaining < 0 then remaining = 0 end
local reset = 0
if curr > 0 then reset = 2 * P - elapsed elseif prev > 0 then reset = P - elapsed end
local retry = 0
if not allowed then
  local function allowed_at(k)
    local j, el = idiv(elapsed + k * MS, P)
    local pv, cu = 0, 0
    if j == 0 then pv = prev; cu = curr elseif j == 1 then pv = curr end
    return estimate(pv, cu, el) + 1 <= COUNT
  end
  local lo = 1
  local hi = idiv(2 * P - elapsed + MS - 1, MS)
  while lo < hi do
    local mid = lo + math.floor((hi - lo) / 2)
    if allowed_at(mid) then hi = mid else lo = mid + 1 end
  end
  retry = lo * MS
end
return {allowed and 1 or 0, remaining, retry, reset}
`

type scriptSet struct {
	tb, sw *goredis.Script
}

func newScriptSet(clock string) scriptSet {
	return scriptSet{
		tb: goredis.NewScript(clock + luaLib + luaTokenBucket),
		sw: goredis.NewScript(clock + luaLib + luaSlidingWindow),
	}
}

var serverScripts = newScriptSet(luaClockServer)
