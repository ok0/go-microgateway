package redis

const luaCacheLib = `
if U > 8912591253740990 then return redis.error_reply('ERR microgateway: clock out of range') end
local function ceil_ms(us, rem)
  local ms, r = idiv(us, 1000)
  if r * 1000 + rem > 0 then ms = ms + 1 end
  return ms
end
local function alive(us, rem)
  return U < us or (U == us and rem > 0)
end
local function later(au, ar, bu, br)
  if au > bu or (au == bu and ar >= br) then return au, ar end
  return bu, br
end
local function parse_dl(s)
  if type(s) ~= 'string' then return nil end
  local a, b = string.match(s, '^(%d+) (%d+)$')
  if not a then return nil end
  b = tonumber(b)
  if b > 999 then return nil end
  return tonumber(a), b
end
local function hmap(key)
  local a = redis.call('HGETALL', key)
  local t = {}
  for i = 1, #a, 2 do t[a[i]] = a[i + 1] end
  return t
end
local function pick(a, b, f)
  local v = a[f]
  if v == nil or v == '' then v = b[f] end
  if v == nil then return '' end
  return v
end
local function epochs(eh, es)
  local a = hmap(eh)
  local b = hmap(es)
  local xu, xr = 0, 0
  local au, ar = parse_dl(a.x)
  if au then xu, xr = au, ar end
  local bu, br = parse_dl(b.x)
  if bu then xu, xr = later(xu, xr, bu, br) end
  return pick(a, b, 'h'), pick(a, b, 's'), pick(a, b, 'q'), xu, xr
end
local function put_epochs(eh, es, H, S, Q, xu, xr)
  local xv = fmtint(xu) .. ' ' .. fmtint(xr)
  local ms = fmtint(ceil_ms(xu, xr))
  local ks = {eh, es}
  for i = 1, 2 do
    local k = ks[i]
    if H ~= '' then redis.call('HSET', k, 'h', H) end
    if S ~= '' then redis.call('HSET', k, 's', S) end
    if Q ~= '' then redis.call('HSET', k, 'q', Q) end
    redis.call('HSET', k, 'x', xv)
    redis.call('PEXPIREAT', k, ms)
  end
end
local function parse_v(s)
  if type(s) ~= 'string' then return nil end
  local slot, fid, st, dlu, dlr, H, S, soft, ins, stamp, tq = string.match(s, '^(%d+) (%x+) (%d+) (%d+) (%d+) (%S+) (%S+) ([01]) (%d+) (%S+) (%S+)$')
  if not slot or #fid ~= 32 then return nil end
  slot = tonumber(slot)
  dlr = tonumber(dlr)
  if slot > 31 or dlr > 999 then return nil end
  return {slot = slot, fid = fid, st = tonumber(st), dlu = tonumber(dlu), dlr = dlr, H = H, S = S, soft = tonumber(soft), ins = tonumber(ins), stamp = stamp, tq = tq}
end
local function fmt_v(r)
  return fmtint(r.slot) .. ' ' .. r.fid .. ' ' .. fmtint(r.st) .. ' ' .. fmtint(r.dlu) .. ' ' .. fmtint(r.dlr) .. ' ' .. r.H .. ' ' .. r.S .. ' ' .. fmtint(r.soft) .. ' ' .. fmtint(r.ins) .. ' ' .. r.stamp .. ' ' .. r.tq
end
local function live(r, H)
  return alive(r.dlu, r.dlr) and r.H == H
end
local function slot_matches(slotkey, fid)
  return redis.call('HGET', slotkey, 'f') == fid
end
local function key_ok(tok, cur)
  return cur == '' or cur == tok
end
local function epoch_ok(tok, cur)
  return cur == '' or cur == tok or (tok == '' and string.sub(cur, 1, 1) == 'b')
end
local function cur_gen(mi, mk)
  local g = mi.g
  if g == nil or g == '' then g = mk.g or '' end
  return g
end
local function qmax(cur, era, v)
  local ce, cv = string.match(cur, '^(%x+) (%d+)$')
  if ce == era and tonumber(cv) >= v then return cur end
  return era .. ' ' .. fmtint(v)
end
local function lines_of(s)
  local out = {}
  local start = 1
  while true do
    local i = string.find(s, '\n', start, true)
    if not i then
      out[#out + 1] = string.sub(s, start)
      return out
    end
    out[#out + 1] = string.sub(s, start, i - 1)
    start = i + 1
  end
end
local function has_line(s, tag)
  local ls = lines_of(s)
  for j = 1, #ls do
    if ls[j] == tag then return true end
  end
  return false
end
local function drop_variant(index, sec, r, slotkey)
  redis.call('HDEL', index, 'v:' .. sec)
  if r and slotkey and slot_matches(slotkey, r.fid) then redis.call('UNLINK', slotkey) end
end
local function index_finish(index, H)
  local m = hmap(index)
  local bu, br = nil, nil
  for f, v in pairs(m) do
    local kind = string.sub(f, 1, 2)
    local u, r = nil, nil
    if kind == 'f:' then
      u, r = parse_dl(lines_of(v)[1])
      if not (u and alive(u, r)) then
        redis.call('HDEL', index, f)
        u = nil
      end
    elseif kind == 'v:' then
      local rec = parse_v(v)
      if rec and rec.H == H and alive(rec.dlu, rec.dlr) then u, r = rec.dlu, rec.dlr end
    end
    if u then
      if bu == nil then bu, br = u, r else bu, br = later(bu, br, u, r) end
    end
  end
  local mu, mr = parse_dl(m.u)
  if mu and alive(mu, mr) then
    if bu == nil then bu, br = mu, mr else bu, br = later(bu, br, mu, mr) end
  end
  if bu == nil then
    redis.call('DEL', index)
  else
    redis.call('PEXPIREAT', index, fmtint(ceil_ms(bu, br)))
  end
end
`

const luaCacheGet = `
local mode = ARGV[1]
local sec = ARGV[2]
local names = ARGV[3]
local H, S, Q = epochs(KEYS[3], KEYS[4])
local mi = hmap(KEYS[1])
local g = cur_gen(mi, hmap(KEYS[2]))
local n = mi.n or ''
local recs = {}
local secs = {}
for f, v in pairs(mi) do
  if string.sub(f, 1, 2) == 'v:' then
    local r = parse_v(v)
    if r and live(r, H) and slot_matches(KEYS[5 + r.slot], r.fid) then
      local name = string.sub(f, 3)
      recs[name] = r
      secs[#secs + 1] = name
    end
  end
end
table.sort(secs)
local function reply(body)
  return {fmtint(U), g, H, S, Q, body}
end
local function stale_of(r)
  if r.soft == 1 or r.S ~= S then return '1' end
  return '0'
end
if #secs == 0 then return reply({'0'}) end
if mode == '1' then
  local body = {'1', n, secs, '0'}
  if n == '' and #secs == 1 then
    local r = recs[secs[1]]
    local vals = redis.call('HMGET', KEYS[5 + r.slot], 'st', 'm', 'b')
    if vals[1] and vals[2] and vals[3] then
      body = {'1', n, secs, '1', secs[1], stale_of(r), vals[1], vals[2], vals[3]}
    end
  end
  return reply(body)
end
local r = recs[sec]
if n ~= names or r == nil then return reply({'1', n, {}, '0'}) end
local vals = redis.call('HMGET', KEYS[5 + r.slot], 'st', 'm', 'b')
if not (vals[1] and vals[2] and vals[3]) then return reply({'1', n, {}, '0'}) end
return reply({'1', n, {}, '1', sec, stale_of(r), vals[1], vals[2], vals[3]})
`

const luaCachePend = `
local fid, sec, tags, meta, body = ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5]
local ttl_us, ttl_rem = tonumber(ARGV[6]), tonumber(ARGV[7])
local H, S = epochs(KEYS[3], KEYS[4])
local mi = hmap(KEYS[1])
if not key_ok(ARGV[8], cur_gen(mi, hmap(KEYS[2]))) or not epoch_ok(ARGV[9], H) or not epoch_ok(ARGV[10], S) then
  return {'refused'}
end
local du, dr = U + ttl_us, ttl_rem
redis.call('HSET', KEYS[5], 'f', fid, 's', sec, 'st', fmtint(U), 'dlu', fmtint(du), 'dlr', fmtint(dr), 'm', meta, 'b', body, 't', tags)
redis.call('PEXPIREAT', KEYS[5], fmtint(ceil_ms(du, dr)))
redis.call('HSET', KEYS[1], 'f:' .. fid, fmtint(du) .. ' ' .. fmtint(dr) .. '\n' .. tags)
index_finish(KEYS[1], H)
return {'ok', fmtint(U)}
`

const luaCacheJoin = `
local member = ARGV[1]
local nu, nr = tonumber(ARGV[2]), tonumber(ARGV[3])
local allow = tonumber(ARGV[4])
local bu, br = nu, nr
local xm = ceil_ms(nu + allow, nr)
for i = 1, 2 do
  local u, r = parse_dl(redis.call('HGET', KEYS[i], member))
  if u then bu, br = later(bu, br, u, r) end
  local x = tonumber(redis.call('HGET', KEYS[i], '!x'))
  if x and x > xm then xm = x end
end
local v = fmtint(bu) .. ' ' .. fmtint(br)
local ms = fmtint(xm)
for i = 1, 2 do
  redis.call('HSET', KEYS[i], member, v, '!x', ms)
  redis.call('PEXPIREAT', KEYS[i], ms)
end
return 'ok'
`

const luaCacheCommit = `
local fid, sec, names = ARGV[1], ARGV[2], ARGV[3]
local tok_g, tok_h, tok_s, base, tok_q = ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8]
local index, marker, pend = KEYS[1], KEYS[2], KEYS[5]
local mi = hmap(index)
local mk = hmap(marker)
local H, S, Q, xu, xr = epochs(KEYS[3], KEYS[4])
local pe = hmap(pend)
local pdu, pdr = parse_dl((pe.dlu or '') .. ' ' .. (pe.dlr or ''))
local refused = pe.f ~= fid or pe.st == nil or pdu == nil or mi['f:' .. fid] == nil or not key_ok(tok_g, cur_gen(mi, mk)) or not epoch_ok(tok_h, H) or not epoch_ok(tok_s, S)
if refused then
  redis.call('DEL', pend)
  redis.call('HDEL', index, 'f:' .. fid)
  index_finish(index, H)
  return {'refused'}
end
if H == '' then H = 'b' .. base end
if S == '' then S = 'b' .. base end
xu, xr = later(xu, xr, pdu, pdr)
put_epochs(KEYS[3], KEYS[4], H, S, Q, xu, xr)
local keep = {}
for f, v in pairs(mi) do
  if string.sub(f, 1, 2) == 'v:' then
    local name = string.sub(f, 3)
    local r = parse_v(v)
    if r and live(r, H) then
      keep[name] = r
    else
      drop_variant(index, name, r, r and KEYS[6 + r.slot])
    end
  end
end
local remembered = nil
if next(keep) ~= nil and (mi.n or '') ~= names then
  for name, r in pairs(keep) do drop_variant(index, name, r, KEYS[6 + r.slot]) end
  keep = {}
elseif keep[sec] ~= nil then
  local r = keep[sec]
  remembered = r.slot
  drop_variant(index, sec, r, KEYS[6 + r.slot])
  keep[sec] = nil
end
local cnt = 0
for _ in pairs(keep) do cnt = cnt + 1 end
while cnt >= 32 do
  local pn, pr = nil, nil
  for name, r in pairs(keep) do
    if pr == nil or r.st < pr.st or (r.st == pr.st and r.ins < pr.ins) then pn, pr = name, r end
  end
  drop_variant(index, pn, pr, KEYS[6 + pr.slot])
  keep[pn] = nil
  cnt = cnt - 1
end
local slot = remembered
if slot == nil then
  local used = {}
  for _, r in pairs(keep) do used[r.slot] = true end
  for i = 0, 31 do
    if not used[i] then
      slot = i
      break
    end
  end
end
local slotkey = KEYS[6 + slot]
redis.call('UNLINK', slotkey)
redis.call('RENAME', pend, slotkey)
local ins = (tonumber(mi.c) or 0) + 1
redis.call('HSET', index, 'n', names, 'c', fmtint(ins))
if (mi.g == nil or mi.g == '') and mk.g and mk.g ~= '' then redis.call('HSET', index, 'g', mk.g) end
redis.call('HSET', index, 'v:' .. sec, fmt_v({slot = slot, fid = fid, st = tonumber(pe.st), dlu = pdu, dlr = pdr, H = H, S = S, soft = 0, ins = ins, stamp = '-', tq = tok_q}))
redis.call('HDEL', index, 'f:' .. fid)
index_finish(index, H)
return {'stored'}
`

const luaCacheAbandon = `
local fid = ARGV[1]
local H = epochs(KEYS[2], KEYS[3])
redis.call('DEL', KEYS[4])
redis.call('HDEL', KEYS[1], 'f:' .. fid)
index_finish(KEYS[1], H)
return 'ok'
`

const luaCacheDelete = `
local index = KEYS[1]
local sec = ARGV[1]
local H = epochs(KEYS[2], KEYS[3])
local r = parse_v(redis.call('HGET', index, 'v:' .. sec))
if not (r and live(r, H) and slot_matches(KEYS[4 + r.slot], r.fid)) then return 0 end
drop_variant(index, sec, r, KEYS[4 + r.slot])
index_finish(index, H)
return 1
`

const luaCachePurgeKey = `
local soft = ARGV[1] == '1'
local gen = ARGV[2]
local ru, rr = tonumber(ARGV[3]), tonumber(ARGV[4])
local index = KEYS[1]
local H = epochs(KEYS[3], KEYS[4])
local mi = hmap(index)
local affected = 0
local hit = false
for f, v in pairs(mi) do
  if string.sub(f, 1, 2) == 'v:' then
    local name = string.sub(f, 3)
    local r = parse_v(v)
    if r and live(r, H) then
      if soft then
        hit = true
        r.soft = 1
        redis.call('HSET', index, f, fmt_v(r))
      else
        affected = affected + 1
        drop_variant(index, name, r, KEYS[5 + r.slot])
      end
    else
      drop_variant(index, name, r, r and KEYS[5 + r.slot])
    end
  end
end
if hit then affected = 1 end
local until_u, until_r = U + ru, rr
local nu, nr = until_u, until_r
local ou, orr = parse_dl(mi.u)
if ou then nu, nr = later(ou, orr, until_u, until_r) end
redis.call('HSET', index, 'g', gen, 'u', fmtint(nu) .. ' ' .. fmtint(nr))
local marker = KEYS[2]
local mk = hmap(marker)
local qu, qr = until_u, until_r
local mu, mr = parse_dl(mk.u)
if mu then qu, qr = later(mu, mr, until_u, until_r) end
redis.call('HSET', marker, 'g', gen, 'u', fmtint(qu) .. ' ' .. fmtint(qr))
redis.call('PEXPIREAT', marker, fmtint(ceil_ms(qu, qr)))
index_finish(index, H)
return fmtint(affected)
`

const luaCachePurgeAll = `
local soft = ARGV[1] == '1'
local nonce = ARGV[2]
local H, S, Q, xu, xr = epochs(KEYS[1], KEYS[2])
if soft then
  S = 'p' .. nonce
  if H == '' then H = 'b' .. nonce end
else
  H = 'p' .. nonce
  S = 'p' .. nonce
end
xu, xr = later(xu, xr, U + tonumber(ARGV[3]), tonumber(ARGV[4]))
put_epochs(KEYS[1], KEYS[2], H, S, Q, xu, xr)
return 'ok'
`

const luaCacheTagCounter = `
local a = hmap(KEYS[1])
local b = hmap(KEYS[2])
local av, bv = tonumber(a.v), tonumber(b.v)
local era, v
if a.e and av then
  era, v = a.e, av
elseif b.e and bv then
  era, v = b.e, bv
else
  era, v = ARGV[1], 0
end
v = v + 1
local nu, nr = U + tonumber(ARGV[2]) + tonumber(ARGV[4]), tonumber(ARGV[3])
local ou, orr = parse_dl(a.x or b.x)
if ou then nu, nr = later(ou, orr, nu, nr) end
local xv = fmtint(nu) .. ' ' .. fmtint(nr)
local ms = fmtint(ceil_ms(nu, nr))
for i = 1, 2 do
  redis.call('HSET', KEYS[i], 'e', era, 'v', fmtint(v), 'x', xv)
  redis.call('PEXPIREAT', KEYS[i], ms)
end
return {era, fmtint(v)}
`

const luaCacheTagBucket = `
local H, S, Q, xu, xr = epochs(KEYS[1], KEYS[2])
Q = qmax(Q, ARGV[1], tonumber(ARGV[2]))
xu, xr = later(xu, xr, U + tonumber(ARGV[3]) + tonumber(ARGV[5]), tonumber(ARGV[4]))
put_epochs(KEYS[1], KEYS[2], H, S, Q, xu, xr)
return 'ok'
`

const luaCacheTagMark = `
local a = hmap(KEYS[1])
local b = hmap(KEYS[2])
local function field(f)
  if a[f] ~= nil then return a[f] end
  return b[f]
end
local t = tonumber(field('t')) or 0
if U > t then t = U end
local q = qmax(field('q') or '', ARGV[1], tonumber(ARGV[2]))
local nu, nr = U + tonumber(ARGV[3]), tonumber(ARGV[4])
local ou, orr = parse_dl(field('u'))
if ou then nu, nr = later(ou, orr, nu, nr) end
local uv = fmtint(nu) .. ' ' .. fmtint(nr)
local ms = fmtint(ceil_ms(nu, nr))
for i = 1, 2 do
  redis.call('HSET', KEYS[i], 't', fmtint(t), 'q', q, 'u', uv)
  redis.call('PEXPIREAT', KEYS[i], ms)
end
return 'ok'
`

const luaCacheTagPrimary = `
local soft = ARGV[1] == '1'
local tag = ARGV[2]
local stamp = ARGV[3]
local era = ARGV[4]
local v = tonumber(ARGV[5])
local k = tonumber(ARGV[6])
local index = KEYS[1]
local H = epochs(KEYS[2], KEYS[3])
local mi = hmap(index)
for f, val in pairs(mi) do
  if string.sub(f, 1, 2) == 'f:' then
    local ls = lines_of(val)
    for j = 2, #ls do
      if ls[j] == tag then
        redis.call('HDEL', index, f)
        break
      end
    end
  end
end
local affected = 0
local dead = {}
for j = 1, k do
  local name = ARGV[6 + j]
  local raw = redis.call('HGET', index, 'v:' .. name)
  local r = parse_v(raw)
  local sk = r and KEYS[4 + r.slot]
  if not (r and live(r, H) and slot_matches(sk, r.fid)) then
    if raw then drop_variant(index, name, r, sk) end
    dead[#dead + 1] = name
  else
    local t = redis.call('HGET', sk, 't')
    if t and has_line(t, tag) then
      local skip = r.stamp == stamp
      local ce, cv = string.match(r.tq, '^(%x+):(%d+)$')
      if ce == era and tonumber(cv) >= v then skip = true end
      if not skip then
        affected = affected + 1
        if soft then
          r.soft = 1
          r.stamp = stamp
          redis.call('HSET', index, 'v:' .. name, fmt_v(r))
        else
          drop_variant(index, name, r, sk)
        end
      end
    end
  end
end
index_finish(index, H)
return {fmtint(affected), dead}
`

const luaCacheTagPrune = `
local allow = tonumber(ARGV[1])
local k = tonumber(ARGV[2])
for i = 1, 2 do
  for j = 1, k do
    local member = ARGV[2 + j]
    local u, r = parse_dl(redis.call('HGET', KEYS[i], member))
    if u and not alive(u + allow, r) then redis.call('HDEL', KEYS[i], member) end
  end
end
return 'ok'
`
