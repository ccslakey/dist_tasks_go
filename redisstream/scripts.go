package redisstream

import "github.com/redis/go-redis/v9"

// Every script that changes a delivered job's state starts with this check.
// A delivery is identified by its stream entry ID; the caller must still be
// the consumer that holds it in the PEL, with the same delivery count it was
// handed. The delivery count works as a fencing token: XAUTOCLAIM bumps it, so
// a worker whose lease expired and was recovered can't settle or extend the
// job afterwards, even if it later reuses the same consumer name.
//
// This is the only place the check is written; every fenced script, single
// or batched, calls it.
//
// The single-delivery scripts below take ARGV[1]=group ARGV[2]=message ID
// ARGV[3]=owner ARGV[4]=token.
const leaseLua = `
local function owns(stream, group, id, owner, token)
  local p = redis.call('XPENDING', stream, group, id, id, 1)
  if #p == 0 then return false end
  return p[1][2] == owner and p[1][4] == tonumber(token)
end

local function owns_argv(stream)
  return owns(stream, ARGV[1], ARGV[2], ARGV[3], ARGV[4])
end

-- settle removes a delivered entry from the PEL and the stream. Deleting it
-- keeps XLEN equal to the backlog instead of growing forever.
local function settle(stream, group, id)
  redis.call('XACK', stream, group, id)
  redis.call('XDEL', stream, id)
end

-- renew resets a delivery's idle time without bumping its delivery count.
local function renew(stream, group, id, owner)
  redis.call('XCLAIM', stream, group, owner, 0, id, 'JUSTID')
end

-- each_owned runs action(id, owner) for every delivery in ARGV[2..] (message
-- ID, owner, token triples; ARGV[1]=group) that the caller still owns, and
-- returns one 1 (done) or 0 (lease lost) per delivery.
local function each_owned(stream, action)
  local out = {}
  for i = 2, #ARGV, 3 do
    if owns(stream, ARGV[1], ARGV[i], ARGV[i + 1], ARGV[i + 2]) then
      action(ARGV[i], ARGV[i + 1])
      out[#out + 1] = 1
    else
      out[#out + 1] = 0
    end
  end
  return out
end
`

// xaddJSONLua adds a stream entry from a job's fields stored as JSON (the
// form the schedule hash and enqueue use).
const xaddJSONLua = `
local function xadd_json(stream, fields_json)
  local fields = {}
  for k, v in pairs(cjson.decode(fields_json)) do
    fields[#fields + 1] = k
    fields[#fields + 1] = v
  end
  redis.call('XADD', stream, '*', unpack(fields))
end
`

// KEYS[1]=stream
var ackScript = redis.NewScript(leaseLua + `
if not owns_argv(KEYS[1]) then return 0 end
settle(KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// KEYS[1]=stream
var extendScript = redis.NewScript(leaseLua + `
if not owns_argv(KEYS[1]) then return 0 end
renew(KEYS[1], ARGV[1], ARGV[2], ARGV[3])
return 1
`)

// Moves the job to the retry schedule and settles the current delivery in one
// step, so a crash can't leave it both scheduled and pending.
// KEYS[1]=stream KEYS[2]=schedule zset KEYS[3]=schedule data hash
// ARGV[5]=run-at (unix ms) ARGV[6]=job ID ARGV[7]=job fields as JSON
var retryScript = redis.NewScript(leaseLua + `
if not owns_argv(KEYS[1]) then return 0 end
redis.call('HSET', KEYS[3], ARGV[6], ARGV[7])
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[6])
settle(KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// KEYS[1]=stream KEYS[2]=dead-letter stream
// ARGV[5..]=field/value pairs for the dead-letter entry
var deadLetterScript = redis.NewScript(leaseLua + `
if not owns_argv(KEYS[1]) then return 0 end
redis.call('XADD', KEYS[2], '*', unpack(ARGV, 5))
settle(KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// Moves due entries from the schedule back onto the stream. Running inside a
// script makes it one atomic step, so concurrent callers can't promote the
// same job twice.
const promoteLua = xaddJSONLua + `
local function promote(zset, data, stream, now, limit)
  local ids = redis.call('ZRANGEBYSCORE', zset, '-inf', now, 'LIMIT', 0, limit)
  for _, id in ipairs(ids) do
    local fields_json = redis.call('HGET', data, id)
    if fields_json then
      xadd_json(stream, fields_json)
    end
    redis.call('HDEL', data, id)
    redis.call('ZREM', zset, id)
  end
  return #ids
end
`

// KEYS[1]=schedule zset KEYS[2]=schedule data hash KEYS[3]=stream
// ARGV[1]=now (unix ms) ARGV[2]=limit
var promoteScript = redis.NewScript(promoteLua + `
return promote(KEYS[1], KEYS[2], KEYS[3], ARGV[1], ARGV[2])
`)

// Promotes due retries and then claims up to ARGV[3] new entries, in one
// round trip. XREADGROUP can't block inside a script, so callers that want to
// wait for new jobs follow an empty result with a blocking XREADGROUP.
// KEYS[1]=stream KEYS[2]=schedule zset KEYS[3]=schedule data hash
// ARGV[1]=group ARGV[2]=consumer ARGV[3]=count ARGV[4]=now (unix ms)
// Returns {{id, fields}, ...}
var claimScript = redis.NewScript(promoteLua + `
promote(KEYS[2], KEYS[3], KEYS[1], ARGV[4], 100)
local res = redis.call('XREADGROUP', 'GROUP', ARGV[1], ARGV[2], 'COUNT', ARGV[3], 'STREAMS', KEYS[1], '>')
if not res then return {} end
return res[1][2]
`)

// Claims up to ARGV[4] deliveries that have been idle at least ARGV[3] ms and
// returns each one with its new delivery count (the caller's fencing token).
// XAUTOCLAIM only examines a bounded slice of the PEL per call, so this keeps
// scanning from the caller's cursor and returns where it stopped.
// KEYS[1]=stream ARGV[1]=group ARGV[2]=consumer ARGV[3]=min idle ms
// ARGV[4]=count ARGV[5]=start cursor
// Returns {next cursor, {{id, fields, delivery count}, ...}}
var recoverScript = redis.NewScript(`
local cursor = ARGV[5]
local want = tonumber(ARGV[4])
local out = {}
for _ = 1, 10 do
  local res = redis.call('XAUTOCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], cursor, 'COUNT', want - #out)
  cursor = res[1]
  for _, m in ipairs(res[2]) do
    local p = redis.call('XPENDING', KEYS[1], ARGV[1], m[1], m[1], 1)
    out[#out + 1] = {m[1], m[2], p[1][4]}
  end
  if #out >= want or cursor == '0-0' then break end
end
return {cursor, out}
`)

// Removes a consumer from the group if it holds no pending entries.
// DELCONSUMER drops a consumer's pending entries along with it, so the check
// and the delete have to be one step.
// KEYS[1]=stream ARGV[1]=group ARGV[2]=consumer
var deregisterScript = redis.NewScript(`
if #redis.call('XPENDING', KEYS[1], ARGV[1], '-', '+', 1, ARGV[2]) > 0 then return 0 end
redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// Removes consumers that hold nothing and have been idle at least ARGV[2] ms:
// workers that crashed or were replaced. A live worker that gets pruned is
// re-created by its next XREADGROUP.
// KEYS[1]=stream ARGV[1]=group ARGV[2]=min idle ms
var pruneConsumersScript = redis.NewScript(`
local removed = 0
for _, c in ipairs(redis.call('XINFO', 'CONSUMERS', KEYS[1], ARGV[1])) do
  local f = {}
  for i = 1, #c, 2 do f[c[i]] = c[i + 1] end
  if f['pending'] == 0 and f['idle'] >= tonumber(ARGV[2]) then
    redis.call('XGROUP', 'DELCONSUMER', KEYS[1], ARGV[1], f['name'])
    removed = removed + 1
  end
end
return removed
`)

// Enqueues a job unless its idempotency key was used within the dedupe
// window, in which case it returns the original job's ID. The check and the
// write are one step, so concurrent retries of the same enqueue can't both win.
// KEYS[1]=dedupe key KEYS[2]=stream KEYS[3]=schedule zset KEYS[4]=schedule data hash
// ARGV[1]=job ID ARGV[2]=dedupe window ms ARGV[3]=run-at unix ms (0 = now)
// ARGV[4]=job fields as JSON
var enqueueOnceScript = redis.NewScript(xaddJSONLua + `
local existing = redis.call('GET', KEYS[1])
if existing then return existing end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
if tonumber(ARGV[3]) > 0 then
  redis.call('HSET', KEYS[4], ARGV[1], ARGV[4])
  redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
else
  xadd_json(KEYS[2], ARGV[4])
end
return ARGV[1]
`)

// Acks a batch of deliveries in one round trip, each fenced like ackScript.
// KEYS[1]=stream ARGV[1]=group, then ARGV[2..] = message ID, owner, token
// triples. Returns one 1 (acked) or 0 (lease lost) per delivery.
var ackBatchScript = redis.NewScript(leaseLua + `
return each_owned(KEYS[1], function(id) settle(KEYS[1], ARGV[1], id) end)
`)

// Renews a batch of leases in one round trip, each fenced like extendScript.
// KEYS[1]=stream ARGV[1]=group, then ARGV[2..] = message ID, owner, token
// triples. Returns one 1 (renewed) or 0 (lease lost) per delivery.
var extendBatchScript = redis.NewScript(leaseLua + `
return each_owned(KEYS[1], function(id, owner) renew(KEYS[1], ARGV[1], id, owner) end)
`)
