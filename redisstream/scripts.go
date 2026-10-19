package redisstream

import "github.com/redis/go-redis/v9"

// Every script that changes a delivered job's state starts with this check.
// A delivery is identified by its stream entry ID; the caller must still be
// the consumer that holds it in the PEL, with the same delivery count it was
// handed. The delivery count works as a fencing token: XAUTOCLAIM bumps it, so
// a worker whose lease expired and was recovered can't settle or extend the
// job afterwards, even if it later reuses the same consumer name.
//
// ARGV[1]=group ARGV[2]=message ID ARGV[3]=owner ARGV[4]=token
const ownsLua = `
local function owns(stream)
  local p = redis.call('XPENDING', stream, ARGV[1], ARGV[2], ARGV[2], 1)
  if #p == 0 then return false end
  return p[1][2] == ARGV[3] and p[1][4] == tonumber(ARGV[4])
end
`

// settle removes a delivered entry from the PEL and the stream. Deleting it
// keeps XLEN equal to the backlog instead of growing forever.
const settleLua = `
local function settle(stream)
  redis.call('XACK', stream, ARGV[1], ARGV[2])
  redis.call('XDEL', stream, ARGV[2])
end
`

// KEYS[1]=stream
var ackScript = redis.NewScript(ownsLua + settleLua + `
if not owns(KEYS[1]) then return 0 end
settle(KEYS[1])
return 1
`)

// Resets the delivery's idle time without bumping its delivery count.
// KEYS[1]=stream
var extendScript = redis.NewScript(ownsLua + `
if not owns(KEYS[1]) then return 0 end
redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], 'JUSTID')
return 1
`)

// Moves the job to the retry schedule and settles the current delivery in one
// step, so a crash can't leave it both scheduled and pending.
// KEYS[1]=stream KEYS[2]=schedule zset KEYS[3]=schedule data hash
// ARGV[5]=run-at (unix ms) ARGV[6]=job ID ARGV[7]=job fields as JSON
var retryScript = redis.NewScript(ownsLua + settleLua + `
if not owns(KEYS[1]) then return 0 end
redis.call('HSET', KEYS[3], ARGV[6], ARGV[7])
redis.call('ZADD', KEYS[2], ARGV[5], ARGV[6])
settle(KEYS[1])
return 1
`)

// KEYS[1]=stream KEYS[2]=dead-letter stream
// ARGV[5..]=field/value pairs for the dead-letter entry
var deadLetterScript = redis.NewScript(ownsLua + settleLua + `
if not owns(KEYS[1]) then return 0 end
redis.call('XADD', KEYS[2], '*', unpack(ARGV, 5))
settle(KEYS[1])
return 1
`)

// Moves due entries from the schedule back onto the stream. It runs as one
// atomic step, so concurrent callers can't promote the same job twice.
// KEYS[1]=schedule zset KEYS[2]=schedule data hash KEYS[3]=stream
// ARGV[1]=now (unix ms) ARGV[2]=limit
var promoteScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, id in ipairs(ids) do
  local data = redis.call('HGET', KEYS[2], id)
  if data then
    local fields = {}
    for k, v in pairs(cjson.decode(data)) do
      fields[#fields + 1] = k
      fields[#fields + 1] = v
    end
    redis.call('XADD', KEYS[3], '*', unpack(fields))
  end
  redis.call('HDEL', KEYS[2], id)
  redis.call('ZREM', KEYS[1], id)
end
return #ids
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
