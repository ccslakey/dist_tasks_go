package redisstream

import "github.com/redis/go-redis/v9"

// promoteRetriesSrc moves every retry-schedule entry whose score (its next-run
// time, in Unix ms) has passed back onto the main stream.
//
// Promotion is a read-modify-write across two keys: find due entries, re-add
// them to the stream, drop them from the schedule. Run as three round trips
// from Go, two workers polling at the same instant both see the same due entry
// and both re-add it, so the retried job runs twice. Redis runs a script to
// completion before serving anything else, so as a script only one worker can
// ever observe a given entry as due.
//
// Entries are stored as flat string maps rather than as a marshalled Job, so
// the script can hand the fields straight to XADD. Decoding a Job here would
// mean round-tripping the job payload through cjson, which does not preserve
// key order and loses precision on large integers.
//
//	KEYS[1] retry sorted set
//	KEYS[2] main stream
//	ARGV[1] now, Unix ms
//	ARGV[2] max entries to promote in one call
//	ARGV[3] approximate MAXLEN for the main stream, 0 to disable trimming
//
// Returns the number of entries promoted.
const promoteRetriesSrc = `
local due    = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local maxlen = tonumber(ARGV[3])
local promoted = 0

for i = 1, #due do
  local member = due[i]
  local ok, job = pcall(cjson.decode, member)

  if ok and type(job) == 'table' and job.task then
    local args = { KEYS[2] }
    if maxlen > 0 then
      args[#args + 1] = 'MAXLEN'
      args[#args + 1] = '~'
      args[#args + 1] = maxlen
    end
    args[#args + 1] = '*'

    local fields = { 'task', 'payload', 'attempt', 'maxAttempts', 'traceID', 'createdAt', 'availableAt' }
    for f = 1, #fields do
      args[#args + 1] = fields[f]
      args[#args + 1] = job[fields[f]] or ''
    end

    redis.call('XADD', unpack(args))
    promoted = promoted + 1
  end

  -- Drop the entry either way. A member that cannot be decoded is corrupt and
  -- would otherwise be retried on every single poll, forever.
  redis.call('ZREM', KEYS[1], member)
end

return promoted
`

// promoteRetriesScript is evaluated with EVALSHA and falls back to EVAL on the
// first call against a fresh Redis, so the source is shipped over the wire once
// per server rather than once per poll.
var promoteRetriesScript = redis.NewScript(promoteRetriesSrc)
