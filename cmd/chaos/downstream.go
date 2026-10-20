package main

import (
	"context"

	"dist_tasks_go/queue"

	"github.com/redis/go-redis/v9"
)

// downstream simulates the external system a job's side effect lands in (a
// payments ledger, say). The queue delivers at-least-once; this is what turns
// that into exactly-once effects. It does two checks:
//
//   - Fencing: it remembers the highest fencing token it has seen for each job
//     and rejects anything older. That stops a zombie worker, paused until its
//     lease was handed to someone else, from applying a stale effect.
//   - Idempotency: it applies each job ID's effect at most once. That absorbs
//     the redelivery after a worker crashes between the effect and the ack.
//
// The queue can't do this for you: once a side effect leaves the process, only
// the system receiving it can refuse a duplicate.
type downstream struct {
	rdb    redis.UniversalClient
	prefix string
}

// keys share the {ds} hash tag so the scripts can use them all on a cluster.
func (d downstream) keys() []string {
	return []string{
		d.prefix + "{ds}:fence",    // job ID -> highest fencing token seen
		d.prefix + "{ds}:outcomes", // outcome -> count
		d.prefix + "{ds}:applied",  // job ID -> payload n, once applied
		d.prefix + "{ds}:effects",  // payload n -> effects applied (must end at 1)
	}
}

const fenceLua = `
local function fence()
  local seen = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '-1')
  if tonumber(ARGV[2]) < seen then
    redis.call('HINCRBY', KEYS[2], 'fenced', 1)
    return false
  end
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  return true
end
`

// ARGV[1]=job ID ARGV[2]=fencing token
var beginScript = redis.NewScript(fenceLua + `
if not fence() then return 0 end
return 1
`)

// ARGV[1]=job ID ARGV[2]=fencing token ARGV[3]=payload n
var applyScript = redis.NewScript(fenceLua + `
if not fence() then return 'fenced' end
if redis.call('HSETNX', KEYS[3], ARGV[1], ARGV[3]) == 0 then
  redis.call('HINCRBY', KEYS[2], 'deduped', 1)
  return 'deduped'
end
redis.call('HINCRBY', KEYS[4], ARGV[3], 1)
redis.call('HINCRBY', KEYS[2], 'applied', 1)
return 'applied'
`)

// begin registers a run's fencing token before it starts work, so a run that
// was superseded while paused is rejected even if it's the first to write.
func (d downstream) begin(ctx context.Context, job queue.Job) (bool, error) {
	ok, err := beginScript.Run(ctx, d.rdb, d.keys(), string(job.ID), job.FencingToken()).Bool()
	return ok, err
}

// apply performs the job's effect and reports "applied", "deduped", or "fenced".
func (d downstream) apply(ctx context.Context, job queue.Job, n int) (string, error) {
	return applyScript.Run(ctx, d.rdb, d.keys(), string(job.ID), job.FencingToken(), n).Text()
}
