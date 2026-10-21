# Demo Walkthrough

A hands-on tour of the queue, from a single job in Redis to worker processes being killed under load. Each part stands on its own, so you can skip around. Expect about 30 minutes for parts 1–5 and another 15 for the rest.

| Part | What you'll see | Needs |
|---|---|---|
| [0. Setup](#0-setup) | Build the binaries, start Redis | Go, Docker |
| [1. Tests](#1-run-the-tests) | Unit and Redis integration tests | Redis |
| [2. One job's life](#2-one-jobs-life-in-redis) | A job's fields in the stream, retries, the dead-letter stream | Redis, 2 terminals |
| [3. Crash recovery](#3-crash-recovery-and-heartbeats) | A SIGKILLed worker's jobs re-delivered; long jobs kept by heartbeats | Redis, 2 terminals |
| [4. Zombie workers](#4-zombie-workers-and-fencing) | A frozen worker wakes up and its results are rejected | Redis, 3 terminals |
| [5. Graceful shutdown](#5-graceful-shutdown) | Ctrl-C drains in-flight jobs before exiting | Redis, 2 terminals |
| [6. Full stack](#6-the-full-stack-grafana-prometheus-jaeger) | Grafana, Prometheus, and Jaeger while you kill and freeze containers | Docker |
| [7. Chaos test](#7-chaos-test) | Thousands of jobs, workers killed and paused, guarantees checked | Redis |
| [8. Benchmarks](#8-benchmarks) | Throughput and pickup latency, polling vs. blocking claims | Redis |
| [9. Redis Cluster](#9-sharding-on-a-redis-cluster-optional) | The sharded backend on a local 3-node cluster | `redis-server` locally |
| [10. Code tour](#10-code-tour) | Where each behavior above lives in the code | — |

## 0. Setup

You need Go (see `go.mod` for the version) and Docker. Build the demo binaries once so you can kill them by PID later (a `go run` wrapper would get in the way):

```sh
go build -o /tmp/dtq/ ./cmd/...
export PATH=/tmp/dtq:$PATH          # enqueue-demo, worker-demo, chaos, bench
```

Start Redis on its own, without the rest of the demo stack. If the full stack from part 6 is already running, stop its workers and producer first, or they'll take your jobs:

```sh
docker compose up -d redis
docker compose stop worker producer   # only if the full stack is running
```

Use `redis-cli` to look inside Redis. If you don't have it installed, this alias runs the one in the container (run it from the repo root):

```sh
alias rcli='docker compose exec redis redis-cli'   # or: alias rcli=redis-cli
rcli FLUSHALL                                      # start from an empty Redis
```

Run every command below from the repo root, and open each extra terminal there too, with the same `PATH` and alias.

## 1. Run the tests

```sh
go test ./...
```

The integration tests connect to `localhost:6379` (override with `TEST_REDIS_ADDR`) and skip if Redis isn't reachable. They run `FLUSHDB` when they finish, so don't run them while something else uses that Redis.

The test names read as a spec. Skim them to see what's covered:

```sh
grep -h '^func Test' queue/*_test.go redisstream/*_test.go | sed 's/func \(Test[A-Za-z]*\).*/\1/'
```

The failure-mode table in the [README](../README.md#delivery-guarantees) maps each failure to the test that proves it's handled.

## 2. One job's life in Redis

**Enqueue with no workers running.** Five emails that succeed, two "flaky" jobs that fail twice before succeeding, and two that always fail:

```sh
enqueue-demo -n 5 -delay 0
enqueue-demo -n 2 -delay 0 -task flaky_email
enqueue-demo -n 2 -delay 0 -task permanent_fail
```

They're all waiting in the main stream:

```sh
rcli XLEN dtq:jobs                      # 9
rcli XRANGE dtq:jobs - + COUNT 1        # one job's fields
```

A job is a stream entry with `id` (a UUID that stays the same across retries and redeliveries), `task`, `payload`, `attempt`, `maxAttempts`, `traceparent`, and timestamps.

**Start a worker** in a second terminal. `-metrics-addr ""` turns off its metrics server, whose default `:9090` would clash with Prometheus from part 6:

```sh
worker-demo -metrics-addr ""
```

In the logs you'll see:

- five `sending email` lines
- `permanent failure (demo)` followed by `job dead-lettered … reason=this task always fails`
- `flaky handler failing (will retry)` at `attempt=0`, again at `attempt=1` about a second later, then `flaky handler succeeded` at `attempt=2`

Back in the first terminal, run these straight away to catch the retries while they're scheduled. Each retry waits in a sorted set scored by its run-at time in Unix ms, and its fields wait in a hash:

```sh
rcli ZRANGE dtq:retry 0 -1 WITHSCORES
rcli HGETALL dtq:retry:jobs
```

A few seconds later, everything has settled:

```sh
rcli XLEN dtq:jobs                      # 0: settled entries are deleted, so XLEN is the backlog
rcli ZCARD dtq:retry                    # 0
rcli XLEN dtq:dead                      # 2
rcli XREVRANGE dtq:dead + - COUNT 1     # the job, plus reason, deliveries, failedAt
```

Each of those moves (ack, schedule a retry, dead-letter) is a single Lua script in [redisstream/scripts.go](../redisstream/scripts.go), so no crash can leave a job half-moved. Stop the worker with Ctrl-C.

## 3. Crash recovery and heartbeats

Start a worker with a 5s lease (the visibility timeout) instead of the default 30s, so you don't have to wait as long:

```sh
worker-demo -metrics-addr "" -visibility 5s     # terminal 2
```

In terminal 1, enqueue four jobs that each take 10 seconds, then SIGKILL the worker while they run. Its PID is the middle part of the `id=` in its `worker started` line, or use `pgrep`:

```sh
enqueue-demo -n 4 -delay 0 -latency 10s
kill -9 $(pgrep -x worker-demo)
```

The jobs aren't lost. They're in the Pending Entries List (PEL), still leased to the dead worker:

```sh
rcli XPENDING dtq:jobs dtq:workers                  # 4 pending, all owned by the dead consumer
rcli XPENDING dtq:jobs dtq:workers - + 10           # per entry: owner, idle ms, delivery count (1)
```

Start a replacement in terminal 2:

```sh
worker-demo -metrics-addr "" -visibility 5s
```

Once the dead worker's leases have gone 5s without a heartbeat, the new worker logs `recovered expired job … delivery=2` four times. Recovery is `XAUTOCLAIM` (`recoverScript`), and the delivery count it bumps doubles as the lease's fencing token.

**Heartbeats.** These jobs run for 10s on a 5s lease, yet the new worker keeps them. While a handler runs, the worker renews its lease (`XCLAIM … JUSTID`), which resets the entry's idle time without bumping its delivery count. While they run, check:

```sh
rcli XPENDING dtq:jobs dtq:workers - + 10     # idle stays under 5000 ms; delivery count stays 2
```

About 10s later the worker logs `sending email` four times and the PEL is empty. Leave this worker running for part 4.

## 4. Zombie workers and fencing

A worker that freezes (a GC pause, a suspended VM, a network partition) can wake up after its lease was given to someone else. It must not be able to ack, retry, or dead-letter the job any more.

Use the worker still running in terminal 2 from part 3 as worker A (or start one with `-visibility 5s`). In terminal 1, enqueue two 8-second jobs, then **immediately press Ctrl-Z in terminal 2**. Ctrl-Z stops the process just like SIGSTOP:

```sh
enqueue-demo -n 2 -delay 0 -latency 8s
```

In a third terminal, start worker B:

```sh
worker-demo -metrics-addr "" -visibility 5s
```

After about 5 seconds B logs `recovered expired job … delivery=2` for both jobs. Now bring A back with `fg` in terminal 2. A's handlers finish (`sending email`), but every settle call is rejected:

```
WARN  lease lost; another worker owns this job now  job_id=…  task=send_email
```

You may also see a `Redis didn't end a blocking read on time` warning from A; that's its blocking read timing out while the process was frozen. B finishes the jobs and acks them.

How the rejection works: every script that changes a delivered job's state first checks that the caller is still the job's owner in the PEL **and** that the delivery count matches the one it was handed (`leaseLua` in [redisstream/scripts.go](../redisstream/scripts.go)). Recovery bumped the count from 1 to 2, so A's token of 1 is stale.

Notice that A's handler *did* run to the end. If it had charged a card, that side effect has already happened. The queue can't take it back; only the downstream system can refuse it. Handlers get `Job.ID` (stable) to dedupe on and `Job.FencingToken()` (increases with every retry and redelivery) to reject stale writers. Part 7 checks this end to end.

Stop both workers with Ctrl-C.

## 5. Graceful shutdown

```sh
worker-demo -metrics-addr ""                     # terminal 2
enqueue-demo -n 3 -delay 0 -latency 4s           # terminal 1
```

Press Ctrl-C in terminal 2 right after the enqueue. The worker stops claiming but doesn't exit straight away: about 4 seconds later it logs three `sending email` lines, acks them, and exits. Nothing is left behind:

```sh
rcli XLEN dtq:jobs                       # 0
rcli XPENDING dtq:jobs dtq:workers       # 0 pending
```

Handlers run on a context detached from the shutdown signal. Only after `ShutdownTimeout` (the visibility timeout by default) are they cancelled, and anything still unsettled is recovered by another worker.

## 6. The full stack: Grafana, Prometheus, Jaeger

```sh
docker compose up -d --build     # Redis, 3 workers, a producer, Prometheus, Grafana, Jaeger
```

If a port is already taken, override it, e.g. `REDIS_PORT=6380 GRAFANA_PORT=3001 docker compose up -d --build` (`PROMETHEUS_PORT` and `JAEGER_PORT` work too).

The producer enqueues ~50 jobs/s: mostly successes, some `flaky_email` jobs that retry twice, and a few permanent failures. Each handler sleeps 200–600ms. The workers run with 5s leases.

**Grafana** at <http://localhost:3000> (no login) opens on the queue dashboard. Give it 30 seconds to fill in. If it says "Failed to load home dashboard", the containers were started before `deploy/` was last checked out and still see the old files; run `docker compose up -d --force-recreate grafana prometheus`. The top row shows workers up, completions per second, backlog, in-flight jobs, scheduled retries, and dead-letter depth.

**Kill a worker mid-job:**

```sh
./scripts/kill-worker.sh
```

The script SIGKILLs a random worker container, waits 3 seconds, and starts a replacement. Watch:

- *Workers up* drops to 2, then returns to 3.
- In *Jobs started per worker*, one line ends and a new one begins (the replacement).
- *Completed / s* dips briefly while the dead worker's jobs wait out their 5s leases, then recovers.

The dead worker's jobs are visible in Redis until recovery takes them. Run this right after the kill:

```sh
rcli XINFO CONSUMERS dtq:jobs dtq:workers   # one consumer with pending > 0 and a growing inactive time
```

Once recovery has taken its jobs, its pending count drops to 0. The empty consumer stays in the group until a worker prunes it after an hour idle (`ConsumerIdleTimeout`).

**Freeze a worker past its lease:**

```sh
./scripts/kill-worker.sh -pause 10
```

The script `docker pause`s a worker for 10 seconds, twice its 5s lease. When it resumes, its stale results are rejected and *Leases lost (zombie results rejected)* spikes. `docker compose logs worker | grep "lease lost"` shows the same thing from the logs.

**Traces** at <http://localhost:16686>: pick service `dtq-worker` and operation `process flaky_email`. One trace holds the producer's enqueue span and every attempt, from whichever workers ran them, with the two failures and the final success. The trace context travels in the job's `traceparent` field.

**Prometheus** at <http://localhost:9090> scrapes each worker replica (found through Docker DNS) and the producer every 2s. Some queries to try:

```promql
sum by (instance) (rate(dtq_jobs_started_total[1m]))
histogram_quantile(0.99, sum by (le) (rate(dtq_job_pickup_seconds_bucket[1m])))
dtq_job_leases_lost_total
dtq_stream_length
```

All metrics are named `dtq_*`; [prommetrics/metrics.go](../prommetrics/metrics.go) lists them.

When you're done:

```sh
docker compose down              # add -v to also delete Redis's data volume
```

## 7. Chaos test

[cmd/chaos](../cmd/chaos/main.go) is the end-to-end proof. It enqueues thousands of jobs, sends 10% of the enqueues twice (as a producer retrying after a lost response would), and runs worker processes while it SIGKILLs one about every second and SIGSTOPs one past its lease about every 4 seconds. Each job applies a side effect through a simulated downstream that dedupes on `Job.ID` and rejects stale fencing tokens ([cmd/chaos/downstream.go](../cmd/chaos/downstream.go)).

```sh
chaos -jobs 2000 -duration 8s     # ~15s; the default (10k jobs, up to 30s of faults) is a fuller run
```

It uses its own key prefix and cleans up afterwards, so it won't touch the `dtq:*` keys. Output from a real run:

```
  faults injected: 7 SIGKILLs, 1 SIGSTOP pauses, elapsed 12.1s

  jobs enqueued                         2000  (200 enqueues retried and deduped)
  completed                             1960
  dead-lettered (permanent failures)      40
  lost                                     0  ok
  permanent failures misrouted             0  ok
  left in queue                            0  ok (stream 0, pending 0, scheduled 0)
  jobs with duplicate side effects         0  ok
  jobs missing their effect                0  ok

  at-least-once delivery, absorbed downstream:
  duplicate runs                          10  crashed or paused between side effect and ack
  jobs with overlapping runs               7  ran concurrently; only paused zombies should do this
  effects deduped by job ID                1  same job ID applied again
  effects rejected by fencing              7  stale fencing token
  zombie queue updates rejected            3  stale results fenced off by lease checks
  consumers left in group                  5  killed workers not yet pruned

PASS
```

How to read it:

- The `ok` lines are the guarantees: nothing lost, nothing stuck, every effect applied exactly once.
- The second block is the price of at-least-once delivery, and how it's absorbed. *Duplicate runs* happen; *duplicate side effects* don't, because the downstream dedupes on the job ID and fences out zombies.
- The numbers vary from run to run; the `ok` lines shouldn't.

More to try:

```sh
chaos -jobs 20000 -workers 8 -kill-every 300ms      # harsher
chaos -jobs 5000 -duration 15s -partitions 8        # the sharded backend
chaos -jobs 2000 -duration 8s -v                    # stream every worker's logs
chaos -jobs 2000 -duration 8s -keep                 # keep the run's Redis keys to inspect
```

## 8. Benchmarks

[cmd/bench](../cmd/bench/main.go) measures throughput (drain a pre-built backlog of no-op jobs) and pickup latency (enqueue to handler start at a steady 200 jobs/s). Each worker is a separate process.

A quick comparison of polling and blocking claims (about a minute):

```sh
bench -modes poll,block -workers 1,4 -jobs 20000 -latency-jobs 200
```

```
| mode | workers | throughput (jobs/s) | redis cpu | enqueue (jobs/s) | pickup p50 | pickup p95 | pickup p99 |
|---|---|---|---|---|---|---|---|
| poll | 1 | 20,597 | 96% | 311,325 | 50.7ms | 99.4ms | 104.7ms |
| poll | 4 | 36,749 | 96% | 316,933 | 45.9ms | 93.8ms | 99.9ms |
| block | 1 | 20,714 | 96% | 329,900 | 0.7ms | 2.7ms | 9.5ms |
| block | 4 | 37,010 | 98% | 351,332 | 0.7ms | 1.9ms | 4.1ms |
```

Throughput is the same in both modes: under a backlog, neither one sleeps. Pickup latency drops from ~50ms to under 1ms, because a polling worker sleeps up to its poll interval when the queue is empty and a blocking one is woken by `XREADGROUP` as soon as a job lands. Note the Redis CPU column: one Redis core is the bottleneck.

The headline numbers in the README come from a heavier run:

```sh
bench -modes block -workers 8 -concurrency 256 -jobs 500000
bench -modes block -workers 8 -concurrency 256 -jobs 500000 -partitions 16
```

Use a Redis with persistence off for benchmarks; the compose Redis has AOF on.

## 9. Sharding on a Redis Cluster (optional)

This needs `redis-server` installed locally (e.g. `brew install redis`). The script starts a 3-node cluster on ports 30001–30003:

```sh
scripts/redis-cluster.sh start
scripts/redis-cluster.sh status

CLUSTER=127.0.0.1:30001,127.0.0.1:30002,127.0.0.1:30003
TEST_REDIS_CLUSTER=$CLUSTER go test ./redisstream/ -run Sharded
chaos -jobs 5000 -duration 15s -cluster $CLUSTER
bench -modes block -workers 8 -concurrency 256 -cluster $CLUSTER

scripts/redis-cluster.sh stop
```

Each partition's keys share a hash tag (`dtq:{p3}:jobs`, `dtq:{p3}:retry`, …), so every Lua script stays on one slot while the partitions spread across nodes. The README's [Sharding](../README.md#sharding) section covers routing, claiming, and the trade-offs.

## 10. Code tour

A reading order that follows a job through the system:

1. [queue/job.go](../queue/job.go): `Job`, `Lease`, `FencingToken()`, and the `NewRetryableError` / `NewPermanentError` results handlers return.
2. [queue/backend.go](../queue/backend.go): the `Backend` interface, the boundary between queue logic and Redis.
3. [queue/queue.go](../queue/queue.go): `Enqueue` / `EnqueueBatch`, defaults, idempotency keys, and the enqueue trace span.
4. [queue/worker.go](../queue/worker.go): the core loop.
   - `Run`: slot-bounded claim loop, backoff on Redis errors, graceful drain.
   - `fetch`: recovery of expired leases, then a blocking claim.
   - `handleOne`: runs the handler and decides between ack, retry, and dead-letter, including the poison-message and missing-handler cases.
   - `ackLoop` and `heartbeat`: group-committed acks and batched lease renewal.
5. [queue/retry.go](../queue/retry.go): exponential backoff with jitter, and `ShouldDeadLetter`.
6. [redisstream/scripts.go](../redisstream/scripts.go): every state change as a Lua script. Start with `leaseLua` (the fencing check), then `claimScript`, `recoverScript`, `retryScript`, and `enqueueOnceScript`.
7. [redisstream/store.go](../redisstream/store.go): the single-stream backend. `claimBlocking` and `blockGrace` show how a blocking read is kept from hanging a worker.
8. [redisstream/sharded.go](../redisstream/sharded.go): partition routing, claim sweeps, and long polls across partitions.
9. [cmd/chaos/downstream.go](../cmd/chaos/downstream.go): an exactly-once side effect, built from the job ID and the fencing token.

The in-process test double `fakeBackend` in [queue/worker_test.go](../queue/worker_test.go) shows the worker's behavior without Redis. `TestWorkerCancelsHandlerWhenLeaseIsLost` and `TestWorkerShutdownDrainsAndAcksInFlightJobs` are good ones to read next to `worker.go`.

## Cleanup

```sh
docker compose down -v            # stop everything and delete Redis's data
scripts/redis-cluster.sh stop     # if you started the cluster
rm -rf /tmp/dtq
```
