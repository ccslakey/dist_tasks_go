# Distributed Task Queue in Go

A distributed task queue on Redis Streams with worker pools, retries with exponential backoff, dead-lettering, lease-based crash recovery with heartbeats and fencing tokens, graceful shutdown, Prometheus metrics, and a fault-injection harness that checks the delivery guarantees hold while workers are killed and paused.

## Why Redis?

A broker is the durable system between code that enqueues work and workers that execute work. Without a broker, jobs usually live only in process memory. With Redis Streams, workers can share jobs across processes, acknowledge successful work, recover jobs abandoned by dead workers, and preserve enough state to make retries and dead-letter queues real.

This repo uses Redis Streams for the production-style backend. You can add an in-memory backend later for tests or learning, but the Redis backend is what makes the resume project more convincing.

## Architecture

```text
enqueue-demo / application code
        |
        v
   queue.Queue
        |
        v
 queue.Backend interface
        |
        v
 redisstream.Store
        |
        v
 Redis Streams + retry schedule + dead-letter stream
        ^
        |
 queue.Worker -> registered task handlers
```

## Delivery Guarantees

Delivery is **at-least-once**. Every job ends up either completed (the handler returned nil *and* the ack landed) or in the dead-letter stream. No job is silently lost when workers crash, hang, or are shut down mid-job. A job can run more than once, so handlers must be idempotent. `Job.ID` is stable across retries and redeliveries, so handlers can use it as an idempotency key.

Every state transition (enqueue with an idempotency key, ack, retry, dead-letter, lease renewal, retry promotion, recovery) is a single Lua script in [redisstream/scripts.go](redisstream/scripts.go). A crash can't leave a job half-moved, and concurrent workers can't race each other.

| Failure | What happens | Mechanism | Proven by |
|---|---|---|---|
| Worker crashes mid-job | Job is re-delivered to another worker once its lease goes `VisibilityTimeout` without a heartbeat | `XAUTOCLAIM` in `recoverScript` | `TestRecoverExpiredRedeliversAndFencesOldLease`, `cmd/chaos` (SIGKILL) |
| Job runs longer than the visibility timeout | Lease is renewed while the handler runs, so it isn't stolen | Heartbeat → `extendScript` (`XCLAIM … JUSTID`) | `TestExtendPreventsRecovery`, `cmd/chaos` (jobs at 2× timeout) |
| Worker pauses (GC, VM freeze) and its lease is taken over | The zombie's ack/retry/dead-letter is rejected and its handler's context is cancelled | Fencing token = Redis delivery count, checked in every script | `TestRecoverExpiredRedeliversAndFencesOldLease`, `TestWorkerCancelsHandlerWhenLeaseIsLost`, `cmd/chaos` (SIGSTOP) |
| Crash between the handler's side effect and the ack | Job runs again (the at-least-once duplicate); the downstream applies the effect once | Downstream dedupes on the stable `Job.ID` | `cmd/chaos` (duplicate runs vs. effects) |
| Zombie applies a side effect after its lease was taken over | The downstream rejects it | `Job.FencingToken()`, checked by the downstream | `TestFencingTokenIncreasesAcrossRetriesAndRedeliveries`, `cmd/chaos` (SIGSTOP) |
| Producer retries an enqueue after a lost response | The retry returns the original job's ID instead of creating a second job | `EnqueueOptions.IdempotencyKey` → `enqueueOnceScript` | `TestIdempotentEnqueue`, `cmd/chaos` (10% of enqueues retried) |
| Crash between scheduling a retry and acking | Can't happen: both are one script | `retryScript` | — |
| Two workers promote the same due retry | Can't happen: promotion is one script | `promoteScript` | `TestConcurrentPromotionIsExactlyOnce` |
| SIGTERM while jobs are running | Worker stops claiming, lets in-flight jobs finish and ack, then exits (cancels them after `ShutdownTimeout`) | Handlers run on a context detached from the shutdown signal | `TestWorkerShutdownDrainsAndAcksInFlightJobs` |
| Redis blip | Worker backs off and keeps going instead of exiting | Exponential backoff in `Worker.Run` | `TestWorkerSurvivesTransientClaimErrors` |
| Job crashes or hangs every worker that runs it (poison message) | Dead-lettered after `MaxDeliveries` deliveries | Delivery count | `TestWorkerDeadLettersPoisonMessage` |
| No handler registered / malformed entry | Dead-lettered instead of stuck pending forever | `Worker.handleOne`, `Store.parseOrQuarantine` | `TestWorkerDeadLettersUnknownTask`, `TestMalformedEntryIsDeadLettered` |

### Exactly-once effects

Exactly-once *delivery* isn't achievable: a worker can always crash after doing the work but before reporting it. Exactly-once *effects* are, if the system receiving the side effect helps. Each run carries two values for it:

- `Job.ID`, stable across retries and redeliveries, as an idempotency key. The downstream applies each ID's effect at most once, which absorbs the redelivery after a crash between effect and ack.
- `Job.FencingToken()`, which increases with every retry and redelivery. The downstream rejects any write carrying a lower token than the highest it has seen for that job. That stops a paused zombie from applying a stale effect after its lease was handed to another worker. The queue's own fencing can't do this, because the effect has already left the process.

[cmd/chaos/downstream.go](cmd/chaos/downstream.go) is a working example: a downstream that does both checks in one Lua script. The chaos test checks every job's effect landed exactly once.

**Known limitations:** Redis is a single point of failure. With `appendfsync everysec`, a Redis crash can lose up to a second of acknowledged writes, and replication is asynchronous, so a failover can too.

## Chaos Testing

[cmd/chaos](cmd/chaos/main.go) enqueues a batch of jobs and runs several worker processes. While they work, it SIGKILLs a random worker (and starts a replacement), and SIGSTOPs workers for longer than the visibility timeout so they come back as zombies. It also sends 10% of enqueues twice, as a producer would after a lost response. Handlers record each run in a ledger and apply their side effect through a simulated downstream, and the harness then checks that:

- no job was lost (each one completed or was dead-lettered)
- permanent failures were dead-lettered and never completed
- the queue fully drained (nothing left in the stream, pending, or scheduled)
- every completed job's side effect landed exactly once

```sh
go run ./cmd/chaos                       # 10k jobs, 4 workers x 8, kill ~1/s, pause ~1/4s
go run ./cmd/chaos -jobs 20000 -workers 8 -kill-every 300ms
```

Results with 5,000 jobs and 15s of faults (about 15 SIGKILLs and 3 pauses per run):

| | Before the fixes | After (12 runs) |
|---|---|---|
| Lost jobs | 159 | 0 |
| Stuck in the pending list | 173 | 0 |
| Permanent failures not dead-lettered | 4 | 0 |
| Duplicate runs | 426 | ~10–40 |
| Duplicate side effects | — | 0 |
| Zombie queue updates rejected | 0 (no fencing) | ~10–60 |
| Zombie side effects rejected by fencing | — | ~10 |

The remaining duplicate runs are the at-least-once cost: a worker died or was paused after the handler's side effect but before its ack. The downstream's idempotency and fencing checks keep them from becoming duplicate effects. The poison guard dead-letters a few of the jobs that outlive the visibility timeout, because their workers keep getting killed mid-run.

## Performance

[cmd/bench](cmd/bench/main.go) measures throughput (draining a 20,000-job backlog of no-op jobs) and pickup latency (enqueue to handler start, at a steady 200 jobs/s). It runs on a MacBook Pro against local Redis 8.6, with 16 concurrent jobs per worker.

```sh
go run ./cmd/bench
```

| claim mode | workers | throughput (jobs/s) | pickup p50 | pickup p95 | pickup p99 |
|---|---|---|---|---|---|
| poll | 1 | 14,988 | 48.0ms | 100.6ms | 105.4ms |
| poll | 8 | 40,319 | 40.1ms | 92.6ms | 99.7ms |
| block | 1 | 14,911 | 0.9ms | 2.7ms | 7.7ms |
| block | 2 | 26,008 | 1.0ms | 2.8ms | 9.4ms |
| block | 4 | 33,046 | 1.1ms | 4.1ms | 11.7ms |
| block | 8 | 39,853 | 1.1ms | 3.7ms | 8.9ms |

- **Blocking claims fix latency, not throughput.** Polling workers sleep up to `PollInterval` (100ms here) when the queue is empty. Blocking in `XREADGROUP` instead picks a job up as soon as it's enqueued: p50 drops from ~45ms to ~1ms. Under a backlog both modes are equally fast, since neither sleeps.
- **Group-committed acks raised throughput ~25–30%** (30k → 40k jobs/s at 8 workers). Acks that arrive while a batch is in flight go out together in one Lua call. At low load each batch has one ack, so it costs no latency.
- **The ceiling is the client, not Redis.** At 40k jobs/s Redis spends about 12µs of CPU per job, so it's roughly 35% busy. The workers and the benchmark share one process on the same laptop as Redis, which is what flattens scaling past 4 workers. Separate worker machines would push further.

## Run Locally

Start Redis:

```sh
docker compose up -d redis
```

Run tests:

```sh
go test ./...
```

Run demos. Each worker needs its own metrics address (or `-metrics-addr ""`):

```sh
go run ./cmd/enqueue-demo -n 50 -task flaky_email
go run ./cmd/worker-demo
go run ./cmd/worker-demo -metrics-addr :9091
```

## Resume Bullet Draft

Built a distributed task queue in Go on Redis Streams with at-least-once delivery: lease-based recovery with heartbeats and fencing tokens, exactly-once side effects via idempotency keys, atomic Lua state transitions, and group-committed acks (40k jobs/s, 1ms p50 pickup). Verified with a fault-injection harness that SIGKILLs and pauses worker processes under load, which found and fixed bugs that lost ~3% of jobs.
