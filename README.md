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

Every state transition (ack, retry, dead-letter, lease renewal, retry promotion, recovery) is a single Lua script in [redisstream/scripts.go](redisstream/scripts.go). A crash can't leave a job half-moved, and concurrent workers can't race each other.

| Failure | What happens | Mechanism | Proven by |
|---|---|---|---|
| Worker crashes mid-job | Job is re-delivered to another worker once its lease goes `VisibilityTimeout` without a heartbeat | `XAUTOCLAIM` in `recoverScript` | `TestRecoverExpiredRedeliversAndFencesOldLease`, `cmd/chaos` (SIGKILL) |
| Job runs longer than the visibility timeout | Lease is renewed while the handler runs, so it isn't stolen | Heartbeat → `extendScript` (`XCLAIM … JUSTID`) | `TestExtendPreventsRecovery`, `cmd/chaos` (jobs at 2× timeout) |
| Worker pauses (GC, VM freeze) and its lease is taken over | The zombie's ack/retry/dead-letter is rejected and its handler's context is cancelled | Fencing token = Redis delivery count, checked in every script | `TestRecoverExpiredRedeliversAndFencesOldLease`, `TestWorkerCancelsHandlerWhenLeaseIsLost`, `cmd/chaos` (SIGSTOP) |
| Crash between the handler's side effect and the ack | Job runs again (the at-least-once duplicate) | Handlers must be idempotent; `Job.ID` is stable | `cmd/chaos` reports duplicate counts |
| Crash between scheduling a retry and acking | Can't happen: both are one script | `retryScript` | — |
| Two workers promote the same due retry | Can't happen: promotion is one script | `promoteScript` | `TestConcurrentPromotionIsExactlyOnce` |
| SIGTERM while jobs are running | Worker stops claiming, lets in-flight jobs finish and ack, then exits (cancels them after `ShutdownTimeout`) | Handlers run on a context detached from the shutdown signal | `TestWorkerShutdownDrainsAndAcksInFlightJobs` |
| Redis blip | Worker backs off and keeps going instead of exiting | Exponential backoff in `Worker.Run` | `TestWorkerSurvivesTransientClaimErrors` |
| Job crashes or hangs every worker that runs it (poison message) | Dead-lettered after `MaxDeliveries` deliveries | Delivery count | `TestWorkerDeadLettersPoisonMessage` |
| No handler registered / malformed entry | Dead-lettered instead of stuck pending forever | `Worker.handleOne`, `Store.parseOrQuarantine` | `TestWorkerDeadLettersUnknownTask`, `TestMalformedEntryIsDeadLettered` |

**Known limitations:** Redis is a single point of failure. With `appendfsync everysec`, a Redis crash can lose up to a second of acknowledged writes, and replication is asynchronous, so a failover can too. Fencing protects the queue's own state, but a zombie's *external* side effects still happen. Making those safe needs the downstream system to check the token or an idempotency key.

## Chaos Testing

[cmd/chaos](cmd/chaos/main.go) enqueues a batch of jobs and runs several worker processes. While they work, it SIGKILLs a random worker (and starts a replacement), and SIGSTOPs workers for longer than the visibility timeout so they come back as zombies. Handlers record each completed run in a ledger, and the harness then checks that:

- no job was lost (each one completed or was dead-lettered)
- permanent failures were dead-lettered and never completed
- the queue fully drained (nothing left in the stream, pending, or scheduled)

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
| Duplicate executions | 426 | ~15–40 |
| Zombie results rejected | 0 (no fencing) | ~20–60 |

The remaining duplicates are the at-least-once cost: a worker died or was paused after the handler's side effect but before its ack. Overlapping runs come only from paused zombies whose leases were taken over. Their queue updates are rejected, but their side effects aren't. The poison guard dead-letters a few of the jobs that outlive the visibility timeout, because their workers keep getting killed mid-run.

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

Built a distributed task queue in Go on Redis Streams with at-least-once delivery: lease-based recovery with heartbeats and fencing tokens, atomic Lua state transitions, retries with backoff, dead-lettering, and graceful drain. Verified with a fault-injection harness that SIGKILLs and pauses worker processes under load, which found and fixed bugs that lost ~3% of jobs.
