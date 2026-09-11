# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
# Start Redis (required for integration tests and demo commands)
docker compose up -d redis

# Run everything the way CI does
DTQ_REQUIRE_REDIS=1 go test -race -count=1 ./...

# Single package
go test ./queue/...

# Demos
go run ./cmd/worker-demo
go run ./cmd/enqueue-demo -task send_email -n 10
go run ./cmd/loadtest -jobs 20000 -workers 4 -concurrency 32
```

## Architecture

A working distributed task queue on Redis Streams. The Redis backend is implemented; see README.md for the full walkthrough.

```
application code (cmd/enqueue-demo)
        |
        v
   queue.Queue          — marshals payloads, applies defaults, emits metrics
        |
        v
 queue.Backend          — interface: Enqueue, Claim, Ack, Retry, DeadLetter,
        |                 RecoverExpired, Stats
        v
 redisstream.Store
        |
        v
 Redis Streams (main stream) + sorted set (retry schedule) + DLQ stream
        ^
        |
 queue.Worker           — bounded goroutine pool; polls backend; routes outcomes
```

**Key types:**

- `queue.Job` — the unit of work: task name, JSON payload, attempt counter, max attempts, timestamps
- `queue.JobResult` — handler return value; use `NewRetryableError` / `NewPermanentError`
- `queue.WorkerConfig` — Concurrency, PollInterval, VisibilityTimeout, RetryPolicy, MaxClaimErrors, Logger
- `queue.Metrics` — interface with a `NoopMetrics` default; Prometheus adapter in `prommetrics`
- `redisstream.Config` — Redis address, stream, consumer group, retry key, DLQ stream, MaxLen

**Default Redis keys** (overridable via `redisstream.Config`):
- Main stream: `dtq:jobs`
- Consumer group: `dtq:workers`
- Retry sorted set: `dtq:retry`
- Dead-letter stream: `dtq:dead`

## Invariants worth not breaking

These are the parts where the obvious implementation is wrong. Each has a test.

1. **The worker never claims more than its free slots.** `Worker.Run` bounds in-flight work with a buffered-channel semaphore and asks `Claim` for `cap(sem) - len(sem)`. Claiming a full batch per tick is unbounded under slow handlers. See `TestWorkerNeverExceedsConcurrency`.

2. **Retry promotion is a single Lua script.** Separate `ZRANGEBYSCORE` / `XADD` / `ZREM` round trips let concurrent workers promote the same entry. The script is in `redisstream/scripts.go`; entries are flat string maps so the script never re-encodes a job payload through `cjson`. See `TestConcurrentPromotionIsExactlyOnce`.

3. **`RecoverExpired` returns jobs, not a count.** `XAUTOCLAIM` transfers ownership, after which `XREADGROUP >` never returns the entry again. Reclaiming without dispatching strands the job. See `TestRecoverExpiredReturnsStrandedJobs`.

4. **Handler contexts are detached from the worker context.** `Worker.jobContext` uses `context.WithoutCancel` plus a `VisibilityTimeout` deadline. Cancelling handlers on shutdown aborts the `Ack` that follows them. See `TestJobContextOutlivesWorkerCancellation`.

5. **Nothing unrunnable stays pending.** Unknown task, panicking handler, and undecodable stream entry all route to the DLQ. A job left pending comes back on every reclaim, forever.

6. **Stream field parsing uses the two-value type assertion.** `msg.Values[name].(string)` panics on a missing key, inside the poll goroutine, which takes down the process.

## Testing

Unit tests live alongside source files. Integration tests use `internal/testredis.Start(t)`, which skips when Redis is unreachable unless `DTQ_REQUIRE_REDIS` is set. CI sets it, because a skipped suite is indistinguishable from a passing one.

`controlledBackend`, `flakyClaimBackend` and `infiniteBackend` in `queue/worker_test.go` are the in-process `Backend` doubles.

## Failure Semantics

- A job is complete only after the handler returns nil **and** `Ack` succeeds.
- Retryable failures go to the retry sorted set with exponential backoff.
- Permanent failures and exhausted jobs (`job.Exhausted()` → `ShouldDeadLetter`) go to the DLQ.
- `Retry` and `DeadLetter` write the new state before acking the old entry: a crash between the two costs a duplicate run rather than a lost job.
- Delivery is at-least-once. Handlers must be idempotent.
