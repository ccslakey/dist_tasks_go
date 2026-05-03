# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```sh
# Start Redis (required for integration tests and demo commands)
docker compose up -d redis

# Run all tests
go test ./...

# Run a single package's tests
go test ./queue/...

# Run demo commands (return ErrNotImplemented until Redis backend is filled in)
go run ./cmd/enqueue-demo
go run ./cmd/worker-demo
```

## Architecture

This is a learning-focused distributed task queue scaffold. The public API, tests, and structure are in place; the Redis backend methods are TODOs to implement.

```
application code (cmd/enqueue-demo)
        |
        v
   queue.Queue          — marshals payloads, applies defaults, calls backend
        |
        v
 queue.Backend          — interface: Enqueue, Claim, Ack, Retry, DeadLetter,
        |                 RecoverExpired, Stats
        v
 redisstream.Store      — all methods currently return ErrNotImplemented
        |
        v
 Redis Streams (main stream) + sorted set (retry schedule) + DLQ stream
        ^
        |
 queue.Worker           — registers HandlerFuncs by task name; Run() loop is a TODO
```

**Key types:**

- `queue.Job` — the unit of work: task name, JSON payload, attempt counter, max attempts, timestamps
- `queue.JobResult` — handler return value; use `NewRetryableError` / `NewPermanentError` to signal failure semantics
- `queue.WorkerConfig` — Concurrency, PollInterval, VisibilityTimeout, RetryPolicy
- `queue.Metrics` — interface with a `NoopMetrics` default; intended for a Prometheus adapter
- `redisstream.Config` — Redis address, stream name, consumer group name, retry sorted-set key, DLQ stream name

**Default Redis keys** (overridable via `redisstream.Config`):
- Main stream: `dtq:jobs`
- Consumer group: `dtq:workers`
- Retry sorted set: `dtq:retry`
- Dead-letter stream: `dtq:dead`

## Implementation Path (ordered TODOs)

1. `redisstream.Store.Enqueue` — `XADD` to main stream
2. Consumer group creation in `redisstream.New`
3. `Store.Claim` — `XREADGROUP` to claim jobs
4. `Store.Ack` — `XACK` after handler success
5. `Store.Retry` — store in sorted set keyed by next-run Unix ms; fill in `promoteRetriesScript` in [redisstream/scripts.go](redisstream/scripts.go) to promote due entries back to the main stream
6. `Store.DeadLetter` — `XADD` to DLQ stream with failure reason
7. `Store.RecoverExpired` — `XAUTOCLAIM` for abandoned leases
8. `Worker.Run` in [queue/worker.go](queue/worker.go) — bounded goroutine pool, poll loop, graceful shutdown
9. `Store.Stats` — `XLEN`, `XPENDING`, sorted-set cardinality
10. Prometheus `Metrics` implementation

## Testing

Unit tests live alongside source files (`queue/*_test.go`, `redisstream/*_test.go`). Integration tests use `internal/testredis.Start(t)`, which currently calls `t.Skip` — replace that with a real Docker-backed Redis connection when implementing the backend.

`fakeBackend` in [queue/worker_test.go](queue/worker_test.go) is the in-process test double for the `Backend` interface.

## Failure Semantics

- A job is complete only after the handler returns a nil error **and** `Ack` succeeds.
- A crashed worker leaves jobs in the PEL (Pending Entries List); `RecoverExpired` / `XAUTOCLAIM` makes them claimable again after `VisibilityTimeout`.
- Retryable failures go to the retry sorted set with exponential backoff (`RetryPolicy.NextDelay`).
- Permanent failures and exhausted jobs (`job.Exhausted()` → `ShouldDeadLetter`) go to the DLQ.
- Handlers must be idempotent — a job may run more than once.
