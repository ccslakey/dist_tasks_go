# Distributed Task Queue in Go

A distributed task queue built on Redis Streams with worker pools, retries with exponential backoff, dead-letter queues, visibility timeout recovery, graceful shutdown, and Prometheus metrics.

## Architecture

```text
enqueue-demo / application code
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
 queue.Worker           — goroutine pool; polls backend; routes outcomes
```

**Key types:**

- `queue.Job` — unit of work: task name, JSON payload, attempt counter, max attempts, timestamps
- `queue.JobResult` — handler return value; use `NewRetryableError` / `NewPermanentError` to signal failure semantics
- `queue.WorkerConfig` — Concurrency, PollInterval, VisibilityTimeout, RetryPolicy
- `queue.Metrics` — interface with `NoopMetrics` default; Prometheus adapter in `prommetrics`
- `redisstream.Config` — Redis address, stream name, consumer group, retry sorted-set key, DLQ stream name

**Default Redis keys:**

| Key | Purpose |
|---|---|
| `dtq:jobs` | Main stream — pending and completed entries |
| `dtq:workers` | Consumer group name |
| `dtq:retry` | Sorted set — jobs scheduled for retry, scored by next-run Unix ms |
| `dtq:dead` | Dead-letter stream — permanent failures and exhausted jobs |

## How It Works

**Enqueue** — `Queue.Enqueue` marshals the payload to JSON and `XADD`s a message to the main stream.

**Claim** — `Store.Claim` first promotes any due retries from the sorted set back into the main stream (Lua script, atomic), then calls `XREADGROUP` to claim up to N messages for this worker. Messages stay in the PEL (pending entries list) until acked or timed out.

**Dispatch** — `Worker.Run` polls on a ticker. Each claimed job is handed to a goroutine from a bounded pool. `Worker.handleOne` looks up the registered handler, calls it, and routes the outcome:

- **Success** → `XACK`, emit `JobCompleted`
- **Retryable error, attempts remaining** → `ZADD` to retry sorted set with exponential backoff delay, emit `JobRetried`
- **Permanent error or exhausted retries** → `XADD` to DLQ stream, emit `JobDeadLettered`

**Visibility timeout recovery** — each poll tick calls `XAUTOCLAIM` to reclaim messages that have been in the PEL longer than `VisibilityTimeout`. This makes jobs claimable again after a worker crashes without acking.

**Graceful shutdown** — cancelling the context stops the poll loop. `Worker.Run` calls `wg.Wait()` before returning, so in-flight handlers are always allowed to complete.

## Failure Semantics

- A job is complete only after the handler returns nil **and** `Ack` succeeds. A handler that panics or a worker that crashes leaves the job in the PEL.
- After `VisibilityTimeout`, `XAUTOCLAIM` makes abandoned jobs claimable again by any worker.
- Retryable failures are delayed with exponential backoff (`RetryPolicy.NextDelay`). The sorted set score is the Unix-ms timestamp at which the job becomes eligible.
- A job that fails with `NewPermanentError`, or whose `Attempt >= MaxAttempts`, is moved to the DLQ stream unconditionally.
- Handlers must be idempotent — a job may run more than once.

## Run Locally

Start Redis:

```sh
docker compose up -d redis
```

Run tests:

```sh
go test ./...
```

In two terminals:

```sh
# Terminal 1 — worker (metrics on :9090/metrics)
go run ./cmd/worker-demo

# Terminal 2 — enqueue jobs
go run ./cmd/enqueue-demo -task send_email -n 10
go run ./cmd/enqueue-demo -task flaky_email -n 3          # retries twice, then succeeds
go run ./cmd/enqueue-demo -task permanent_fail -n 2       # goes straight to DLQ
go run ./cmd/enqueue-demo -task process_image -n 5 -latency 500ms  # artificial latency
```

**enqueue-demo flags:**

| Flag | Default | Description |
|---|---|---|
| `-task` | `send_email` | `send_email`, `process_image`, `permanent_fail`, `flaky_email` |
| `-n` | `1` | Number of jobs to enqueue |
| `-delay` | `50ms` | Delay between enqueues |
| `-latency` | `0` | Artificial handler latency injected via the job payload |

## Metrics

The worker-demo exposes Prometheus metrics at `http://localhost:9090/metrics`.

| Metric | Type | Description |
|---|---|---|
| `dtq_jobs_enqueued_total` | Counter | Jobs enqueued, by task |
| `dtq_jobs_started_total` | Counter | Handler invocations, by task |
| `dtq_jobs_completed_total` | Counter | Successful completions, by task |
| `dtq_jobs_failed_total` | Counter | Handler failures, by task and retryable |
| `dtq_jobs_retried_total` | Counter | Jobs scheduled for retry, by task |
| `dtq_jobs_dead_lettered_total` | Counter | Jobs moved to DLQ, by task |
| `dtq_job_duration_seconds` | Histogram | Handler runtime, by task |
| `dtq_stream_length` | Gauge | `XLEN` of main stream (includes acked entries) |
| `dtq_in_flight_jobs` | Gauge | Current PEL size (`XPENDING`) |
| `dtq_retry_scheduled` | Gauge | Jobs in the retry sorted set (`ZCARD`) |
| `dtq_dead_letter_depth` | Gauge | Entries in the DLQ stream (`XLEN`) |

## Implementation Checklist

- [x] Queue and worker public API
- [x] Redis `XADD` enqueue
- [x] Redis consumer group creation
- [x] `XREADGROUP` claim
- [x] `XACK` success path
- [x] `RetryPolicy.NextDelay` — exponential backoff
- [x] `Worker.handleOne` — dispatch and outcome routing
- [x] Retry scheduling — sorted set scored by next-run Unix ms
- [x] `promoteRetries` — Lua script promotes due entries back to main stream atomically
- [x] Dead-letter — `XADD` to DLQ with failure reason
- [x] Visibility timeout recovery — `XAUTOCLAIM`
- [x] `Worker.Run` — bounded goroutine pool, poll loop, graceful shutdown
- [x] `Store.Stats` — `XLEN`, `XPENDING`, `ZCARD`
- [x] Docker-backed Redis test harness (`internal/testredis`)
- [x] Integration tests — enqueue/claim/ack, retry promotion, dead-letter
- [x] Unit tests — `handleOne` routing (success, retryable, exhausted, permanent), `Run` shutdown and in-flight wait
- [x] Prometheus metrics adapter — counters, histogram, gauges wired into both demo commands

## Resume Bullet

Built a distributed task queue in Go on Redis Streams: worker pools with bounded concurrency, exponential-backoff retries via a sorted set, dead-letter queues, visibility timeout recovery with `XAUTOCLAIM`, graceful shutdown, and a Prometheus metrics adapter.
