# Distributed Task Queue in Go

[![CI](https://github.com/ccslakey/dist_tasks_go/actions/workflows/ci.yml/badge.svg)](https://github.com/ccslakey/dist_tasks_go/actions/workflows/ci.yml)

A distributed task queue built on Redis Streams: worker pools with bounded concurrency, exponential-backoff retries, dead-letter queues, crash recovery via visibility timeouts, graceful shutdown, and Prometheus metrics.

Delivery is at-least-once. Handlers must be idempotent. The [Failure Semantics](#failure-semantics) section is specific about where duplicates come from and why the alternative is worse.

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
 queue.Worker           — bounded goroutine pool; polls backend; routes outcomes
```

**Key types:**

- `queue.Job` — unit of work: task name, JSON payload, attempt counter, max attempts, timestamps
- `queue.JobResult` — handler return value; use `NewRetryableError` / `NewPermanentError` to signal failure semantics
- `queue.WorkerConfig` — Concurrency, PollInterval, VisibilityTimeout, RetryPolicy, MaxClaimErrors, Logger
- `queue.Metrics` — interface with `NoopMetrics` default; Prometheus adapter in `prommetrics`
- `redisstream.Config` — Redis address, stream name, consumer group, retry sorted-set key, DLQ stream name, MaxLen

**Default Redis keys:**

| Key | Purpose |
|---|---|
| `dtq:jobs` | Main stream, capped at ~100k entries (`Config.MaxLen`) |
| `dtq:workers` | Consumer group name |
| `dtq:retry` | Sorted set, jobs scheduled for retry, scored by next-run Unix ms |
| `dtq:dead` | Dead-letter stream, permanent failures and exhausted jobs |

## How It Works

**Enqueue.** `Queue.Enqueue` marshals the payload to JSON and `XADD`s an entry to the main stream. Every field is stored as a string, because Redis stream values are strings and because the Lua promotion script has to produce the identical shape.

**Poll.** Each tick, `Worker.Run` does three things in order.

1. Reclaims expired leases and dispatches what it gets back.
2. Claims new jobs, asking only for as many as it has free concurrency slots.
3. Hands each job to a goroutine from a bounded pool.

**Bounded concurrency.** A buffered channel is the semaphore. The worker computes `free = cap(sem) - len(sem)` and never claims more than that, so in-flight handlers cannot exceed `Concurrency` no matter how slow they are or how fast the ticker runs. Claiming a full batch every tick regardless of what is still running is the easy version of this loop and it is unbounded: with a 250ms tick and 2-second handlers, the in-flight count climbs every tick. `TestWorkerNeverExceedsConcurrency` reproduces that (it peaks in the hundreds against an unbounded loop) and pins the fixed behaviour.

**Retry promotion.** `Store.Claim` first runs a Lua script that moves every schedule entry whose score has passed back onto the main stream, then calls `XREADGROUP`. Promotion is a read-modify-write across two keys: find due entries, re-add them to the stream, drop them from the schedule. Run as three round trips from Go, two workers polling at the same instant both see the same due entry and both re-add it, so the retried job runs twice. Redis runs a script to completion before serving anything else, so as a script only one worker can observe a given entry as due. `TestConcurrentPromotionIsExactlyOnce` runs eight goroutines at 200 due retries and asserts the total promoted count is exactly 200.

Schedule entries are stored as flat string maps rather than marshalled `Job`s. The script needs to hand fields straight to `XADD`, and decoding a `Job` in Lua would mean round-tripping the job payload through `cjson`, which does not preserve key order and loses precision on large integers.

**Crash recovery.** Each tick calls `XAUTOCLAIM` for entries idle longer than `VisibilityTimeout`. The important detail is that `RecoverExpired` returns the jobs rather than a count: `XAUTOCLAIM` transfers the entry to the reclaiming consumer, and from that point `XREADGROUP` with `>` will never return it again. Reclaiming without dispatching strands the job in the new owner's pending list until the next timeout, forever.

**Graceful shutdown.** Cancelling the context stops new claims and `Run` waits on the in-flight pool. Handlers run on contexts *detached* from the worker's, via `context.WithoutCancel`, with a deadline of `VisibilityTimeout`. Cancelling handlers on shutdown would abort the `Ack` that follows them and leave completed work sitting in the pending list. The deadline is `VisibilityTimeout` because past that point another worker may legally reclaim the job, and letting the original handler keep running only widens the window where both are running.

**Poison pills.** Three kinds, all routed to the DLQ rather than left pending:

- A job whose task has no registered handler. It can never succeed in this process, and leaving it pending means every reclaim hands it back.
- A handler that panics. `Worker.invoke` recovers and converts it to a permanent failure. Without that, a panic in a pool goroutine takes down the whole process. Permanent rather than retryable, because a panic is usually deterministic and retrying burns the attempt budget to reach the same place.
- A stream entry the backend cannot decode. `Store.decodeMessages` quarantines it with the reason and acks it, instead of failing the whole claim.

## Failure Semantics

- A job is complete only after the handler returns nil **and** `Ack` succeeds. An `Ack` that fails leaves the job pending, so it will be reclaimed and run again.
- After `VisibilityTimeout`, `XAUTOCLAIM` hands abandoned jobs to whichever worker reclaims them first.
- Retryable failures are delayed with exponential backoff (`RetryPolicy.NextDelay`). The sorted-set score is the Unix-ms timestamp at which the job becomes eligible.
- `Retry` and `DeadLetter` both write the new state before acking the old entry. A crash between the two costs one duplicate run. The reverse order would lose the job, which is the worse failure.
- A job that fails with `NewPermanentError`, or whose `Attempt >= MaxAttempts`, goes to the DLQ unconditionally.
- `Claim` failures are tolerated up to `MaxClaimErrors` consecutive attempts (default 5) before `Run` gives up. A transient Redis blip should not kill a worker; a permanent misconfiguration should not spin forever.
- Handlers must be idempotent. Every bullet above is a place a job can run twice.

## Run Locally

```sh
docker compose up -d redis
go test ./...
```

Integration tests skip when Redis is unreachable so a unit-only run is not blocked. In CI that is the wrong default, because a skipped suite looks like a passing one, so `DTQ_REQUIRE_REDIS=1` turns the skip into a failure.

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

## Benchmarks

`cmd/loadtest` enqueues a fixed number of no-op jobs, drains them with a set of workers, and reports throughput and end-to-end latency. Handler time defaults to zero, so the numbers describe queue overhead rather than whatever the handler does.

```sh
go run ./cmd/loadtest -jobs 20000 -workers 4 -concurrency 32
```

<!-- TODO: paste a real run here. Numbers below are placeholders. -->

| Setup | Throughput | p50 | p99 |
|---|---|---|---|
| 4 workers x 32, local Redis | _run it_ | _run it_ | _run it_ |

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
| `dtq_in_flight_jobs` | Gauge | Current pending-list size (`XPENDING`) |
| `dtq_retry_scheduled` | Gauge | Jobs in the retry sorted set (`ZCARD`) |
| `dtq_dead_letter_depth` | Gauge | Entries in the DLQ stream (`XLEN`) |

## Known Limits

- One stream, one consumer group. No priorities, no per-tenant isolation, no fair scheduling across tasks.
- `Stats` returns point-in-time gauges only. Lifetime counters live in the `Metrics` implementation, not in Redis.
- Promotion, reclaim and claim all run on the same tick, so `PollInterval` is the floor on retry punctuality.
- No scheduled or recurring jobs beyond `EnqueueOptions.Delay`.
- The retry schedule format changed when promotion moved into Lua. Entries written by an older build will be quarantined rather than promoted.

## Resume Bullet

Built a distributed task queue in Go on Redis Streams: worker pools with semaphore-bounded concurrency, exponential-backoff retries promoted by an atomic Lua script, dead-letter routing for poison pills, crash recovery through `XAUTOCLAIM` visibility timeouts, panic-isolated handlers, and a Prometheus metrics adapter. Tests reproduce the concurrent-promotion race and the unbounded-pool bug before pinning the fixes.
