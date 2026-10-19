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

# Run demo commands (run several worker-demos with distinct -metrics-addr, or "" to disable)
go run ./cmd/enqueue-demo
go run ./cmd/worker-demo

# Fault-injection test: worker processes are SIGKILLed/SIGSTOPped while they run
go run ./cmd/chaos
go run ./cmd/chaos -jobs 5000 -duration 15s -partitions 8   # sharded queue

# Throughput and pickup-latency benchmark
go run ./cmd/bench
go run ./cmd/bench -modes block -workers 8 -concurrency 256 -partitions 16

# Local 3-node Redis Cluster on ports 30001-30003 (macOS AirPlay holds 7000)
scripts/redis-cluster.sh start|status|stop
go run ./cmd/bench -modes block -workers 8 -concurrency 256 -cluster 127.0.0.1:30001,127.0.0.1:30002,127.0.0.1:30003
TEST_REDIS_CLUSTER=127.0.0.1:30001,127.0.0.1:30002,127.0.0.1:30003 go test ./redisstream/ -run Sharded

# Full demo stack (workers, producer, Prometheus :9090, Grafana :3000, Jaeger :16686)
docker compose up -d --build
./scripts/kill-worker.sh [-pause N]
```

The Grafana dashboard is generated JSON in `deploy/grafana/dashboards/dtq.json`; Prometheus finds worker replicas via Docker DNS (`deploy/prometheus.yml`). Demo binaries read `DTQ_REDIS_ADDR` and export traces when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

## Architecture

A distributed task queue on Redis Streams with at-least-once delivery.

```
application code (cmd/enqueue-demo)
        |
        v
   queue.Queue          — marshals payloads, applies defaults, calls backend
        |
        v
 queue.Backend          — interface: Enqueue, Claim, RecoverExpired, Extend,
        |                 Ack, Retry, DeadLetter, Stats
        v
 redisstream.Store      — every state change is a Lua script in redisstream/scripts.go
   or redisstream.Sharded — N Stores with hash-tagged keys, for Redis Cluster
        |
        v
 Redis Streams (main stream) + sorted set & hash (retry/delay schedule) + DLQ stream
        ^
        |
 queue.Worker           — slot-bounded blocking claim loop, lease heartbeats,
                          group-committed acks (AckBatch), graceful drain
```

**Key types:**

- `queue.Job` — task name, JSON payload, `Attempt` (previous failed runs), `MaxAttempts` (total runs allowed), timestamps. `ID` is a stable UUID that survives retries and redeliveries; `FencingToken()` increases with every retry and redelivery, for downstream systems to reject stale runs.
- `queue.Lease` — one delivery of a job to a worker: stream message ID, owner, and fencing token (the Redis delivery count). Ack/Retry/DeadLetter/Extend are fenced by it and return `queue.ErrLeaseLost` when another worker has taken over.
- `queue.JobResult` — handler return value; use `NewRetryableError` / `NewPermanentError` to signal failure semantics
- `queue.WorkerConfig` — Concurrency, PollInterval (claim block time), PollOnly, VisibilityTimeout, HeartbeatInterval, ShutdownTimeout, MaxDeliveries, StatsInterval, RetryPolicy (with Jitter)
- `queue.Metrics` — interface with a `NoopMetrics` default; `prommetrics.Metrics` is the Prometheus adapter
- `redisstream.Config` — Redis address, stream name, consumer group name, retry sorted-set key, DLQ stream name
- `redisstream.Sharded` / `ShardedConfig` — partitions (default 16), seeds, cluster mode, key prefix. Unkeyed jobs go round-robin, idempotency keys hash to a fixed partition, and `Lease.Partition` routes settling calls back. `Claim` sweeps partitions, then long-polls all of them at once on a separate client that `Close` cuts off; extra jobs are buffered. Use one per worker process. Trade-offs are in the README's Sharding section.
- `internal/redisutil.Target` — the `-redis`/`-partitions`/`-cluster` flags shared by cmd/chaos and cmd/bench, plus prefix cleanup across cluster nodes

**Default Redis keys** (overridable via `redisstream.Config`):
- Main stream: `dtq:jobs` (settled entries are deleted, so `XLEN` is the backlog)
- Consumer group: `dtq:workers`
- Retry/delay schedule: `dtq:retry` (sorted set of job IDs by run-at ms) + `dtq:retry:jobs` (hash of job fields)
- Dead-letter stream: `dtq:dead`
- Enqueue idempotency keys: `dtq:dedupe:<key>` (expire after `DedupeWindow`, 24h)
- Sharded: the same per partition under a hash tag, e.g. `dtq:{p3}:jobs`, `dtq:{p3}:retry`, `dtq:{p3}:dedupe:<key>`; group `dtq:workers`

## Testing

Unit tests live alongside source files (`queue/*_test.go`, `redisstream/*_test.go`). Integration tests use `internal/testredis.Start(t)`, which connects to `TEST_REDIS_ADDR` (default `localhost:6379`), skips if Redis is unreachable, and runs `FLUSHDB` on cleanup — don't run them while `cmd/chaos` or `cmd/bench` is running against the same Redis. Sharded tests also run against a cluster when `TEST_REDIS_CLUSTER` is set (they clean up by key prefix). `redisstream/stuck_test.go` uses a fake Redis that never answers blocking reads, to check claims and `Close` still return on time.

If blocking claims ever hang past their block time against a local Redis, check whether `INFO server`'s `lru_clock` advances. A long-running Homebrew Redis was once found with its timers stopped, so blocked clients were only released when other traffic arrived (a fresh `redis-server` of the same version behaved). Restart it. The code copes either way (`blockGrace` in redisstream/store.go).

`fakeBackend` in [queue/worker_test.go](queue/worker_test.go) is the in-process test double for the `Backend` interface.

## Failure Semantics

- A job is complete only after the handler returns a nil error **and** `Ack` succeeds.
- A crashed worker leaves jobs in the PEL (Pending Entries List); `RecoverExpired` (`XAUTOCLAIM`) re-delivers them once their lease has gone `VisibilityTimeout` without a heartbeat. Running jobs are heartbeated (`Extend`) so long jobs aren't stolen.
- A worker whose lease was taken over (e.g. it was paused) can't settle the job: the fencing check rejects it, and its handler's context is cancelled with `ErrLeaseLost`.
- On shutdown the worker stops claiming and lets in-flight jobs finish and settle (up to `ShutdownTimeout`); handlers run on a context detached from the shutdown signal.
- Jobs delivered more than `MaxDeliveries` times without settling (poison messages) and jobs with no registered handler go to the DLQ.
- Retryable failures go to the retry sorted set with exponential backoff (`RetryPolicy.NextDelay`).
- Permanent failures and exhausted jobs (`job.Exhausted()` → `ShouldDeadLetter`) go to the DLQ.
- Handlers must be idempotent — a job may run more than once.
