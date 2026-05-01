# Distributed Task Queue in Go

This repo is a learning-focused scaffold for a distributed task queue. The structure, public API, examples, and tests are in place, but the core queue semantics are intentionally left as TODOs for you to implement.

The finished project should demonstrate worker pools, retries, dead-letter queues, visibility timeouts, graceful shutdown, Redis Streams, and metrics.

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
 redisstream.Backend
        |
        v
 Redis Streams + retry schedule + dead-letter stream
        ^
        |
 queue.Worker -> registered task handlers
```

## Current Status

- [x] Go module scaffold
- [x] Queue and worker types
- [x] Redis Streams backend shape
- [x] Example task handlers
- [x] Demo commands
- [x] Tests that document expected behavior
- [x] Redis `XADD` enqueue
- [x] Redis consumer group claim/dequeue
- [x] Redis `XACK` success path
- [x] `RetryPolicy.NextDelay` exponential backoff
- [ ] `Worker.handleOne` dispatch and outcome routing
- [ ] Retry scheduling and promotion
- [ ] Dead-letter movement
- [ ] Visibility timeout recovery with `XAUTOCLAIM`
- [ ] Worker concurrency loop
- [ ] Graceful shutdown of in-flight jobs
- [ ] Prometheus metrics adapter

## Run Locally

Start Redis:

```sh
docker compose up -d redis
```

Run tests:

```sh
go test ./...
```

The enqueue demo works end-to-end. The worker demo will run but handlers won't be dispatched until `Worker.Run` and `Worker.handleOne` are implemented:

```sh
go run ./cmd/enqueue-demo
go run ./cmd/worker-demo
```

## Implementation Path

1. ~~Implement `redisstream.Backend.Enqueue` with `XADD`.~~
2. ~~Create or ensure the Redis consumer group during backend initialization.~~
3. ~~Implement `Claim` using `XREADGROUP`.~~
4. ~~Implement `Ack` using `XACK`.~~
5. Implement `RetryPolicy.NextDelay` with exponential backoff (`queue/retry.go`).
6. Implement `Worker.handleOne` dispatch and outcome routing (`queue/worker.go`).
7. Implement retry scheduling with a Redis sorted set keyed by next-run timestamp.
8. Promote due retries back into the main stream (fill in `promoteRetriesScript` in `redisstream/scripts.go`).
9. Implement `DeadLetter` with a separate stream that stores the original job and failure reason.
10. Implement lease recovery with `XAUTOCLAIM` or `XPENDING` plus `XCLAIM`.
11. Replace `Worker.Run` with a real bounded goroutine worker loop.
12. Add a Prometheus-backed `queue.Metrics` implementation.

## Failure Semantics To Understand

- A job is complete only after the handler succeeds and the worker acks it.
- A worker can crash after claiming a job but before acking it.
- Visibility timeout recovery makes that abandoned job available again.
- Retryable handler failures should be delayed with exponential backoff.
- Permanent failures and exhausted retries should move to the dead-letter queue.
- Handlers should be idempotent because a job may run more than once.

## Resume Bullet Draft

Built a distributed task queue in Go using Redis Streams with worker pools, concurrency limits, visibility timeouts, retries, dead-letter queues, graceful shutdown, and metrics.
