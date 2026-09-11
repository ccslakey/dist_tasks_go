# Changelog

## Unreleased

Correctness pass over the worker loop and the Redis backend. Three things the
README described were not what the code did, and fixing those turned up two
further bugs in the same area.

### Correctness

**Bounded concurrency.** `Worker.Run` claimed up to `Concurrency` jobs on every
tick with no accounting of what was still running, so in-flight handlers grew
without limit whenever handlers were slower than the poll interval. In-flight
work is now bounded by a buffered-channel semaphore, and the worker asks
`Claim` for `cap(sem) - len(sem)` rather than the full concurrency.
`TestWorkerNeverExceedsConcurrency` peaks at 561 simultaneous handlers against
the old loop and at exactly `Concurrency` against the new one.

**Atomic retry promotion.** Promotion ran as three separate round trips
(`ZRANGEBYSCORE`, `XADD`, `ZREM`), so two workers polling at the same instant
both saw the same due entry and both re-queued it. It is now a single Lua
script in `redisstream/scripts.go`. Eight concurrent workers against 500 due
retries produced 3307 promotions (2807 duplicate runs) under the old code and
exactly 500 under the script.

Schedule entries changed shape as part of this: they are flat string maps
rather than marshalled `Job`s, so the script can pass fields straight to `XADD`.
Decoding a job in Lua would round-trip the payload through `cjson`, which does
not preserve key order and loses precision on large integers. Entries also
carry a `prevID` so two distinct jobs with identical content do not collapse
into one sorted-set member.

**Reclaimed jobs were stranded.** `XAUTOCLAIM` transfers an entry to the
reclaiming consumer, after which `XREADGROUP` with `>` never returns it again.
`RecoverExpired` returned a count, `Run` discarded it, and the job sat in the
new owner's pending list until the next visibility timeout, forever. It now
returns `[]queue.Job` and `Run` dispatches them like any other job. This is a
`Backend` interface change.

**Handler panics took down the process.** A panic inside a pool goroutine
cannot be recovered by its parent. `Worker.invoke` now recovers and converts it
to a permanent failure, which routes to the DLQ with the panic message. A panic
is treated as permanent rather than retryable because it is almost always
deterministic and retrying only burns the attempt budget.

**Unknown tasks were a poison pill.** `handleOne` returned `ErrUnknownTask`
without acking or dead-lettering, the error was discarded by the dispatch
closure, and the entry was reclaimed on every visibility timeout forever. It
now goes to the DLQ.

**Malformed stream entries panicked the poll loop.** Field reads used the
single-value type assertion (`msg.Values["attempt"].(string)`), which panics on
a missing key. Parsing is now two-value throughout, and an entry that cannot be
decoded is quarantined to the DLQ with the reason and acked rather than failing
the whole claim.

**Shutdown cancelled in-flight handlers.** Handlers ran on the worker's
context, so cancelling it aborted the `Ack` that follows a successful handler
and left completed work pending. Handlers now run on a context detached with
`context.WithoutCancel`, with a deadline of `VisibilityTimeout` (past that
point another worker may legally reclaim the job, so a longer-running handler
only widens the overlap). `Run` also waits on the pool before returning from
the claim-error path, not just on cancellation.

**Claim errors no longer kill the worker on the first failure.** `Run`
tolerates `MaxClaimErrors` consecutive failures (default 5) before giving up,
so a transient Redis blip does not take a worker down while a permanent
misconfiguration still does.

### Operational

- The main stream is capped with `MAXLEN ~` (`redisstream.Config.MaxLen`,
  default 100k). Redis streams retain entries after acknowledgement, so it
  previously grew for the life of the process.
- `handleOne` failures are logged instead of discarded. `WorkerConfig.Logger`
  defaults to `slog.Default()`.
- `redisstream.Store` wraps its errors with the operation and key involved, and
  gained a `Close`.

### Tooling

- GitHub Actions workflow: gofmt check, `go mod tidy` check, `go vet`, and
  `go test -race` against a Redis service container.
- `internal/testredis` fails instead of skipping when `DTQ_REQUIRE_REDIS` is
  set. CI sets it, because a skipped integration suite looks identical to a
  passing one.
- `cmd/loadtest` reports drain throughput and end-to-end latency percentiles.
- `go.mod`: direct dependencies are no longer all marked `// indirect`, the
  module path is `github.com/ccslakey/dist_tasks_go` so the repo is
  go-gettable, and the Go directive is 1.24 (nothing required 1.26 once
  `sync.WaitGroup.Go` was replaced).

### Tests

New coverage for each of the above: `TestWorkerNeverExceedsConcurrency`,
`TestHandlerPanicIsDeadLetteredNotFatal`, `TestWorkerRunDispatchesReclaimedJobs`,
`TestJobContextOutlivesWorkerCancellation`,
`TestWorkerRunSurvivesTransientClaimErrors`,
`TestWorkerRunGivesUpAfterRepeatedClaimErrors`,
`TestRecoverExpiredReturnsStrandedJobs`,
`TestConcurrentPromotionIsExactlyOnce`, `TestMalformedEntryIsQuarantined`,
`TestEnqueueTrimsStream`.

### Migration

The retry schedule format changed. Entries written to `dtq:retry` by an earlier
build will be quarantined rather than promoted. Clear the key before the first
run on an existing Redis:

```sh
redis-cli DEL dtq:retry
```
