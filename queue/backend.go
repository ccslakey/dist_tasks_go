package queue

import (
	"context"
	"time"
)

type Backend interface {
	Enqueue(ctx context.Context, job Job) (JobID, error)
	Claim(ctx context.Context, workerID string, limit int) ([]Job, error)
	Ack(ctx context.Context, jobID JobID) error
	Retry(ctx context.Context, job Job, nextRunAt time.Time) error
	DeadLetter(ctx context.Context, job Job, reason string) error
	// RecoverExpired takes ownership of up to limit jobs that have sat unacked
	// for longer than visibilityTimeout, and returns them so the caller can run
	// them. It returns the jobs rather than a count because reclaiming without
	// dispatching strands them: once an entry moves to this consumer, a normal
	// claim will never hand it back.
	RecoverExpired(ctx context.Context, visibilityTimeout time.Duration, workerID string, limit int) ([]Job, error)
	Stats(ctx context.Context) (Stats, error)
}

type Stats struct {
	// Point-in-time gauges (derivable from Redis state):
	StreamLength    int64 // total entries in the main stream — XLEN <stream>
	InFlight        int64 // jobs claimed but not yet Ack'd (PEL size) — XPending(...).Count
	DeadLetterDepth int64 // jobs in the DLQ stream — XLEN <dlq>
	RetryScheduled  int64 // jobs waiting in the retry sorted set — ZCARD <retryKey>

	// Lifetime counters (NOT derivable from Redis — these need to be tracked
	// elsewhere, e.g. via the Metrics interface or a separate counter store).
	// For now, leave these zero in the Redis backend.
	TotalEnqueued     int64
	TotalCompleted    int64
	TotalFailed       int64
	TotalRetried      int64
	TotalDeadLettered int64
}
