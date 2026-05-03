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
	RecoverExpired(ctx context.Context, visibilityTimeout time.Duration, workerId string) (int, error)
	Stats(ctx context.Context) (Stats, error)
}

type Stats struct {
	// Point-in-time gauges (derivable from Redis state):
	QueueDepth      int64 // jobs sitting in the main stream — XLEN <stream>
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
