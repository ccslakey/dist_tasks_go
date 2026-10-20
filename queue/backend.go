package queue

import (
	"context"
	"time"
)

type Backend interface {
	// Enqueue stores a new job and returns its stable ID. Jobs whose
	// AvailableAt is in the future are scheduled rather than made runnable.
	Enqueue(ctx context.Context, job Job) (JobID, error)
	// Claim leases up to limit never-delivered jobs to workerID. If none are
	// ready it waits up to wait for one to arrive (0 means don't wait).
	Claim(ctx context.Context, workerID string, limit int, wait time.Duration) ([]Job, error)
	// RecoverExpired leases up to limit jobs whose previous lease has been
	// idle longer than visibilityTimeout (e.g. their worker crashed).
	RecoverExpired(ctx context.Context, visibilityTimeout time.Duration, workerID string, limit int) ([]Job, error)
	// Extend renews job's lease so it isn't recovered while still running.
	Extend(ctx context.Context, job Job) error

	// Ack, Retry, and DeadLetter settle a job. Each is atomic and fenced by
	// job.Lease: they return ErrLeaseLost if the caller no longer owns it.
	Ack(ctx context.Context, job Job) error
	Retry(ctx context.Context, job Job, nextRunAt time.Time) error
	DeadLetter(ctx context.Context, job Job, reason string) error

	Stats(ctx context.Context) (Stats, error)
}

type Stats struct {
	// Point-in-time gauges (derivable from Redis state):
	StreamLength    int64 // runnable + in-flight jobs in the main stream (settled entries are deleted) — XLEN <stream>
	InFlight        int64 // jobs claimed but not yet settled (PEL size) — XPending(...).Count
	DeadLetterDepth int64 // jobs in the DLQ stream — XLEN <dlq>
	RetryScheduled  int64 // jobs waiting for a retry or a delayed start — ZCARD <retryKey>

	// Lifetime counters (NOT derivable from Redis — these need to be tracked
	// elsewhere, e.g. via the Metrics interface or a separate counter store).
	// For now, leave these zero in the Redis backend.
	TotalEnqueued     int64
	TotalCompleted    int64
	TotalFailed       int64
	TotalRetried      int64
	TotalDeadLettered int64
}
