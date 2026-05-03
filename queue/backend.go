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
	QueueDepth        int64
	InFlight          int64
	DeadLetterDepth   int64
	RetryScheduled    int64
	TotalEnqueued     int64
	TotalCompleted    int64
	TotalFailed       int64
	TotalRetried      int64
	TotalDeadLettered int64
}
