package redisstream

import (
	"context"
	"time"

	"dist_tasks_go/queue"
)

type Config struct {
	Addr       string
	Stream     string
	Group      string
	RetryKey   string
	DeadLetter string
}

type Backend struct {
	config Config
}

func New(config Config) (*Backend, error) {
	if config.Addr == "" {
		config.Addr = "localhost:6379"
	}
	if config.Stream == "" {
		config.Stream = "dtq:jobs"
	}
	if config.Group == "" {
		config.Group = "dtq:workers"
	}
	if config.RetryKey == "" {
		config.RetryKey = "dtq:retry"
	}
	if config.DeadLetter == "" {
		config.DeadLetter = "dtq:dead"
	}
	return &Backend{config: config}, nil
}

func (b *Backend) Enqueue(ctx context.Context, job queue.Job) (queue.JobID, error) {
	// TODO: Use Redis XADD against b.config.Stream.
	// Store task, payload, attempt, max attempts, timestamps, and trace id as fields.
	return "", queue.ErrNotImplemented
}

func (b *Backend) Claim(ctx context.Context, workerID string, limit int) ([]queue.Job, error) {
	// TODO: Use XREADGROUP to claim new jobs for workerID.
	// Later, combine this with retry promotion so delayed retries re-enter the stream.
	return nil, queue.ErrNotImplemented
}

func (b *Backend) Ack(ctx context.Context, jobID queue.JobID) error {
	// TODO: Use XACK after a handler succeeds.
	return queue.ErrNotImplemented
}

func (b *Backend) Retry(ctx context.Context, job queue.Job, nextRunAt time.Time) error {
	// TODO: Increment attempt metadata and schedule for nextRunAt.
	// A Redis sorted set keyed by Unix milliseconds is a simple v1 option.
	return queue.ErrNotImplemented
}

func (b *Backend) DeadLetter(ctx context.Context, job queue.Job, reason string) error {
	// TODO: Move job details plus reason into b.config.DeadLetter.
	return queue.ErrNotImplemented
}

func (b *Backend) RecoverExpired(ctx context.Context, visibilityTimeout time.Duration) (int, error) {
	// TODO: Use XPENDING/XAUTOCLAIM so jobs held by dead workers become claimable again.
	return 0, queue.ErrNotImplemented
}

func (b *Backend) Stats(ctx context.Context) (queue.Stats, error) {
	// TODO: Use XLEN, XPENDING, and retry/DLQ lengths.
	return queue.Stats{}, queue.ErrNotImplemented
}
