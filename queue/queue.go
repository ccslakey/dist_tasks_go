package queue

import (
	"context"
	"encoding/json"
	"time"
)

type Queue struct {
	backend Backend
	metrics Metrics
	now     func() time.Time
}

type QueueOption func(*Queue)

func WithMetrics(metrics Metrics) QueueOption {
	return func(q *Queue) {
		if metrics != nil {
			q.metrics = metrics
		}
	}
}

func WithClock(now func() time.Time) QueueOption {
	return func(q *Queue) {
		if now != nil {
			q.now = now
		}
	}
}

func New(backend Backend, opts ...QueueOption) *Queue {
	q := &Queue{
		backend: backend,
		metrics: NoopMetrics{},
		now:     time.Now,
	}
	for _, opt := range opts {
		opt(q)
	}
	return q
}

func (q *Queue) Enqueue(ctx context.Context, task string, payload any, opts EnqueueOptions) (JobID, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	now := q.now().UTC()
	job := Job{
		ID:             NewJobID(),
		Task:           task,
		Payload:        raw,
		Attempt:        0,
		MaxAttempts:    maxAttempts,
		CreatedAt:      now,
		AvailableAt:    now.Add(opts.Delay),
		IdempotencyKey: opts.IdempotencyKey,
	}
	if err := job.Validate(); err != nil {
		return "", err
	}

	ctx, span := startEnqueueSpan(ctx, &job)
	defer span.End()
	id, err := q.backend.Enqueue(ctx, job)
	if err != nil {
		setOutcome(ctx, "failed", err)
		return "", err
	}
	if id == job.ID {
		q.metrics.JobEnqueued(ctx, job)
		setOutcome(ctx, "enqueued", nil)
	} else {
		setOutcome(ctx, "duplicate", nil) // an earlier enqueue used the same idempotency key
	}
	return id, nil
}
