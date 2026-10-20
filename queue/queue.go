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
	job, err := q.newJob(task, payload, opts)
	if err != nil {
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

// EnqueueRequest is one job for EnqueueBatch.
type EnqueueRequest struct {
	Task    string
	Payload any
	Options EnqueueOptions
}

// EnqueueBatch enqueues several jobs in one round trip when the backend
// supports it. It returns one ID per request; a request whose idempotency key
// was already used gets the original job's ID. All jobs in the batch share one
// trace span.
//
// On error, the IDs still say which jobs were stored: an empty ID means that
// request wasn't, so retry only those (or give every request an idempotency
// key and retry them all). A single Redis stores the whole batch or none of
// it, but a sharded backend commits each partition separately, and backends
// without batching enqueue one job at a time, so part of a batch can land.
func (q *Queue) EnqueueBatch(ctx context.Context, reqs []EnqueueRequest) ([]JobID, error) {
	jobs := make([]Job, len(reqs))
	for i, r := range reqs {
		job, err := q.newJob(r.Task, r.Payload, r.Options)
		if err != nil {
			return nil, err
		}
		jobs[i] = job
	}
	if len(jobs) == 0 {
		return nil, nil
	}

	ctx, span := startEnqueueSpan(ctx, &jobs[0])
	defer span.End()
	for i := range jobs[1:] {
		jobs[i+1].TraceParent = jobs[0].TraceParent
	}

	ids := make([]JobID, len(jobs))
	var err error
	if b, ok := q.backend.(batchEnqueuer); ok {
		var got []JobID
		got, err = b.EnqueueBatch(ctx, jobs)
		copy(ids, got) // nil when nothing was stored
	} else {
		for i, job := range jobs {
			if ids[i], err = q.backend.Enqueue(ctx, job); err != nil {
				break
			}
		}
	}
	for i, job := range jobs {
		if ids[i] == job.ID {
			q.metrics.JobEnqueued(ctx, job)
		}
	}
	if err != nil {
		setOutcome(ctx, "failed", err)
		return ids, err
	}
	setOutcome(ctx, "enqueued", nil)
	return ids, nil
}

// batchEnqueuer is implemented by backends that can enqueue many jobs in one
// round trip. On error it returns the IDs of the jobs it did store (empty for
// the rest), or nil if it stored none.
type batchEnqueuer interface {
	EnqueueBatch(ctx context.Context, jobs []Job) ([]JobID, error)
}

func (q *Queue) newJob(task string, payload any, opts EnqueueOptions) (Job, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Job{}, err
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
		return Job{}, err
	}
	return job, nil
}
