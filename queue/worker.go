package queue

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type HandlerFunc func(ctx context.Context, job Job) JobResult

type WorkerConfig struct {
	ID                string
	Concurrency       int
	PollInterval      time.Duration
	VisibilityTimeout time.Duration
	RetryPolicy       RetryPolicy
}

type Worker struct {
	backend  Backend
	metrics  Metrics
	config   WorkerConfig
	handlers map[string]HandlerFunc
}

func NewWorker(backend Backend, config WorkerConfig, metrics Metrics) *Worker {
	if config.Concurrency <= 0 {
		config.Concurrency = 1
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.VisibilityTimeout <= 0 {
		config.VisibilityTimeout = 30 * time.Second
	}
	if config.RetryPolicy.BaseDelay <= 0 {
		config.RetryPolicy = DefaultRetryPolicy()
	}
	if metrics == nil {
		metrics = NoopMetrics{}
	}
	return &Worker{
		backend:  backend,
		metrics:  metrics,
		config:   config,
		handlers: make(map[string]HandlerFunc),
	}
}

func (w *Worker) Register(task string, handler HandlerFunc) error {
	if task == "" || handler == nil {
		return ErrInvalidJob
	}
	w.handlers[task] = handler
	return nil
}

func (w *Worker) Run(ctx context.Context) error {
	if w.config.ID == "" {
		return fmt.Errorf("worker id is required")
	}

	//worker loop.
	// 1. periodically call RecoverExpired so abandoned leases become runnable again
	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()
	var wg sync.WaitGroup

	for {
		select {
		case <-ticker.C:
			w.backend.RecoverExpired(ctx, w.config.VisibilityTimeout, w.config.ID)

			// 2. claim up to available concurrency from the backend
			claimedJobs, err := w.backend.Claim(ctx, w.config.ID, w.config.Concurrency)
			if err != nil {
				return err
			}

			// 3. run handlers in goroutines, bounded by Concurrency
			for _, job := range claimedJobs {
				wg.Go(func() {
					w.handleOne(ctx, job)
				})
			}

		case <-ctx.Done():
			// 4.  stop claiming on cancellation and wait for in-flight handlers to finish
			wg.Wait()
			return ctx.Err()
		}
	}

}

// Implement job dispatch and outcome routing.
func (w *Worker) handleOne(ctx context.Context, job Job) error {
	// 1. Look up the handler by job.Task; return ErrUnknownTask if not registered.
	handl, ok := w.handlers[job.Task]
	if !ok {
		return ErrUnknownTask
	}
	// 2. Record JobStarted metric, then call the handler.
	w.metrics.JobStarted(ctx, job)
	jobRes := handl(ctx, job)
	// 3. On success (result.Err == nil): Ack the job and record JobCompleted.
	if jobRes.Err == nil {
		if err := w.backend.Ack(ctx, job.ID); err != nil {
			return err
		}
		w.metrics.JobCompleted(ctx, job)
		return nil
	}

	// handler failed
	w.metrics.JobFailed(ctx, job, jobRes.Retryable)
	if !jobRes.Retryable || ShouldDeadLetter(job) {
		err := w.backend.DeadLetter(ctx, job, jobRes.Err.Error())
		w.metrics.JobDeadLettered(ctx, job)
		return err
	} else {
		nextRunAt := time.Now().UTC().Add(w.config.RetryPolicy.NextDelay(job.Attempt + 1))
		err := w.backend.Retry(ctx, job, nextRunAt)
		w.metrics.JobRetried(ctx, job)
		return err
	}
}
