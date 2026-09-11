package queue

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// HandlerFunc processes a single job. Handlers must be idempotent: a job can be
// delivered more than once (see the at-least-once notes on Worker.Run).
type HandlerFunc func(ctx context.Context, job Job) JobResult

type WorkerConfig struct {
	// ID names this worker's Redis consumer. It must be unique per process,
	// because the consumer name is what owns entries in the pending list.
	ID string

	// Concurrency bounds how many handlers run at once. The worker never claims
	// more jobs than it has free slots for, so in-flight work cannot exceed it.
	Concurrency int

	// PollInterval is how often the worker looks for work.
	PollInterval time.Duration

	// VisibilityTimeout is how long a claimed job may stay unacked before
	// another worker is allowed to reclaim it. It doubles as the per-job
	// handler deadline, since a handler still running past this point is racing
	// whichever worker reclaimed its job.
	VisibilityTimeout time.Duration

	RetryPolicy RetryPolicy

	// MaxClaimErrors is how many consecutive claim failures Run tolerates
	// before returning. A transient Redis blip should not kill a worker, but a
	// permanent misconfiguration should not spin forever either. Default 5.
	MaxClaimErrors int

	// Logger receives handler panics, reclaim notices and failures that have
	// nowhere else to go. Defaults to slog.Default().
	Logger *slog.Logger
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
	if config.MaxClaimErrors <= 0 {
		config.MaxClaimErrors = 5
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
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

// Register binds a handler to a task name. It is not safe to call concurrently
// with Run; register everything before starting the worker.
func (w *Worker) Register(task string, handler HandlerFunc) error {
	if task == "" || handler == nil {
		return ErrInvalidJob
	}
	w.handlers[task] = handler
	return nil
}

// Run polls the backend until ctx is cancelled.
//
// Each tick does three things, in order:
//
//  1. Reclaims jobs whose lease expired (a worker crashed holding them) and
//     dispatches them like any other job. Reclaiming without dispatching would
//     strand them: once XAUTOCLAIM moves an entry to this consumer, XREADGROUP
//     with ">" will never return it again.
//  2. Claims new jobs, but only as many as there are free concurrency slots.
//  3. Hands each job to a goroutine from a bounded pool.
//
// On cancellation Run stops claiming and waits for in-flight handlers. Those
// handlers run on contexts detached from ctx, so a shutdown does not cancel
// work that is already running and leave it unacked in the pending list.
func (w *Worker) Run(ctx context.Context) error {
	if w.config.ID == "" {
		return errors.New("worker id is required")
	}

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	// sem bounds in-flight handlers. cap(sem)-len(sem) is the number of free
	// slots; it can only be an underestimate, since the only sends happen on
	// this goroutine and receives happen as handlers finish.
	sem := make(chan struct{}, w.config.Concurrency)
	claimErrors := 0

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()

		case <-ticker.C:
			// select picks randomly among ready cases, so re-check rather than
			// starting another claim on the tick after cancellation.
			if ctx.Err() != nil {
				continue
			}

			free := cap(sem) - len(sem)
			if free == 0 {
				continue
			}

			batch := w.reclaim(ctx, free)

			if len(batch) < free {
				claimed, err := w.backend.Claim(ctx, w.config.ID, free-len(batch))
				if err != nil {
					claimErrors++
					w.config.Logger.Warn("claim failed",
						"err", err,
						"consecutive", claimErrors,
						"limit", w.config.MaxClaimErrors)
					if claimErrors >= w.config.MaxClaimErrors {
						wg.Wait()
						return fmt.Errorf("claim failed %d times in a row: %w", claimErrors, err)
					}
					continue
				}
				claimErrors = 0
				batch = append(batch, claimed...)
			}

			if st, err := w.backend.Stats(ctx); err == nil {
				w.metrics.StatsObserved(ctx, st)
			}

			for _, job := range batch {
				sem <- struct{}{}
				wg.Add(1)
				go func(job Job) {
					defer wg.Done()
					defer func() { <-sem }()

					jobCtx, cancel := w.jobContext(ctx)
					defer cancel()

					if err := w.handleOne(jobCtx, job); err != nil {
						w.config.Logger.Error("job handling failed",
							"task", job.Task,
							"job_id", string(job.ID),
							"err", err)
					}
				}(job)
			}
		}
	}
}

// reclaim picks up jobs abandoned by dead workers. Failures here are logged
// rather than fatal: recovery is a background concern and should not stop a
// healthy worker from claiming new work.
func (w *Worker) reclaim(ctx context.Context, limit int) []Job {
	jobs, err := w.backend.RecoverExpired(ctx, w.config.VisibilityTimeout, w.config.ID, limit)
	if err != nil {
		w.config.Logger.Warn("recover expired failed", "err", err)
		return nil
	}
	if len(jobs) > 0 {
		w.config.Logger.Info("reclaimed abandoned jobs", "count", len(jobs))
	}
	return jobs
}

// jobContext returns the context a handler runs on.
//
// It is deliberately detached from the worker's context. Cancelling the worker
// stops new claims, but it must not abort a handler mid-flight, because the Ack
// that follows the handler would then fail and the job would sit in the pending
// list until its lease expired. The deadline is VisibilityTimeout: past that
// point another worker may legally reclaim the job, so letting the original
// handler keep running only widens the window where both run at once.
func (w *Worker) jobContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), w.config.VisibilityTimeout)
}

// handleOne dispatches one job and routes the outcome.
func (w *Worker) handleOne(ctx context.Context, job Job) error {
	handler, ok := w.handlers[job.Task]
	if !ok {
		// Poison pill. Nothing in this process can ever run this job, and
		// leaving it in the pending list means the reclaim path hands it back
		// on every visibility timeout, forever. Dead-letter it instead.
		reason := fmt.Sprintf("no handler registered for task %q", job.Task)
		if err := w.backend.DeadLetter(ctx, job, reason); err != nil {
			return errors.Join(ErrUnknownTask, err)
		}
		w.metrics.JobDeadLettered(ctx, job)
		return ErrUnknownTask
	}

	w.metrics.JobStarted(ctx, job)
	result := w.invoke(ctx, handler, job)

	if result.Err == nil {
		if err := w.backend.Ack(ctx, job.ID); err != nil {
			// The handler already ran. The job stays in the pending list and
			// will be reclaimed and run again, which is why handlers have to be
			// idempotent.
			return fmt.Errorf("ack job %s: %w", job.ID, err)
		}
		w.metrics.JobCompleted(ctx, job)
		return nil
	}

	w.metrics.JobFailed(ctx, job, result.Retryable)

	if !result.Retryable || ShouldDeadLetter(job) {
		if err := w.backend.DeadLetter(ctx, job, result.Err.Error()); err != nil {
			return fmt.Errorf("dead-letter job %s: %w", job.ID, err)
		}
		w.metrics.JobDeadLettered(ctx, job)
		return nil
	}

	nextRunAt := time.Now().UTC().Add(w.config.RetryPolicy.NextDelay(job.Attempt + 1))
	if err := w.backend.Retry(ctx, job, nextRunAt); err != nil {
		return fmt.Errorf("schedule retry for job %s: %w", job.ID, err)
	}
	w.metrics.JobRetried(ctx, job)
	return nil
}

// invoke calls a handler and converts a panic into a permanent failure.
//
// Without this, one bad handler takes down the whole worker process: a panic in
// a goroutine cannot be recovered by its parent. A panic is treated as
// permanent rather than retryable because it is almost always deterministic
// (a nil dereference on a malformed payload), so retrying it just burns the
// attempt budget before landing in the same place.
func (w *Worker) invoke(ctx context.Context, handler HandlerFunc, job Job) (result JobResult) {
	defer func() {
		if r := recover(); r != nil {
			w.config.Logger.Error("handler panicked",
				"task", job.Task,
				"job_id", string(job.ID),
				"panic", r,
				"stack", string(debug.Stack()))
			result = NewPermanentError(fmt.Errorf("handler panicked: %v", r))
		}
	}()
	return handler(ctx, job)
}

// discardLogger is a convenience for callers (and tests) that want a worker to
// stay quiet.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
