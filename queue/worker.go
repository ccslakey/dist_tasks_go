package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type HandlerFunc func(ctx context.Context, job Job) JobResult

type WorkerConfig struct {
	ID          string
	Concurrency int
	// PollInterval is how long a claim blocks waiting for new jobs when the
	// queue is empty (or, with PollOnly, how long the worker sleeps between
	// polls). It also paces lease recovery.
	PollInterval time.Duration
	// PollOnly makes the worker poll with non-blocking claims and sleep
	// PollInterval between empty polls, instead of blocking in the claim.
	// Blocking is faster to pick up new jobs; this exists for comparison.
	PollOnly bool
	// VisibilityTimeout is how long a lease can go without a heartbeat
	// before other workers may recover the job.
	VisibilityTimeout time.Duration
	// HeartbeatInterval is how often running jobs' leases are renewed.
	// Defaults to VisibilityTimeout/3.
	HeartbeatInterval time.Duration
	// ShutdownTimeout bounds how long Run waits for in-flight jobs after its
	// context is cancelled. When it expires their contexts are cancelled and
	// any unsettled jobs are left for recovery. Defaults to VisibilityTimeout.
	ShutdownTimeout time.Duration
	// MaxDeliveries dead-letters a job that has been delivered this many
	// times without being settled: a poison message that keeps crashing or
	// hanging its workers. Defaults to 5.
	MaxDeliveries int
	// StatsInterval is how often backend Stats are reported to Metrics.
	// Defaults to 5s.
	StatsInterval time.Duration
	RetryPolicy   RetryPolicy
}

type Worker struct {
	backend  Backend
	metrics  Metrics
	config   WorkerConfig
	handlers map[string]HandlerFunc
	acks     chan ackRequest // set during Run if the backend can batch acks
}

// errShutdown is the cancellation cause for jobs still running when the
// shutdown timeout expires.
var errShutdown = errors.New("worker shutdown timed out")

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
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = config.VisibilityTimeout / 3
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = config.VisibilityTimeout
	}
	if config.MaxDeliveries <= 0 {
		config.MaxDeliveries = 5
	}
	if config.StatsInterval <= 0 {
		config.StatsInterval = 5 * time.Second
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

// runningJob is a job this worker currently holds a lease on.
type runningJob struct {
	job    Job
	cancel context.CancelCauseFunc
}

// Run claims and processes jobs until ctx is cancelled, then stops claiming
// and drains in-flight jobs (see WorkerConfig.ShutdownTimeout).
func (w *Worker) Run(ctx context.Context) error {
	if w.config.ID == "" {
		return fmt.Errorf("worker id is required")
	}

	// Handlers and the backend calls that settle their jobs must outlive ctx
	// so in-flight work can finish and be acked during shutdown. jobsCtx is
	// cancelled only if the drain times out.
	jobsCtx, cancelJobs := context.WithCancelCause(context.WithoutCancel(ctx))
	defer cancelJobs(nil)

	var (
		jobs     sync.WaitGroup
		running  sync.Map // Lease -> *runningJob; keyed by lease, not message, since a job can be redelivered to the worker already running it
		inFlight atomic.Int64
		freed    = make(chan struct{}, 1)
	)

	// Leases are renewed until every in-flight job has finished, including
	// during the drain.
	stopHeartbeat := make(chan struct{})
	var background sync.WaitGroup
	background.Go(func() { w.heartbeat(jobsCtx, &running, stopHeartbeat) })
	if b, ok := w.backend.(batchAcker); ok {
		w.acks = make(chan ackRequest, w.config.Concurrency)
		background.Go(func() { w.ackLoop(jobsCtx, b, stopHeartbeat) })
	}
	background.Go(func() { w.reportStats(ctx) })

	var (
		backoff     time.Duration
		lastRecover time.Time
	)
	for ctx.Err() == nil {
		free := w.config.Concurrency - int(inFlight.Load())
		var claimed []Job
		if free > 0 {
			var err error
			claimed, err = w.fetch(ctx, free, &lastRecover)
			// Start whatever was leased even if part of the fetch failed.
			for _, job := range claimed {
				jctx, cancel := context.WithCancelCause(jobsCtx)
				running.Store(job.Lease, &runningJob{job: job, cancel: cancel})
				inFlight.Add(1)
				jobs.Go(func() {
					defer func() {
						running.Delete(job.Lease)
						cancel(nil)
						inFlight.Add(-1)
						select {
						case freed <- struct{}{}:
						default:
						}
					}()
					w.process(jctx, job)
				})
			}
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				// A broker blip shouldn't kill the worker: back off and retry.
				backoff = min(max(2*backoff, w.config.PollInterval), 10*time.Second)
				slog.Warn("fetching jobs failed; backing off", "worker", w.config.ID, "err", err, "backoff", backoff)
				sleep(ctx, backoff)
				continue
			}
			backoff = 0
			if len(claimed) == free || !w.config.PollOnly {
				// Either more may be ready right now, or the claim already
				// blocked waiting for jobs and should be retried at once.
				continue
			}
		}

		select {
		case <-ctx.Done():
		case <-freed:
		case <-time.After(w.config.PollInterval):
		}
	}

	drained := make(chan struct{})
	go func() {
		jobs.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(w.config.ShutdownTimeout):
		slog.Warn("shutdown timeout expired; cancelling in-flight jobs", "worker", w.config.ID, "in_flight", inFlight.Load())
		cancelJobs(errShutdown)
		<-drained
	}
	close(stopHeartbeat)
	background.Wait()

	if d, ok := w.backend.(deregisterer); ok {
		if err := d.Deregister(context.WithoutCancel(ctx), w.config.ID); err != nil {
			slog.Warn("deregistering worker failed", "worker", w.config.ID, "err", err)
		}
	}
	return ctx.Err()
}

// deregisterer is implemented by backends that track workers and can forget
// one after a clean shutdown.
type deregisterer interface {
	Deregister(ctx context.Context, workerID string) error
}

// fetch leases up to limit jobs: expired leases from dead workers first, then
// new jobs. Recovery runs at most every PollInterval unless it keeps finding
// work, so the PEL scan stays off the hot path.
func (w *Worker) fetch(ctx context.Context, limit int, lastRecover *time.Time) ([]Job, error) {
	wait := w.config.PollInterval
	if w.config.PollOnly {
		wait = 0
	}
	var jobs []Job
	if time.Since(*lastRecover) >= w.config.PollInterval {
		recovered, err := w.backend.RecoverExpired(ctx, w.config.VisibilityTimeout, w.config.ID, limit)
		if err != nil {
			return nil, fmt.Errorf("recover expired: %w", err)
		}
		if len(recovered) < limit {
			*lastRecover = time.Now()
		}
		for _, job := range recovered {
			slog.Info("recovered expired job", "worker", w.config.ID, "job_id", job.ID, "task", job.Task, "delivery", job.Lease.Token)
		}
		jobs = recovered
	}
	if len(jobs) < limit {
		// Don't block if recovery already found work to start.
		if len(jobs) > 0 {
			wait = 0
		}
		claimed, err := w.backend.Claim(ctx, w.config.ID, limit-len(jobs), wait)
		jobs = append(jobs, claimed...)
		if err != nil {
			return jobs, fmt.Errorf("claim: %w", err)
		}
	}
	return jobs, nil
}

func (w *Worker) process(ctx context.Context, job Job) {
	err := w.handleOne(ctx, job)
	switch {
	case err == nil:
	case errors.Is(err, ErrLeaseLost):
		w.metrics.JobLeaseLost(ctx, job)
		slog.Warn("lease lost; another worker owns this job now", "worker", w.config.ID, "job_id", job.ID, "task", job.Task)
	default:
		// The job is still leased to us and will be recovered after the
		// visibility timeout.
		slog.Error("settling job failed", "worker", w.config.ID, "job_id", job.ID, "task", job.Task, "err", err)
	}
}

// handleOne runs one job and settles it: ack on success, retry with backoff on
// a retryable failure, dead-letter on a permanent failure or when attempts are
// exhausted. A job is complete only once the handler succeeds and the ack
// lands, so a crash anywhere before that means it will run again.
func (w *Worker) handleOne(ctx context.Context, job Job) error {
	ctx, span := startProcessSpan(ctx, job, w.config.ID)
	defer span.End()

	if job.Lease.Token > int64(w.config.MaxDeliveries) {
		return w.deadLetter(ctx, job, fmt.Sprintf("delivered %d times without being settled", job.Lease.Token))
	}
	handl, ok := w.handlers[job.Task]
	if !ok {
		return w.deadLetter(ctx, job, fmt.Sprintf("%v %q", ErrUnknownTask, job.Task))
	}

	w.metrics.JobStarted(ctx, job)
	jobRes := handl(ctx, job)
	if ctx.Err() != nil {
		// The lease was lost or shutdown timed out mid-run. Whoever owns the
		// job now (or recovery) decides its fate.
		setOutcome(ctx, "abandoned", context.Cause(ctx))
		return context.Cause(ctx)
	}

	if jobRes.Err == nil {
		if err := w.ack(ctx, job); err != nil {
			setOutcome(ctx, "settle_failed", err)
			return err
		}
		w.metrics.JobCompleted(ctx, job)
		setOutcome(ctx, "completed", nil)
		return nil
	}

	w.metrics.JobFailed(ctx, job, jobRes.Retryable)
	if !jobRes.Retryable || ShouldDeadLetter(job) {
		return w.deadLetter(ctx, job, jobRes.Err.Error())
	}
	nextRunAt := time.Now().UTC().Add(w.config.RetryPolicy.NextDelay(job.Attempt + 1))
	if err := w.backend.Retry(ctx, job, nextRunAt); err != nil {
		setOutcome(ctx, "settle_failed", err)
		return err
	}
	w.metrics.JobRetried(ctx, job)
	setOutcome(ctx, "retried", jobRes.Err)
	return nil
}

func (w *Worker) deadLetter(ctx context.Context, job Job, reason string) error {
	if err := w.backend.DeadLetter(ctx, job, reason); err != nil {
		setOutcome(ctx, "settle_failed", err)
		return err
	}
	w.metrics.JobDeadLettered(ctx, job)
	setOutcome(ctx, "dead_lettered", errors.New(reason))
	slog.Warn("job dead-lettered", "worker", w.config.ID, "job_id", job.ID, "task", job.Task, "reason", reason)
	return nil
}

// batchAcker is implemented by backends that can ack many jobs in one
// round trip. It returns one error per job.
type batchAcker interface {
	AckBatch(ctx context.Context, jobs []Job) []error
}

type ackRequest struct {
	job  Job
	done chan error
}

// maxBatch caps how many acks or lease renewals go out in one round trip.
const maxBatch = 256

func (w *Worker) ack(ctx context.Context, job Job) error {
	if w.acks == nil {
		return w.backend.Ack(ctx, job)
	}
	req := ackRequest{job: job, done: make(chan error, 1)}
	w.acks <- req
	return <-req.done
}

// ackLoop group-commits acks: while one batch is in flight, new acks queue
// up and go out together in the next one. Under light load every batch has
// one ack, so this adds no latency; under heavy load it cuts round trips.
func (w *Worker) ackLoop(ctx context.Context, b batchAcker, stop <-chan struct{}) {
	for {
		var batch []ackRequest
		select {
		case <-stop:
			return
		case req := <-w.acks:
			batch = append(batch, req)
		}
	more:
		for len(batch) < maxBatch {
			select {
			case req := <-w.acks:
				batch = append(batch, req)
			default:
				break more
			}
		}
		jobs := make([]Job, len(batch))
		for i, req := range batch {
			jobs[i] = req.job
		}
		for i, err := range b.AckBatch(ctx, jobs) {
			batch[i].done <- err
		}
	}
}

// heartbeat renews the lease of every running job. A job whose lease was
// taken over has its context cancelled so the handler can stop early.
func (w *Worker) heartbeat(ctx context.Context, running *sync.Map, stop <-chan struct{}) {
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		var held []*runningJob
		running.Range(func(_, v any) bool {
			held = append(held, v.(*runningJob))
			return true
		})
		for batch := range slices.Chunk(held, maxBatch) {
			for i, err := range w.extend(ctx, batch) {
				rj := batch[i]
				if errors.Is(err, ErrLeaseLost) {
					rj.cancel(ErrLeaseLost)
				} else if err != nil {
					slog.Warn("renewing lease failed", "worker", w.config.ID, "job_id", rj.job.ID, "err", err)
				}
			}
		}
	}
}

// batchExtender is implemented by backends that can renew many leases in
// one round trip. It returns one error per job.
type batchExtender interface {
	ExtendBatch(ctx context.Context, jobs []Job) []error
}

// extend renews the leases of running jobs, in one call if the backend
// supports it.
func (w *Worker) extend(ctx context.Context, running []*runningJob) []error {
	jobs := make([]Job, len(running))
	for i, rj := range running {
		jobs[i] = rj.job
	}
	if b, ok := w.backend.(batchExtender); ok {
		return b.ExtendBatch(ctx, jobs)
	}
	errs := make([]error, len(jobs))
	for i, job := range jobs {
		errs[i] = w.backend.Extend(ctx, job)
	}
	return errs
}

func (w *Worker) reportStats(ctx context.Context) {
	ticker := time.NewTicker(w.config.StatsInterval)
	defer ticker.Stop()
	for {
		if st, err := w.backend.Stats(ctx); err == nil {
			w.metrics.StatsObserved(ctx, st)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
