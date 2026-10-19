package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerRegisterRejectsInvalidHandlers(t *testing.T) {
	worker := NewWorker(nil, WorkerConfig{ID: "worker-1"}, nil)

	if err := worker.Register("", func(context.Context, Job) JobResult { return JobResult{} }); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("got %v, want ErrInvalidJob", err)
	}
	if err := worker.Register("email", nil); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("got %v, want ErrInvalidJob", err)
	}
}

func TestWorkerDeadLettersUnknownTask(t *testing.T) {
	backend := &fakeBackend{}
	worker := NewWorker(backend, WorkerConfig{ID: "worker-1"}, nil)
	if err := worker.handleOne(t.Context(), testJob("job-1", "missing")); err != nil {
		t.Fatalf("handleOne: %v", err)
	}
	if _, ok := backend.deadLettered()["job-1"]; !ok {
		t.Fatal("unknown task was not dead-lettered")
	}
}

func TestWorkerMaxAttemptsIsTotalRuns(t *testing.T) {
	backend := &fakeBackend{}
	worker := NewWorker(backend, WorkerConfig{ID: "worker-1"}, nil)
	worker.Register("flaky", func(context.Context, Job) JobResult {
		return NewRetryableError(errors.New("boom"))
	})

	job := testJob("job-1", "flaky")
	job.MaxAttempts = 3
	for attempt := 0; attempt < 3; attempt++ {
		job.Attempt = attempt
		if err := worker.handleOne(t.Context(), job); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if got := len(backend.retried); got != 2 {
		t.Errorf("retries = %d, want 2", got)
	}
	if _, ok := backend.deadLettered()["job-1"]; !ok {
		t.Error("job not dead-lettered after its third run")
	}
}

func TestWorkerDeadLettersPoisonMessage(t *testing.T) {
	backend := &fakeBackend{}
	worker := NewWorker(backend, WorkerConfig{ID: "worker-1", MaxDeliveries: 3}, nil)
	ran := false
	worker.Register("email", func(context.Context, Job) JobResult { ran = true; return JobResult{} })

	job := testJob("job-1", "email")
	job.Lease.Token = 4
	if err := worker.handleOne(t.Context(), job); err != nil {
		t.Fatalf("handleOne: %v", err)
	}
	if ran {
		t.Error("handler ran for a job past MaxDeliveries")
	}
	if _, ok := backend.deadLettered()["job-1"]; !ok {
		t.Error("poison message was not dead-lettered")
	}
}

func TestWorkerBoundsConcurrency(t *testing.T) {
	const jobs, concurrency = 30, 3
	backend := &fakeBackend{}
	for i := range jobs {
		backend.ready = append(backend.ready, testJob(JobID(fmt.Sprint(i)), "slow"))
	}
	worker := NewWorker(backend, WorkerConfig{ID: "worker-1", Concurrency: concurrency, PollInterval: time.Millisecond}, nil)

	var running, peak atomic.Int64
	worker.Register("slow", func(context.Context, Job) JobResult {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
		running.Add(-1)
		return JobResult{}
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, worker)
	waitFor(t, func() bool { return backend.ackedCount() == jobs })
	cancel()
	<-done

	if p := peak.Load(); p > concurrency {
		t.Errorf("peak concurrent handlers = %d, want <= %d", p, concurrency)
	}
}

func TestWorkerRenewsLeasesInBatches(t *testing.T) {
	backend := &batchingBackend{fakeBackend: &fakeBackend{extendErr: ErrLeaseLost}}
	for i := range 20 {
		backend.ready = append(backend.ready, testJob(JobID(fmt.Sprint(i)), "slow"))
	}
	worker := NewWorker(backend, WorkerConfig{
		ID:                "worker-1",
		Concurrency:       20,
		PollInterval:      time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond,
	}, nil)
	var cancelled atomic.Int64
	worker.Register("slow", func(ctx context.Context, _ Job) JobResult {
		<-ctx.Done()
		cancelled.Add(1)
		return JobResult{}
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, worker)
	waitFor(t, func() bool { return cancelled.Load() == 20 })
	cancel()
	<-done

	if backend.extendBatches > 2 {
		t.Errorf("renewing 20 leases took %d calls, want them batched", backend.extendBatches)
	}
}

func TestWorkerBatchesAcks(t *testing.T) {
	const jobs = 50
	backend := &batchingBackend{fakeBackend: &fakeBackend{}}
	for i := range jobs {
		backend.ready = append(backend.ready, testJob(JobID(fmt.Sprint(i)), "email"))
	}
	worker := NewWorker(backend, WorkerConfig{ID: "worker-1", Concurrency: 8, PollInterval: time.Millisecond}, nil)
	worker.Register("email", func(context.Context, Job) JobResult { return JobResult{} })

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, worker)
	waitFor(t, func() bool { return backend.ackedCount() == jobs })
	cancel()
	<-done
}

// batchingBackend adds AckBatch and ExtendBatch to fakeBackend.
type batchingBackend struct {
	*fakeBackend
	extendBatches int
}

func (b *batchingBackend) ExtendBatch(ctx context.Context, jobs []Job) []error {
	b.mu.Lock()
	b.extendBatches++
	b.mu.Unlock()
	errs := make([]error, len(jobs))
	for i, job := range jobs {
		errs[i] = b.Extend(ctx, job)
	}
	return errs
}

func (b *batchingBackend) AckBatch(ctx context.Context, jobs []Job) []error {
	errs := make([]error, len(jobs))
	for i, job := range jobs {
		errs[i] = b.Ack(ctx, job)
	}
	return errs
}

func TestWorkerShutdownDrainsAndAcksInFlightJobs(t *testing.T) {
	backend := &fakeBackend{ready: []Job{testJob("job-1", "slow")}}
	worker := NewWorker(backend, WorkerConfig{ID: "worker-1", PollInterval: time.Millisecond}, nil)

	started, release := make(chan struct{}), make(chan struct{})
	worker.Register("slow", func(context.Context, Job) JobResult {
		close(started)
		<-release
		return JobResult{}
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, worker)
	<-started
	cancel() // SIGTERM while the job is running
	time.Sleep(20 * time.Millisecond)
	close(release)
	<-done

	if backend.ackedCount() != 1 {
		t.Fatal("job that finished during shutdown was not acked")
	}
}

// A sharded backend can lease jobs from some partitions and then fail on
// another. The leased jobs must still run instead of waiting out another
// visibility timeout.
func TestWorkerRunsJobsRecoveredAlongsideAnError(t *testing.T) {
	backend := &fakeBackend{
		recoverable: []Job{testJob("job-1", "email"), testJob("job-2", "email")},
		recoverErr:  errors.New("partition 3: connection refused"),
	}
	worker := NewWorker(backend, WorkerConfig{ID: "worker-1", Concurrency: 4, PollInterval: time.Millisecond}, nil)
	worker.Register("email", func(context.Context, Job) JobResult { return JobResult{} })

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, worker)
	waitFor(t, func() bool { return backend.ackedCount() == 2 })
	cancel()
	<-done
}

func TestWorkerSurvivesTransientClaimErrors(t *testing.T) {
	backend := &fakeBackend{ready: []Job{testJob("job-1", "email")}, claimErrs: 2}
	worker := NewWorker(backend, WorkerConfig{ID: "worker-1", PollInterval: time.Millisecond}, nil)
	worker.Register("email", func(context.Context, Job) JobResult { return JobResult{} })

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, worker)
	waitFor(t, func() bool { return backend.ackedCount() == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run returned %v, want context.Canceled", err)
	}
}

func TestWorkerCancelsHandlerWhenLeaseIsLost(t *testing.T) {
	backend := &fakeBackend{ready: []Job{testJob("job-1", "slow")}, extendErr: ErrLeaseLost}
	worker := NewWorker(backend, WorkerConfig{
		ID:                "worker-1",
		PollInterval:      time.Millisecond,
		HeartbeatInterval: 5 * time.Millisecond,
	}, nil)

	cause := make(chan error, 1)
	worker.Register("slow", func(ctx context.Context, _ Job) JobResult {
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return NewRetryableError(ctx.Err())
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := runWorker(ctx, worker)
	select {
	case err := <-cause:
		if !errors.Is(err, ErrLeaseLost) {
			t.Errorf("handler cancelled with %v, want ErrLeaseLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not cancelled after its lease was lost")
	}
	cancel()
	<-done

	if backend.ackedCount() != 0 || len(backend.retried) != 0 {
		t.Error("worker settled a job whose lease it had lost")
	}
}

func testJob(id JobID, task string) Job {
	return Job{
		ID:          id,
		Task:        task,
		Payload:     []byte(`{}`),
		MaxAttempts: 3,
		Lease:       Lease{MessageID: string(id), Owner: "worker-1", Token: 1},
	}
}

func runWorker(ctx context.Context, w *Worker) <-chan error {
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	return done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(time.Millisecond)
	}
}

// fakeBackend is an in-memory Backend. Like go-redis, it rejects calls made
// with a cancelled context.
type fakeBackend struct {
	mu        sync.Mutex
	ready     []Job
	claimErrs int   // fail this many Claim calls before succeeding
	extendErr error // returned by every Extend

	recoverable []Job // returned once by RecoverExpired, along with recoverErr
	recoverErr  error

	acked   []Job
	retried []Job
	dead    map[JobID]string
}

func (f *fakeBackend) Enqueue(_ context.Context, job Job) (JobID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ready = append(f.ready, job)
	return job.ID, nil
}

func (f *fakeBackend) Claim(ctx context.Context, _ string, limit int, wait time.Duration) ([]Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	if f.claimErrs > 0 {
		f.claimErrs--
		f.mu.Unlock()
		return nil, errors.New("connection refused")
	}
	n := min(limit, len(f.ready))
	jobs := f.ready[:n:n]
	f.ready = f.ready[n:]
	f.mu.Unlock()
	if n == 0 {
		sleep(ctx, wait) // like a blocking XREADGROUP on an empty stream
	}
	return jobs, nil
}

func (f *fakeBackend) RecoverExpired(ctx context.Context, _ time.Duration, _ string, _ int) ([]Job, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	jobs := f.recoverable
	f.recoverable = nil
	return jobs, f.recoverErr
}

func (f *fakeBackend) Extend(ctx context.Context, _ Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f.extendErr
}

func (f *fakeBackend) Ack(ctx context.Context, job Job) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acked = append(f.acked, job)
	return nil
}

func (f *fakeBackend) Retry(ctx context.Context, job Job, _ time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retried = append(f.retried, job)
	return nil
}

func (f *fakeBackend) DeadLetter(ctx context.Context, job Job, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dead == nil {
		f.dead = map[JobID]string{}
	}
	f.dead[job.ID] = reason
	return nil
}

func (f *fakeBackend) Stats(context.Context) (Stats, error) { return Stats{}, nil }

func (f *fakeBackend) ackedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acked)
}

func (f *fakeBackend) deadLettered() map[JobID]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dead
}

func TestEnqueueBatchFallsBackToSingleEnqueues(t *testing.T) {
	backend := &fakeBackend{}
	ids, err := New(backend).EnqueueBatch(t.Context(), []EnqueueRequest{
		{Task: "email", Payload: 1},
		{Task: "email", Payload: 2},
		{Task: "image", Payload: 3},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if len(ids) != 3 || len(backend.ready) != 3 || backend.ready[2].ID != ids[2] {
		t.Fatalf("ids=%v ready=%d, want 3 jobs in order", ids, len(backend.ready))
	}
}
