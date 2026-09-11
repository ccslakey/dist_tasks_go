package queue

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

// --- existing registration tests ---

func TestWorkerRegisterRejectsInvalidHandlers(t *testing.T) {
	worker := NewWorker(nil, WorkerConfig{ID: "worker-1"}, nil)

	if err := worker.Register("", func(context.Context, Job) JobResult { return JobResult{} }); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("got %v, want ErrInvalidJob", err)
	}
	if err := worker.Register("email", nil); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("got %v, want ErrInvalidJob", err)
	}
}

func TestWorkerHandleOneUnknownTask(t *testing.T) {
	b := &controlledBackend{}
	worker := workerWith(b, &spyMetrics{})

	err := worker.handleOne(context.Background(), Job{ID: "job-1", Task: "missing", MaxAttempts: 3})
	if !errors.Is(err, ErrUnknownTask) {
		t.Fatalf("got %v, want ErrUnknownTask", err)
	}

	// A job with no handler can never succeed in this process. Leaving it
	// pending would mean the reclaim path hands it back on every visibility
	// timeout, forever, so it has to go to the DLQ.
	b.assertDeadLettered(t, "job-1")
	b.assertNotRetried(t)
}

// --- handleOne outcome routing ---

func TestHandleOneSuccess(t *testing.T) {
	b := &controlledBackend{}
	m := &spyMetrics{}
	w := workerWith(b, m)
	w.Register("task", okHandler)

	job := Job{ID: "j1", Task: "task", MaxAttempts: 3}
	if err := w.handleOne(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	b.assertAcked(t, "j1")
	b.assertNotRetried(t)
	b.assertNotDeadLettered(t)
	m.assertCalls(t, []string{"started", "completed"})
}

func TestHandleOneRetryableError(t *testing.T) {
	b := &controlledBackend{}
	m := &spyMetrics{}
	w := workerWith(b, m)
	w.Register("task", retryableHandler)

	job := Job{ID: "j1", Task: "task", Attempt: 0, MaxAttempts: 3}
	if err := w.handleOne(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	b.assertNotAcked(t)
	b.assertRetried(t, "j1")
	b.assertNotDeadLettered(t)
	m.assertCalls(t, []string{"started", "failed", "retried"})
}

func TestHandleOneExhaustedRetryableMovesToDLQ(t *testing.T) {
	b := &controlledBackend{}
	m := &spyMetrics{}
	w := workerWith(b, m)
	w.Register("task", retryableHandler)

	// Attempt == MaxAttempts → Exhausted() == true → dead letter despite retryable flag
	job := Job{ID: "j1", Task: "task", Attempt: 3, MaxAttempts: 3}
	if err := w.handleOne(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	b.assertNotAcked(t)
	b.assertNotRetried(t)
	b.assertDeadLettered(t, "j1")
	m.assertCalls(t, []string{"started", "failed", "deadLettered"})
}

func TestHandleOnePermanentError(t *testing.T) {
	b := &controlledBackend{}
	m := &spyMetrics{}
	w := workerWith(b, m)
	w.Register("task", permanentHandler)

	job := Job{ID: "j1", Task: "task", Attempt: 0, MaxAttempts: 3}
	if err := w.handleOne(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	b.assertNotAcked(t)
	b.assertNotRetried(t)
	b.assertDeadLettered(t, "j1")
	m.assertCalls(t, []string{"started", "failed", "deadLettered"})
}

// --- Worker.Run lifecycle ---

func TestWorkerRunShutdown(t *testing.T) {
	b := &controlledBackend{}
	w := workerWith(b, &spyMetrics{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
}

func TestWorkerRunGivesUpAfterRepeatedClaimErrors(t *testing.T) {
	sentinel := errors.New("claim exploded")
	b := &controlledBackend{claimErr: sentinel}
	w := workerWith(b, &spyMetrics{})
	w.config.MaxClaimErrors = 3

	err := w.Run(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want %v", err, sentinel)
	}

	b.mu.Lock()
	calls := b.claimCalls
	b.mu.Unlock()
	if calls != 3 {
		t.Errorf("Claim called %d times, want 3 (one per tolerated failure)", calls)
	}
}

func TestWorkerRunSurvivesTransientClaimErrors(t *testing.T) {
	b := &flakyClaimBackend{failures: 2, job: Job{ID: "j1", Task: "task", MaxAttempts: 3}}
	w := workerWith(b, &spyMetrics{})
	w.Register("task", okHandler)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = w.Run(ctx)

	// The worker must ride out the first two failures and still run the job.
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.delivered {
		t.Fatal("job was never claimed after transient Claim errors")
	}
}

func TestWorkerRunWaitsForInFlightJobs(t *testing.T) {
	started := make(chan struct{})
	finished := make(chan struct{})

	slow := func(_ context.Context, job Job) JobResult {
		close(started)
		<-finished
		return JobResult{}
	}

	b := &controlledBackend{
		claimJobs: []Job{{ID: "j1", Task: "slow", MaxAttempts: 1}},
	}
	w := workerWith(b, &spyMetrics{})
	w.Register("slow", slow)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Wait until the slow handler has started, then cancel.
	<-started
	cancel()

	// Run must not return before we unblock the handler.
	select {
	case <-done:
		t.Fatal("Run returned before in-flight handler finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(finished)
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after in-flight handler finished")
	}
}

// --- helpers ---

func workerWith(b Backend, m Metrics) *Worker {
	return NewWorker(b, WorkerConfig{
		ID:           "test-worker",
		Concurrency:  4,
		PollInterval: 10 * time.Millisecond,
		RetryPolicy:  DefaultRetryPolicy(),
		Logger:       discardLogger(),
	}, m)
}

var okHandler HandlerFunc = func(_ context.Context, _ Job) JobResult { return JobResult{} }
var retryableHandler HandlerFunc = func(_ context.Context, _ Job) JobResult {
	return NewRetryableError(errors.New("transient"))
}
var permanentHandler HandlerFunc = func(_ context.Context, _ Job) JobResult {
	return NewPermanentError(errors.New("permanent"))
}

// controlledBackend is a configurable Backend test double that records calls.
type controlledBackend struct {
	mu              sync.Mutex
	claimJobs       []Job // returned once on the first Claim call; empty thereafter
	claimErr        error // returned by every Claim call if set
	claimCalls      int
	claimLimits     []int // limit argument passed to each Claim call
	reclaimJobs     []Job // returned once on the first RecoverExpired call
	reclaimErr      error
	ackedIDs        []JobID
	retriedIDs      []JobID
	deadLetteredIDs []JobID
}

func (b *controlledBackend) Claim(_ context.Context, _ string, limit int) ([]Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.claimCalls++
	b.claimLimits = append(b.claimLimits, limit)
	if b.claimErr != nil {
		return nil, b.claimErr
	}
	jobs := b.claimJobs
	b.claimJobs = nil
	return jobs, nil
}

func (b *controlledBackend) Ack(_ context.Context, id JobID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ackedIDs = append(b.ackedIDs, id)
	return nil
}

func (b *controlledBackend) Retry(_ context.Context, job Job, _ time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retriedIDs = append(b.retriedIDs, job.ID)
	return nil
}

func (b *controlledBackend) DeadLetter(_ context.Context, job Job, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deadLetteredIDs = append(b.deadLetteredIDs, job.ID)
	return nil
}

func (b *controlledBackend) Enqueue(_ context.Context, job Job) (JobID, error) { return job.ID, nil }
func (b *controlledBackend) RecoverExpired(_ context.Context, _ time.Duration, _ string, _ int) ([]Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	jobs := b.reclaimJobs
	b.reclaimJobs = nil
	return jobs, b.reclaimErr
}
func (b *controlledBackend) Stats(_ context.Context) (Stats, error) { return Stats{}, nil }

func (b *controlledBackend) assertAcked(t *testing.T, id JobID) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, v := range b.ackedIDs {
		if v == id {
			return
		}
	}
	t.Errorf("expected Ack(%q) but was not called; acked: %v", id, b.ackedIDs)
}

func (b *controlledBackend) assertNotAcked(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.ackedIDs) > 0 {
		t.Errorf("expected no Ack calls, got %v", b.ackedIDs)
	}
}

func (b *controlledBackend) assertRetried(t *testing.T, id JobID) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, v := range b.retriedIDs {
		if v == id {
			return
		}
	}
	t.Errorf("expected Retry(%q) but was not called; retried: %v", id, b.retriedIDs)
}

func (b *controlledBackend) assertNotRetried(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.retriedIDs) > 0 {
		t.Errorf("expected no Retry calls, got %v", b.retriedIDs)
	}
}

func (b *controlledBackend) assertDeadLettered(t *testing.T, id JobID) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, v := range b.deadLetteredIDs {
		if v == id {
			return
		}
	}
	t.Errorf("expected DeadLetter(%q) but was not called; dead-lettered: %v", id, b.deadLetteredIDs)
}

func (b *controlledBackend) assertNotDeadLettered(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.deadLetteredIDs) > 0 {
		t.Errorf("expected no DeadLetter calls, got %v", b.deadLetteredIDs)
	}
}

// spyMetrics records which Metrics callbacks were invoked, in order.
type spyMetrics struct {
	mu    sync.Mutex
	calls []string
}

func (m *spyMetrics) record(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, name)
}

func (m *spyMetrics) assertCalls(t *testing.T, want []string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) != len(want) {
		t.Errorf("metric calls: got %v, want %v", m.calls, want)
		return
	}
	for i := range want {
		if m.calls[i] != want[i] {
			t.Errorf("metric call[%d]: got %q, want %q (full: %v)", i, m.calls[i], want[i], m.calls)
		}
	}
}

func (m *spyMetrics) JobEnqueued(_ context.Context, _ Job)       { m.record("enqueued") }
func (m *spyMetrics) JobStarted(_ context.Context, _ Job)        { m.record("started") }
func (m *spyMetrics) JobCompleted(_ context.Context, _ Job)      { m.record("completed") }
func (m *spyMetrics) JobFailed(_ context.Context, _ Job, _ bool) { m.record("failed") }
func (m *spyMetrics) JobRetried(_ context.Context, _ Job)        { m.record("retried") }
func (m *spyMetrics) JobDeadLettered(_ context.Context, _ Job)   { m.record("deadLettered") }
func (m *spyMetrics) StatsObserved(_ context.Context, _ Stats)   {}

// fakeBackend is a zero-value stub that satisfies Backend. Used by tests that
// only care about registration/dispatch logic, not call recording.
type fakeBackend struct{}

func (fakeBackend) Enqueue(context.Context, Job) (JobID, error)       { return "job-1", nil }
func (fakeBackend) Claim(context.Context, string, int) ([]Job, error) { return nil, nil }
func (fakeBackend) Ack(context.Context, JobID) error                  { return nil }
func (fakeBackend) Retry(context.Context, Job, time.Time) error       { return nil }
func (fakeBackend) DeadLetter(context.Context, Job, string) error     { return nil }
func (fakeBackend) RecoverExpired(context.Context, time.Duration, string, int) ([]Job, error) {
	return nil, nil
}
func (fakeBackend) Stats(context.Context) (Stats, error) { return Stats{}, nil }

// --- concurrency bound, panic recovery, reclaim dispatch ---

func TestWorkerNeverExceedsConcurrency(t *testing.T) {
	const concurrency = 3

	var (
		mu      sync.Mutex
		running int
		peak    int
	)
	release := make(chan struct{})

	blocking := func(_ context.Context, _ Job) JobResult {
		mu.Lock()
		running++
		if running > peak {
			peak = running
		}
		mu.Unlock()

		<-release

		mu.Lock()
		running--
		mu.Unlock()
		return JobResult{}
	}

	// An endless supply of work: every Claim hands back exactly what was asked
	// for, so nothing but the semaphore limits in-flight jobs.
	b := &infiniteBackend{}
	w := NewWorker(b, WorkerConfig{
		ID:           "test-worker",
		Concurrency:  concurrency,
		PollInterval: time.Millisecond,
		RetryPolicy:  DefaultRetryPolicy(),
		Logger:       discardLogger(),
	}, &spyMetrics{})
	w.Register("task", blocking)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	// Give the poll loop many ticks to over-claim if it is going to.
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	gotPeak := peak
	mu.Unlock()

	cancel()
	close(release)
	<-done

	if gotPeak > concurrency {
		t.Fatalf("peak in-flight handlers = %d, want <= %d", gotPeak, concurrency)
	}
	if gotPeak != concurrency {
		t.Errorf("peak in-flight handlers = %d, want %d (pool never filled)", gotPeak, concurrency)
	}

	// Every claim must ask for free slots only, never more.
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, limit := range b.limits {
		if limit > concurrency {
			t.Fatalf("Claim call %d asked for %d jobs, want <= %d", i, limit, concurrency)
		}
	}
}

func TestHandlerPanicIsDeadLetteredNotFatal(t *testing.T) {
	b := &controlledBackend{}
	m := &spyMetrics{}
	w := workerWith(b, m)
	w.Register("task", func(_ context.Context, _ Job) JobResult {
		var p *Job
		_ = p.Task // nil dereference
		return JobResult{}
	})

	job := Job{ID: "j1", Task: "task", Attempt: 0, MaxAttempts: 3}
	// The point of the test is that this returns at all rather than taking the
	// process down with it.
	if err := w.handleOne(context.Background(), job); err != nil {
		t.Fatal(err)
	}

	// A panic is deterministic often enough that retrying just burns attempts,
	// so it is routed as a permanent failure.
	b.assertDeadLettered(t, "j1")
	b.assertNotRetried(t)
	b.assertNotAcked(t)
	m.assertCalls(t, []string{"started", "failed", "deadLettered"})
}

func TestWorkerRunDispatchesReclaimedJobs(t *testing.T) {
	ran := make(chan JobID, 1)

	b := &controlledBackend{
		reclaimJobs: []Job{{ID: "orphan", Task: "task", MaxAttempts: 3}},
	}
	w := workerWith(b, &spyMetrics{})
	w.Register("task", func(_ context.Context, job Job) JobResult {
		select {
		case ran <- job.ID:
		default:
		}
		return JobResult{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	select {
	case id := <-ran:
		if id != "orphan" {
			t.Fatalf("ran job %q, want %q", id, "orphan")
		}
	case <-time.After(2 * time.Second):
		// Reclaiming a job without running it strands it: once XAUTOCLAIM moves
		// an entry to this consumer, a normal claim will never return it again.
		t.Fatal("reclaimed job was never dispatched")
	}
}

func TestJobContextOutlivesWorkerCancellation(t *testing.T) {
	w := workerWith(&controlledBackend{}, &spyMetrics{})

	parent, cancel := context.WithCancel(context.Background())
	jobCtx, release := w.jobContext(parent)
	defer release()

	cancel()

	select {
	case <-jobCtx.Done():
		t.Fatal("handler context was cancelled by worker shutdown; the Ack that " +
			"follows the handler would fail and leave the job pending")
	case <-time.After(50 * time.Millisecond):
	}

	if _, ok := jobCtx.Deadline(); !ok {
		t.Error("handler context has no deadline; a hung handler would hold a slot forever")
	}
}

// flakyClaimBackend fails the first n Claim calls, then delivers one job.
type flakyClaimBackend struct {
	mu        sync.Mutex
	failures  int
	calls     int
	job       Job
	delivered bool
}

func (b *flakyClaimBackend) Claim(_ context.Context, _ string, _ int) ([]Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.calls <= b.failures {
		return nil, errors.New("transient redis blip")
	}
	if b.delivered {
		return nil, nil
	}
	b.delivered = true
	return []Job{b.job}, nil
}

func (b *flakyClaimBackend) Enqueue(_ context.Context, job Job) (JobID, error) { return job.ID, nil }
func (b *flakyClaimBackend) Ack(context.Context, JobID) error                  { return nil }
func (b *flakyClaimBackend) Retry(context.Context, Job, time.Time) error       { return nil }
func (b *flakyClaimBackend) DeadLetter(context.Context, Job, string) error     { return nil }
func (b *flakyClaimBackend) RecoverExpired(context.Context, time.Duration, string, int) ([]Job, error) {
	return nil, nil
}
func (b *flakyClaimBackend) Stats(context.Context) (Stats, error) { return Stats{}, nil }

// infiniteBackend always returns exactly as many jobs as it is asked for, and
// records the limits it was asked with.
type infiniteBackend struct {
	mu     sync.Mutex
	limits []int
	next   int
}

func (b *infiniteBackend) Claim(_ context.Context, _ string, limit int) ([]Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.limits = append(b.limits, limit)
	jobs := make([]Job, 0, limit)
	for range limit {
		b.next++
		jobs = append(jobs, Job{ID: JobID(strconv.Itoa(b.next)), Task: "task", MaxAttempts: 3})
	}
	return jobs, nil
}

func (b *infiniteBackend) Enqueue(_ context.Context, job Job) (JobID, error) { return job.ID, nil }
func (b *infiniteBackend) Ack(context.Context, JobID) error                  { return nil }
func (b *infiniteBackend) Retry(context.Context, Job, time.Time) error       { return nil }
func (b *infiniteBackend) DeadLetter(context.Context, Job, string) error     { return nil }
func (b *infiniteBackend) RecoverExpired(context.Context, time.Duration, string, int) ([]Job, error) {
	return nil, nil
}
func (b *infiniteBackend) Stats(context.Context) (Stats, error) { return Stats{}, nil }
