package queue

import (
	"context"
	"errors"
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
	worker := NewWorker(fakeBackend{}, WorkerConfig{ID: "worker-1"}, nil)
	err := worker.handleOne(context.Background(), Job{ID: "job-1", Task: "missing", MaxAttempts: 3})
	if !errors.Is(err, ErrUnknownTask) {
		t.Fatalf("got %v, want ErrUnknownTask", err)
	}
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

func TestWorkerRunClaimErrorPropagates(t *testing.T) {
	sentinel := errors.New("claim exploded")
	b := &controlledBackend{claimErr: sentinel}
	w := workerWith(b, &spyMetrics{})

	ctx := context.Background()
	err := w.Run(ctx)
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want %v", err, sentinel)
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
	mu           sync.Mutex
	claimJobs    []Job   // returned once on the first Claim call; empty thereafter
	claimErr     error   // returned by every Claim call if set
	ackedIDs     []JobID
	retriedIDs   []JobID
	deadLetteredIDs []JobID
}

func (b *controlledBackend) Claim(_ context.Context, _ string, _ int) ([]Job, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
func (b *controlledBackend) RecoverExpired(_ context.Context, _ time.Duration, _ string) (int, error) {
	return 0, nil
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
	mu   sync.Mutex
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

func (m *spyMetrics) JobEnqueued(_ context.Context, _ Job)     { m.record("enqueued") }
func (m *spyMetrics) JobStarted(_ context.Context, _ Job)      { m.record("started") }
func (m *spyMetrics) JobCompleted(_ context.Context, _ Job)    { m.record("completed") }
func (m *spyMetrics) JobFailed(_ context.Context, _ Job, _ bool) { m.record("failed") }
func (m *spyMetrics) JobRetried(_ context.Context, _ Job)      { m.record("retried") }
func (m *spyMetrics) JobDeadLettered(_ context.Context, _ Job) { m.record("deadLettered") }
func (m *spyMetrics) StatsObserved(_ context.Context, _ Stats) {}

// fakeBackend is a zero-value stub that satisfies Backend. Used by tests that
// only care about registration/dispatch logic, not call recording.
type fakeBackend struct{}

func (fakeBackend) Enqueue(context.Context, Job) (JobID, error)                        { return "job-1", nil }
func (fakeBackend) Claim(context.Context, string, int) ([]Job, error)                  { return nil, nil }
func (fakeBackend) Ack(context.Context, JobID) error                                   { return nil }
func (fakeBackend) Retry(context.Context, Job, time.Time) error                        { return nil }
func (fakeBackend) DeadLetter(context.Context, Job, string) error                      { return nil }
func (fakeBackend) RecoverExpired(context.Context, time.Duration, string) (int, error) { return 0, nil }
func (fakeBackend) Stats(context.Context) (Stats, error)                               { return Stats{}, nil }
