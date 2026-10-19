package redisstream

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"dist_tasks_go/internal/testredis"
	"dist_tasks_go/queue"

	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	inst := testredis.Start(t)
	// Namespace keys per test so concurrent test runs (or leftover state)
	// don't collide on the default dtq:* keys.
	prefix := "test:" + strings.ReplaceAll(t.Name(), "/", "_") + ":"
	store, err := New(Config{
		Addr:       inst.Addr,
		Stream:     prefix + "jobs",
		Group:      prefix + "workers",
		RetryKey:   prefix + "retry",
		DeadLetter: prefix + "dead",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func newJob(maxAttempts int) queue.Job {
	now := time.Now().UTC()
	return queue.Job{
		Task:        "email",
		Payload:     []byte(`{"to":"x@y"}`),
		MaxAttempts: maxAttempts,
		CreatedAt:   now,
		AvailableAt: now,
	}
}

// claimOne enqueues a job and claims it as workerID.
func claimOne(t *testing.T, store *Store, workerID string) queue.Job {
	t.Helper()
	if _, err := store.Enqueue(t.Context(), newJob(3)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	jobs, err := store.Claim(t.Context(), workerID, 10)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Claim: jobs=%d err=%v", len(jobs), err)
	}
	return jobs[0]
}

func TestEnqueueClaimAck(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	id, err := store.Enqueue(ctx, newJob(3))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if id == "" {
		t.Fatal("Enqueue returned empty JobID")
	}

	jobs, err := store.Claim(ctx, "worker-1", 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("Claim returned %d jobs, want 1", len(jobs))
	}
	if jobs[0].ID != id || jobs[0].Task != "email" {
		t.Errorf("claimed job = %+v, want ID %s task email", jobs[0], id)
	}

	if err := store.Ack(ctx, jobs[0]); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.InFlight != 0 || stats.StreamLength != 0 {
		t.Errorf("after Ack: InFlight=%d StreamLength=%d, want 0 and 0", stats.InFlight, stats.StreamLength)
	}
}

func TestRetryPromotionKeepsJobID(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	claimed := claimOne(t, store, "worker-1")
	if err := store.Retry(ctx, claimed, time.Now().UTC().Add(-time.Second)); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.RetryScheduled != 1 || stats.InFlight != 0 {
		t.Errorf("after Retry: RetryScheduled=%d InFlight=%d, want 1 and 0", stats.RetryScheduled, stats.InFlight)
	}

	// Next Claim promotes due retries first, so it picks up the job in the same call.
	jobs, err := store.Claim(ctx, "worker-1", 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("Claim after promotion returned %d jobs, want 1", len(jobs))
	}
	if jobs[0].Attempt != 1 {
		t.Errorf("promoted Attempt = %d, want 1 (incremented by Retry)", jobs[0].Attempt)
	}
	if jobs[0].ID != claimed.ID {
		t.Errorf("promoted ID = %s, want stable ID %s", jobs[0].ID, claimed.ID)
	}
}

func TestDeadLetter(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	claimed := claimOne(t, store, "worker-1")
	if err := store.DeadLetter(ctx, claimed, "boom"); err != nil {
		t.Fatalf("DeadLetter: %v", err)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.DeadLetterDepth != 1 {
		t.Errorf("DeadLetterDepth = %d, want 1", stats.DeadLetterDepth)
	}
	if stats.InFlight != 0 {
		t.Errorf("InFlight after DeadLetter = %d, want 0", stats.InFlight)
	}
}

// A worker that crashes mid-job leaves it in the PEL. Once the lease expires,
// another worker must get the job back, and the crashed worker's lease must no
// longer be able to settle it.
func TestRecoverExpiredRedeliversAndFencesOldLease(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	stale := claimOne(t, store, "worker-1")
	const visibility = 50 * time.Millisecond

	if got, err := store.RecoverExpired(ctx, visibility, "worker-2", 10); err != nil || len(got) != 0 {
		t.Fatalf("RecoverExpired before expiry: jobs=%d err=%v, want none", len(got), err)
	}
	time.Sleep(2 * visibility)

	recovered, err := store.RecoverExpired(ctx, visibility, "worker-2", 10)
	if err != nil {
		t.Fatalf("RecoverExpired: %v", err)
	}
	if len(recovered) != 1 {
		t.Fatalf("RecoverExpired returned %d jobs, want 1", len(recovered))
	}
	job := recovered[0]
	if job.ID != stale.ID || job.Lease.Owner != "worker-2" || job.Lease.Token != 2 {
		t.Errorf("recovered %s lease %+v, want ID %s owner worker-2 token 2", job.ID, job.Lease, stale.ID)
	}

	for name, call := range map[string]func() error{
		"Ack":        func() error { return store.Ack(ctx, stale) },
		"Extend":     func() error { return store.Extend(ctx, stale) },
		"Retry":      func() error { return store.Retry(ctx, stale, time.Now()) },
		"DeadLetter": func() error { return store.DeadLetter(ctx, stale, "zombie") },
	} {
		if err := call(); !errors.Is(err, queue.ErrLeaseLost) {
			t.Errorf("%s with stale lease: got %v, want ErrLeaseLost", name, err)
		}
	}

	if err := store.Ack(ctx, job); err != nil {
		t.Fatalf("Ack with current lease: %v", err)
	}
}

func TestExtendPreventsRecovery(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	job := claimOne(t, store, "worker-1")
	const visibility = 100 * time.Millisecond
	for range 4 {
		time.Sleep(visibility / 2)
		if err := store.Extend(ctx, job); err != nil {
			t.Fatalf("Extend: %v", err)
		}
		if got, err := store.RecoverExpired(ctx, visibility, "worker-2", 10); err != nil || len(got) != 0 {
			t.Fatalf("RecoverExpired stole a heartbeating job: jobs=%d err=%v", len(got), err)
		}
	}
	// Extending must not bump the fencing token.
	if err := store.Ack(ctx, job); err != nil {
		t.Fatalf("Ack after extends: %v", err)
	}
}

func TestConcurrentPromotionIsExactlyOnce(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	const jobs = 200
	for i := range jobs {
		job := newJob(3)
		job.ID = queue.JobID(fmt.Sprint(i))
		if err := store.schedule(ctx, job, time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("schedule: %v", err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	promoted := 0
	for range 8 {
		wg.Go(func() {
			for {
				n, err := store.promoteRetries(ctx, 10)
				if err != nil {
					t.Errorf("promoteRetries: %v", err)
					return
				}
				if n == 0 {
					return
				}
				mu.Lock()
				promoted += n
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if promoted != jobs || stats.StreamLength != jobs || stats.RetryScheduled != 0 {
		t.Errorf("promoted=%d stream=%d scheduled=%d, want %d, %d, 0", promoted, stats.StreamLength, stats.RetryScheduled, jobs, jobs)
	}
}

func TestDelayedEnqueue(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	job := newJob(3)
	job.AvailableAt = time.Now().Add(150 * time.Millisecond)
	id, err := store.Enqueue(ctx, job)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if jobs, err := store.Claim(ctx, "worker-1", 10); err != nil || len(jobs) != 0 {
		t.Fatalf("Claim before delay: jobs=%d err=%v, want none", len(jobs), err)
	}
	time.Sleep(200 * time.Millisecond)
	jobs, err := store.Claim(ctx, "worker-1", 10)
	if err != nil || len(jobs) != 1 || jobs[0].ID != id {
		t.Fatalf("Claim after delay: jobs=%v err=%v, want job %s", jobs, err, id)
	}
}

func TestMalformedEntryIsDeadLettered(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	if err := store.rds.XAdd(ctx, &redis.XAddArgs{Stream: store.config.Stream, Values: map[string]any{"task": "email"}}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}
	good := claimOne(t, store, "worker-1")
	if good.Task != "email" {
		t.Fatalf("well-formed job not returned alongside the malformed one")
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.DeadLetterDepth != 1 || stats.InFlight != 1 {
		t.Errorf("DeadLetterDepth=%d InFlight=%d, want 1 and 1", stats.DeadLetterDepth, stats.InFlight)
	}
}
