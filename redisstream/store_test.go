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
		Addr:         inst.Addr,
		Stream:       prefix + "jobs",
		Group:        prefix + "workers",
		RetryKey:     prefix + "retry",
		DeadLetter:   prefix + "dead",
		DedupePrefix: prefix + "dedupe:",
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
	jobs, err := store.Claim(t.Context(), workerID, 10, 0)
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

	jobs, err := store.Claim(ctx, "worker-1", 10, 0)
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
	jobs, err := store.Claim(ctx, "worker-1", 10, 0)
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

	if jobs, err := store.Claim(ctx, "worker-1", 10, 0); err != nil || len(jobs) != 0 {
		t.Fatalf("Claim before delay: jobs=%d err=%v, want none", len(jobs), err)
	}
	time.Sleep(200 * time.Millisecond)
	jobs, err := store.Claim(ctx, "worker-1", 10, 0)
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

func TestConsumerCleanupKeepsConsumersWithPendingJobs(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	held := claimOne(t, store, "busy")
	if _, err := store.Claim(ctx, "idle", 10, 0); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := store.Deregister(ctx, "busy"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	store.config.ConsumerIdleTimeout = time.Millisecond
	time.Sleep(5 * time.Millisecond)
	if err := store.pruneConsumers(ctx); err != nil {
		t.Fatalf("pruneConsumers: %v", err)
	}

	consumers, err := store.rds.XInfoConsumers(ctx, store.config.Stream, store.config.Group).Result()
	if err != nil {
		t.Fatalf("XInfoConsumers: %v", err)
	}
	if len(consumers) != 1 || consumers[0].Name != "busy" {
		t.Fatalf("consumers = %+v, want only busy (it holds a pending job)", consumers)
	}
	if err := store.Ack(ctx, held); err != nil {
		t.Fatalf("Ack after cleanup: %v", err)
	}
	if err := store.Deregister(ctx, "busy"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	if consumers, _ := store.rds.XInfoConsumers(ctx, store.config.Stream, store.config.Group).Result(); len(consumers) != 0 {
		t.Errorf("consumers after deregistering an empty one = %+v, want none", consumers)
	}
}

func TestIdempotentEnqueue(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	for _, delay := range []time.Duration{0, time.Hour} {
		key := fmt.Sprintf("order-42-%s", delay)
		var ids []queue.JobID
		// Concurrent retries of the same enqueue, e.g. after a timeout.
		var wg sync.WaitGroup
		var mu sync.Mutex
		for range 10 {
			wg.Go(func() {
				job := newJob(3)
				job.AvailableAt = time.Now().Add(delay)
				job.IdempotencyKey = key
				id, err := store.Enqueue(ctx, job)
				if err != nil {
					t.Errorf("Enqueue: %v", err)
				}
				mu.Lock()
				ids = append(ids, id)
				mu.Unlock()
			})
		}
		wg.Wait()
		for _, id := range ids {
			if id != ids[0] {
				t.Fatalf("delay %s: enqueues with one key returned different IDs %v", delay, ids)
			}
		}
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.StreamLength != 1 || stats.RetryScheduled != 1 {
		t.Errorf("stream=%d scheduled=%d, want one job each", stats.StreamLength, stats.RetryScheduled)
	}
}

func TestBlockingClaimReturnsAsSoonAsAJobArrives(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	go func() {
		time.Sleep(50 * time.Millisecond)
		store.Enqueue(ctx, newJob(3))
	}()
	start := time.Now()
	jobs, err := store.Claim(ctx, "worker-1", 10, 2*time.Second)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("Claim: jobs=%d err=%v, want 1", len(jobs), err)
	}
	if waited := time.Since(start); waited > time.Second {
		t.Errorf("blocking claim took %s; it should return when the job arrives", waited)
	}
}

func TestAckBatchFencesEachJob(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	current := claimOne(t, store, "worker-1")
	stale := claimOne(t, store, "worker-1")
	stale.Lease.Token = 99

	errs := store.AckBatch(ctx, []queue.Job{current, stale})
	if errs[0] != nil || !errors.Is(errs[1], queue.ErrLeaseLost) {
		t.Fatalf("AckBatch errors = %v, want [nil ErrLeaseLost]", errs)
	}
	if stats, _ := store.Stats(ctx); stats.InFlight != 1 {
		t.Errorf("InFlight = %d, want 1 (only the stale job left)", stats.InFlight)
	}
}

func TestExtendBatchFencesEachJob(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	current := claimOne(t, store, "worker-1")
	stale := claimOne(t, store, "worker-1")
	stale.Lease.Token = 99
	const visibility = 100 * time.Millisecond

	time.Sleep(2 * visibility)
	errs := store.ExtendBatch(ctx, []queue.Job{current, stale})
	if errs[0] != nil || !errors.Is(errs[1], queue.ErrLeaseLost) {
		t.Fatalf("ExtendBatch errors = %v, want [nil ErrLeaseLost]", errs)
	}
	// Only the renewed job is safe from recovery.
	recovered, err := store.RecoverExpired(ctx, visibility, "worker-2", 10)
	if err != nil {
		t.Fatalf("RecoverExpired: %v", err)
	}
	if len(recovered) != 1 || recovered[0].Lease.MessageID != stale.Lease.MessageID {
		t.Errorf("recovered %v, want only the job whose lease wasn't renewed", recovered)
	}
}

func TestEnqueueBatch(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	keyed := newJob(3)
	keyed.IdempotencyKey = "order-7"
	firstID, err := store.Enqueue(ctx, keyed)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	delayed := newJob(3)
	delayed.AvailableAt = time.Now().Add(time.Hour)
	again := newJob(3)
	again.IdempotencyKey = "order-7"
	batch := []queue.Job{newJob(3), newJob(3), delayed, again}
	ids, err := store.EnqueueBatch(ctx, batch)
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if len(ids) != 4 || ids[3] != firstID {
		t.Fatalf("ids = %v, want 4 with the keyed one deduped to %s", ids, firstID)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	// 1 earlier keyed job + 2 immediate; the delayed one is scheduled.
	if stats.StreamLength != 3 || stats.RetryScheduled != 1 {
		t.Errorf("stream=%d scheduled=%d, want 3 and 1", stats.StreamLength, stats.RetryScheduled)
	}
}

// A batch with idempotency keys must not depend on the script cache: Redis
// doesn't roll back an EXEC when one command fails, so a NOSCRIPT inside the
// transaction would store part of the batch.
func TestEnqueueBatchSurvivesAFlushedScriptCache(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	keyed := newJob(3)
	keyed.IdempotencyKey = "order-9"
	if err := store.rds.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if _, err := store.EnqueueBatch(ctx, []queue.Job{newJob(3), keyed, newJob(3)}); err != nil {
		t.Fatalf("EnqueueBatch after SCRIPT FLUSH: %v", err)
	}
	if st, _ := store.Stats(ctx); st.StreamLength != 3 {
		t.Errorf("stream length = %d, want 3", st.StreamLength)
	}
}
