package redisstream

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ccslakey/dist_tasks_go/internal/testredis"
	"github.com/ccslakey/dist_tasks_go/queue"
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

func TestEnqueueClaimAck(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	job := queue.Job{
		Task:        "email",
		Payload:     []byte(`{"to":"x@y"}`),
		MaxAttempts: 3,
		CreatedAt:   time.Now().UTC(),
		AvailableAt: time.Now().UTC(),
	}
	id, err := store.Enqueue(ctx, job)
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
	if jobs[0].Task != "email" {
		t.Errorf("Task = %q, want %q", jobs[0].Task, "email")
	}

	if err := store.Ack(ctx, jobs[0].ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.InFlight != 0 {
		t.Errorf("InFlight after Ack = %d, want 0", stats.InFlight)
	}
}

func TestRetryPromotion(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	job := queue.Job{
		Task:        "email",
		Payload:     []byte(`{}`),
		MaxAttempts: 3,
		CreatedAt:   time.Now().UTC(),
		AvailableAt: time.Now().UTC(),
	}
	if _, err := store.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	claimed, err := store.Claim(ctx, "worker-1", 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: jobs=%d err=%v", len(claimed), err)
	}

	if err := store.Retry(ctx, claimed[0], time.Now().UTC().Add(-time.Second)); err != nil {
		t.Fatalf("Retry: %v", err)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.RetryScheduled != 1 {
		t.Errorf("RetryScheduled = %d, want 1", stats.RetryScheduled)
	}

	// Next Claim runs promoteRetries first, then XREADGROUP — so it picks up
	// the promoted job in the same call.
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
}

func TestDeadLetter(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	job := queue.Job{
		Task:        "email",
		Payload:     []byte(`{}`),
		MaxAttempts: 1,
		CreatedAt:   time.Now().UTC(),
		AvailableAt: time.Now().UTC(),
	}
	id, err := store.Enqueue(ctx, job)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := store.Claim(ctx, "worker-1", 10); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	job.ID = id
	if err := store.DeadLetter(ctx, job, "boom"); err != nil {
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

// TestRecoverExpiredReturnsStrandedJobs pins the behaviour that makes crash
// recovery work at all. XAUTOCLAIM moves an entry to the reclaiming consumer,
// after which XREADGROUP with ">" will never return it again, so a
// RecoverExpired that reports only a count leaves the job stranded forever.
func TestRecoverExpiredReturnsStrandedJobs(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	if _, err := store.Enqueue(ctx, testJob("email", `{"to":"x@y"}`, 3)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// worker-1 claims the job and then "crashes" without acking.
	claimed, err := store.Claim(ctx, "worker-1", 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim: jobs=%d err=%v", len(claimed), err)
	}

	time.Sleep(50 * time.Millisecond)

	reclaimed, err := store.RecoverExpired(ctx, 10*time.Millisecond, "worker-2", 10)
	if err != nil {
		t.Fatalf("RecoverExpired: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("RecoverExpired returned %d jobs, want 1", len(reclaimed))
	}
	if reclaimed[0].Task != "email" {
		t.Errorf("reclaimed Task = %q, want %q", reclaimed[0].Task, "email")
	}
	if string(reclaimed[0].Payload) != `{"to":"x@y"}` {
		t.Errorf("reclaimed Payload = %s, want %s", reclaimed[0].Payload, `{"to":"x@y"}`)
	}

	// A normal claim must not hand it back: it belongs to worker-2 now.
	again, err := store.Claim(ctx, "worker-2", 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("Claim returned %d reclaimed jobs, want 0 (they come back via RecoverExpired)", len(again))
	}

	if err := store.Ack(ctx, reclaimed[0].ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.InFlight != 0 {
		t.Errorf("InFlight after acking reclaimed job = %d, want 0", stats.InFlight)
	}
}

// TestConcurrentPromotionIsExactlyOnce is the reason promotion is a Lua script.
// Done as separate ZRANGEBYSCORE / XADD / ZREM calls, every worker polling at
// the same instant sees the same due entries and re-queues all of them.
func TestConcurrentPromotionIsExactlyOnce(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	const (
		jobs    = 200
		workers = 8
	)

	due := time.Now().UTC().Add(-time.Second)
	for i := range jobs {
		job := testJob("email", fmt.Sprintf(`{"n":%d}`, i), 3)
		job.ID = queue.JobID(fmt.Sprintf("%d-0", i))
		if err := store.Retry(ctx, job, due); err != nil {
			t.Fatalf("Retry %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	var promoted atomic.Int64
	start := make(chan struct{})

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 20 {
				n, err := store.promoteRetries(ctx, 25)
				if err != nil {
					t.Errorf("promoteRetries: %v", err)
					return
				}
				promoted.Add(int64(n))
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := promoted.Load(); got != jobs {
		t.Errorf("total promoted = %d, want %d (a higher number means workers raced and re-queued the same entry)", got, jobs)
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.RetryScheduled != 0 {
		t.Errorf("RetryScheduled = %d, want 0", stats.RetryScheduled)
	}

	claimedPayloads := map[string]int{}
	for {
		batch, err := store.Claim(ctx, "worker-1", 100)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(batch) == 0 {
			break
		}
		for _, job := range batch {
			claimedPayloads[string(job.Payload)]++
		}
	}
	if len(claimedPayloads) != jobs {
		t.Errorf("distinct jobs on the stream = %d, want %d", len(claimedPayloads), jobs)
	}
	for payload, count := range claimedPayloads {
		if count != 1 {
			t.Errorf("job %s was promoted %d times, want 1", payload, count)
		}
	}
}

// TestMalformedEntryIsQuarantined covers the other poison pill: an entry the
// backend cannot decode. Returning an error from Claim would stall every poll,
// and leaving it pending means every reclaim hands it straight back.
func TestMalformedEntryIsQuarantined(t *testing.T) {
	store := newTestStore(t)
	ctx := t.Context()

	// An entry with no attempt/createdAt fields at all, written straight to the
	// stream the way a different or older producer might.
	if err := store.rds.XAdd(ctx, &redis.XAddArgs{
		Stream: store.config.Stream,
		Values: map[string]any{"task": "email", "payload": "{}"},
	}).Err(); err != nil {
		t.Fatalf("XAdd: %v", err)
	}

	jobs, err := store.Claim(ctx, "worker-1", 10)
	if err != nil {
		t.Fatalf("Claim returned an error instead of quarantining: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("Claim returned %d jobs, want 0", len(jobs))
	}

	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.DeadLetterDepth != 1 {
		t.Errorf("DeadLetterDepth = %d, want 1", stats.DeadLetterDepth)
	}
	if stats.InFlight != 0 {
		t.Errorf("InFlight = %d, want 0 (a quarantined entry must be acked, not left pending)", stats.InFlight)
	}
}

func TestEnqueueTrimsStream(t *testing.T) {
	inst := testredis.Start(t)
	prefix := "test:" + t.Name() + ":"
	store, err := New(Config{
		Addr:       inst.Addr,
		Stream:     prefix + "jobs",
		Group:      prefix + "workers",
		RetryKey:   prefix + "retry",
		DeadLetter: prefix + "dead",
		MaxLen:     10,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	ctx := t.Context()
	for i := range 200 {
		if _, err := store.Enqueue(ctx, testJob("email", fmt.Sprintf(`{"n":%d}`, i), 3)); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	// MAXLEN ~ trims on node boundaries, so the exact length is not pinned;
	// what matters is that acked entries stop accumulating without bound.
	length, err := store.rds.XLen(ctx, store.config.Stream).Result()
	if err != nil {
		t.Fatalf("XLen: %v", err)
	}
	if length >= 200 {
		t.Errorf("stream length = %d after 200 enqueues with MaxLen 10; nothing was trimmed", length)
	}
}

func testJob(task, payload string, maxAttempts int) queue.Job {
	now := time.Now().UTC()
	return queue.Job{
		Task:        task,
		Payload:     []byte(payload),
		MaxAttempts: maxAttempts,
		CreatedAt:   now,
		AvailableAt: now,
	}
}
