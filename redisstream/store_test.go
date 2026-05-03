package redisstream

import (
	"strings"
	"testing"
	"time"

	"dist_tasks_go/internal/testredis"
	"dist_tasks_go/queue"
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
