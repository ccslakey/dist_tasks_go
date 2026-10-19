package redisstream

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"dist_tasks_go/internal/testredis"
	"dist_tasks_go/queue"
)

// newTestSharded returns a sharded queue on the test Redis, or on the Redis
// Cluster at TEST_REDIS_CLUSTER (comma-separated seed addresses) if set.
func newTestSharded(t *testing.T, partitions int) *Sharded {
	t.Helper()
	config := ShardedConfig{
		Partitions: partitions,
		Prefix:     "test:" + strings.ReplaceAll(t.Name(), "/", "_"),
	}
	if seeds := os.Getenv("TEST_REDIS_CLUSTER"); seeds != "" {
		config.Addrs = strings.Split(seeds, ",")
		config.Cluster = true
	} else {
		config.Addrs = []string{testredis.Start(t).Addr}
	}
	s, err := NewSharded(config)
	if err != nil {
		t.Fatalf("NewSharded: %v", err)
	}
	t.Cleanup(func() {
		s.Close()
		if config.Cluster {
			cleanupCluster(t, config)
		}
	})
	return s
}

// cleanupCluster deletes a test's keys, since FLUSHDB only reaches one node.
func cleanupCluster(t *testing.T, config ShardedConfig) {
	s, err := NewSharded(ShardedConfig{Addrs: config.Addrs, Cluster: true, Partitions: config.Partitions, Prefix: config.Prefix})
	if err != nil {
		return
	}
	defer s.Close()
	for _, part := range s.parts {
		c := part.config
		s.client.Del(t.Context(), c.Stream, c.RetryKey, part.retryData, c.DeadLetter)
	}
}

func TestShardedSpreadsJobsAndDrainsThemAll(t *testing.T) {
	s := newTestSharded(t, 4)
	ctx := t.Context()

	const n = 100
	batch := make([]queue.Job, n)
	for i := range batch {
		batch[i] = newJob(3)
	}
	if _, err := s.EnqueueBatch(ctx, batch); err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	for p, part := range s.Partitions() {
		if st, _ := part.Stats(ctx); st.StreamLength == 0 {
			t.Errorf("partition %d got no jobs", p)
		}
	}

	var claimed []queue.Job
	for len(claimed) < n {
		jobs, err := s.Claim(ctx, "worker-1", 30, 0)
		if err != nil {
			t.Fatalf("Claim: %v", err)
		}
		if len(jobs) == 0 {
			t.Fatalf("Claim returned nothing after %d of %d jobs", len(claimed), n)
		}
		claimed = append(claimed, jobs...)
	}
	for _, err := range s.AckBatch(ctx, claimed) {
		if err != nil {
			t.Fatalf("AckBatch: %v", err)
		}
	}
	if st, _ := s.Stats(ctx); st.StreamLength != 0 || st.InFlight != 0 {
		t.Errorf("after acking everything: stream=%d in flight=%d, want 0", st.StreamLength, st.InFlight)
	}
}

func TestShardedIdempotencyKeyAlwaysRoutesToOnePartition(t *testing.T) {
	s := newTestSharded(t, 8)
	ctx := t.Context()

	var ids []queue.JobID
	for range 5 {
		job := newJob(3)
		job.IdempotencyKey = "order-42"
		id, err := s.Enqueue(ctx, job)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("one key produced different job IDs: %v", ids)
		}
	}
	if st, _ := s.Stats(ctx); st.StreamLength != 1 {
		t.Errorf("stream length = %d, want 1", st.StreamLength)
	}
}

func TestShardedBlockingClaimWakesOnAnyPartition(t *testing.T) {
	s := newTestSharded(t, 4)
	ctx := t.Context()

	for i := range 4 { // round-robin puts each one on a different partition
		go func() {
			time.Sleep(50 * time.Millisecond)
			s.Enqueue(ctx, newJob(3))
		}()
		start := time.Now()
		jobs, err := s.Claim(ctx, "worker-1", 10, 2*time.Second)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("claim %d: jobs=%d err=%v, want 1", i, len(jobs), err)
		}
		if waited := time.Since(start); waited > time.Second {
			t.Errorf("claim %d took %s; it should wake when a job lands on any partition", i, waited)
		}
		if err := s.Ack(ctx, jobs[0]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

func TestShardedRecoveryAndFencing(t *testing.T) {
	s := newTestSharded(t, 4)
	ctx := t.Context()

	for range 4 {
		if _, err := s.Enqueue(ctx, newJob(3)); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	stale, err := s.Claim(ctx, "worker-1", 10, 0)
	if err != nil || len(stale) != 4 {
		t.Fatalf("Claim: jobs=%d err=%v", len(stale), err)
	}
	const visibility = 50 * time.Millisecond
	time.Sleep(2 * visibility)

	recovered, err := s.RecoverExpired(ctx, visibility, "worker-2", 10)
	if err != nil || len(recovered) != 4 {
		t.Fatalf("RecoverExpired: jobs=%d err=%v, want 4", len(recovered), err)
	}
	for _, job := range stale {
		if err := s.Ack(ctx, job); !errors.Is(err, queue.ErrLeaseLost) {
			t.Errorf("stale Ack: got %v, want ErrLeaseLost", err)
		}
	}
	for _, job := range recovered {
		if err := s.Retry(ctx, job, time.Now().Add(-time.Second)); err != nil {
			t.Fatalf("Retry: %v", err)
		}
	}
	// Retries stay in their partitions and come back through Claim.
	retried, err := s.Claim(ctx, "worker-2", 10, 0)
	if err != nil || len(retried) != 4 {
		t.Fatalf("Claim after retry: jobs=%d err=%v, want 4", len(retried), err)
	}
	for _, job := range retried {
		if job.Attempt != 1 {
			t.Errorf("retried job attempt = %d, want 1", job.Attempt)
		}
	}
}

func TestShardedRejectsUnknownPartition(t *testing.T) {
	s := newTestSharded(t, 2)
	job := newJob(3)
	job.Lease.Partition = 5
	if err := s.Ack(t.Context(), job); err == nil || !strings.Contains(err.Error(), "unknown partition") {
		t.Errorf("Ack with partition 5 of 2: got %v", err)
	}
}
