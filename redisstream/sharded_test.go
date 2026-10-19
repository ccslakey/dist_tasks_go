package redisstream

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"dist_tasks_go/internal/testredis"
	"dist_tasks_go/queue"

	"github.com/redis/go-redis/v9"
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

// cleanupCluster deletes a test's keys from every node.
func cleanupCluster(t *testing.T, config ShardedConfig) {
	client := redis.NewClusterClient(&redis.ClusterOptions{Addrs: config.Addrs})
	defer client.Close()
	err := client.ForEachMaster(context.Background(), func(ctx context.Context, node *redis.Client) error {
		iter := node.Scan(ctx, 0, config.Prefix+":*", 1000).Iterator()
		for iter.Next(ctx) {
			node.Del(ctx, iter.Val())
		}
		return iter.Err()
	})
	if err != nil {
		t.Logf("cleaning up cluster keys: %v", err)
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
		jobs, pickup := claimWhileEnqueueing(t, s, 50*time.Millisecond, 2*time.Second)
		if len(jobs) != 1 {
			t.Fatalf("claim %d: got %d jobs, want 1", i, len(jobs))
		}
		if pickup > 100*time.Millisecond {
			t.Errorf("claim %d picked its job up %s after it was enqueued; it should wake when a job lands on any partition", i, pickup)
		}
		if err := s.Ack(ctx, jobs[0]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

// claimWhileEnqueueing starts a Claim that waits up to wait, enqueues a job
// after delay, and returns what the Claim got and how long after the enqueue
// it returned.
func claimWhileEnqueueing(t *testing.T, s *Sharded, delay, wait time.Duration) ([]queue.Job, time.Duration) {
	t.Helper()
	enqueued := make(chan time.Time, 1)
	go func() {
		time.Sleep(delay)
		if _, err := s.Enqueue(t.Context(), newJob(3)); err != nil {
			t.Errorf("Enqueue: %v", err)
		}
		enqueued <- time.Now()
	}()
	jobs, err := s.Claim(t.Context(), "worker-1", 10, wait)
	returned := time.Now()
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	return jobs, returned.Sub(<-enqueued)
}

// An idle worker calls Claim again as soon as one times out. Polls from the
// old Claim may still be finishing then; they must be reissued for the new
// one, or a job landing on their partitions waits out the whole Claim.
func TestShardedPickupRightAfterAnIdleClaimTimesOut(t *testing.T) {
	s := newTestSharded(t, 4)
	ctx := t.Context()

	for i := range 8 {
		if jobs, err := s.Claim(ctx, "worker-1", 10, 20*time.Millisecond); err != nil || len(jobs) != 0 {
			t.Fatalf("idle claim: jobs=%d err=%v", len(jobs), err)
		}
		jobs, pickup := claimWhileEnqueueing(t, s, 5*time.Millisecond, 2*time.Second)
		if len(jobs) != 1 {
			t.Fatalf("claim %d: got %d jobs, want 1", i, len(jobs))
		}
		if pickup > 100*time.Millisecond {
			t.Errorf("claim %d picked its job up %s after it was enqueued", i, pickup)
		}
		if err := s.Ack(ctx, jobs[0]); err != nil {
			t.Fatalf("Ack: %v", err)
		}
	}
}

func TestShardedCloseDoesNotWaitOutLongPolls(t *testing.T) {
	s := newTestSharded(t, 4)
	go s.Claim(t.Context(), "worker-1", 10, 10*time.Second)
	time.Sleep(100 * time.Millisecond) // let the polls block

	start := time.Now()
	s.Close()
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("Close took %s; it should cut off blocked polls", took)
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
