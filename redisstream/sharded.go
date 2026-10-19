package redisstream

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"dist_tasks_go/queue"

	"github.com/redis/go-redis/v9"
)

// ShardedConfig configures a queue split into partitions.
type ShardedConfig struct {
	// Addrs are Redis addresses: one for a single Redis, or the seed nodes of
	// a Redis Cluster (set Cluster, or pass more than one address).
	Addrs   []string
	Cluster bool
	// Partitions is how many partitions the queue is split into. Defaults to
	// 16. It can grow later (old partitions are still consumed), but not
	// shrink: jobs left in a dropped partition would never be claimed.
	// Growing re-routes idempotency keys, so dedupe windows reset.
	Partitions int
	// Prefix namespaces every key. Defaults to "dtq".
	Prefix              string
	DedupeWindow        time.Duration
	ConsumerIdleTimeout time.Duration
}

// Sharded is a queue.Backend that spreads jobs over several partitions, each
// a Store whose keys share a Redis Cluster hash tag ({p0}, {p1}, ...). Each
// partition's scripts therefore stay on one cluster slot, while partitions
// spread across cluster nodes, so throughput grows with the cluster.
//
// Jobs are routed round-robin, except that a job with an idempotency key
// always goes to the partition its key hashes to, so dedupe still works. A
// job's retries stay in its partition, and its lease records the partition
// so settling calls go back to it. There's no ordering across partitions.
//
// Claim sweeps the partitions for ready jobs. When all are empty, it
// long-polls every partition at once (a blocking read can't span cluster
// slots) and returns as soon as any delivers; jobs the other polls bring back
// are buffered for the next Claim. Use one Sharded per worker: buffered jobs
// are leased to the worker that started the poll.
type Sharded struct {
	client redis.UniversalClient
	parts  []*Store

	enqueueNext atomic.Uint64 // round-robin enqueue cursor
	claimNext   atomic.Uint64 // rotates where claim sweeps start

	pollCtx   context.Context
	stopPolls context.CancelFunc
	pollers   sync.WaitGroup
	mu        sync.Mutex
	polling   []bool      // partitions with a long poll in flight
	buffered  []queue.Job // claimed by long polls, not yet handed out
	arrived   chan struct{}
}

func NewSharded(config ShardedConfig) (*Sharded, error) {
	if len(config.Addrs) == 0 {
		config.Addrs = []string{"localhost:6379"}
	}
	if config.Partitions <= 0 {
		config.Partitions = 16
	}
	if config.Prefix == "" {
		config.Prefix = "dtq"
	}
	client := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:         config.Addrs,
		IsClusterMode: config.Cluster,
		Protocol:      2,
	})

	s := &Sharded{
		client:  client,
		polling: make([]bool, config.Partitions),
		arrived: make(chan struct{}, 1),
	}
	s.pollCtx, s.stopPolls = context.WithCancel(context.Background())
	for p := range config.Partitions {
		tag := fmt.Sprintf("%s:{p%d}:", config.Prefix, p)
		part, err := NewWithClient(client, Config{
			Stream:              tag + "jobs",
			Group:               config.Prefix + ":workers",
			RetryKey:            tag + "retry",
			DeadLetter:          tag + "dead",
			DedupePrefix:        tag + "dedupe:",
			DedupeWindow:        config.DedupeWindow,
			ConsumerIdleTimeout: config.ConsumerIdleTimeout,
		})
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("partition %d: %w", p, err)
		}
		s.parts = append(s.parts, part)
	}
	return s, nil
}

func (s *Sharded) Close() error {
	s.stopPolls()
	s.pollers.Wait()
	return s.client.Close()
}

// Partitions returns the partition stores, e.g. to inspect them one by one.
func (s *Sharded) Partitions() []*Store { return s.parts }

func (s *Sharded) route(job queue.Job) int {
	if job.IdempotencyKey != "" {
		h := fnv.New32a()
		h.Write([]byte(job.IdempotencyKey))
		return int(h.Sum32() % uint32(len(s.parts)))
	}
	return int(s.enqueueNext.Add(1) % uint64(len(s.parts)))
}

func (s *Sharded) partOf(job queue.Job) (*Store, error) {
	p := job.Lease.Partition
	if p < 0 || p >= len(s.parts) {
		return nil, fmt.Errorf("job %s has unknown partition %d", job.ID, p)
	}
	return s.parts[p], nil
}

func tag(jobs []queue.Job, partition int) []queue.Job {
	for i := range jobs {
		jobs[i].Lease.Partition = partition
	}
	return jobs
}

func (s *Sharded) Enqueue(ctx context.Context, job queue.Job) (queue.JobID, error) {
	return s.parts[s.route(job)].Enqueue(ctx, job)
}

// EnqueueBatch groups jobs by partition and enqueues each group in one
// transaction, in parallel. It's all or nothing per partition, not across
// the whole batch: on error, some partitions' jobs may have been stored.
func (s *Sharded) EnqueueBatch(ctx context.Context, jobs []queue.Job) ([]queue.JobID, error) {
	groups := map[int][]int{} // partition -> indexes into jobs
	for i := range jobs {
		if jobs[i].ID == "" {
			jobs[i].ID = queue.NewJobID()
		}
		p := s.route(jobs[i])
		groups[p] = append(groups[p], i)
	}

	ids := make([]queue.JobID, len(jobs))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	for p, idx := range groups {
		wg.Go(func() {
			batch := make([]queue.Job, len(idx))
			for i, j := range idx {
				batch[i] = jobs[j]
			}
			got, err := s.parts[p].EnqueueBatch(ctx, batch)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("partition %d: %w", p, err)
				}
				return
			}
			for i, j := range idx {
				ids[j] = got[i]
			}
		})
	}
	wg.Wait()
	return ids, firstErr
}

func (s *Sharded) Claim(ctx context.Context, workerID string, limit int, wait time.Duration) ([]queue.Job, error) {
	jobs := s.takeBuffered(limit)

	start := int(s.claimNext.Add(1))
	for i := 0; i < len(s.parts) && len(jobs) < limit; i++ {
		p := (start + i) % len(s.parts)
		got, err := s.parts[p].claimReady(ctx, workerID, limit-len(jobs))
		jobs = append(jobs, tag(got, p)...)
		if err != nil {
			return jobs, fmt.Errorf("partition %d: %w", p, err) // jobs already leased must still run
		}
	}
	if len(jobs) > 0 || wait <= 0 {
		return jobs, nil
	}
	return s.waitForJobs(ctx, workerID, limit, wait), nil
}

// waitForJobs long-polls every partition that doesn't already have a poll in
// flight and returns as soon as any of them delivers, or after wait. Each
// poll asks for a share of limit, so at most about limit+partitions jobs end
// up buffered.
func (s *Sharded) waitForJobs(ctx context.Context, workerID string, limit int, wait time.Duration) []queue.Job {
	share := max(1, (limit+len(s.parts)-1)/len(s.parts))
	s.mu.Lock()
	for p := range s.parts {
		if !s.polling[p] {
			s.polling[p] = true
			s.pollers.Go(func() { s.poll(p, workerID, share, wait) })
		}
	}
	s.mu.Unlock()

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-s.arrived:
	case <-timer.C:
	case <-ctx.Done():
	}
	return s.takeBuffered(limit)
}

func (s *Sharded) poll(p int, workerID string, count int, wait time.Duration) {
	defer func() {
		s.mu.Lock()
		s.polling[p] = false
		s.mu.Unlock()
	}()
	got, err := s.parts[p].claimBlocking(s.pollCtx, workerID, count, wait)
	if err != nil {
		if s.pollCtx.Err() == nil {
			slog.Warn("long poll failed", "partition", p, "err", err)
		}
		return
	}
	if len(got) == 0 {
		return
	}
	s.mu.Lock()
	s.buffered = append(s.buffered, tag(got, p)...)
	s.mu.Unlock()
	select {
	case s.arrived <- struct{}{}:
	default:
	}
}

func (s *Sharded) takeBuffered(limit int) []queue.Job {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := min(limit, len(s.buffered))
	if n == 0 {
		return nil
	}
	jobs := append([]queue.Job(nil), s.buffered[:n]...)
	s.buffered = s.buffered[n:]
	return jobs
}

// RecoverExpired sweeps the partitions for expired leases, starting where the
// last sweep left off so no partition is starved.
func (s *Sharded) RecoverExpired(ctx context.Context, visibilityTimeout time.Duration, workerID string, limit int) ([]queue.Job, error) {
	var jobs []queue.Job
	start := int(s.claimNext.Load())
	for i := 0; i < len(s.parts) && len(jobs) < limit; i++ {
		p := (start + i) % len(s.parts)
		got, err := s.parts[p].RecoverExpired(ctx, visibilityTimeout, workerID, limit-len(jobs))
		jobs = append(jobs, tag(got, p)...)
		if err != nil {
			return jobs, fmt.Errorf("partition %d: %w", p, err)
		}
	}
	return jobs, nil
}

func (s *Sharded) Extend(ctx context.Context, job queue.Job) error {
	part, err := s.partOf(job)
	if err != nil {
		return err
	}
	return part.Extend(ctx, job)
}

func (s *Sharded) Ack(ctx context.Context, job queue.Job) error {
	part, err := s.partOf(job)
	if err != nil {
		return err
	}
	return part.Ack(ctx, job)
}

func (s *Sharded) Retry(ctx context.Context, job queue.Job, nextRunAt time.Time) error {
	part, err := s.partOf(job)
	if err != nil {
		return err
	}
	return part.Retry(ctx, job, nextRunAt)
}

func (s *Sharded) DeadLetter(ctx context.Context, job queue.Job, reason string) error {
	part, err := s.partOf(job)
	if err != nil {
		return err
	}
	return part.DeadLetter(ctx, job, reason)
}

func (s *Sharded) AckBatch(ctx context.Context, jobs []queue.Job) []error {
	return s.perPartition(jobs, func(part *Store, batch []queue.Job) []error { return part.AckBatch(ctx, batch) })
}

func (s *Sharded) ExtendBatch(ctx context.Context, jobs []queue.Job) []error {
	return s.perPartition(jobs, func(part *Store, batch []queue.Job) []error { return part.ExtendBatch(ctx, batch) })
}

// perPartition splits jobs by partition, runs call on each group in
// parallel, and returns one error per job in the original order.
func (s *Sharded) perPartition(jobs []queue.Job, call func(*Store, []queue.Job) []error) []error {
	errs := make([]error, len(jobs))
	groups := map[int][]int{}
	for i, job := range jobs {
		if _, err := s.partOf(job); err != nil {
			errs[i] = err
			continue
		}
		groups[job.Lease.Partition] = append(groups[job.Lease.Partition], i)
	}
	var wg sync.WaitGroup
	for p, idx := range groups {
		wg.Go(func() {
			batch := make([]queue.Job, len(idx))
			for i, j := range idx {
				batch[i] = jobs[j]
			}
			for i, err := range call(s.parts[p], batch) {
				errs[idx[i]] = err
			}
		})
	}
	wg.Wait()
	return errs
}

func (s *Sharded) Deregister(ctx context.Context, workerID string) error {
	for p, part := range s.parts {
		if err := part.Deregister(ctx, workerID); err != nil {
			return fmt.Errorf("partition %d: %w", p, err)
		}
	}
	return nil
}

func (s *Sharded) Stats(ctx context.Context) (queue.Stats, error) {
	var total queue.Stats
	for p, part := range s.parts {
		st, err := part.Stats(ctx)
		if err != nil {
			return queue.Stats{}, fmt.Errorf("partition %d: %w", p, err)
		}
		total.StreamLength += st.StreamLength
		total.InFlight += st.InFlight
		total.DeadLetterDepth += st.DeadLetterDepth
		total.RetryScheduled += st.RetryScheduled
	}
	return total, nil
}
