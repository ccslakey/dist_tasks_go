// Command loadtest measures end-to-end throughput and latency against a real
// Redis.
//
// It enqueues a fixed number of no-op jobs, runs a set of workers against them,
// and reports how long the drain took. Handler time is configurable but zero by
// default, so the numbers describe queue overhead rather than whatever the
// handler happens to do.
//
//	go run ./cmd/loadtest -jobs 20000 -workers 4 -concurrency 32
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ccslakey/dist_tasks_go/queue"
	"github.com/ccslakey/dist_tasks_go/redisstream"
)

func main() {
	jobs := flag.Int("jobs", 10000, "number of jobs to enqueue")
	workers := flag.Int("workers", 4, "number of worker processes to simulate")
	concurrency := flag.Int("concurrency", 32, "handler concurrency per worker")
	handlerTime := flag.Duration("handler-time", 0, "artificial work per job")
	addr := flag.String("addr", "localhost:6379", "redis address")
	timeout := flag.Duration("timeout", 2*time.Minute, "give up after this long")
	flag.Parse()

	// A fresh key namespace per run, so a load test never mixes with demo data
	// and never inherits a backlog from a previous run.
	prefix := fmt.Sprintf("dtq:loadtest:%d:", time.Now().UnixNano())

	backend, err := redisstream.New(redisstream.Config{
		Addr:       *addr,
		Stream:     prefix + "jobs",
		Group:      prefix + "workers",
		RetryKey:   prefix + "retry",
		DeadLetter: prefix + "dead",
		MaxLen:     -1, // no trimming; we want to see everything we enqueued
	})
	if err != nil {
		log.Fatalf("connect to redis at %s: %v", *addr, err)
	}
	defer backend.Close()

	collector := &collector{total: *jobs, done: make(chan struct{})}
	q := queue.New(backend, queue.WithMetrics(collector))

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	fmt.Printf("enqueueing %d jobs...\n", *jobs)
	enqueueStart := time.Now()
	for i := range *jobs {
		_, err := q.Enqueue(ctx, "noop", map[string]int{"n": i}, queue.EnqueueOptions{MaxAttempts: 1})
		if err != nil {
			log.Fatalf("enqueue job %d: %v", i, err)
		}
	}
	enqueueElapsed := time.Since(enqueueStart)

	handler := func(ctx context.Context, job queue.Job) queue.JobResult {
		if *handlerTime > 0 {
			select {
			case <-time.After(*handlerTime):
			case <-ctx.Done():
				return queue.NewRetryableError(ctx.Err())
			}
		}
		return queue.JobResult{}
	}

	fmt.Printf("draining with %d workers x %d concurrency...\n", *workers, *concurrency)
	drainStart := time.Now()

	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	var wg sync.WaitGroup
	for w := range *workers {
		worker := queue.NewWorker(backend, queue.WorkerConfig{
			ID:                fmt.Sprintf("loadtest-%d", w),
			Concurrency:       *concurrency,
			PollInterval:      5 * time.Millisecond,
			VisibilityTimeout: 30 * time.Second,
			RetryPolicy:       queue.DefaultRetryPolicy(),
		}, collector)
		if err := worker.Register("noop", handler); err != nil {
			log.Fatalf("register handler: %v", err)
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = worker.Run(workerCtx)
		}()
	}

	select {
	case <-collector.done:
	case <-ctx.Done():
		fmt.Fprintf(os.Stderr, "timed out after %s with %d/%d jobs completed\n",
			*timeout, collector.completed(), *jobs)
		stopWorkers()
		wg.Wait()
		os.Exit(1)
	}

	drainElapsed := time.Since(drainStart)
	stopWorkers()
	wg.Wait()

	report(*jobs, *workers, *concurrency, *handlerTime, enqueueElapsed, drainElapsed, collector.latencies())
}

func report(jobs, workers, concurrency int, handlerTime, enqueueElapsed, drainElapsed time.Duration, lat []time.Duration) {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	fmt.Printf("\njobs            %d\n", jobs)
	fmt.Printf("workers         %d x %d concurrency\n", workers, concurrency)
	fmt.Printf("handler time    %s\n", handlerTime)
	fmt.Printf("enqueue         %s (%.0f jobs/sec)\n", enqueueElapsed.Round(time.Millisecond), rate(jobs, enqueueElapsed))
	fmt.Printf("drain           %s (%.0f jobs/sec)\n", drainElapsed.Round(time.Millisecond), rate(jobs, drainElapsed))

	if len(lat) > 0 {
		fmt.Printf("\nend-to-end latency (enqueue to ack)\n")
		fmt.Printf("  p50           %s\n", percentile(lat, 0.50).Round(time.Microsecond))
		fmt.Printf("  p95           %s\n", percentile(lat, 0.95).Round(time.Microsecond))
		fmt.Printf("  p99           %s\n", percentile(lat, 0.99).Round(time.Microsecond))
		fmt.Printf("  max           %s\n", lat[len(lat)-1].Round(time.Microsecond))
	}
}

func rate(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)-1))
	return sorted[i]
}

// collector implements queue.Metrics and closes done once every job has been
// completed. Latency is measured from the job's CreatedAt, so it covers the
// full round trip rather than just handler time.
type collector struct {
	mu    sync.Mutex
	lat   []time.Duration
	count int

	total int
	done  chan struct{}
	once  sync.Once
}

func (c *collector) JobCompleted(_ context.Context, job queue.Job) {
	c.mu.Lock()
	c.lat = append(c.lat, time.Since(job.CreatedAt))
	c.count++
	reached := c.count >= c.total
	c.mu.Unlock()

	if reached {
		c.once.Do(func() { close(c.done) })
	}
}

func (c *collector) completed() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (c *collector) latencies() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.lat))
	copy(out, c.lat)
	return out
}

func (c *collector) JobEnqueued(context.Context, queue.Job)     {}
func (c *collector) JobStarted(context.Context, queue.Job)      {}
func (c *collector) JobFailed(context.Context, queue.Job, bool) {}
func (c *collector) JobRetried(context.Context, queue.Job)      {}
func (c *collector) JobDeadLettered(context.Context, queue.Job) {}
func (c *collector) StatsObserved(context.Context, queue.Stats) {}
