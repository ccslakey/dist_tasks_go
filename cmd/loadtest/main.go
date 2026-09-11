// Command loadtest measures throughput and latency against a real Redis.
//
// Handler time is configurable but zero by default, so the numbers describe
// queue overhead rather than whatever the handler happens to do.
//
// Two modes, because they measure different things and the same run cannot do
// both honestly:
//
//	go run ./cmd/loadtest                  # steady state: workers run while jobs arrive
//	go run ./cmd/loadtest -prefill         # backlog drain: fill the queue, then start workers
//
// Steady state is where the latency percentiles mean something: a job's clock
// starts when it is enqueued and there is already a worker free to take it, so
// the number is queue overhead. Prefill deliberately builds a backlog to find
// peak drain throughput, which means the first job waits out the entire
// enqueue phase, so per-job latency there measures the backlog and is not
// reported.
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
	prefill := flag.Bool("prefill", false, "enqueue every job before starting workers, to measure peak drain throughput instead of steady-state latency")
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

	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	var wg sync.WaitGroup
	startWorkers := func() {
		fmt.Printf("running %d workers x %d concurrency...\n", *workers, *concurrency)
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
	}

	// In steady state the workers have to be up before the first job lands,
	// otherwise every job's clock starts while there is nobody to run it and
	// the percentiles just measure the enqueue loop.
	if !*prefill {
		startWorkers()
	}

	fmt.Printf("enqueueing %d jobs...\n", *jobs)
	runStart := time.Now()
	for i := range *jobs {
		_, err := q.Enqueue(ctx, "noop", map[string]int{"n": i}, queue.EnqueueOptions{MaxAttempts: 1})
		if err != nil {
			log.Fatalf("enqueue job %d: %v", i, err)
		}
	}
	enqueueElapsed := time.Since(runStart)

	drainStart := time.Now()
	if *prefill {
		startWorkers()
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
	totalElapsed := time.Since(runStart)
	stopWorkers()
	wg.Wait()

	report(params{
		jobs:        *jobs,
		workers:     *workers,
		concurrency: *concurrency,
		handlerTime: *handlerTime,
		prefill:     *prefill,
		enqueue:     enqueueElapsed,
		drain:       drainElapsed,
		total:       totalElapsed,
	}, collector.latencies())
}

type params struct {
	jobs, workers, concurrency int
	handlerTime                time.Duration
	prefill                    bool
	enqueue, drain, total      time.Duration
}

func report(p params, lat []time.Duration) {
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	mode := "steady state"
	if p.prefill {
		mode = "backlog drain"
	}

	fmt.Printf("\nmode            %s\n", mode)
	fmt.Printf("jobs            %d\n", p.jobs)
	fmt.Printf("workers         %d x %d concurrency\n", p.workers, p.concurrency)
	fmt.Printf("handler time    %s\n", p.handlerTime)
	fmt.Printf("enqueue         %s (%.0f jobs/sec)\n", p.enqueue.Round(time.Millisecond), rate(p.jobs, p.enqueue))

	if p.prefill {
		fmt.Printf("drain           %s (%.0f jobs/sec)\n", p.drain.Round(time.Millisecond), rate(p.jobs, p.drain))
		fmt.Printf("\nlatency not reported in this mode: every job waits out the\n")
		fmt.Printf("enqueue phase before a worker exists, so the percentiles would\n")
		fmt.Printf("describe the backlog rather than the queue.\n")
		return
	}

	fmt.Printf("end to end      %s (%.0f jobs/sec sustained)\n", p.total.Round(time.Millisecond), rate(p.jobs, p.total))

	if len(lat) > 0 {
		fmt.Printf("\nlatency, enqueue to ack\n")
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
