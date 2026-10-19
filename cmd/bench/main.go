// Command bench measures the queue's throughput and pickup latency against a
// real Redis, across worker counts and claim modes (blocking vs polling).
//
//   - Throughput: enqueue a backlog of no-op jobs, then time how long the
//     workers take to drain it.
//   - Latency: enqueue at a steady rate the workers can easily keep up with,
//     and record how long each job waited between enqueue and its handler
//     starting.
//
// Workers run in-process, each with its own Redis connection pool, so the
// numbers reflect the broker round trips rather than process overhead.
//
//	go run ./cmd/bench
//	go run ./cmd/bench -workers 1,4,16 -jobs 50000 -modes block
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"dist_tasks_go/queue"
	"dist_tasks_go/redisstream"

	"github.com/redis/go-redis/v9"
)

type options struct {
	redisAddr    string
	jobs         int
	workers      []int
	concurrency  int
	modes        []string
	pollInterval time.Duration
	rate         int
	latencyJobs  int
}

func main() {
	var o options
	var workers, modes string
	flag.StringVar(&o.redisAddr, "redis", "localhost:6379", "Redis address")
	flag.IntVar(&o.jobs, "jobs", 20000, "backlog size for the throughput test")
	flag.StringVar(&workers, "workers", "1,2,4,8", "comma-separated worker counts to test")
	flag.IntVar(&o.concurrency, "concurrency", 16, "concurrency per worker")
	flag.StringVar(&modes, "modes", "poll,block", "claim modes to compare: poll, block")
	flag.DurationVar(&o.pollInterval, "poll", 100*time.Millisecond, "worker PollInterval")
	flag.IntVar(&o.rate, "rate", 200, "jobs per second for the latency test")
	flag.IntVar(&o.latencyJobs, "latency-jobs", 1000, "jobs to send in the latency test")
	flag.Parse()
	for _, w := range strings.Split(workers, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(w))
		if err != nil {
			log.Fatalf("bad -workers: %v", err)
		}
		o.workers = append(o.workers, n)
	}
	o.modes = strings.Split(modes, ",")

	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	rdb := redis.NewClient(&redis.Options{Addr: o.redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis unavailable at %s: %v", o.redisAddr, err)
	}

	fmt.Printf("concurrency %d per worker, poll interval %s, latency test at %d jobs/s\n\n",
		o.concurrency, o.pollInterval, o.rate)
	fmt.Println("| mode | workers | throughput (jobs/s) | pickup p50 | pickup p95 | pickup p99 |")
	fmt.Println("|---|---|---|---|---|---|")
	for _, mode := range o.modes {
		for _, w := range o.workers {
			tput := throughput(o, rdb, mode, w)
			p50, p95, p99 := latency(o, rdb, mode, w)
			fmt.Printf("| %s | %d | %s | %s | %s | %s |\n", mode, w, commas(int(tput)), ms(p50), ms(p95), ms(p99))
		}
	}
}

// run is one benchmark run with its own key namespace.
type run struct {
	prefix string
	rdb    *redis.Client
	o      options
}

func newRun(o options, rdb *redis.Client) *run {
	return &run{prefix: fmt.Sprintf("bench:%d:", time.Now().UnixNano()), rdb: rdb, o: o}
}

func (r *run) store() *redisstream.Store {
	s, err := redisstream.New(redisstream.Config{
		Addr:       r.o.redisAddr,
		Stream:     r.prefix + "jobs",
		Group:      r.prefix + "workers",
		RetryKey:   r.prefix + "retry",
		DeadLetter: r.prefix + "dead",
	})
	if err != nil {
		log.Fatal(err)
	}
	return s
}

// startWorkers runs n workers calling handle until the returned stop is called.
func (r *run) startWorkers(mode string, n int, handle queue.HandlerFunc) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range n {
		store := r.store()
		w := queue.NewWorker(store, queue.WorkerConfig{
			ID:           fmt.Sprintf("bench-%d", i),
			Concurrency:  r.o.concurrency,
			PollInterval: r.o.pollInterval,
			PollOnly:     mode == "poll",
		}, nil)
		w.Register("noop", handle)
		wg.Go(func() {
			if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				log.Fatalf("worker: %v", err)
			}
			store.Close()
		})
	}
	return func() {
		cancel()
		wg.Wait()
		r.cleanup()
	}
}

func (r *run) cleanup() {
	ctx := context.Background()
	iter := r.rdb.Scan(ctx, 0, r.prefix+"*", 1000).Iterator()
	for iter.Next(ctx) {
		r.rdb.Del(ctx, iter.Val())
	}
}

func throughput(o options, rdb *redis.Client, mode string, workers int) float64 {
	r := newRun(o, rdb)
	store := r.store()
	defer store.Close()
	q := queue.New(store)

	// Enqueue the backlog in parallel; it isn't part of the measurement.
	var wg sync.WaitGroup
	for p := range 16 {
		wg.Go(func() {
			for i := p; i < o.jobs; i += 16 {
				if _, err := q.Enqueue(context.Background(), "noop", i, queue.EnqueueOptions{}); err != nil {
					log.Fatalf("enqueue: %v", err)
				}
			}
		})
	}
	wg.Wait()

	var done atomic.Int64
	finished := make(chan struct{})
	start := time.Now()
	stop := r.startWorkers(mode, workers, func(context.Context, queue.Job) queue.JobResult {
		if done.Add(1) == int64(o.jobs) {
			close(finished)
		}
		return queue.JobResult{}
	})
	<-finished
	elapsed := time.Since(start)
	stop()
	return float64(o.jobs) / elapsed.Seconds()
}

func latency(o options, rdb *redis.Client, mode string, workers int) (p50, p95, p99 time.Duration) {
	r := newRun(o, rdb)
	store := r.store()
	defer store.Close()
	q := queue.New(store)

	var mu sync.Mutex
	waits := make([]time.Duration, 0, o.latencyJobs)
	finished := make(chan struct{})
	stop := r.startWorkers(mode, workers, func(_ context.Context, job queue.Job) queue.JobResult {
		wait := time.Since(job.CreatedAt)
		mu.Lock()
		waits = append(waits, wait)
		if len(waits) == o.latencyJobs {
			close(finished)
		}
		mu.Unlock()
		return queue.JobResult{}
	})
	time.Sleep(200 * time.Millisecond) // let workers settle into their idle loop

	tick := time.NewTicker(time.Second / time.Duration(o.rate))
	for i := range o.latencyJobs {
		<-tick.C
		if _, err := q.Enqueue(context.Background(), "noop", i, queue.EnqueueOptions{}); err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	}
	tick.Stop()
	<-finished
	stop()

	slices.Sort(waits)
	at := func(p float64) time.Duration { return waits[int(p*float64(len(waits)-1))] }
	return at(0.50), at(0.95), at(0.99)
}

func ms(d time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func commas(n int) string {
	s := strconv.Itoa(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
