// Command bench measures the queue's throughput and pickup latency against a
// real Redis, across worker counts and claim modes (blocking vs polling).
// Each worker is a separate process, as in a real deployment.
//
//   - Throughput: enqueue a backlog of no-op jobs, start the workers together,
//     and time how long they take to drain it. Also reports how fast the
//     backlog was enqueued and how busy Redis was.
//   - Latency: enqueue at a steady rate the workers can easily keep up with,
//     and record how long each job waited between enqueue and its handler
//     starting.
//
//	go run ./cmd/bench
//	go run ./cmd/bench -workers 1,4,8 -jobs 200000 -modes block
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
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
	producers    int

	// Worker-process flags, set by the controller.
	worker   bool
	workerID string
	prefix   string
	mode     string
	latency  bool
}

func main() {
	var o options
	var workers, modes string
	flag.StringVar(&o.redisAddr, "redis", "localhost:6379", "Redis address")
	flag.IntVar(&o.jobs, "jobs", 100000, "backlog size for the throughput test")
	flag.StringVar(&workers, "workers", "1,2,4,8", "comma-separated worker process counts to test")
	flag.IntVar(&o.concurrency, "concurrency", 16, "concurrency per worker")
	flag.StringVar(&modes, "modes", "poll,block", "claim modes to compare: poll, block")
	flag.DurationVar(&o.pollInterval, "poll", 100*time.Millisecond, "worker PollInterval")
	flag.IntVar(&o.rate, "rate", 200, "jobs per second for the latency test")
	flag.IntVar(&o.latencyJobs, "latency-jobs", 1000, "jobs to send in the latency test")
	flag.IntVar(&o.producers, "producers", 32, "concurrent enqueuers when building the backlog")
	flag.BoolVar(&o.worker, "worker", false, "")
	flag.StringVar(&o.workerID, "worker-id", "", "")
	flag.StringVar(&o.prefix, "prefix", "", "")
	flag.StringVar(&o.mode, "mode", "block", "")
	flag.BoolVar(&o.latency, "latency", false, "")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if o.worker {
		if err := runWorker(o); err != nil {
			log.Fatalf("worker %s: %v", o.workerID, err)
		}
		return
	}

	for _, w := range strings.Split(workers, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(w))
		if err != nil {
			log.Fatalf("bad -workers: %v", err)
		}
		o.workers = append(o.workers, n)
	}
	o.modes = strings.Split(modes, ",")
	rdb := redis.NewClient(&redis.Options{Addr: o.redisAddr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("redis unavailable at %s: %v", o.redisAddr, err)
	}
	exe, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%d jobs, concurrency %d per worker process, poll interval %s, latency test at %d jobs/s\n\n",
		o.jobs, o.concurrency, o.pollInterval, o.rate)
	fmt.Println("| mode | workers | throughput (jobs/s) | redis cpu | enqueue (jobs/s) | pickup p50 | pickup p95 | pickup p99 |")
	fmt.Println("|---|---|---|---|---|---|---|---|")
	for _, mode := range o.modes {
		for _, w := range o.workers {
			c := &controller{o: o, rdb: rdb, exe: exe, mode: mode, workers: w}
			t := c.throughput()
			p50, p95, p99 := (&controller{o: o, rdb: rdb, exe: exe, mode: mode, workers: w}).latency()
			fmt.Printf("| %s | %d | %s | %.0f%% | %s | %s | %s | %s |\n", mode, w, commas(int(t.jobsPerSec)),
				t.redisBusy*100, commas(int(t.enqueuePerSec)), ms(p50), ms(p95), ms(p99))
		}
	}
}

func storeConfig(addr, prefix string) redisstream.Config {
	return redisstream.Config{
		Addr:       addr,
		Stream:     prefix + "jobs",
		Group:      prefix + "workers",
		RetryKey:   prefix + "retry",
		DeadLetter: prefix + "dead",
	}
}

// ---------------------------------------------------------------- worker ---

// runWorker processes no-op jobs. It reports readiness and waits for the
// controller's go signal before claiming, so process startup isn't timed.
func runWorker(o options) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	store, err := redisstream.New(storeConfig(o.redisAddr, o.prefix))
	if err != nil {
		return err
	}
	rdb := redis.NewClient(&redis.Options{Addr: o.redisAddr})

	w := queue.NewWorker(store, queue.WorkerConfig{
		ID:           o.workerID,
		Concurrency:  o.concurrency,
		PollInterval: o.pollInterval,
		PollOnly:     o.mode == "poll",
	}, nil)
	w.Register("noop", func(ctx context.Context, job queue.Job) queue.JobResult {
		if o.latency {
			wait := time.Since(job.CreatedAt).Microseconds()
			rdb.RPush(context.WithoutCancel(ctx), o.prefix+"latency", wait)
		}
		return queue.JobResult{}
	})

	if err := rdb.Incr(ctx, o.prefix+"ready").Err(); err != nil {
		return err
	}
	for rdb.Exists(ctx, o.prefix+"go").Val() == 0 {
		if ctx.Err() != nil {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// ------------------------------------------------------------ controller ---

type controller struct {
	o       options
	rdb     *redis.Client
	exe     string
	mode    string
	workers int
	prefix  string
	procs   []*exec.Cmd
}

type throughputResult struct {
	jobsPerSec    float64
	enqueuePerSec float64
	redisBusy     float64 // fraction of wall time Redis was on CPU
}

func (c *controller) start(latency bool) {
	c.prefix = fmt.Sprintf("bench:%d:", time.Now().UnixNano())
	for i := range c.workers {
		cmd := exec.Command(c.exe, "-worker", "-worker-id", fmt.Sprintf("bench-%d", i),
			"-prefix", c.prefix, "-redis", c.o.redisAddr, "-mode", c.mode,
			"-concurrency", strconv.Itoa(c.o.concurrency), "-poll", c.o.pollInterval.String(),
			"-latency="+strconv.FormatBool(latency))
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			log.Fatalf("start worker: %v", err)
		}
		c.procs = append(c.procs, cmd)
	}
}

// release waits for every worker to be ready, then lets them all start.
func (c *controller) release() {
	ctx := context.Background()
	for {
		n, _ := c.rdb.Get(ctx, c.prefix+"ready").Int()
		if n == c.workers {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.rdb.Set(ctx, c.prefix+"go", 1, 0)
}

func (c *controller) stop() {
	for _, p := range c.procs {
		p.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range c.procs {
		p.Wait()
	}
	ctx := context.Background()
	iter := c.rdb.Scan(ctx, 0, c.prefix+"*", 1000).Iterator()
	for iter.Next(ctx) {
		c.rdb.Del(ctx, iter.Val())
	}
}

func (c *controller) enqueue(store *redisstream.Store, n int) time.Duration {
	q := queue.New(store)
	start := time.Now()
	var wg sync.WaitGroup
	for p := range c.o.producers {
		wg.Go(func() {
			for i := p; i < n; i += c.o.producers {
				if _, err := q.Enqueue(context.Background(), "noop", i, queue.EnqueueOptions{}); err != nil {
					log.Fatalf("enqueue: %v", err)
				}
			}
		})
	}
	wg.Wait()
	return time.Since(start)
}

func (c *controller) throughput() throughputResult {
	c.start(false)
	defer c.stop()
	store, err := redisstream.New(storeConfig(c.o.redisAddr, c.prefix))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	enqueueTime := c.enqueue(store, c.o.jobs)
	cpuBefore := redisCPU(c.rdb)
	start := time.Now()
	c.release()
	for {
		st, err := store.Stats(context.Background())
		if err == nil && st.StreamLength == 0 && st.InFlight == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	elapsed := time.Since(start)
	return throughputResult{
		jobsPerSec:    float64(c.o.jobs) / elapsed.Seconds(),
		enqueuePerSec: float64(c.o.jobs) / enqueueTime.Seconds(),
		redisBusy:     (redisCPU(c.rdb) - cpuBefore) / elapsed.Seconds(),
	}
}

func (c *controller) latency() (p50, p95, p99 time.Duration) {
	c.start(true)
	defer c.stop()
	store, err := redisstream.New(storeConfig(c.o.redisAddr, c.prefix))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	c.release()
	time.Sleep(300 * time.Millisecond) // let workers settle into their idle loop

	q := queue.New(store)
	tick := time.NewTicker(time.Second / time.Duration(c.o.rate))
	for i := range c.o.latencyJobs {
		<-tick.C
		if _, err := q.Enqueue(context.Background(), "noop", i, queue.EnqueueOptions{}); err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	}
	tick.Stop()

	ctx := context.Background()
	for c.rdb.LLen(ctx, c.prefix+"latency").Val() < int64(c.o.latencyJobs) {
		time.Sleep(10 * time.Millisecond)
	}
	raw, err := c.rdb.LRange(ctx, c.prefix+"latency", 0, -1).Result()
	if err != nil {
		log.Fatal(err)
	}
	waits := make([]time.Duration, len(raw))
	for i, s := range raw {
		us, _ := strconv.ParseInt(s, 10, 64)
		waits[i] = time.Duration(us) * time.Microsecond
	}
	slices.Sort(waits)
	at := func(p float64) time.Duration { return waits[int(p*float64(len(waits)-1))] }
	return at(0.50), at(0.95), at(0.99)
}

// redisCPU returns the CPU seconds Redis has used so far.
func redisCPU(rdb *redis.Client) float64 {
	info, err := rdb.Info(context.Background(), "cpu").Result()
	if err != nil {
		return 0
	}
	var total float64
	for _, line := range strings.Split(info, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if ok && (k == "used_cpu_user" || k == "used_cpu_sys") {
			f, _ := strconv.ParseFloat(v, 64)
			total += f
		}
	}
	return total
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
