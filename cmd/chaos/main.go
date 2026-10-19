// Command chaos is a fault-injection test for the queue. It enqueues a batch
// of jobs, runs several worker processes against them, and SIGKILLs and
// SIGSTOPs workers at random while they work. Then it checks the delivery
// guarantees against a ledger the handlers write to:
//
//   - no job is lost: each one either completed or was dead-lettered
//   - jobs that fail permanently are dead-lettered and never complete
//   - the queue drains completely (nothing stuck pending or scheduled)
//
// Duplicate executions are expected (delivery is at-least-once) and are
// counted, not treated as failures.
//
//	go run ./cmd/chaos
//	go run ./cmd/chaos -jobs 20000 -workers 6 -kill-every 500ms
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand/v2"
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
	redisAddr   string
	prefix      string
	jobs        int
	workers     int
	concurrency int
	duration    time.Duration
	killEvery   time.Duration
	pauseEvery  time.Duration
	visibility  time.Duration
	failRate    float64
	timeout     time.Duration
	keep        bool
	verbose     bool

	worker   bool
	workerID string
}

// payload controls how the chaos handler behaves for one job.
type payload struct {
	N         int  `json:"n"`
	LatencyMs int  `json:"latency_ms"`
	Permanent bool `json:"permanent,omitempty"`
}

const (
	task           = "chaos"
	maxAttempts    = 8
	permanentEvery = 50 // every Nth job fails permanently and must be dead-lettered
)

func main() {
	var o options
	flag.StringVar(&o.redisAddr, "redis", "localhost:6379", "Redis address")
	flag.IntVar(&o.jobs, "jobs", 10000, "jobs to enqueue")
	flag.IntVar(&o.workers, "workers", 4, "worker processes to keep running")
	flag.IntVar(&o.concurrency, "concurrency", 8, "concurrency per worker")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "max time to inject faults (stops early once the queue drains)")
	flag.DurationVar(&o.killEvery, "kill-every", time.Second, "mean time between worker SIGKILLs (0 disables)")
	flag.DurationVar(&o.pauseEvery, "pause-every", 4*time.Second, "mean time between worker SIGSTOP pauses longer than the visibility timeout (0 disables)")
	flag.DurationVar(&o.visibility, "visibility", 2*time.Second, "worker visibility timeout")
	flag.Float64Var(&o.failRate, "fail-rate", 0.1, "probability that any run fails with a retryable error")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Minute, "max time to wait for the queue to drain after faults stop")
	flag.BoolVar(&o.keep, "keep", false, "keep the run's Redis keys for inspection")
	flag.BoolVar(&o.verbose, "v", false, "show worker logs")
	// Internal: set when the controller starts this binary as a worker.
	flag.BoolVar(&o.worker, "worker", false, "")
	flag.StringVar(&o.workerID, "worker-id", "", "")
	flag.StringVar(&o.prefix, "prefix", "", "")
	flag.Parse()

	if o.worker {
		if err := runWorker(o); err != nil {
			log.Fatalf("worker %s: %v", o.workerID, err)
		}
		return
	}
	os.Exit(runController(o))
}

func storeConfig(o options) redisstream.Config {
	return redisstream.Config{
		Addr:       o.redisAddr,
		Stream:     o.prefix + "jobs",
		Group:      o.prefix + "workers",
		RetryKey:   o.prefix + "retry",
		DeadLetter: o.prefix + "dead",
	}
}

// Ledger keys. Handlers append one entry per completed execution:
// "<payload n> <start unix µs> <end unix µs> <worker id>". Jobs are identified
// by payload number rather than job ID so the check doesn't depend on the
// backend keeping IDs stable.
func ledgerKey(prefix string) string    { return prefix + "ledger" }
func leaseLostKey(prefix string) string { return prefix + "lease-lost" }

// ---------------------------------------------------------------- worker ---

func runWorker(o options) error {
	if !o.verbose {
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := redisstream.New(storeConfig(o))
	if err != nil {
		return err
	}
	rdb := redis.NewClient(&redis.Options{Addr: o.redisAddr})

	w := queue.NewWorker(store, queue.WorkerConfig{
		ID:                o.workerID,
		Concurrency:       o.concurrency,
		PollInterval:      50 * time.Millisecond,
		VisibilityTimeout: o.visibility,
		RetryPolicy:       queue.RetryPolicy{BaseDelay: 50 * time.Millisecond, MaxDelay: time.Second},
	}, leaseLostCounter{rdb: rdb, key: leaseLostKey(o.prefix)})

	w.Register(task, func(ctx context.Context, job queue.Job) queue.JobResult {
		var p payload
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			return queue.NewPermanentError(err)
		}
		start := time.Now()
		select {
		case <-time.After(time.Duration(p.LatencyMs) * time.Millisecond):
		case <-ctx.Done(): // lease lost: stop before producing a side effect
			return queue.NewRetryableError(ctx.Err())
		}
		if p.Permanent {
			return queue.NewPermanentError(errors.New("permanent failure (chaos)"))
		}
		if rand.Float64() < o.failRate {
			return queue.NewRetryableError(errors.New("transient failure (chaos)"))
		}
		// The job's side effect. A crash between here and the ack is what
		// makes delivery at-least-once.
		entry := fmt.Sprintf("%d %d %d %s", p.N, start.UnixMicro(), time.Now().UnixMicro(), o.workerID)
		if err := rdb.RPush(ctx, ledgerKey(o.prefix), entry).Err(); err != nil {
			return queue.NewRetryableError(err)
		}
		return queue.JobResult{}
	})

	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// leaseLostCounter records lease-lost events in Redis so the controller can
// report how many zombie results were rejected.
type leaseLostCounter struct {
	queue.NoopMetrics
	rdb *redis.Client
	key string
}

func (m leaseLostCounter) JobLeaseLost(ctx context.Context, _ queue.Job) {
	m.rdb.Incr(context.WithoutCancel(ctx), m.key)
}

// ------------------------------------------------------------ controller ---

type proc struct {
	id     string
	cmd    *exec.Cmd
	done   chan struct{}
	killed bool // set by the controller before it SIGKILLs the process
	paused bool
}

type controller struct {
	o    options
	exe  string
	rng  *rand.Rand
	next int

	mu      sync.Mutex
	procs   []*proc
	crashed []string // workers that exited without being killed

	kills, pauses int
}

func runController(o options) int {
	o.prefix = fmt.Sprintf("chaos:%d:", time.Now().UnixNano())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	exe, err := os.Executable()
	if err != nil {
		log.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: o.redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis unavailable at %s: %v", o.redisAddr, err)
	}
	store, err := redisstream.New(storeConfig(o))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	if !o.keep {
		defer cleanup(rdb, o.prefix)
	}

	fmt.Printf("chaos run %s\n", strings.TrimSuffix(o.prefix, ":"))
	fmt.Printf("  %d jobs, %d workers x %d, visibility %s, fail rate %.0f%%\n",
		o.jobs, o.workers, o.concurrency, o.visibility, o.failRate*100)

	// Enqueue everything up front so "queue drained" means "all jobs settled".
	q := queue.New(store)
	for i := range o.jobs {
		p := payload{N: i, LatencyMs: 10 + rand.IntN(90), Permanent: i%permanentEvery == 0}
		if i%100 == 1 {
			p.LatencyMs = int(2 * o.visibility.Milliseconds()) // outlives the visibility timeout; needs heartbeats
		}
		if _, err := q.Enqueue(ctx, task, p, queue.EnqueueOptions{MaxAttempts: maxAttempts}); err != nil {
			log.Fatalf("enqueue: %v", err)
		}
	}

	c := &controller{o: o, exe: exe, rng: rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))}
	start := time.Now()
	for range o.workers {
		c.spawn()
	}

	drained := func() bool {
		st, err := store.Stats(ctx)
		return err == nil && st.StreamLength == 0 && st.InFlight == 0 && st.RetryScheduled == 0
	}

	c.injectFaults(ctx, drained)
	fmt.Printf("  faults stopped after %s; waiting for the queue to drain\n", time.Since(start).Round(100*time.Millisecond))
	c.resumeAll()

	deadline := time.Now().Add(o.timeout)
	for !drained() && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
	}
	elapsed := time.Since(start)
	if !drained() {
		c.printPending(ctx, rdb)
	}
	c.stopAll()

	return c.report(context.Background(), rdb, store, elapsed)
}

func (c *controller) spawn() {
	c.next++
	id := fmt.Sprintf("chaos-worker-%d", c.next)
	o := c.o
	cmd := exec.Command(c.exe,
		"-worker", "-worker-id", id, "-prefix", o.prefix, "-redis", o.redisAddr,
		"-concurrency", strconv.Itoa(o.concurrency), "-visibility", o.visibility.String(),
		"-fail-rate", strconv.FormatFloat(o.failRate, 'f', -1, 64), "-v="+strconv.FormatBool(o.verbose))
	if o.verbose {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	}
	if err := cmd.Start(); err != nil {
		log.Fatalf("start worker: %v", err)
	}
	p := &proc{id: id, cmd: cmd, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		c.mu.Lock()
		if !p.killed && err != nil {
			c.crashed = append(c.crashed, fmt.Sprintf("%s: %v", id, err))
		}
		c.procs = slices.DeleteFunc(c.procs, func(q *proc) bool { return q == p })
		c.mu.Unlock()
		close(p.done)
	}()
	c.mu.Lock()
	c.procs = append(c.procs, p)
	c.mu.Unlock()
}

// pick returns a random live worker that isn't paused.
func (c *controller) pick() *proc {
	c.mu.Lock()
	defer c.mu.Unlock()
	var live []*proc
	for _, p := range c.procs {
		if !p.paused && !p.killed {
			live = append(live, p)
		}
	}
	if len(live) == 0 {
		return nil
	}
	return live[c.rng.IntN(len(live))]
}

// jitter returns a random duration in [0.5, 1.5) x mean.
func (c *controller) jitter(mean time.Duration) time.Duration {
	return time.Duration((0.5 + c.rng.Float64()) * float64(mean))
}

func (c *controller) injectFaults(ctx context.Context, drained func() bool) {
	never := make(<-chan time.Time)
	timer := func(mean time.Duration) <-chan time.Time {
		if mean <= 0 {
			return never
		}
		return time.After(c.jitter(mean))
	}
	kill, pause := timer(c.o.killEvery), timer(c.o.pauseEvery)
	stop := time.After(c.o.duration)
	check := time.NewTicker(200 * time.Millisecond)
	defer check.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-check.C:
			if drained() {
				return
			}
		case <-kill:
			kill = timer(c.o.killEvery)
			if p := c.pick(); p != nil {
				c.mu.Lock()
				p.killed = true
				c.mu.Unlock()
				p.cmd.Process.Signal(syscall.SIGKILL)
				<-p.done
				c.kills++
				c.spawn() // like an orchestrator replacing a crashed pod
			}
		case <-pause:
			pause = timer(c.o.pauseEvery)
			if p := c.pick(); p != nil {
				// Freeze the worker long enough for its leases to expire, then
				// let it resume as a zombie that still thinks it owns its jobs.
				c.mu.Lock()
				p.paused = true
				c.mu.Unlock()
				p.cmd.Process.Signal(syscall.SIGSTOP)
				c.pauses++
				time.AfterFunc(c.jitter(2*c.o.visibility), func() { c.resume(p) })
			}
		}
	}
}

func (c *controller) resume(p *proc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if p.paused {
		p.paused = false
		p.cmd.Process.Signal(syscall.SIGCONT)
	}
}

func (c *controller) resumeAll() {
	c.mu.Lock()
	procs := slices.Clone(c.procs)
	c.mu.Unlock()
	for _, p := range procs {
		c.resume(p)
	}
}

// stopAll shuts workers down gracefully, the way a deploy would.
func (c *controller) stopAll() {
	c.mu.Lock()
	procs := slices.Clone(c.procs)
	c.mu.Unlock()
	for _, p := range procs {
		p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for _, p := range procs {
		<-p.done
	}
}

func (c *controller) report(ctx context.Context, rdb *redis.Client, store *redisstream.Store, elapsed time.Duration) int {
	type execution struct{ start, end int64 }
	executions := map[int][]execution{}
	ledger, err := rdb.LRange(ctx, ledgerKey(c.o.prefix), 0, -1).Result()
	if err != nil {
		log.Fatalf("read ledger: %v", err)
	}
	for _, line := range ledger {
		var n int
		var e execution
		var worker string
		if _, err := fmt.Sscanf(line, "%d %d %d %s", &n, &e.start, &e.end, &worker); err != nil {
			log.Fatalf("bad ledger entry %q: %v", line, err)
		}
		executions[n] = append(executions[n], e)
	}

	deadReason := map[int]string{}
	dead, err := rdb.XRange(ctx, c.o.prefix+"dead", "-", "+").Result()
	if err != nil {
		log.Fatalf("read dead-letter stream: %v", err)
	}
	for _, m := range dead {
		var p payload
		raw, _ := m.Values["payload"].(string)
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			log.Fatalf("bad dead-letter payload %q: %v", raw, err)
		}
		reason, _ := m.Values["reason"].(string)
		deadReason[p.N] = reason
	}

	var completed, deadExpected, duplicates, overlapping int
	var lost, misrouted []int
	otherDead := map[string]int{}
	for id := range c.o.jobs {
		execs := executions[id]
		reason, isDead := deadReason[id]
		switch {
		case id%permanentEvery == 0:
			if !isDead || len(execs) > 0 {
				misrouted = append(misrouted, id)
			} else {
				deadExpected++
			}
			continue
		case len(execs) == 0 && !isDead:
			lost = append(lost, id)
			continue
		case isDead:
			otherDead[reason]++
		}
		if len(execs) > 0 {
			completed++
			duplicates += len(execs) - 1
			slices.SortFunc(execs, func(a, b execution) int { return int(a.start - b.start) })
			for i := 1; i < len(execs); i++ {
				if execs[i].start < execs[i-1].end {
					overlapping++
					break
				}
			}
		}
	}

	st, _ := store.Stats(ctx)
	leaseLost, _ := rdb.Get(ctx, leaseLostKey(c.o.prefix)).Int()
	leftover := st.StreamLength + st.InFlight + st.RetryScheduled

	ok := func(good bool) string {
		if good {
			return "ok"
		}
		return "FAIL"
	}
	fmt.Printf("  faults injected: %d SIGKILLs, %d SIGSTOP pauses, elapsed %s\n\n", c.kills, c.pauses, elapsed.Round(100*time.Millisecond))
	fmt.Printf("  %-34s %7d\n", "jobs enqueued", c.o.jobs)
	fmt.Printf("  %-34s %7d\n", "completed", completed)
	fmt.Printf("  %-34s %7d\n", "dead-lettered (permanent failures)", deadExpected)
	for reason, n := range otherDead {
		fmt.Printf("  %-34s %7d  %s\n", "dead-lettered (poison guard etc.)", n, reason)
	}
	fmt.Printf("  %-34s %7d  %s\n", "lost", len(lost), ok(len(lost) == 0))
	fmt.Printf("  %-34s %7d  %s\n", "permanent failures misrouted", len(misrouted), ok(len(misrouted) == 0))
	fmt.Printf("  %-34s %7d  %s (stream %d, pending %d, scheduled %d)\n", "left in queue", leftover, ok(leftover == 0),
		st.StreamLength, st.InFlight, st.RetryScheduled)
	fmt.Printf("  %-34s %7d  crashed or paused between side effect and ack\n", "duplicate executions", duplicates)
	fmt.Printf("  %-34s %7d  ran concurrently; only paused zombies should do this\n", "jobs with overlapping runs", overlapping)
	fmt.Printf("  %-34s %7d  stale results fenced off by lease checks\n", "zombie results rejected", leaseLost)
	c.mu.Lock()
	for _, crash := range c.crashed {
		fmt.Printf("  worker exited unexpectedly: %s\n", crash)
	}
	crashed := len(c.crashed)
	c.mu.Unlock()

	if len(lost) > 0 {
		fmt.Printf("\n  first lost jobs (payload n): %v\n", lost[:min(5, len(lost))])
	}
	if len(lost) > 0 || len(misrouted) > 0 || leftover > 0 || crashed > 0 {
		fmt.Println("\nFAIL")
		return 1
	}
	fmt.Println("\nPASS")
	return 0
}

// printPending shows who holds each unsettled delivery, to tell a job that is
// still being heartbeated apart from one that recovery never reaches.
func (c *controller) printPending(ctx context.Context, rdb *redis.Client) {
	pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: c.o.prefix + "jobs", Group: c.o.prefix + "workers", Start: "-", End: "+", Count: 20,
	}).Result()
	if err != nil {
		fmt.Printf("  xpending: %v\n", err)
		return
	}
	c.mu.Lock()
	live := map[string]bool{}
	for _, p := range c.procs {
		live[p.id] = true
	}
	c.mu.Unlock()
	fmt.Println("\n  unsettled deliveries:")
	for _, p := range pending {
		fmt.Printf("    %s owner=%s (alive: %t) idle=%s deliveries=%d\n",
			p.ID, p.Consumer, live[p.Consumer], p.Idle.Round(time.Millisecond), p.RetryCount)
	}
}

func cleanup(rdb *redis.Client, prefix string) {
	ctx := context.Background()
	iter := rdb.Scan(ctx, 0, prefix+"*", 1000).Iterator()
	for iter.Next(ctx) {
		rdb.Del(ctx, iter.Val())
	}
}
