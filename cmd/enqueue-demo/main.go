package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"dist_tasks_go/examples/tasks"
	"dist_tasks_go/internal/prettylog"
	"dist_tasks_go/internal/telemetry"
	"dist_tasks_go/prommetrics"
	"dist_tasks_go/queue"
	"dist_tasks_go/redisstream"
)

func main() {
	n := flag.Int("n", 1, "number of jobs to enqueue (0 = until interrupted)")
	delay := flag.Duration("delay", 50*time.Millisecond, "delay between enqueues")
	task := flag.String("task", "send_email", "task name: send_email | process_image | permanent_fail | flaky_email | mix (a weighted blend of all four)")
	latency := flag.Duration("latency", 0, "artificial latency injected into each job handler (e.g. 500ms); randomized ±50% for -task mix")
	redisAddr := flag.String("redis", envOr("DTQ_REDIS_ADDR", "localhost:6379"), "Redis address")
	metricsAddr := flag.String("metrics-addr", "", "address for a /metrics endpoint (empty disables it)")
	flag.Parse()

	slog.SetDefault(slog.New(prettylog.NewHandler(os.Stdout)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Setup(ctx, "dtq-producer")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdownTracing(context.Background())

	backend, err := redisstream.New(redisstream.Config{Addr: *redisAddr})
	if err != nil {
		log.Fatal(err)
	}

	m := prommetrics.New(nil)
	q := queue.New(backend, queue.WithMetrics(m))
	if *metricsAddr != "" {
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
				slog.Error("metrics server", "err", err)
			}
		}()
	}

	for i := 0; *n == 0 || i < *n; i++ {
		name, ms := *task, int(latency.Milliseconds())
		if name == "mix" {
			name = pickTask()
			ms = ms/2 + rand.IntN(ms+1)
		}
		payload, maxAttempts := payloadFor(name, ms)
		id, err := q.Enqueue(ctx, name, payload, queue.EnqueueOptions{MaxAttempts: maxAttempts})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("enqueue failed", "err", err, "i", i)
		} else {
			slog.Info("enqueued", "job_id", id, "task", name, "i", i)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(*delay):
		}
	}
}

// pickTask returns a task for -task mix: mostly successes, with some retries
// and the occasional permanent failure.
func pickTask() string {
	switch r := rand.IntN(100); {
	case r < 70:
		return "send_email"
	case r < 85:
		return "process_image"
	case r < 97:
		return "flaky_email"
	default:
		return "permanent_fail"
	}
}

func payloadFor(task string, latencyMs int) (any, int) {
	switch task {
	case "process_image":
		return tasks.ImagePayload{ImageID: "img-demo", URL: "https://example.com/demo.jpg", LatencyMs: latencyMs}, 3
	case "permanent_fail":
		return map[string]any{}, 1
	case "flaky_email":
		return tasks.EmailPayload{
			To:        "dev@example.com",
			Subject:   "Flaky email demo",
			Body:      "This job fails twice then succeeds.",
			LatencyMs: latencyMs,
		}, 5
	default: // send_email
		return tasks.EmailPayload{
			To:        "dev@example.com",
			Subject:   "Distributed queue demo",
			Body:      "This job should eventually be handled by a worker.",
			LatencyMs: latencyMs,
		}, 3
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
