package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"time"

	"dist_tasks_go/examples/tasks"
	"dist_tasks_go/internal/prettylog"
	"dist_tasks_go/prommetrics"
	"dist_tasks_go/queue"
	"dist_tasks_go/redisstream"
)

func main() {
	n := flag.Int("n", 1, "number of jobs to enqueue")
	delay := flag.Duration("delay", 50*time.Millisecond, "delay between enqueues")
	task := flag.String("task", "send_email", "task name: send_email | process_image | permanent_fail | flaky_email")
	latency := flag.Duration("latency", 0, "artificial latency injected into each job handler (e.g. 500ms)")
	flag.Parse()

	slog.SetDefault(slog.New(prettylog.NewHandler(os.Stdout)))

	ctx := context.Background()

	backend, err := redisstream.New(redisstream.Config{})
	if err != nil {
		log.Fatal(err)
	}

	m := prommetrics.New(nil)
	q := queue.New(backend, queue.WithMetrics(m))

	payload, maxAttempts := payloadFor(*task, int(latency.Milliseconds()))

	for i := range *n {
		id, err := q.Enqueue(ctx, *task, payload, queue.EnqueueOptions{MaxAttempts: maxAttempts})
		if err != nil {
			slog.Error("enqueue failed", "err", err, "i", i)
			continue
		}
		slog.Info("enqueued", "job_id", id, "task", *task, "i", i, "total", *n)

		if i < *n-1 {
			time.Sleep(*delay)
		}
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
