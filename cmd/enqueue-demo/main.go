package main

import (
	"context"
	"dist_tasks_go/examples/tasks"
	"dist_tasks_go/internal/prettylog"
	"dist_tasks_go/prommetrics"
	"dist_tasks_go/queue"
	"dist_tasks_go/redisstream"
	"fmt"
	"log"
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(prettylog.NewHandler(os.Stdout)))

	ctx := context.Background()

	backend, err := redisstream.New(redisstream.Config{})
	if err != nil {
		log.Fatal(err)
	}

	m := prommetrics.New(nil)
	q := queue.New(backend, queue.WithMetrics(m))
	id, err := q.Enqueue(ctx, "send_email", tasks.EmailPayload{
		To:      "dev@example.com",
		Subject: "Distributed queue demo",
		Body:    "This job should eventually be handled by a worker.",
	}, queue.EnqueueOptions{MaxAttempts: 3})
	if err != nil {
		slog.Error(err.Error())
	}

	slog.Info(fmt.Sprintf("enqueued job %s\n", id))
}
