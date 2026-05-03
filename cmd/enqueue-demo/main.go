package main

import (
	"context"
	"fmt"
	"log"

	"dist_tasks_go/examples/tasks"
	"dist_tasks_go/prommetrics"
	"dist_tasks_go/queue"
	"dist_tasks_go/redisstream"
)

func main() {
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
		log.Fatal(err)
	}

	fmt.Printf("enqueued job %s\n", id)
}
