package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"dist_tasks_go/examples/tasks"
	"dist_tasks_go/queue"
	"dist_tasks_go/redisstream"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	backend, err := redisstream.New(redisstream.Config{})
	if err != nil {
		log.Fatal(err)
	}

	worker := queue.NewWorker(backend, queue.WorkerConfig{
		ID:                "worker-demo-1",
		Concurrency:       4,
		PollInterval:      250 * time.Millisecond,
		VisibilityTimeout: 30 * time.Second,
		RetryPolicy:       queue.DefaultRetryPolicy(),
	}, nil)

	if err := worker.Register("send_email", tasks.SendEmail); err != nil {
		log.Fatal(err)
	}
	if err := worker.Register("process_image", tasks.ProcessImage); err != nil {
		log.Fatal(err)
	}

	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		log.Fatal(err)
	}
}
