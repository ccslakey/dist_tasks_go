package main

import (
	"context"
	"dist_tasks_go/examples/tasks"
	"dist_tasks_go/internal/prettylog"
	"dist_tasks_go/internal/telemetry"
	"dist_tasks_go/prommetrics"
	"dist_tasks_go/queue"
	"dist_tasks_go/redisstream"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	host, _ := os.Hostname()
	id := flag.String("id", fmt.Sprintf("%s-%d", host, os.Getpid()), "worker ID; must be unique per running worker")
	metricsAddr := flag.String("metrics-addr", ":9090", "address for the /metrics endpoint (empty disables it)")
	concurrency := flag.Int("concurrency", 4, "max jobs this worker runs at once")
	redisAddr := flag.String("redis", envOr("DTQ_REDIS_ADDR", "localhost:6379"), "Redis address")
	flag.Parse()

	slog.SetDefault(slog.New(prettylog.NewHandler(os.Stdout)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := telemetry.Setup(ctx, "dtq-worker")
	if err != nil {
		slog.Error("tracing setup failed", "err", err)
		os.Exit(1)
	}
	defer shutdownTracing(context.Background())

	backend, err := redisstream.New(redisstream.Config{Addr: *redisAddr})
	if err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	m := prommetrics.New(nil)

	if *metricsAddr != "" {
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			slog.Info("metrics listening", "addr", *metricsAddr)
			if err := http.ListenAndServe(*metricsAddr, nil); err != nil {
				slog.Error(fmt.Sprintf("metrics server: %v", err))
			}
		}()
	}

	worker := queue.NewWorker(backend, queue.WorkerConfig{
		ID:                *id,
		Concurrency:       *concurrency,
		PollInterval:      250 * time.Millisecond,
		VisibilityTimeout: 30 * time.Second,
		RetryPolicy:       queue.DefaultRetryPolicy(),
	}, m)

	if err := worker.Register("send_email", tasks.SendEmail); err != nil {
		slog.Error(err.Error())
	}
	if err := worker.Register("process_image", tasks.ProcessImage); err != nil {
		slog.Error(err.Error())
	}
	if err := worker.Register("permanent_fail", tasks.PermanentFail); err != nil {
		slog.Error(err.Error())
	}
	if err := worker.Register("flaky_email", tasks.FlakyEmail); err != nil {
		slog.Error(err.Error())
	}

	slog.Info("worker started", "id", *id, "concurrency", *concurrency)
	if err := worker.Run(ctx); err != nil && err != context.Canceled {
		slog.Error(err.Error())
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
