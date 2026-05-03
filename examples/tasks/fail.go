package tasks

import (
	"context"
	"errors"
	"log/slog"

	"dist_tasks_go/queue"
)

// PermanentFail always returns a permanent error, sending the job straight to
// the dead-letter queue. Use it in the enqueue-demo to exercise the DLQ path:
//
//	go run ./cmd/enqueue-demo -task permanent_fail -n 3
func PermanentFail(ctx context.Context, job queue.Job) queue.JobResult {
	slog.WarnContext(ctx, "permanent failure (demo)",
		"job_id", job.ID,
		"attempt", job.Attempt,
	)
	return queue.NewPermanentError(errors.New("this task always fails"))
}
