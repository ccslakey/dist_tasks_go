package tasks

import (
	"context"
	"errors"
	"log/slog"

	"dist_tasks_go/queue"
)

// FlakyEmail fails with a retryable error on the first two attempts and
// succeeds on the third. Use it to exercise the retry path in the demo:
//
//	go run ./cmd/enqueue-demo -task flaky_email -n 3
func FlakyEmail(ctx context.Context, job queue.Job) queue.JobResult {
	const succeedOnAttempt = 2

	if job.Attempt < succeedOnAttempt {
		slog.WarnContext(ctx, "flaky handler failing (will retry)",
			"job_id", job.ID,
			"attempt", job.Attempt,
			"succeed_on", succeedOnAttempt,
		)
		return queue.NewRetryableError(errors.New("transient failure (demo)"))
	}

	slog.InfoContext(ctx, "flaky handler succeeded",
		"job_id", job.ID,
		"attempt", job.Attempt,
	)
	return queue.JobResult{}
}
