package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"dist_tasks_go/queue"
)

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func SendEmail(ctx context.Context, job queue.Job) queue.JobResult {
	var payload EmailPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return queue.NewPermanentError(err)
	}
	if payload.To == "" {
		return queue.NewPermanentError(errors.New("email recipient is required"))
	}

	slog.InfoContext(ctx, "sending email",
		"job_id", job.ID,
		"attempt", job.Attempt,
		"to", payload.To,
		"subject", payload.Subject,
	)
	return queue.JobResult{}
}
