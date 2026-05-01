package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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

	// TODO: Replace this print with a realistic side effect once the queue core works.
	fmt.Printf("send email to=%s subject=%q\n", payload.To, payload.Subject)
	return queue.JobResult{}
}
