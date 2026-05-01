package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"dist_tasks_go/queue"
)

type ImagePayload struct {
	ImageID string `json:"image_id"`
	URL     string `json:"url"`
}

func ProcessImage(ctx context.Context, job queue.Job) queue.JobResult {
	var payload ImagePayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return queue.NewPermanentError(err)
	}
	if payload.ImageID == "" {
		return queue.NewPermanentError(errors.New("image id is required"))
	}

	// TODO: Add a controlled flaky branch to exercise retries during demos.
	fmt.Printf("process image id=%s url=%s\n", payload.ImageID, payload.URL)
	return queue.JobResult{}
}
