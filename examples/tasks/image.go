package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"dist_tasks_go/queue"
)

type ImagePayload struct {
	ImageID   string `json:"image_id"`
	URL       string `json:"url"`
	LatencyMs int    `json:"latency_ms,omitempty"`
}

func ProcessImage(ctx context.Context, job queue.Job) queue.JobResult {
	var payload ImagePayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return queue.NewPermanentError(err)
	}
	if payload.ImageID == "" {
		return queue.NewPermanentError(errors.New("image id is required"))
	}

	if payload.LatencyMs > 0 {
		time.Sleep(time.Duration(payload.LatencyMs) * time.Millisecond)
	}

	slog.InfoContext(ctx, "processing image",
		"job_id", job.ID,
		"attempt", job.Attempt,
		"image_id", payload.ImageID,
		"url", payload.URL,
		"latency_ms", payload.LatencyMs,
	)
	return queue.JobResult{}
}
