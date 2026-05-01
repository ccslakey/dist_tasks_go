package redisstream

import (
	"context"
	"dist_tasks_go/queue"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Addr       string
	Stream     string
	Group      string
	RetryKey   string
	DeadLetter string
}

type Backend struct {
	config Config
	rds    *redis.Client
}

func New(config Config) (*Backend, error) {
	if config.Addr == "" {
		config.Addr = "localhost:6379"
	}
	if config.Stream == "" {
		config.Stream = "dtq:jobs"
	}
	if config.Group == "" {
		config.Group = "dtq:workers"
	}
	if config.RetryKey == "" {
		config.RetryKey = "dtq:retry"
	}
	if config.DeadLetter == "" {
		config.DeadLetter = "dtq:dead"
	}
	rdsCli := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: "", // no password
		DB:       0,  // use default DB
		Protocol: 2,
	})

	_, err := rdsCli.XGroupCreateMkStream(context.Background(), config.Stream, config.Group, "$").Result()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return nil, err
	}

	return &Backend{config: config, rds: rdsCli}, nil
}

func (b *Backend) Enqueue(ctx context.Context, job queue.Job) (queue.JobID, error) {
	// Use Redis XADD against b.config.Stream.
	// Store task, payload, attempt, max attempts,
	// timestamps, and trace id as fields.
	args := redis.XAddArgs{
		Stream: b.config.Stream,
		Values: map[string]interface{}{
			"task":        job.Task,
			"payload":     string(job.Payload),
			"attempt":     job.Attempt,
			"maxAttempts": job.MaxAttempts,
			"traceID":     job.TraceID,
			"createdAt":   job.CreatedAt.Format(time.RFC3339Nano),
			"availableAt": job.AvailableAt.Format(time.RFC3339Nano),
		},
	}
	val, err := b.rds.XAdd(ctx, &args).Result()

	return queue.JobID(val), err

}

func (b *Backend) Claim(ctx context.Context, workerID string, limit int) ([]queue.Job, error) {
	// TODO: Use XREADGROUP to claim new jobs for workerID.

	args := redis.XReadGroupArgs{
		Group:    b.config.Group,
		Consumer: workerID,
		Count:    int64(limit),
		Streams:  []string{b.config.Stream, ">"},
	}

	streams, xrErr := b.rds.XReadGroup(ctx, &args).Result()

	if errors.Is(xrErr, redis.Nil) {
		return []queue.Job{}, nil
	}

	if xrErr != nil {
		return nil, fmt.Errorf("err in xreadgroup. workerId:err %s: %w", workerID, xrErr)
	}

	// Later, combine this with retry promotion so delayed retries re-enter the stream.
	var jobs []queue.Job

	for _, s := range streams {

		for _, messageJob := range s.Messages {
			jobVals := messageJob.Values

			att, attErr := strconv.Atoi(jobVals["attempt"].(string))
			if attErr != nil {
				return nil, fmt.Errorf("invalid attempt field for job %s: %w", messageJob.ID, attErr)
			}
			maxAtt, maxAttErr := strconv.Atoi(jobVals["maxAttempts"].(string))
			if maxAttErr != nil {
				return nil, fmt.Errorf("invalid max attempt field for job %s: %w", messageJob.ID, maxAttErr)
			}
			creatP, creatPErr := time.Parse(time.RFC3339Nano, jobVals["createdAt"].(string))
			if creatPErr != nil {
				return nil, fmt.Errorf("invalid created at field for job %s: %w", messageJob.ID, creatPErr)
			}
			avaiP, avaiPErr := time.Parse(time.RFC3339Nano, jobVals["availableAt"].(string))
			if avaiPErr != nil {
				return nil, fmt.Errorf("invalid available at field for job %s: %w", messageJob.ID, avaiPErr)
			}
			task, taskOk := jobVals["task"].(string)
			if !taskOk {
				return nil, fmt.Errorf("invalid task field for job %s", messageJob.ID)
			}
			pl, plOk := jobVals["payload"].(string)
			if !plOk {
				return nil, fmt.Errorf("invalid payload field for job %s", messageJob.ID)
			}
			tId, tIdOk := jobVals["traceID"].(string)
			if !tIdOk {
				return nil, fmt.Errorf("invalid trace id field for job %s", messageJob.ID)
			}

			j := queue.Job{
				ID:          queue.JobID(messageJob.ID),
				Task:        task,
				Payload:     json.RawMessage(pl),
				Attempt:     att,
				MaxAttempts: maxAtt,
				TraceID:     tId,
				CreatedAt:   creatP,
				AvailableAt: avaiP,
			}

			jobs = append(jobs, j)
		}
	}

	return jobs, nil
}

func (b *Backend) Ack(ctx context.Context, jobID queue.JobID) error {
	// TODO: Use XACK after a handler succeeds.
	_, err := b.rds.XAck(ctx, b.config.Stream, b.config.Group, string(jobID)).Result()
	return err
}

func (b *Backend) Retry(ctx context.Context, job queue.Job, nextRunAt time.Time) error {
	// TODO: Increment attempt metadata and schedule for nextRunAt.
	// A Redis sorted set keyed by Unix milliseconds is a simple v1 option.
	return queue.ErrNotImplemented
}

func (b *Backend) DeadLetter(ctx context.Context, job queue.Job, reason string) error {
	// TODO: Move job details plus reason into b.config.DeadLetter.
	return queue.ErrNotImplemented
}

func (b *Backend) RecoverExpired(ctx context.Context, visibilityTimeout time.Duration) (int, error) {
	// TODO: Use XPENDING/XAUTOCLAIM so jobs held by dead workers become claimable again.
	return 0, queue.ErrNotImplemented
}

func (b *Backend) Stats(ctx context.Context) (queue.Stats, error) {
	// TODO: Use XLEN, XPENDING, and retry/DLQ lengths.
	return queue.Stats{}, queue.ErrNotImplemented
}
