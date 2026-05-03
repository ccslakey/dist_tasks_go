package redisstream

import (
	"context"
	"dist_tasks_go/queue"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
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

type Store struct {
	config Config
	rds    *redis.Client
}

func New(config Config) (*Store, error) {
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
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, err
	}

	return &Store{config: config, rds: rdsCli}, nil
}

func (b *Store) Enqueue(ctx context.Context, job queue.Job) (queue.JobID, error) {
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

func (b *Store) Claim(ctx context.Context, workerID string, limit int) ([]queue.Job, error) {
	// Promote any due retries first so they're visible to this XREADGROUP call.
	b.promoteRetries(ctx, 100)

	args := redis.XReadGroupArgs{
		Group:    b.config.Group,
		Consumer: workerID,
		Count:    int64(limit),
		Streams:  []string{b.config.Stream, ">"},
		Block:    -1, // non-blocking; go-redis treats 0 as BLOCK 0 (block forever)
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

func (b *Store) Ack(ctx context.Context, jobID queue.JobID) error {
	// XACK after a handler succeeds.
	_, err := b.rds.XAck(ctx, b.config.Stream, b.config.Group, string(jobID)).Result()
	return err
}

func (b *Store) Retry(ctx context.Context, job queue.Job, nextRunAt time.Time) error {
	// Schedule the job for a future retry using a Redis sorted set.
	// - Increment job.Attempt before storing so the next execution reflects the right attempt number.
	job.Attempt++

	// - Serialize the updated job to JSON (json.Marshal).
	jobBytes, err := json.Marshal(job)
	if err != nil {
		return err
	}
	// - Use ZADD on b.config.RetryKey with score = nextRunAt.UnixMilli() and member = serialized job.
	_, err = b.rds.ZAdd(ctx, b.config.RetryKey, redis.Z{
		Score:  float64(nextRunAt.UnixMilli()),
		Member: jobBytes,
	}).Result()

	if err != nil {
		return err
	}
	// The sorted set acts as a delay queue: jobs become "due" once the current time passes their score.
	// Claim will later promote due entries back into the main stream (see promoteRetriesScript).
	return b.Ack(ctx, job.ID)
}

func (b *Store) DeadLetter(ctx context.Context, job queue.Job, reason string) error {
	//  Permanently record the failed job in the dead-letter stream.
	// - Use XADD on b.config.DeadLetter (a separate stream from the main one).
	// - Store the same fields as Enqueue, plus a "reason" field containing the failure message.
	args := redis.XAddArgs{
		Stream: b.config.DeadLetter,
		Values: map[string]interface{}{
			"task":        job.Task,
			"payload":     string(job.Payload),
			"attempt":     job.Attempt,
			"maxAttempts": job.MaxAttempts,
			"traceID":     job.TraceID,
			"createdAt":   job.CreatedAt.Format(time.RFC3339Nano),
			"availableAt": job.AvailableAt.Format(time.RFC3339Nano),
			"reason":      reason,
		},
	}
	_, err := b.rds.XAdd(ctx, &args).Result()
	if err != nil {
		return err
	}

	return b.Ack(ctx, job.ID)
}

func (b *Store) RecoverExpired(ctx context.Context, visibilityTimeout time.Duration, workerID string) (int, error) {
	// Use XPENDING/XAUTOCLAIM so jobs held by dead workers become claimable again.
	claimArgs := redis.XAutoClaimArgs{
		Stream:   b.config.Stream,
		MinIdle:  visibilityTimeout,
		Group:    b.config.Group,
		Start:    "0-0",
		Consumer: workerID,
	}

	val, _, err := b.rds.XAutoClaim(ctx, &claimArgs).Result()
	if err != nil {
		return 0, err
	}

	return len(val), err
}

// promoteRetries moves any retry-sorted-set entries whose score (next-run time
// in Unix ms) is <= now back into the main stream so they can be claimed again.
// Returns the number of jobs promoted.
//
// Steps:
//  1. ZRANGEBYSCORE b.config.RetryKey -inf <now-ms> LIMIT 0 limit
//  2. For each due entry, XADD it back onto b.config.Stream
//  3. ZREM the promoted entries from the sorted set
func (b *Store) promoteRetries(ctx context.Context, limit int) (int, error) {
	nowMs := time.Now().UTC().UnixMilli()
	entries, err := b.rds.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key:     b.config.RetryKey,
		Start:   "-inf",
		Stop:    strconv.FormatInt(nowMs, 10),
		Offset:  0,
		Count:   int64(limit),
		ByScore: true,
	}).Result()
	if err != nil {
		return 0, err
	}

	for _, val := range entries {
		var job queue.Job
		if err := json.Unmarshal([]byte(val.Member.(string)), &job); err != nil {
			return 0, err
		}
		vals := map[string]any{
			"task":        job.Task,
			"payload":     string(job.Payload),
			"attempt":     job.Attempt,
			"maxAttempts": job.MaxAttempts,
			"traceID":     job.TraceID,
			"createdAt":   job.CreatedAt.Format(time.RFC3339Nano),
			"availableAt": job.AvailableAt.Format(time.RFC3339Nano),
		}
		_, err = b.rds.XAdd(ctx, &redis.XAddArgs{
			Stream: b.config.Stream,
			Values: vals,
		}).Result()
		if err != nil {
			return 0, err
		}

		_, err = b.rds.ZRem(ctx, b.config.RetryKey, val.Member).Result()
		if err != nil {
			return 0, err
		}
	}

	return len(entries), nil
}

func (b *Store) Stats(ctx context.Context) (queue.Stats, error) {

	streamLen, err := b.rds.XLen(ctx, b.config.Stream).Result()
	if err != nil {
		return queue.Stats{}, err
	}
	pend, err := b.rds.XPending(ctx, b.config.Stream, b.config.Group).Result()
	if err != nil {
		return queue.Stats{}, err
	}
	deadLen, err := b.rds.XLen(ctx, b.config.DeadLetter).Result()
	if err != nil {
		return queue.Stats{}, err
	}
	retries, err := b.rds.ZCard(ctx, b.config.RetryKey).Result()
	if err != nil {
		return queue.Stats{}, err
	}

	return queue.Stats{
		QueueDepth:        streamLen,
		InFlight:          pend.Count,
		DeadLetterDepth:   deadLen,
		RetryScheduled:    retries,
		TotalEnqueued:     0, //from metrics/counter store
		TotalCompleted:    0, //
		TotalFailed:       0, //
		TotalRetried:      0, //
		TotalDeadLettered: 0, //
	}, nil
}
