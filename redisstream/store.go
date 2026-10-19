package redisstream

import (
	"context"
	"dist_tasks_go/queue"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	Addr       string
	Stream     string
	Group      string
	RetryKey   string // sorted set of scheduled job IDs (retries and delayed jobs), scored by run-at unix ms
	DeadLetter string
	// DedupePrefix and DedupeWindow control enqueue idempotency keys: a key is
	// remembered at DedupePrefix+key for DedupeWindow. Defaults: "dtq:dedupe:", 24h.
	DedupePrefix string
	DedupeWindow time.Duration
	// ConsumerIdleTimeout is how long a consumer can hold nothing and go
	// without reading before it's removed from the group. Defaults to 1h.
	ConsumerIdleTimeout time.Duration
}

type Store struct {
	config Config
	rds    *redis.Client
	// retryData holds the fields of each scheduled job, keyed by job ID.
	retryData string

	// recoverCursor is where the next RecoverExpired scan resumes in the PEL.
	mu            sync.Mutex
	recoverCursor string
	lastPrune     time.Time
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
	if config.DedupePrefix == "" {
		config.DedupePrefix = "dtq:dedupe:"
	}
	if config.DedupeWindow <= 0 {
		config.DedupeWindow = 24 * time.Hour
	}
	if config.ConsumerIdleTimeout <= 0 {
		config.ConsumerIdleTimeout = time.Hour
	}
	rdsCli := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: "", // no password
		DB:       0,  // use default DB
		Protocol: 2,
	})

	// "0" rather than "$" so entries added before the group existed still get processed.
	_, err := rdsCli.XGroupCreateMkStream(context.Background(), config.Stream, config.Group, "0").Result()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, err
	}

	return &Store{
		config:        config,
		rds:           rdsCli,
		retryData:     config.RetryKey + ":jobs",
		recoverCursor: "0-0",
	}, nil
}

func (b *Store) Close() error {
	return b.rds.Close()
}

// jobFields is how a job is stored in the stream and in the retry schedule.
func jobFields(job queue.Job) map[string]string {
	return map[string]string{
		"id":          string(job.ID),
		"task":        job.Task,
		"payload":     string(job.Payload),
		"attempt":     strconv.Itoa(job.Attempt),
		"maxAttempts": strconv.Itoa(job.MaxAttempts),
		"traceID":     job.TraceID,
		"createdAt":   job.CreatedAt.Format(time.RFC3339Nano),
		"availableAt": job.AvailableAt.Format(time.RFC3339Nano),
	}
}

func (b *Store) Enqueue(ctx context.Context, job queue.Job) (queue.JobID, error) {
	if job.ID == "" {
		job.ID = queue.NewJobID()
	}
	if job.IdempotencyKey != "" {
		return b.enqueueOnce(ctx, job)
	}

	// Delayed jobs wait in the retry schedule until they're due.
	if job.AvailableAt.After(time.Now()) {
		if err := b.schedule(ctx, job, job.AvailableAt); err != nil {
			return "", err
		}
		return job.ID, nil
	}

	err := b.rds.XAdd(ctx, &redis.XAddArgs{
		Stream: b.config.Stream,
		Values: jobFields(job),
	}).Err()
	if err != nil {
		return "", err
	}
	return job.ID, nil
}

func (b *Store) enqueueOnce(ctx context.Context, job queue.Job) (queue.JobID, error) {
	data, err := json.Marshal(jobFields(job))
	if err != nil {
		return "", err
	}
	var runAt int64
	if job.AvailableAt.After(time.Now()) {
		runAt = job.AvailableAt.UnixMilli()
	}
	id, err := enqueueOnceScript.Run(ctx, b.rds,
		[]string{b.config.DedupePrefix + job.IdempotencyKey, b.config.Stream, b.config.RetryKey, b.retryData},
		string(job.ID), b.config.DedupeWindow.Milliseconds(), runAt, data).Text()
	return queue.JobID(id), err
}

func (b *Store) schedule(ctx context.Context, job queue.Job, runAt time.Time) error {
	data, err := json.Marshal(jobFields(job))
	if err != nil {
		return err
	}
	_, err = b.rds.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HSet(ctx, b.retryData, string(job.ID), data)
		p.ZAdd(ctx, b.config.RetryKey, redis.Z{Score: float64(runAt.UnixMilli()), Member: string(job.ID)})
		return nil
	})
	return err
}

func (b *Store) Claim(ctx context.Context, workerID string, limit int) ([]queue.Job, error) {
	// Promote any due retries first so they're visible to this XREADGROUP call.
	if _, err := b.promoteRetries(ctx, 100); err != nil {
		return nil, fmt.Errorf("promote retries: %w", err)
	}

	streams, err := b.rds.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    b.config.Group,
		Consumer: workerID,
		Count:    int64(limit),
		Streams:  []string{b.config.Stream, ">"},
		Block:    -1, // non-blocking; go-redis treats 0 as BLOCK 0 (block forever)
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("xreadgroup for %s: %w", workerID, err)
	}

	var jobs []queue.Job
	for _, s := range streams {
		for _, msg := range s.Messages {
			// A fresh delivery always has delivery count 1.
			job, ok := b.parseOrQuarantine(ctx, msg.ID, msg.Values, queue.Lease{MessageID: msg.ID, Owner: workerID, Token: 1})
			if ok {
				jobs = append(jobs, job)
			}
		}
	}
	return jobs, nil
}

// parseOrQuarantine parses a delivered entry. An entry that can't be parsed
// would fail the same way on every redelivery, so it goes straight to the
// dead-letter stream with its raw fields.
func (b *Store) parseOrQuarantine(ctx context.Context, msgID string, vals map[string]any, lease queue.Lease) (queue.Job, bool) {
	job, err := parseJob(msgID, vals, lease)
	if err == nil {
		return job, true
	}
	pairs := []any{"reason", "malformed entry: " + err.Error()}
	for k, v := range vals {
		if k != "reason" {
			pairs = append(pairs, k, v)
		}
	}
	if err := b.fenced(ctx, deadLetterScript, []string{b.config.Stream, b.config.DeadLetter}, queue.Job{Lease: lease}, pairs...); err != nil {
		slog.Warn("dead-lettering malformed entry failed", "message_id", msgID, "err", err)
	}
	return job, false
}

func parseJob(msgID string, vals map[string]any, lease queue.Lease) (queue.Job, error) {
	str := func(name string) (string, error) {
		v, ok := vals[name].(string)
		if !ok {
			return "", fmt.Errorf("invalid %s field for message %s", name, msgID)
		}
		return v, nil
	}
	num := func(name string) (int, error) {
		s, err := str(name)
		if err != nil {
			return 0, err
		}
		return strconv.Atoi(s)
	}
	ts := func(name string) (time.Time, error) {
		s, err := str(name)
		if err != nil {
			return time.Time{}, err
		}
		return time.Parse(time.RFC3339Nano, s)
	}

	job := queue.Job{Lease: lease}
	var err error
	if job.Task, err = str("task"); err != nil {
		return job, err
	}
	var payload string
	if payload, err = str("payload"); err != nil {
		return job, err
	}
	job.Payload = json.RawMessage(payload)
	if job.Attempt, err = num("attempt"); err != nil {
		return job, err
	}
	if job.MaxAttempts, err = num("maxAttempts"); err != nil {
		return job, err
	}
	if job.TraceID, err = str("traceID"); err != nil {
		return job, err
	}
	if job.CreatedAt, err = ts("createdAt"); err != nil {
		return job, err
	}
	if job.AvailableAt, err = ts("availableAt"); err != nil {
		return job, err
	}
	// Entries written before jobs had stable IDs fall back to the entry ID.
	job.ID = queue.JobID(msgID)
	if id, ok := vals["id"].(string); ok && id != "" {
		job.ID = queue.JobID(id)
	}
	return job, nil
}

// fenced runs a lease-checked script and maps "not the owner" to ErrLeaseLost.
func (b *Store) fenced(ctx context.Context, script *redis.Script, keys []string, job queue.Job, extra ...any) error {
	args := append([]any{b.config.Group, job.Lease.MessageID, job.Lease.Owner, job.Lease.Token}, extra...)
	n, err := script.Run(ctx, b.rds, keys, args...).Int()
	if err != nil {
		return err
	}
	if n == 0 {
		return queue.ErrLeaseLost
	}
	return nil
}

func (b *Store) Ack(ctx context.Context, job queue.Job) error {
	return b.fenced(ctx, ackScript, []string{b.config.Stream}, job)
}

func (b *Store) Extend(ctx context.Context, job queue.Job) error {
	return b.fenced(ctx, extendScript, []string{b.config.Stream}, job)
}

func (b *Store) Retry(ctx context.Context, job queue.Job, nextRunAt time.Time) error {
	job.Attempt++
	data, err := json.Marshal(jobFields(job))
	if err != nil {
		return err
	}
	return b.fenced(ctx, retryScript,
		[]string{b.config.Stream, b.config.RetryKey, b.retryData},
		job, nextRunAt.UnixMilli(), string(job.ID), data)
}

func (b *Store) DeadLetter(ctx context.Context, job queue.Job, reason string) error {
	fields := jobFields(job)
	fields["reason"] = reason
	fields["deliveries"] = strconv.FormatInt(job.Lease.Token, 10)
	fields["failedAt"] = time.Now().UTC().Format(time.RFC3339Nano)
	var pairs []any
	for k, v := range fields {
		pairs = append(pairs, k, v)
	}
	return b.fenced(ctx, deadLetterScript, []string{b.config.Stream, b.config.DeadLetter}, job, pairs...)
}

func (b *Store) RecoverExpired(ctx context.Context, visibilityTimeout time.Duration, workerID string, limit int) ([]queue.Job, error) {
	if limit <= 0 {
		return nil, nil
	}
	b.mu.Lock()
	cursor := b.recoverCursor
	prune := time.Since(b.lastPrune) >= min(time.Minute, b.config.ConsumerIdleTimeout/2)
	if prune {
		b.lastPrune = time.Now()
	}
	b.mu.Unlock()

	if prune {
		if err := b.pruneConsumers(ctx); err != nil {
			slog.Warn("pruning idle consumers failed", "err", err)
		}
	}

	res, err := recoverScript.Run(ctx, b.rds, []string{b.config.Stream},
		b.config.Group, workerID, visibilityTimeout.Milliseconds(), limit, cursor).Slice()
	if err != nil {
		return nil, err
	}
	if len(res) != 2 {
		return nil, fmt.Errorf("recover: unexpected reply %v", res)
	}

	if next, ok := res[0].(string); ok {
		b.mu.Lock()
		b.recoverCursor = next
		b.mu.Unlock()
	}

	entries, _ := res[1].([]any)
	jobs := make([]queue.Job, 0, len(entries))
	for _, e := range entries {
		entry, ok := e.([]any)
		if !ok || len(entry) != 3 {
			return nil, fmt.Errorf("recover: unexpected entry %v", e)
		}
		id, _ := entry[0].(string)
		flat, _ := entry[1].([]any)
		token, _ := entry[2].(int64)
		vals := make(map[string]any, len(flat)/2)
		for i := 0; i+1 < len(flat); i += 2 {
			k, _ := flat[i].(string)
			vals[k] = flat[i+1]
		}
		if job, ok := b.parseOrQuarantine(ctx, id, vals, queue.Lease{MessageID: id, Owner: workerID, Token: token}); ok {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

// Deregister removes workerID's consumer from the group, unless it still
// holds pending entries (those must stay recoverable). Workers call it on a
// clean shutdown.
func (b *Store) Deregister(ctx context.Context, workerID string) error {
	return deregisterScript.Run(ctx, b.rds, []string{b.config.Stream}, b.config.Group, workerID).Err()
}

// pruneConsumers removes consumers left behind by workers that crashed.
func (b *Store) pruneConsumers(ctx context.Context) error {
	return pruneConsumersScript.Run(ctx, b.rds, []string{b.config.Stream},
		b.config.Group, b.config.ConsumerIdleTimeout.Milliseconds()).Err()
}

// promoteRetries atomically moves schedule entries that are due back onto the
// main stream so they can be claimed again. Returns the number promoted.
func (b *Store) promoteRetries(ctx context.Context, limit int) (int, error) {
	return promoteScript.Run(ctx, b.rds,
		[]string{b.config.RetryKey, b.retryData, b.config.Stream},
		time.Now().UnixMilli(), limit).Int()
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
		StreamLength:    streamLen,
		InFlight:        pend.Count,
		DeadLetterDepth: deadLen,
		RetryScheduled:  retries,
	}, nil
}
