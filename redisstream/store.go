package redisstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ccslakey/dist_tasks_go/queue"
)

// DefaultMaxLen caps the main stream. Redis streams keep entries after they are
// acknowledged, so without a cap the stream grows for the life of the process.
// The tilde form of MAXLEN lets Redis trim on whole-node boundaries, which is
// O(1) amortised instead of O(n).
const DefaultMaxLen int64 = 100_000

type Config struct {
	Addr       string
	Stream     string
	Group      string
	RetryKey   string
	DeadLetter string

	// MaxLen approximately caps the main stream. Zero uses DefaultMaxLen; a
	// negative value disables trimming.
	MaxLen int64
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
	if config.MaxLen == 0 {
		config.MaxLen = DefaultMaxLen
	}

	rds := redis.NewClient(&redis.Options{Addr: config.Addr})

	err := rds.XGroupCreateMkStream(context.Background(), config.Stream, config.Group, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		rds.Close()
		return nil, fmt.Errorf("create consumer group %q on %q: %w", config.Group, config.Stream, err)
	}

	return &Store{config: config, rds: rds}, nil
}

// Close releases the underlying Redis connection pool.
func (b *Store) Close() error { return b.rds.Close() }

// maxLen is the value to pass to XADD, where a negative config means "no trim".
func (b *Store) maxLen() int64 {
	if b.config.MaxLen < 0 {
		return 0
	}
	return b.config.MaxLen
}

func (b *Store) Enqueue(ctx context.Context, job queue.Job) (queue.JobID, error) {
	id, err := b.rds.XAdd(ctx, &redis.XAddArgs{
		Stream: b.config.Stream,
		MaxLen: b.maxLen(),
		Approx: true,
		Values: streamFields(job),
	}).Result()
	if err != nil {
		return "", fmt.Errorf("enqueue %q: %w", job.Task, err)
	}
	return queue.JobID(id), nil
}

func (b *Store) Claim(ctx context.Context, workerID string, limit int) ([]queue.Job, error) {
	if limit <= 0 {
		return nil, nil
	}

	// Promote due retries first so they are visible to the read below, and so a
	// retry that came due is picked up in the same poll rather than the next.
	if _, err := b.promoteRetries(ctx, limit); err != nil {
		return nil, err
	}

	streams, err := b.rds.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    b.config.Group,
		Consumer: workerID,
		Count:    int64(limit),
		Streams:  []string{b.config.Stream, ">"},
		Block:    -1, // non-blocking; go-redis treats 0 as BLOCK 0 (forever)
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim for worker %q: %w", workerID, err)
	}

	var jobs []queue.Job
	for _, s := range streams {
		jobs = append(jobs, b.decodeMessages(ctx, s.Messages)...)
	}
	return jobs, nil
}

func (b *Store) Ack(ctx context.Context, jobID queue.JobID) error {
	if err := b.rds.XAck(ctx, b.config.Stream, b.config.Group, string(jobID)).Err(); err != nil {
		return fmt.Errorf("ack %s: %w", jobID, err)
	}
	return nil
}

// Retry schedules a job to run again at nextRunAt.
//
// The sorted set is a delay queue: the score is the instant the job becomes
// eligible, and Claim promotes anything whose score has passed. The ZADD
// happens before the XACK on purpose. A crash between the two leaves the job
// both scheduled and pending, which costs one duplicate run; the reverse order
// would lose the job entirely.
func (b *Store) Retry(ctx context.Context, job queue.Job, nextRunAt time.Time) error {
	prevID := job.ID
	job.Attempt++

	member, err := json.Marshal(newRetryEntry(prevID, job))
	if err != nil {
		return fmt.Errorf("encode retry entry for %s: %w", prevID, err)
	}

	err = b.rds.ZAdd(ctx, b.config.RetryKey, redis.Z{
		Score:  float64(nextRunAt.UnixMilli()),
		Member: member,
	}).Err()
	if err != nil {
		return fmt.Errorf("schedule retry for %s: %w", prevID, err)
	}

	return b.Ack(ctx, prevID)
}

func (b *Store) DeadLetter(ctx context.Context, job queue.Job, reason string) error {
	fields := streamFields(job)
	fields["reason"] = reason

	err := b.rds.XAdd(ctx, &redis.XAddArgs{
		Stream: b.config.DeadLetter,
		Values: fields,
	}).Err()
	if err != nil {
		return fmt.Errorf("dead-letter %s: %w", job.ID, err)
	}

	return b.Ack(ctx, job.ID)
}

// RecoverExpired reclaims jobs whose lease expired and returns them so the
// caller can run them. XAUTOCLAIM moves the entries to this consumer, after
// which XREADGROUP with ">" will never return them again, so anything reclaimed
// here has to be handed back to the caller or it is stranded.
func (b *Store) RecoverExpired(ctx context.Context, visibilityTimeout time.Duration, workerID string, limit int) ([]queue.Job, error) {
	if limit <= 0 {
		return nil, nil
	}

	messages, _, err := b.rds.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   b.config.Stream,
		Group:    b.config.Group,
		Consumer: workerID,
		MinIdle:  visibilityTimeout,
		Start:    "0-0",
		Count:    int64(limit),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("recover expired for worker %q: %w", workerID, err)
	}

	return b.decodeMessages(ctx, messages), nil
}

func (b *Store) promoteRetries(ctx context.Context, limit int) (int, error) {
	n, err := promoteRetriesScript.Run(ctx, b.rds,
		[]string{b.config.RetryKey, b.config.Stream},
		time.Now().UTC().UnixMilli(),
		limit,
		b.maxLen(),
	).Int()
	if err != nil {
		return 0, fmt.Errorf("promote due retries: %w", err)
	}
	return n, nil
}

func (b *Store) Stats(ctx context.Context) (queue.Stats, error) {
	streamLen, err := b.rds.XLen(ctx, b.config.Stream).Result()
	if err != nil {
		return queue.Stats{}, fmt.Errorf("stats, stream length: %w", err)
	}
	pending, err := b.rds.XPending(ctx, b.config.Stream, b.config.Group).Result()
	if err != nil {
		return queue.Stats{}, fmt.Errorf("stats, pending: %w", err)
	}
	deadLen, err := b.rds.XLen(ctx, b.config.DeadLetter).Result()
	if err != nil {
		return queue.Stats{}, fmt.Errorf("stats, dead-letter length: %w", err)
	}
	scheduled, err := b.rds.ZCard(ctx, b.config.RetryKey).Result()
	if err != nil {
		return queue.Stats{}, fmt.Errorf("stats, retry schedule: %w", err)
	}

	return queue.Stats{
		StreamLength:    streamLen,
		InFlight:        pending.Count,
		DeadLetterDepth: deadLen,
		RetryScheduled:  scheduled,
	}, nil
}

// decodeMessages turns stream entries into jobs. An entry that cannot be
// decoded is dead-lettered and acked rather than returned: a malformed entry is
// a poison pill, and leaving it in the pending list means every reclaim hands
// it straight back.
func (b *Store) decodeMessages(ctx context.Context, messages []redis.XMessage) []queue.Job {
	jobs := make([]queue.Job, 0, len(messages))
	for _, msg := range messages {
		job, err := jobFromMessage(msg)
		if err != nil {
			b.quarantine(ctx, msg, err)
			continue
		}
		jobs = append(jobs, job)
	}
	return jobs
}

func (b *Store) quarantine(ctx context.Context, msg redis.XMessage, cause error) {
	fields := map[string]any{"reason": fmt.Sprintf("undecodable stream entry: %v", cause)}
	for k, v := range msg.Values {
		fields[k] = fmt.Sprint(v)
	}
	// Best effort. If either call fails the entry stays pending and is retried
	// on the next reclaim, which is the safe direction to fail in.
	_ = b.rds.XAdd(ctx, &redis.XAddArgs{Stream: b.config.DeadLetter, Values: fields}).Err()
	_ = b.Ack(ctx, queue.JobID(msg.ID))
}

// streamFields flattens a job into the field map stored on a stream entry.
// Everything is a string: Redis stream values are strings, and keeping the
// conversion in one place means Claim and the Lua promotion script agree on the
// format.
func streamFields(job queue.Job) map[string]any {
	return map[string]any{
		"task":        job.Task,
		"payload":     string(job.Payload),
		"attempt":     strconv.Itoa(job.Attempt),
		"maxAttempts": strconv.Itoa(job.MaxAttempts),
		"traceID":     job.TraceID,
		"createdAt":   job.CreatedAt.Format(time.RFC3339Nano),
		"availableAt": job.AvailableAt.Format(time.RFC3339Nano),
	}
}

// retryEntry is the sorted-set member for a scheduled retry. Every field is a
// string so the Lua promotion script can pass them to XADD untouched.
//
// PrevID is the stream entry the retry came from. It is not written back to the
// stream; it is there to keep members unique, since the sorted set dedupes by
// value and two distinct jobs with the same task, payload and timestamps would
// otherwise collide into one.
type retryEntry struct {
	PrevID      string `json:"prevID"`
	Task        string `json:"task"`
	Payload     string `json:"payload"`
	Attempt     string `json:"attempt"`
	MaxAttempts string `json:"maxAttempts"`
	TraceID     string `json:"traceID"`
	CreatedAt   string `json:"createdAt"`
	AvailableAt string `json:"availableAt"`
}

func newRetryEntry(prevID queue.JobID, job queue.Job) retryEntry {
	return retryEntry{
		PrevID:      string(prevID),
		Task:        job.Task,
		Payload:     string(job.Payload),
		Attempt:     strconv.Itoa(job.Attempt),
		MaxAttempts: strconv.Itoa(job.MaxAttempts),
		TraceID:     job.TraceID,
		CreatedAt:   job.CreatedAt.Format(time.RFC3339Nano),
		AvailableAt: job.AvailableAt.Format(time.RFC3339Nano),
	}
}

func jobFromMessage(msg redis.XMessage) (queue.Job, error) {
	task, err := field(msg, "task")
	if err != nil {
		return queue.Job{}, err
	}
	payload, err := field(msg, "payload")
	if err != nil {
		return queue.Job{}, err
	}
	attempt, err := intField(msg, "attempt")
	if err != nil {
		return queue.Job{}, err
	}
	maxAttempts, err := intField(msg, "maxAttempts")
	if err != nil {
		return queue.Job{}, err
	}
	createdAt, err := timeField(msg, "createdAt")
	if err != nil {
		return queue.Job{}, err
	}
	availableAt, err := timeField(msg, "availableAt")
	if err != nil {
		return queue.Job{}, err
	}
	traceID, _ := field(msg, "traceID") // optional

	return queue.Job{
		ID:          queue.JobID(msg.ID),
		Task:        task,
		Payload:     json.RawMessage(payload),
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		TraceID:     traceID,
		CreatedAt:   createdAt,
		AvailableAt: availableAt,
	}, nil
}

// field reads a stream field. The two-value type assertion matters: a missing
// key yields a nil interface, and the one-value form panics on it, which would
// take down the poll loop on a single malformed entry.
func field(msg redis.XMessage, name string) (string, error) {
	raw, ok := msg.Values[name]
	if !ok {
		return "", fmt.Errorf("entry %s: missing field %q", msg.ID, name)
	}
	s, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("entry %s: field %q is %T, want string", msg.ID, name, raw)
	}
	return s, nil
}

func intField(msg redis.XMessage, name string) (int, error) {
	s, err := field(msg, name)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("entry %s: field %q: %w", msg.ID, name, err)
	}
	return n, nil
}

func timeField(msg redis.XMessage, name string) (time.Time, error) {
	s, err := field(msg, name)
	if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("entry %s: field %q: %w", msg.ID, name, err)
	}
	return t, nil
}
