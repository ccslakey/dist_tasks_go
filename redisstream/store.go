package redisstream

import (
	"context"
	"dist_tasks_go/queue"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	config     Config
	rds        redis.UniversalClient
	ownsClient bool // Close closes rds only if New created it
	// blockRds runs blocking reads. It's rds unless Sharded gives its long
	// polls a client of their own, which it can close to cut them off.
	blockRds redis.UniversalClient
	// retryData holds the fields of each scheduled job, keyed by job ID.
	retryData string

	// recoverCursor is where the next RecoverExpired scan resumes in the PEL.
	mu            sync.Mutex
	recoverCursor string
	lastPrune     time.Time
}

// New connects to the Redis at config.Addr and returns a store for one queue.
func New(config Config) (*Store, error) {
	if config.Addr == "" {
		config.Addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{
		Addr:     config.Addr,
		Password: "", // no password
		DB:       0,  // use default DB
		Protocol: 2,
		// Lets claimBlocking's deadline cut off a blocking read that the
		// server fails to end on time.
		ContextTimeoutEnabled: true,
	})
	store, err := NewWithClient(client, config)
	if err != nil {
		client.Close()
		return nil, err
	}
	store.ownsClient = true
	return store, nil
}

// NewWithClient returns a store that uses an existing client, which may be a
// Redis Cluster client. config.Addr is ignored. Every key the store uses for
// one queue must hash to the same cluster slot, since its Lua scripts touch
// several at once; Sharded arranges that with hash tags.
func NewWithClient(client redis.UniversalClient, config Config) (*Store, error) {
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
	// "0" rather than "$" so entries added before the group existed still get processed.
	_, err := client.XGroupCreateMkStream(context.Background(), config.Stream, config.Group, "0").Result()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return nil, err
	}

	return &Store{
		config:        config,
		rds:           client,
		blockRds:      client,
		retryData:     config.RetryKey + ":jobs",
		recoverCursor: "0-0",
	}, nil
}

// Config returns the store's configuration, with defaults filled in.
func (b *Store) Config() Config { return b.config }

func (b *Store) Close() error {
	if !b.ownsClient {
		return nil
	}
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
		"traceparent": job.TraceParent,
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

// EnqueueBatch enqueues jobs in one round trip as a MULTI/EXEC transaction,
// so either all of them are stored or none are. Jobs with idempotency keys
// go through the same dedupe script as Enqueue.
func (b *Store) EnqueueBatch(ctx context.Context, jobs []queue.Job) ([]queue.JobID, error) {
	now := time.Now()
	results := make([]*redis.Cmd, len(jobs))
	_, err := b.rds.TxPipelined(ctx, func(p redis.Pipeliner) error {
		for i := range jobs {
			job := &jobs[i]
			if job.ID == "" {
				job.ID = queue.NewJobID()
			}
			fields := jobFields(*job)
			var runAt int64
			if job.AvailableAt.After(now) {
				runAt = job.AvailableAt.UnixMilli()
			}
			if job.IdempotencyKey == "" && runAt == 0 {
				p.XAdd(ctx, &redis.XAddArgs{Stream: b.config.Stream, Values: fields})
				continue
			}
			// Keyed and delayed jobs store their fields as JSON.
			data, err := json.Marshal(fields)
			if err != nil {
				return err
			}
			switch {
			case job.IdempotencyKey != "":
				// EVAL, not EVALSHA: Redis doesn't roll back the rest of an
				// EXEC when one command fails, so a NOSCRIPT here (script
				// cache flushed by a restart or failover) would commit the
				// other jobs and break all-or-nothing.
				results[i] = enqueueOnceScript.Eval(ctx, p,
					[]string{b.config.DedupePrefix + job.IdempotencyKey, b.config.Stream, b.config.RetryKey, b.retryData},
					string(job.ID), b.config.DedupeWindow.Milliseconds(), runAt, data)
			default:
				p.HSet(ctx, b.retryData, string(job.ID), data)
				p.ZAdd(ctx, b.config.RetryKey, redis.Z{Score: float64(runAt), Member: string(job.ID)})
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	ids := make([]queue.JobID, len(jobs))
	for i, job := range jobs {
		ids[i] = job.ID
		if results[i] != nil {
			id, err := results[i].Text()
			if err != nil {
				return nil, err
			}
			ids[i] = queue.JobID(id)
		}
	}
	return ids, nil
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

func (b *Store) Claim(ctx context.Context, workerID string, limit int, wait time.Duration) ([]queue.Job, error) {
	jobs, err := b.claimReady(ctx, workerID, limit)
	if err != nil || len(jobs) > 0 || wait <= 0 {
		return jobs, err
	}
	return b.claimBlocking(ctx, workerID, limit, wait)
}

// claimReady promotes due retries and claims what's ready, in one round trip.
func (b *Store) claimReady(ctx context.Context, workerID string, limit int) ([]queue.Job, error) {
	res, err := claimScript.Run(ctx, b.rds,
		[]string{b.config.Stream, b.config.RetryKey, b.retryData},
		b.config.Group, workerID, limit, time.Now().UnixMilli()).Slice()
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	var jobs []queue.Job
	for _, e := range res {
		entry, ok := e.([]any)
		if !ok || len(entry) != 2 {
			return nil, fmt.Errorf("claim: unexpected entry %v", e)
		}
		id, _ := entry[0].(string)
		// A fresh delivery always has delivery count 1.
		if job, ok := b.parseOrQuarantine(ctx, id, flatToMap(entry[1]), queue.Lease{MessageID: id, Owner: workerID, Token: 1}); ok {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

// blockGrace is how long past its BLOCK time a blocking read may run before
// the client gives up on it. Redis normally ends a block on time, so this only
// matters when it doesn't: a stalled network, or a server whose timers have
// stopped firing (seen on a long-running macOS Redis, which released blocked
// clients only when unrelated traffic happened to wake its event loop). The
// deadline is enforced only by clients with ContextTimeoutEnabled; others
// fall back to go-redis's own limit of BLOCK + 10s.
const blockGrace = 500 * time.Millisecond

// claimBlocking waits up to wait for new entries and claims them. It doesn't
// promote retries: XREADGROUP can't block inside a script.
//
// A read that outlives wait+blockGrace is abandoned and treated as finding
// nothing, so a stuck server can't hold up the caller. If Redis delivered
// entries just as the client gave up, they stay pending under workerID and
// are recovered after the visibility timeout, like any lost delivery.
func (b *Store) claimBlocking(ctx context.Context, workerID string, limit int, wait time.Duration) ([]queue.Job, error) {
	readCtx, cancel := context.WithTimeout(ctx, wait+blockGrace)
	defer cancel()
	streams, err := b.blockRds.XReadGroup(readCtx, &redis.XReadGroupArgs{
		Group:    b.config.Group,
		Consumer: workerID,
		Count:    int64(limit),
		Streams:  []string{b.config.Stream, ">"},
		Block:    wait,
	}).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		if ctx.Err() == nil && isTimeout(err) {
			b.warnStuckBlock(wait)
			return nil, nil
		}
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

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout())
}

// lastStuckWarn is when warnStuckBlock last logged (unix ns).
var lastStuckWarn atomic.Int64

// warnStuckBlock logs, at most once a minute per process, that Redis didn't
// end a blocking read on time.
func (b *Store) warnStuckBlock(wait time.Duration) {
	now := time.Now().UnixNano()
	last := lastStuckWarn.Load()
	if now-last < int64(time.Minute) || !lastStuckWarn.CompareAndSwap(last, now) {
		return
	}
	slog.Warn("Redis didn't end a blocking read on time; treating it as empty. If this keeps happening, the server's timers may be stuck: restart it",
		"stream", b.config.Stream, "block", wait, "gave_up_after", wait+blockGrace)
}

// flatToMap converts a script's [field, value, ...] reply into a map.
func flatToMap(v any) map[string]any {
	flat, _ := v.([]any)
	vals := make(map[string]any, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		k, _ := flat[i].(string)
		vals[k] = flat[i+1]
	}
	return vals
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
	job.TraceParent, _ = vals["traceparent"].(string) // optional
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

// AckBatch acks several jobs in one round trip. The result has one entry
// per job: nil, ErrLeaseLost, or the error that failed the whole batch.
func (b *Store) AckBatch(ctx context.Context, jobs []queue.Job) []error {
	return b.fencedBatch(ctx, ackBatchScript, jobs)
}

// ExtendBatch renews several leases in one round trip, with results like
// AckBatch.
func (b *Store) ExtendBatch(ctx context.Context, jobs []queue.Job) []error {
	return b.fencedBatch(ctx, extendBatchScript, jobs)
}

func (b *Store) fencedBatch(ctx context.Context, script *redis.Script, jobs []queue.Job) []error {
	args := []any{b.config.Group}
	for _, job := range jobs {
		args = append(args, job.Lease.MessageID, job.Lease.Owner, job.Lease.Token)
	}
	errs := make([]error, len(jobs))
	res, err := script.Run(ctx, b.rds, []string{b.config.Stream}, args...).Int64Slice()
	for i := range jobs {
		switch {
		case err != nil:
			errs[i] = err
		case i >= len(res) || res[i] == 0:
			errs[i] = queue.ErrLeaseLost
		}
	}
	return errs
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
		token, _ := entry[2].(int64)
		if job, ok := b.parseOrQuarantine(ctx, id, flatToMap(entry[1]), queue.Lease{MessageID: id, Owner: workerID, Token: token}); ok {
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

// Stats reads the queue's gauges in one pipelined round trip.
func (b *Store) Stats(ctx context.Context) (queue.Stats, error) {
	var (
		streamLen, deadLen *redis.IntCmd
		pend               *redis.XPendingCmd
		retries            *redis.IntCmd
	)
	_, err := b.rds.Pipelined(ctx, func(p redis.Pipeliner) error {
		streamLen = p.XLen(ctx, b.config.Stream)
		pend = p.XPending(ctx, b.config.Stream, b.config.Group)
		deadLen = p.XLen(ctx, b.config.DeadLetter)
		retries = p.ZCard(ctx, b.config.RetryKey)
		return nil
	})
	if err != nil {
		return queue.Stats{}, err
	}
	return queue.Stats{
		StreamLength:    streamLen.Val(),
		InFlight:        pend.Val().Count,
		DeadLetterDepth: deadLen.Val(),
		RetryScheduled:  retries.Val(),
	}, nil
}
