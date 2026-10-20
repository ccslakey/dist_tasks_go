package queue

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

type JobID string

// NewJobID returns a random 128-bit job ID. Backends assign one at enqueue
// time; it stays stable across retries and redeliveries, so handlers can use
// it as an idempotency key.
func NewJobID() JobID {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return JobID(hex.EncodeToString(b[:]))
}

type Job struct {
	ID          JobID           `json:"id"`                     // stable logical ID, assigned once at enqueue
	Task        string          `json:"task"`                   // stored/retrieved as a plain string field
	Payload     json.RawMessage `json:"payload"`                // stored as string(payload); cast back with json.RawMessage(s)
	Attempt     int             `json:"attempt"`                // number of previous failed attempts (0 on the first run)
	MaxAttempts int             `json:"max_attempts"`           // total runs allowed before the job is dead-lettered
	TraceParent string          `json:"trace_parent,omitempty"` // W3C traceparent of the enqueue span
	CreatedAt   time.Time       `json:"created_at"`             // store/parse as time.RFC3339Nano
	AvailableAt time.Time       `json:"available_at"`           // store/parse as time.RFC3339Nano

	// IdempotencyKey, if set at enqueue, makes the enqueue idempotent: another
	// enqueue with the same key inside the backend's dedupe window returns the
	// original job's ID instead of creating a second job.
	IdempotencyKey string `json:"-"`

	// Lease is set by the backend when the job is claimed or recovered. It is
	// not part of the job's stored state.
	Lease Lease `json:"-"`
}

// FencingToken orders every run of a job: it increases with each retry and
// each redelivery, so a run that holds an older token has been superseded.
// Pass it to downstream systems along with job.ID, and have them reject
// writes carrying a lower token than the highest they've seen for that job.
// That stops a paused "zombie" worker from applying a stale side effect
// after its lease was taken over, which the queue alone can't prevent.
func (j Job) FencingToken() int64 {
	return int64(j.Attempt)<<32 | j.Lease.Token
}

// Lease identifies one delivery of a job to one worker. Backends use it to
// fence Extend/Ack/Retry/DeadLetter: once another worker has taken the job
// over, calls made with the old lease fail with ErrLeaseLost instead of
// changing the job's state.
type Lease struct {
	MessageID string // backend delivery handle (the Redis stream entry ID)
	Owner     string // worker ID holding the lease
	Token     int64  // fencing token; increases every time the job is redelivered
	// Partition identifies which partition of a sharded backend holds the
	// delivery, so Ack and friends can be routed back to it. 0 if unsharded.
	Partition int
}

type EnqueueOptions struct {
	MaxAttempts int
	Delay       time.Duration
	// IdempotencyKey dedupes retried enqueues; see Job.IdempotencyKey.
	IdempotencyKey string
}

type JobResult struct {
	Retryable bool
	Err       error
}

func NewRetryableError(err error) JobResult {
	return JobResult{Retryable: true, Err: err}
}

func NewPermanentError(err error) JobResult {
	return JobResult{Retryable: false, Err: err}
}

func (j Job) Validate() error {
	if j.Task == "" || len(j.Payload) == 0 {
		return ErrInvalidJob
	}
	if j.MaxAttempts <= 0 {
		return ErrInvalidJob
	}
	return nil
}

// Exhausted reports whether the current run is the job's last allowed
// attempt, so a failure now should dead-letter rather than retry.
func (j Job) Exhausted() bool {
	return j.Attempt+1 >= j.MaxAttempts
}
