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
	ID          JobID           `json:"id"`           // stable logical ID, assigned once at enqueue
	Task        string          `json:"task"`         // stored/retrieved as a plain string field
	Payload     json.RawMessage `json:"payload"`      // stored as string(payload); cast back with json.RawMessage(s)
	Attempt     int             `json:"attempt"`      // number of previous failed attempts (0 on the first run)
	MaxAttempts int             `json:"max_attempts"` // total runs allowed before the job is dead-lettered
	TraceID     string          `json:"trace_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`   // store/parse as time.RFC3339Nano
	AvailableAt time.Time       `json:"available_at"` // store/parse as time.RFC3339Nano

	// Lease is set by the backend when the job is claimed or recovered. It is
	// not part of the job's stored state.
	Lease Lease `json:"-"`
}

// Lease identifies one delivery of a job to one worker. Backends use it to
// fence Extend/Ack/Retry/DeadLetter: once another worker has taken the job
// over, calls made with the old lease fail with ErrLeaseLost instead of
// changing the job's state.
type Lease struct {
	MessageID string // backend delivery handle (the Redis stream entry ID)
	Owner     string // worker ID holding the lease
	Token     int64  // fencing token; increases every time the job is redelivered
}

type EnqueueOptions struct {
	MaxAttempts int
	Delay       time.Duration
	TraceID     string
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
