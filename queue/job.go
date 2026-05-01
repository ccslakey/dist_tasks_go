package queue

import (
	"encoding/json"
	"time"
)

type JobID string

type Job struct {
	ID          JobID           `json:"id"`           // stream entry ID assigned by Redis on XADD
	Task        string          `json:"task"`         // stored/retrieved as a plain string field
	Payload     json.RawMessage `json:"payload"`      // stored as string(payload); cast back with json.RawMessage(s)
	Attempt     int             `json:"attempt"`      // stored as a decimal string; parse with strconv.Atoi
	MaxAttempts int             `json:"max_attempts"` // stored as a decimal string; parse with strconv.Atoi
	TraceID     string          `json:"trace_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`   // store/parse as time.RFC3339Nano
	AvailableAt time.Time       `json:"available_at"` // store/parse as time.RFC3339Nano
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

func (j Job) Exhausted() bool {
	return j.Attempt >= j.MaxAttempts
}
