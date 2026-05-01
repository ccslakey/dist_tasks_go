package queue

import (
	"encoding/json"
	"time"
)

type JobID string

type Job struct {
	ID          JobID           `json:"id"`
	Task        string          `json:"task"`
	Payload     json.RawMessage `json:"payload"`
	Attempt     int             `json:"attempt"`
	MaxAttempts int             `json:"max_attempts"`
	TraceID     string          `json:"trace_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	AvailableAt time.Time       `json:"available_at"`
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
