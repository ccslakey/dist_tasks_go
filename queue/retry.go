package queue

import "time"

type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		BaseDelay: time.Second,
		MaxDelay:  30 * time.Second,
	}
}

func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	// TODO: Implement exponential backoff.
	// - Treat attempt <= 0 as attempt 1.
	// - Fall back to BaseDelay=1s and MaxDelay=30s if the policy fields are zero.
	// - Delay for attempt N is BaseDelay * 2^(N-1), capped at MaxDelay.
	return 0
}

func ShouldDeadLetter(job Job) bool {
	return job.Exhausted()
}
