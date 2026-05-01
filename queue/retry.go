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

// Implement exponential backoff.
func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	// - Treat attempt <= 0 as attempt 1.
	if attempt < 1 {
		attempt = 1
	}

	// - Fall back to BaseDelay=1s and MaxDelay=30s if the policy fields are zero.
	if p.BaseDelay == 0 {
		p.BaseDelay = 1 * time.Second
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = 30 * time.Second
	}

	// - Delay for attempt N is BaseDelay * 2^(N-1), capped at MaxDelay.
	calcDelay := p.BaseDelay
	for i := 1; i < attempt; i++ {
		calcDelay *= 2
		if calcDelay > p.MaxDelay {
			return p.MaxDelay
		}
	}

	return calcDelay
}

func ShouldDeadLetter(job Job) bool {
	return job.Exhausted()
}
