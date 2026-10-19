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

// NextDelay returns the exponential backoff before retry number attempt
// (1-based): BaseDelay * 2^(attempt-1), capped at MaxDelay.
func (p RetryPolicy) NextDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if p.BaseDelay == 0 {
		p.BaseDelay = 1 * time.Second
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = 30 * time.Second
	}

	calcDelay := p.BaseDelay
	for i := 1; i < attempt; i++ {
		calcDelay *= 2
		if calcDelay > p.MaxDelay {
			return p.MaxDelay
		}
	}

	return calcDelay
}

// ShouldDeadLetter reports whether a failed job has used all its attempts.
func ShouldDeadLetter(job Job) bool {
	return job.Exhausted()
}
