package queue

import (
	"math/rand/v2"
	"time"
)

type RetryPolicy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	// Jitter is the fraction of each delay (0 to 1) that is randomized, so
	// jobs that failed together (say, during a downstream outage) don't all
	// retry at the same instant. 0.5 means the delay falls in [d/2, d).
	Jitter float64
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		BaseDelay: time.Second,
		MaxDelay:  30 * time.Second,
		Jitter:    0.5,
	}
}

// NextDelay returns the backoff before retry number attempt (1-based):
// BaseDelay * 2^(attempt-1), capped at MaxDelay, with the last Jitter
// fraction of it randomized.
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

	delay := p.BaseDelay
	for i := 1; i < attempt && delay < p.MaxDelay; i++ {
		delay *= 2
	}
	delay = min(delay, p.MaxDelay)

	if j := min(max(p.Jitter, 0), 1); j > 0 {
		spread := float64(delay) * j
		delay = time.Duration(float64(delay) - spread + rand.Float64()*spread)
	}
	return delay
}

// ShouldDeadLetter reports whether a failed job has used all its attempts.
func ShouldDeadLetter(job Job) bool {
	return job.Exhausted()
}
