package queue

import (
	"testing"
	"time"
)

func TestRetryPolicyNextDelay(t *testing.T) {
	policy := RetryPolicy{BaseDelay: time.Second, MaxDelay: 10 * time.Second}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 5, want: 10 * time.Second},
	}

	for _, tt := range tests {
		got := policy.NextDelay(tt.attempt)
		if got != tt.want {
			t.Fatalf("attempt %d: got %s, want %s", tt.attempt, got, tt.want)
		}
	}
}

func TestShouldDeadLetter(t *testing.T) {
	job := Job{Attempt: 3, MaxAttempts: 3}
	if !ShouldDeadLetter(job) {
		t.Fatal("expected exhausted job to dead-letter")
	}
}
