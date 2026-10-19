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

func TestRetryPolicyJitterStaysInRange(t *testing.T) {
	policy := RetryPolicy{BaseDelay: time.Second, MaxDelay: 10 * time.Second, Jitter: 0.5}
	seen := map[time.Duration]bool{}
	for range 100 {
		got := policy.NextDelay(3) // 4s before jitter
		if got < 2*time.Second || got >= 4*time.Second {
			t.Fatalf("jittered delay %s outside [2s, 4s)", got)
		}
		seen[got] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct delays in 100 draws; jitter isn't spreading retries", len(seen))
	}
}

func TestFencingTokenIncreasesAcrossRetriesAndRedeliveries(t *testing.T) {
	runs := []Job{
		{Attempt: 0, Lease: Lease{Token: 1}},
		{Attempt: 0, Lease: Lease{Token: 2}}, // redelivered after a crash
		{Attempt: 1, Lease: Lease{Token: 1}}, // retried: new stream entry, delivery count restarts
		{Attempt: 1, Lease: Lease{Token: 7}},
		{Attempt: 2, Lease: Lease{Token: 1}},
	}
	for i := 1; i < len(runs); i++ {
		if runs[i].FencingToken() <= runs[i-1].FencingToken() {
			t.Errorf("run %d token %d not greater than run %d token %d", i, runs[i].FencingToken(), i-1, runs[i-1].FencingToken())
		}
	}
}
