package prommetrics

import (
	"testing"

	"dist_tasks_go/queue"

	"github.com/prometheus/client_golang/prometheus"
)

// Compile-time check that *Metrics satisfies queue.Metrics.
var _ queue.Metrics = (*Metrics)(nil)

func TestNewRegistersWithoutPanic(t *testing.T) {
	// Use a fresh registry so this test doesn't pollute the default one.
	_ = New(prometheus.NewRegistry())
}

// pendingStarts counts entries currently sitting in the startTimes map.
// Each JobStarted call adds one; each terminal callback (Completed / Retried /
// DeadLettered) should remove the corresponding one. Anything else is a leak.
func pendingStarts(m *Metrics) int {
	count := 0
	m.startTimes.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}

func newMetrics() *Metrics {
	return New(prometheus.NewRegistry())
}

// Success path: Started → Completed should leave the map empty.
func TestStartTimesCleanedUpOnCompleted(t *testing.T) {
	m := newMetrics()
	job := queue.Job{ID: "job-1", Task: "email"}

	m.JobStarted(t.Context(), job)
	if got := pendingStarts(m); got != 1 {
		t.Fatalf("after JobStarted: %d entries, want 1", got)
	}

	m.JobCompleted(t.Context(), job)
	if got := pendingStarts(m); got != 0 {
		t.Errorf("after JobCompleted: %d entries leaked, want 0", got)
	}
}

// Retryable failure path: Started → Failed → Retried should leave the map empty.
// Currently leaks because neither JobFailed nor JobRetried calls LoadAndDelete.
func TestStartTimesCleanedUpOnRetried(t *testing.T) {
	m := newMetrics()
	job := queue.Job{ID: "job-2", Task: "email"}

	m.JobStarted(t.Context(), job)
	m.JobFailed(t.Context(), job, true)
	m.JobRetried(t.Context(), job)

	if got := pendingStarts(m); got != 0 {
		t.Errorf("after JobFailed+JobRetried: %d entries leaked, want 0", got)
	}
}

// Permanent failure path: Started → Failed → DeadLettered should leave the map empty.
// Currently leaks for the same reason.
func TestStartTimesCleanedUpOnDeadLettered(t *testing.T) {
	m := newMetrics()
	job := queue.Job{ID: "job-3", Task: "email"}

	m.JobStarted(t.Context(), job)
	m.JobFailed(t.Context(), job, false)
	m.JobDeadLettered(t.Context(), job)

	if got := pendingStarts(m); got != 0 {
		t.Errorf("after JobFailed+JobDeadLettered: %d entries leaked, want 0", got)
	}
}

// Worst case: a worker that crashes (or a buggy handler path) calls JobStarted
// many times without ever reaching a terminal callback. The map will grow
// unboundedly. This test documents the failure mode — there is no automatic
// cleanup. A future fix could add a TTL sweep or expose a max-age cap.
func TestStartTimesGrowWithoutTerminalCallback(t *testing.T) {
	m := newMetrics()

	const n = 1000
	for i := 0; i < n; i++ {
		m.JobStarted(t.Context(), queue.Job{ID: queue.JobID(string(rune(i))), Task: "email"})
	}

	if got := pendingStarts(m); got != n {
		t.Errorf("after %d JobStarted with no terminal callback: %d entries, want %d (this test exists to make the leak visible)", n, got, n)
	}
}
