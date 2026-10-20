package prommetrics

import (
	"testing"
	"time"

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

// Runs that never reach a terminal callback (an ack that failed on a network
// error, a run abandoned at shutdown) must not leave their start time in the
// map forever.
func TestStaleStartTimesAreSwept(t *testing.T) {
	m := newMetrics()
	m.sweepEvery = 0 // sweep on every JobStarted; nothing is stale yet (1h)

	const n = 1000
	for i := 0; i < n; i++ {
		m.JobStarted(t.Context(), queue.Job{ID: queue.JobID(string(rune(i))), Task: "email"})
	}
	if got := pendingStarts(m); got != n {
		t.Fatalf("after %d JobStarted: %d entries, want %d", n, got, n)
	}

	m.staleAfter = 0 // now everything older than the next start is stale
	time.Sleep(time.Millisecond)
	m.JobStarted(t.Context(), queue.Job{ID: "fresh", Task: "email"})
	if got := pendingStarts(m); got != 1 {
		t.Errorf("after the stale entries aged out: %d entries, want only the fresh one", got)
	}
}

// The first lease lost for a task must be visible to rate(), which needs the
// series to exist (at zero) before the increment.
func TestOutcomeSeriesExistBeforeFirstIncrement(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.JobStarted(t.Context(), queue.Job{ID: "job-1", Task: "email"})

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, f := range families {
		if f.GetName() == "dtq_job_leases_lost_total" {
			return
		}
	}
	t.Error("dtq_job_leases_lost_total has no series for a task that has started jobs")
}
