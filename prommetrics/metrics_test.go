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
