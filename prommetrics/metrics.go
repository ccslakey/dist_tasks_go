// Package prommetrics provides a Prometheus-backed implementation of
// queue.Metrics. Register a *Metrics with a prometheus.Registerer (or pass
// nil to use the default registry) and pass it to queue.NewWorker / queue.New.
//
// Expose the registry over HTTP with promhttp.Handler() — see the example in
// cmd/worker-demo (TODO) or:
//
//	http.Handle("/metrics", promhttp.Handler())
//	http.ListenAndServe(":9090", nil)
package prommetrics

import (
	"context"
	"time"

	"dist_tasks_go/queue"

	"github.com/prometheus/client_golang/prometheus"
)

// taskLabel is the only label we attach to per-job metrics. Keep cardinality
// low — never label by job ID, trace ID, or error message.
const taskLabel = "task"

type Metrics struct {
	enqueued     *prometheus.CounterVec
	started      *prometheus.CounterVec
	completed    *prometheus.CounterVec
	failed       *prometheus.CounterVec // labeled by task + retryable
	retried      *prometheus.CounterVec
	deadLettered *prometheus.CounterVec
	duration     *prometheus.HistogramVec

	queueDepth      prometheus.Gauge
	inFlight        prometheus.Gauge
	deadLetterDepth prometheus.Gauge
	retryScheduled  prometheus.Gauge

	// startTimes tracks per-job start timestamps so JobCompleted/JobFailed can
	// observe duration. The map is keyed by JobID. A real implementation
	// should use a sync.Map or stash the start time on the Job itself; this
	// scaffold leaves it as a TODO.
	// startTimes sync.Map
}

// New constructs a Metrics and registers all collectors with reg. Pass
// prometheus.DefaultRegisterer (or nil, which is the same) to use the
// process-global default registry.
func New(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	factory := promauto(reg)

	m := &Metrics{
		enqueued: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dtq_jobs_enqueued_total",
			Help: "Total jobs enqueued, partitioned by task.",
		}, []string{taskLabel}),
		started: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dtq_jobs_started_total",
			Help: "Total jobs handed to a handler, partitioned by task.",
		}, []string{taskLabel}),
		completed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dtq_jobs_completed_total",
			Help: "Total jobs that finished successfully, partitioned by task.",
		}, []string{taskLabel}),
		failed: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dtq_jobs_failed_total",
			Help: "Total handler failures, partitioned by task and whether the failure was retryable.",
		}, []string{taskLabel, "retryable"}),
		retried: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dtq_jobs_retried_total",
			Help: "Total jobs scheduled for retry, partitioned by task.",
		}, []string{taskLabel}),
		deadLettered: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dtq_jobs_dead_lettered_total",
			Help: "Total jobs moved to the dead-letter stream, partitioned by task.",
		}, []string{taskLabel}),
		duration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "dtq_job_duration_seconds",
			Help:    "Handler runtime in seconds, partitioned by task.",
			Buckets: prometheus.DefBuckets, // 5ms .. 10s; tune per workload
		}, []string{taskLabel}),

		queueDepth: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dtq_queue_depth",
			Help: "Current number of jobs waiting in the main stream (XLEN).",
		}),
		inFlight: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dtq_in_flight_jobs",
			Help: "Current number of claimed-but-unacked jobs (XPENDING count).",
		}),
		deadLetterDepth: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dtq_dead_letter_depth",
			Help: "Current number of jobs in the dead-letter stream.",
		}),
		retryScheduled: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dtq_retry_scheduled",
			Help: "Current number of jobs waiting in the retry sorted set.",
		}),
	}
	return m
}

func (m *Metrics) JobEnqueued(_ context.Context, job queue.Job) {
	m.enqueued.WithLabelValues(job.Task).Inc()
}

func (m *Metrics) JobStarted(_ context.Context, job queue.Job) {
	m.started.WithLabelValues(job.Task).Inc()
	// TODO: stash time.Now() so JobCompleted/JobFailed can call duration.Observe.
	// Either attach to ctx, store on the Job, or use an internal sync.Map keyed by JobID.
}

func (m *Metrics) JobCompleted(_ context.Context, job queue.Job) {
	m.completed.WithLabelValues(job.Task).Inc()
	// TODO: m.duration.WithLabelValues(job.Task).Observe(time.Since(start).Seconds())
	_ = time.Second
}

func (m *Metrics) JobFailed(_ context.Context, job queue.Job, retryable bool) {
	r := "false"
	if retryable {
		r = "true"
	}
	m.failed.WithLabelValues(job.Task, r).Inc()
}

func (m *Metrics) JobRetried(_ context.Context, job queue.Job) {
	m.retried.WithLabelValues(job.Task).Inc()
}

func (m *Metrics) JobDeadLettered(_ context.Context, job queue.Job) {
	m.deadLettered.WithLabelValues(job.Task).Inc()
}

func (m *Metrics) StatsObserved(_ context.Context, stats queue.Stats) {
	m.queueDepth.Set(float64(stats.QueueDepth))
	m.inFlight.Set(float64(stats.InFlight))
	m.deadLetterDepth.Set(float64(stats.DeadLetterDepth))
	m.retryScheduled.Set(float64(stats.RetryScheduled))
}

// promauto is a tiny wrapper that mimics promauto.With(reg) without pulling
// in the promauto subpackage. It panics on duplicate registration, which is
// exactly what you want at startup.
type autoFactory struct{ reg prometheus.Registerer }

func promauto(reg prometheus.Registerer) autoFactory { return autoFactory{reg: reg} }

func (f autoFactory) NewCounterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	f.reg.MustRegister(c)
	return c
}

func (f autoFactory) NewHistogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(opts, labels)
	f.reg.MustRegister(h)
	return h
}

func (f autoFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(opts)
	f.reg.MustRegister(g)
	return g
}
