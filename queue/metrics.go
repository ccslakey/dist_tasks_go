package queue

import "context"

type Metrics interface {
	JobEnqueued(ctx context.Context, job Job)
	JobStarted(ctx context.Context, job Job)
	JobCompleted(ctx context.Context, job Job)
	JobFailed(ctx context.Context, job Job, retryable bool)
	JobRetried(ctx context.Context, job Job)
	JobDeadLettered(ctx context.Context, job Job)
	StatsObserved(ctx context.Context, stats Stats)
}

type NoopMetrics struct{}

func (NoopMetrics) JobEnqueued(context.Context, Job)     {}
func (NoopMetrics) JobStarted(context.Context, Job)      {}
func (NoopMetrics) JobCompleted(context.Context, Job)    {}
func (NoopMetrics) JobFailed(context.Context, Job, bool) {}
func (NoopMetrics) JobRetried(context.Context, Job)      {}
func (NoopMetrics) JobDeadLettered(context.Context, Job) {}
func (NoopMetrics) StatsObserved(context.Context, Stats) {}
