package queue

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tracing uses the global OpenTelemetry providers, which do nothing until the
// application installs an SDK (see internal/telemetry). Enqueue starts a
// producer span and stores its context on the job as a W3C traceparent; each
// run of the job starts a consumer span parented to it, so one trace shows
// the enqueue, the time spent queued, and every attempt.
var tracer = otel.Tracer("dist_tasks_go/queue")

const traceparentKey = "traceparent"

// traceCarrier exposes Job.TraceParent to OpenTelemetry propagators.
type traceCarrier struct{ job *Job }

func (c traceCarrier) Get(key string) string {
	if key == traceparentKey {
		return c.job.TraceParent
	}
	return ""
}

func (c traceCarrier) Set(key, value string) {
	if key == traceparentKey {
		c.job.TraceParent = value
	}
}

func (c traceCarrier) Keys() []string { return []string{traceparentKey} }

func startEnqueueSpan(ctx context.Context, job *Job) (context.Context, trace.Span) {
	ctx, span := tracer.Start(ctx, "enqueue "+job.Task,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("dtq.task", job.Task),
			attribute.String("dtq.job_id", string(job.ID)),
		))
	otel.GetTextMapPropagator().Inject(ctx, traceCarrier{job})
	return ctx, span
}

func startProcessSpan(ctx context.Context, job Job, workerID string) (context.Context, trace.Span) {
	ctx = otel.GetTextMapPropagator().Extract(ctx, traceCarrier{&job})
	return tracer.Start(ctx, "process "+job.Task,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "redis"),
			attribute.String("dtq.task", job.Task),
			attribute.String("dtq.job_id", string(job.ID)),
			attribute.Int("dtq.attempt", job.Attempt),
			attribute.Int64("dtq.delivery", job.Lease.Token),
			attribute.String("dtq.worker", workerID),
		))
}

// setOutcome records how a run ended on its span: completed, retried,
// dead_lettered, or abandoned (lease lost or shutdown timed out).
func setOutcome(ctx context.Context, outcome string, err error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("dtq.outcome", outcome))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
