package queue

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestEveryRunJoinsTheEnqueueTrace(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(sdktrace.NewTracerProvider())
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	})

	backend := &fakeBackend{}
	if _, err := New(backend).Enqueue(t.Context(), "flaky", map[string]int{"n": 1}, EnqueueOptions{}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	job := backend.ready[0]
	if job.TraceParent == "" {
		t.Fatal("enqueue didn't record a traceparent on the job")
	}

	worker := NewWorker(backend, WorkerConfig{ID: "worker-1"}, nil)
	fail := true
	worker.Register("flaky", func(context.Context, Job) JobResult {
		if fail {
			fail = false
			return NewRetryableError(errors.New("boom"))
		}
		return JobResult{}
	})
	for attempt := range 2 { // fails, then succeeds on retry
		job.Attempt = attempt
		if err := worker.handleOne(t.Context(), job); err != nil {
			t.Fatalf("handleOne: %v", err)
		}
	}

	spans := recorder.Ended()
	if len(spans) != 3 {
		t.Fatalf("got %d spans, want enqueue + 2 runs", len(spans))
	}
	enqueue := spans[0]
	for _, run := range spans[1:] {
		if run.Parent().SpanID() != enqueue.SpanContext().SpanID() || run.SpanContext().TraceID() != enqueue.SpanContext().TraceID() {
			t.Errorf("span %q isn't a child of %q", run.Name(), enqueue.Name())
		}
	}
	outcomes := []string{}
	for _, run := range spans[1:] {
		for _, a := range run.Attributes() {
			if a.Key == "dtq.outcome" {
				outcomes = append(outcomes, a.Value.AsString())
			}
		}
	}
	if len(outcomes) != 2 || outcomes[0] != "retried" || outcomes[1] != "completed" {
		t.Errorf("run outcomes = %v, want [retried completed]", outcomes)
	}
}
