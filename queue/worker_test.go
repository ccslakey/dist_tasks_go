package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWorkerRegisterRejectsInvalidHandlers(t *testing.T) {
	worker := NewWorker(nil, WorkerConfig{ID: "worker-1"}, nil)

	if err := worker.Register("", func(context.Context, Job) JobResult { return JobResult{} }); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("got %v, want ErrInvalidJob", err)
	}
	if err := worker.Register("email", nil); !errors.Is(err, ErrInvalidJob) {
		t.Fatalf("got %v, want ErrInvalidJob", err)
	}
}

func TestWorkerHandleOneUnknownTask(t *testing.T) {
	worker := NewWorker(fakeBackend{}, WorkerConfig{ID: "worker-1"}, nil)
	err := worker.handleOne(context.Background(), Job{ID: "job-1", Task: "missing", MaxAttempts: 3})
	if !errors.Is(err, ErrUnknownTask) {
		t.Fatalf("got %v, want ErrUnknownTask", err)
	}
}

func TestWorkerRunTODO(t *testing.T) {
	t.Skip("TODO: implement once Worker.Run contains the real claim/concurrency/shutdown loop")
}

type fakeBackend struct{}

func (fakeBackend) Enqueue(context.Context, Job) (JobID, error)                { return "job-1", nil }
func (fakeBackend) Claim(context.Context, string, int) ([]Job, error)          { return nil, nil }
func (fakeBackend) Ack(context.Context, JobID) error                           { return nil }
func (fakeBackend) Retry(context.Context, Job, time.Time) error                { return nil }
func (fakeBackend) DeadLetter(context.Context, Job, string) error              { return nil }
func (fakeBackend) RecoverExpired(context.Context, time.Duration) (int, error) { return 0, nil }
func (fakeBackend) Stats(context.Context) (Stats, error)                       { return Stats{}, nil }
