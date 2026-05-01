package redisstream

import (
	"errors"
	"testing"

	"dist_tasks_go/queue"
)

func TestBackendMethodsAreTODOs(t *testing.T) {
	backend, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := backend.Enqueue(t.Context(), queue.Job{Task: "email", Payload: []byte(`{}`), MaxAttempts: 3}); !errors.Is(err, queue.ErrNotImplemented) {
		t.Fatalf("got %v, want ErrNotImplemented", err)
	}
}

func TestRedisIntegrationTODO(t *testing.T) {
	t.Skip("TODO: enable after Redis client wiring; should cover enqueue, claim, ack, retry, DLQ, lease recovery")
}
