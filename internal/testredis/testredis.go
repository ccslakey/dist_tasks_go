package testredis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type Instance struct {
	Addr string
}

// Start returns an Instance pointing at a running Redis. The address can be
// overridden with TEST_REDIS_ADDR; otherwise it defaults to localhost:6379
// (matches docker-compose.yml). If Redis is unreachable the test is skipped
// rather than failed, so unit-only runs aren't blocked. The DB is FLUSHDB'd on
// cleanup so each test gets a fresh state.
func Start(t *testing.T) Instance {
	t.Helper()

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("redis unavailable at %s: %v (start with `docker compose up -d redis`)", addr, err)
	}

	t.Cleanup(func() {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer flushCancel()
		_ = client.FlushDB(flushCtx).Err()
		_ = client.Close()
	})

	return Instance{Addr: addr}
}
