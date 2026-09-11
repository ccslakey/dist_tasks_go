package testredis

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type Instance struct {
	Addr string
}

// Start returns an Instance pointing at a running Redis. The address can be
// overridden with TEST_REDIS_ADDR; otherwise it defaults to localhost:6379,
// which matches docker-compose.yml.
//
// If Redis is unreachable the test is skipped, so a local unit-only run is not
// blocked by a missing container. That is the wrong behaviour in CI, where a
// skipped integration suite looks exactly like a passing one: set
// DTQ_REQUIRE_REDIS=1 there and an unreachable Redis fails the run instead.
//
// The database is flushed on cleanup so each test starts from a known state.
func Start(t *testing.T) Instance {
	t.Helper()

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	// Probe with a plain TCP dial first. Letting go-redis discover that nothing
	// is listening means its pool retries the dial and logs several lines, per
	// test, which turns a missing Redis into seconds of noise across the
	// package instead of one clear message.
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		unavailable(t, addr, err)
	}
	conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		MaxRetries:  -1, // something is listening; if it does not answer PING, say so once
		DialTimeout: time.Second,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		unavailable(t, addr, err)
	}

	t.Cleanup(func() {
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer flushCancel()
		_ = client.FlushDB(flushCtx).Err()
		_ = client.Close()
	})

	return Instance{Addr: addr}
}

func unavailable(t *testing.T, addr string, err error) {
	t.Helper()
	const msg = "redis unavailable at %s: %v (start it with `docker compose up -d redis`)"
	if os.Getenv("DTQ_REQUIRE_REDIS") != "" {
		t.Fatalf(msg, addr, err)
	}
	t.Skipf(msg, addr, err)
}
