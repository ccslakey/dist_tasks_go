// Package redisutil has helpers the tools share for working with a single
// Redis or a Redis Cluster.
package redisutil

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"dist_tasks_go/queue"
	"dist_tasks_go/redisstream"

	"github.com/redis/go-redis/v9"
)

// Target is the Redis a tool runs against and how the queue is laid out on
// it: one stream (Partitions 0) or a sharded queue.
type Target struct {
	Addr       string // a single Redis
	Cluster    string // comma-separated Redis Cluster seed addresses; overrides Addr
	Partitions int
}

func (t *Target) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&t.Addr, "redis", "localhost:6379", "Redis address")
	fs.StringVar(&t.Cluster, "cluster", "", "comma-separated Redis Cluster seed addresses (implies -partitions 16 unless set)")
	fs.IntVar(&t.Partitions, "partitions", 0, "split the queue into this many partitions (0 = one unsharded stream)")
}

// Args returns the flags that recreate t, to pass to a child process.
func (t Target) Args() []string {
	return []string{"-redis", t.Addr, "-cluster", t.Cluster, "-partitions", strconv.Itoa(t.normalized().Partitions)}
}

func (t Target) normalized() Target {
	if t.Cluster != "" && t.Partitions <= 0 {
		t.Partitions = 16
	}
	return t
}

func (t Target) seeds() []string {
	if t.Cluster != "" {
		return strings.Split(t.Cluster, ",")
	}
	return []string{t.Addr}
}

func (t Target) String() string {
	t = t.normalized()
	switch {
	case t.Cluster != "":
		return fmt.Sprintf("Redis Cluster at %s, %d partitions", t.Cluster, t.Partitions)
	case t.Partitions > 0:
		return fmt.Sprintf("Redis at %s, %d partitions", t.Addr, t.Partitions)
	default:
		return fmt.Sprintf("Redis at %s, unsharded", t.Addr)
	}
}

// Client returns a client for t's Redis or cluster, for keys outside the queue.
func (t Target) Client() redis.UniversalClient {
	return redis.NewUniversalClient(&redis.UniversalOptions{Addrs: t.seeds(), IsClusterMode: t.Cluster != ""})
}

// Backend is a queue backend that can be closed.
type Backend interface {
	queue.Backend
	Close() error
}

// Open opens the queue whose keys all start with prefix, and returns the
// configs of its streams (one per partition) for inspecting them directly.
func (t Target) Open(prefix string, consumerIdleTimeout time.Duration) (Backend, []redisstream.Config, error) {
	t = t.normalized()
	if t.Partitions == 0 {
		store, err := redisstream.New(redisstream.Config{
			Addr:                t.Addr,
			Stream:              prefix + "jobs",
			Group:               prefix + "workers",
			RetryKey:            prefix + "retry",
			DeadLetter:          prefix + "dead",
			DedupePrefix:        prefix + "dedupe:",
			ConsumerIdleTimeout: consumerIdleTimeout,
		})
		if err != nil {
			return nil, nil, err
		}
		return store, []redisstream.Config{store.Config()}, nil
	}
	s, err := redisstream.NewSharded(redisstream.ShardedConfig{
		Addrs:               t.seeds(),
		Cluster:             t.Cluster != "",
		Partitions:          t.Partitions,
		Prefix:              strings.TrimSuffix(prefix, ":"),
		ConsumerIdleTimeout: consumerIdleTimeout,
	})
	if err != nil {
		return nil, nil, err
	}
	var configs []redisstream.Config
	for _, part := range s.Partitions() {
		configs = append(configs, part.Config())
	}
	return s, configs, nil
}

// forEachNode runs fn on every primary of a cluster, or on the one Redis.
func forEachNode(ctx context.Context, c redis.UniversalClient, fn func(context.Context, *redis.Client) error) error {
	switch c := c.(type) {
	case *redis.ClusterClient:
		return c.ForEachMaster(ctx, fn)
	case *redis.Client:
		return fn(ctx, c)
	default:
		panic("redisutil: unsupported client type")
	}
}

// DeletePrefix deletes every key starting with prefix. On a cluster it scans
// each primary, since SCAN only sees the node it runs on.
func DeletePrefix(ctx context.Context, c redis.UniversalClient, prefix string) error {
	return forEachNode(ctx, c, func(ctx context.Context, node *redis.Client) error {
		iter := node.Scan(ctx, 0, prefix+"*", 1000).Iterator()
		for iter.Next(ctx) {
			// One key per DEL: keys in different slots can't share one.
			if err := node.Del(ctx, iter.Val()).Err(); err != nil {
				return err
			}
		}
		return iter.Err()
	})
}

// CPUSeconds returns the CPU time (user + sys) each Redis node has used so
// far, by address: one entry for a single Redis, one per primary for a
// cluster.
func CPUSeconds(ctx context.Context, c redis.UniversalClient) map[string]float64 {
	var mu sync.Mutex
	out := map[string]float64{}
	forEachNode(ctx, c, func(ctx context.Context, node *redis.Client) error {
		info, err := node.Info(ctx, "cpu").Result()
		if err != nil {
			return err
		}
		var total float64
		for _, line := range strings.Split(info, "\n") {
			k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
			if ok && (k == "used_cpu_user" || k == "used_cpu_sys") {
				f, _ := strconv.ParseFloat(v, 64)
				total += f
			}
		}
		mu.Lock()
		out[node.Options().Addr] = total
		mu.Unlock()
		return nil
	})
	return out
}
