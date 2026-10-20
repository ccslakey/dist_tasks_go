package redisstream

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// stuckRedis starts a fake Redis that answers every command with +OK except
// blocking reads, which it never answers. That's how a Redis whose timers
// have stopped firing looks to a client: a BLOCK on an empty stream doesn't
// end until some unrelated traffic happens to wake the server.
func stuckRedis(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			go serveStuck(conn)
		}
	}()
	return ln.Addr().String()
}

func serveStuck(conn net.Conn) {
	r := bufio.NewReader(conn)
	for {
		args, err := readCommand(r)
		if err != nil {
			return
		}
		switch {
		case len(args) > 0 && strings.EqualFold(args[0], "HELLO"):
			conn.Write([]byte("-ERR unknown command 'HELLO'\r\n")) // like Redis 5
		case slices.ContainsFunc(args, func(a string) bool { return strings.EqualFold(a, "BLOCK") }):
			// never answered
		default:
			conn.Write([]byte("+OK\r\n"))
		}
	}
}

// readCommand reads one RESP command: an array of bulk strings.
func readCommand(r *bufio.Reader) ([]string, error) {
	header := func(prefix byte) (int, error) {
		line, err := r.ReadString('\n')
		if err != nil {
			return 0, err
		}
		if len(line) < 3 || line[0] != prefix {
			return 0, fmt.Errorf("unexpected line %q", line)
		}
		return strconv.Atoi(strings.TrimSpace(line[1:]))
	}
	n, err := header('*')
	if err != nil {
		return nil, err
	}
	args := make([]string, n)
	for i := range args {
		size, err := header('$')
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}

func TestBlockingClaimGivesUpWhenServerNeverEndsTheBlock(t *testing.T) {
	store, err := New(Config{Addr: stuckRedis(t)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	const wait = 100 * time.Millisecond
	start := time.Now()
	jobs, err := store.claimBlocking(t.Context(), "worker-1", 1, wait)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("claimBlocking: jobs=%d err=%v, want none and no error", len(jobs), err)
	}
	// Without a deadline, go-redis waits BLOCK + 10s.
	if took := time.Since(start); took > wait+blockGrace+time.Second {
		t.Errorf("claimBlocking took %s; it should give up after %s", took, wait+blockGrace)
	}
}

func TestShardedClaimReturnsOnTimeWhenPollsHang(t *testing.T) {
	s, err := NewSharded(ShardedConfig{Addrs: []string{stuckRedis(t)}, Partitions: 4})
	if err != nil {
		t.Fatalf("NewSharded: %v", err)
	}
	defer s.Close()

	// Claim's sweep needs real scripts, so go straight to the long poll.
	const wait = 100 * time.Millisecond
	start := time.Now()
	if jobs := s.waitForJobs(t.Context(), "worker-1", 8, wait); len(jobs) != 0 {
		t.Fatalf("got %d jobs from a server that never answers", len(jobs))
	}
	if took := time.Since(start); took > wait+200*time.Millisecond {
		t.Errorf("waitForJobs took %s with wait %s; hung polls must not hold it up", took, wait)
	}

	// With no Claim waiting, the pollers give up on their hung reads and stop.
	deadline := time.Now().Add(wait + blockGrace + time.Second)
	for {
		s.mu.Lock()
		running := slices.Contains(s.polling, true)
		s.mu.Unlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pollers still running long after their reads' deadline")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestShardedCloseCutsOffHungPolls(t *testing.T) {
	s, err := NewSharded(ShardedConfig{Addrs: []string{stuckRedis(t)}, Partitions: 4})
	if err != nil {
		t.Fatalf("NewSharded: %v", err)
	}

	waited := make(chan struct{})
	go func() {
		s.waitForJobs(t.Context(), "worker-1", 8, 10*time.Second)
		close(waited)
	}()
	time.Sleep(100 * time.Millisecond) // let the polls block

	start := time.Now()
	s.Close()
	if took := time.Since(start); took > 200*time.Millisecond {
		t.Errorf("Close took %s; it should cut off polls blocked in Redis", took)
	}
	select {
	case <-waited:
	case <-time.After(200 * time.Millisecond):
		t.Error("a Claim waiting on long polls didn't return when the backend closed")
	}
}
