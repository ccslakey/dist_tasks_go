// Package prettylog formats structured JSON log lines into human-readable
// strings for local development, and provides a slog.Handler that writes
// pretty output while storing records as JSON.
package prettylog

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// Format parses one JSON log line and returns a human-readable string.
// Unknown or missing fields are silently omitted. Example:
//
//	10:42:01.123 INFO  sending email  job_id=1777786121957-0 attempt=0 to=dev@example.com
func Format(line []byte) string {
	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(line), &entry); err != nil {
		return string(line)
	}

	var b strings.Builder

	if ts, ok := entry["time"].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			fmt.Fprintf(&b, "%s ", t.Format("15:04:05.000"))
		}
	}

	if level, ok := entry["level"].(string); ok {
		fmt.Fprintf(&b, "%-5s ", level)
	}

	if msg, ok := entry["msg"].(string); ok {
		b.WriteString(msg)
	}

	skip := map[string]bool{"time": true, "level": true, "msg": true}
	for k, v := range entry {
		if !skip[k] {
			fmt.Fprintf(&b, "  %s=%v", k, v)
		}
	}

	return b.String()
}

// handler is a slog.Handler that writes pretty-formatted lines to out.
// It formats records directly from slog.Record rather than via JSON so
// there are no shared-buffer races between concurrent goroutines.
type handler struct {
	out   io.Writer
	mu    sync.Mutex // serialises writes to out
	attrs []slog.Attr
}

// NewHandler returns a slog.Handler that writes pretty-formatted output to
// out. Pass nil to default to os.Stderr.
func NewHandler(out io.Writer) slog.Handler {
	if out == nil {
		out = os.Stderr
	}
	return &handler{out: out}
}

func (h *handler) Enabled(_ context.Context, _ slog.Level) bool { return true }

func (h *handler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder

	fmt.Fprintf(&b, "%s ", r.Time.Format("15:04:05.000"))
	fmt.Fprintf(&b, "%-5s ", r.Level)
	b.WriteString(r.Message)

	for _, a := range h.attrs {
		fmt.Fprintf(&b, "  %s=%v", a.Key, a.Value)
	}
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, "  %s=%v", a.Key, a.Value)
		return true
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := fmt.Fprintln(h.out, b.String())
	return err
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(merged, h.attrs)
	copy(merged[len(h.attrs):], attrs)
	return &handler{out: h.out, attrs: merged}
}

func (h *handler) WithGroup(name string) slog.Handler {
	// Prefix subsequent attrs with the group name.
	return &handler{out: h.out, attrs: append(h.attrs, slog.String("group", name))}
}
