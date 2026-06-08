package logger

import (
	"fmt"
	"sync"
	"time"
)

// sprintf formats a log message identically to the underlying log package.
func sprintf(format string, v ...interface{}) string {
	return fmt.Sprintf(format, v...)
}

// LogEntry represents a single captured log line.
type LogEntry struct {
	Time    int64  `json:"time"`  // Unix milliseconds
	Level   string `json:"level"` // debug/info/warn/error
	Message string `json:"message"`
}

// ringBuffer keeps the most recent log entries in memory for the admin UI.
type ringBuffer struct {
	mu      sync.RWMutex
	entries []LogEntry
	size    int
}

const defaultLogBufferSize = 200

var logBuffer = &ringBuffer{size: defaultLogBufferSize}

func (rb *ringBuffer) add(level, message string) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.entries = append(rb.entries, LogEntry{
		Time:    time.Now().UnixMilli(),
		Level:   level,
		Message: message,
	})
	if len(rb.entries) > rb.size {
		// Drop oldest entries, keep the newest rb.size.
		rb.entries = rb.entries[len(rb.entries)-rb.size:]
	}
}

func (rb *ringBuffer) snapshot(limit int) []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	n := len(rb.entries)
	if limit <= 0 || limit > n {
		limit = n
	}
	out := make([]LogEntry, limit)
	copy(out, rb.entries[n-limit:])
	return out
}

// RecentLogs returns up to limit most recent log entries (oldest first).
// A limit <= 0 returns all buffered entries.
func RecentLogs(limit int) []LogEntry {
	return logBuffer.snapshot(limit)
}
