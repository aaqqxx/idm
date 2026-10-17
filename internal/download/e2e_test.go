package download

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// rangeServer serves fixed payload with proper 206 support and records how
// many distinct Range requests arrived, which proves parallelism happened.
func rangeServer(t *testing.T, payload []byte, delay time.Duration) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	etag := fmt.Sprintf(`"%x"`, sha256.Sum256(payload))

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")

		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}

		rangeHdr := r.Header.Get("Range")
		if rangeHdr == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(payload)
			return
		}

		start, end, ok := parseRange(rangeHdr, int64(len(payload)))
		if !ok {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		chunk := payload[start : end+1]
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
		w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
		w.WriteHeader(http.StatusPartialContent)
		if delay > 0 {
			// Trickle so multiple connections overlap.
			half := len(chunk) / 2
			_, _ = w.Write(chunk[:half])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(delay)
			_, _ = w.Write(chunk[half:])
			return
		}
		_, _ = w.Write(chunk)
	}))
	return srv, &requests
}

func parseRange(h string, total int64) (int64, int64, bool) {
	if !strings.HasPrefix(h, "bytes=") {
		return 0, 0, false
	}
	spec := strings.TrimPrefix(h, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	end := total - 1
	if parts[1] != "" {
		e, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, false
		}
		end = e
	}
	if start < 0 || end >= total || start > end {
		return 0, 0, false
	}
	return start, end, true
}

func TestRunSessionEndToEnd(t *testing.T) {
	payload := make([]byte, 8<<20) // 8 MiB
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	srv, reqs := rangeServer(t, payload, 5*time.Millisecond)
	defer srv.Close()

	dir := t.TempDir()
	savePath := filepath.Join(dir, "out.bin")

	res, err := RunSession(context.Background(), SessionConfig{
		URL:            srv.URL + "/file.bin",
		SavePath:       savePath,
		StateDir:       filepath.Join(dir, "ckpt"),
		EnableResume:   true,
		Engine:         EngineConfig{MaxConnections: 8, HardCap: 32},
		MinChunk:       64 * 1024,
		MinWorkUnit:    256 * 1024,
		IdleTimeout:    5 * time.Second,
		MaxRetries:     3,
		BufferSize:     64 * 1024,
		UserAgent:      "test-agent",
		DialTimeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatalf("RunSession: %v", err)
	}

	got, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if res.Result.Size != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", res.Result.Size, len(payload))
	}
	if n := reqs.Load(); n < 2 {
		t.Fatalf("expected multiple range requests, got %d", n)
	}
	if res.Segments < 2 {
		t.Fatalf("expected several segments, got %d", res.Segments)
	}
}

func TestRunSessionResumeAfterInterrupt(t *testing.T) {
	payload := make([]byte, 4<<20)
	for i := range payload {
		payload[i] = byte((i * 7) % 253)
	}
	srv, _ := rangeServer(t, payload, 10*time.Millisecond)
	defer srv.Close()

	dir := t.TempDir()
	savePath := filepath.Join(dir, "resume.bin")
	stateDir := filepath.Join(dir, "ckpt")

	cfg := SessionConfig{
		URL:          srv.URL + "/r.bin",
		SavePath:     savePath,
		StateDir:     stateDir,
		EnableResume: true,
		Engine:       EngineConfig{MaxConnections: 4, HardCap: 32},
		MinChunk:     32 * 1024,
		MinWorkUnit:  128 * 1024,
		IdleTimeout:  2 * time.Second,
		MaxRetries:   2,
		BufferSize:   32 * 1024,
	}

	// First attempt: cancel quickly so a checkpoint is written.
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	_, _ = RunSession(ctx, cfg)
	cancel()

	// Second attempt must complete correctly; whether it resumed or restarted,
	// the bytes must match.
	if _, err := RunSession(context.Background(), cfg); err != nil {
		t.Fatalf("second RunSession: %v", err)
	}
	got, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("resumed payload mismatch: got %d, want %d", len(got), len(payload))
	}
}

// A server that ignores Range must still yield correct bytes on one conn.
func TestRunSessionNoRangeSupport(t *testing.T) {
	payload := bytes.Repeat([]byte("idm"), 10000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.Header().Set("Content-Type", "text/plain")
		// Deliberately no Accept-Ranges and no Range handling.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	dir := t.TempDir()
	savePath := filepath.Join(dir, "norange.bin")
	res, err := RunSession(context.Background(), SessionConfig{
		URL:        srv.URL + "/n.txt",
		SavePath:   savePath,
		MinChunk:   10 * 1024,
		BufferSize: 16 * 1024,
	})
	if err != nil {
		t.Fatalf("RunSession: %v", err)
	}
	if res.Probe.SupportsRange {
		t.Fatal("probe should report no range support")
	}
	got, _ := os.ReadFile(savePath)
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d want %d", len(got), len(payload))
	}
}
