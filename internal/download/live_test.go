package download

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

type atomicFloat struct{ v atomic.Uint64 }

func (a *atomicFloat) store(f float64) { a.v.Store(math.Float64bits(f)) }
func (a *atomicFloat) load() float64  { return math.Float64frombits(a.v.Load()) }

// TestLiveDownload exercises a real remote file. It is skipped unless
// IDM_LIVE_URL is set, so the default `go test` stays hermetic.
func TestLiveDownload(t *testing.T) {
	url := os.Getenv("IDM_LIVE_URL")
	if url == "" {
		t.Skip("set IDM_LIVE_URL to run a live download")
	}

	// IDM_LIVE_LIMIT exercises the throttle end to end.
	limit := int64(0)
	if v := os.Getenv("IDM_LIVE_LIMIT"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("bad IDM_LIVE_LIMIT: %v", err)
		}
		limit = n
	}

	dir := t.TempDir()
	savePath := filepath.Join(dir, "live.bin")

	var lastPct atomicFloat
	var samples atomic.Int64
	start := time.Now()

	res, err := RunSession(context.Background(), SessionConfig{
		URL:               url,
		SavePath:          savePath,
		StateDir:          filepath.Join(dir, "ckpt"),
		EnableResume:      true,
		Engine:            EngineConfig{MaxConnections: 8, HardCap: 32},
		MinChunk:          64 * 1024,
		MinWorkUnit:       512 * 1024,
		IdleTimeout:       15 * time.Second,
		MaxRetries:        3,
		BufferSize:        128 * 1024,
		MaxBytesPerSecond: limit,
		Progress: func(p Progress) {
			if p.Total > 0 {
				lastPct.store(float64(p.Completed) / float64(p.Total) * 100)
			}
			samples.Add(1)
		},
	})
	if err != nil {
		t.Fatalf("live download: %v", err)
	}

	fi, err := os.Stat(savePath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != res.Result.Size {
		t.Fatalf("file size = %d, declared %d", fi.Size(), res.Result.Size)
	}

	h, readBack := hashFile(t, savePath)
	elapsed := time.Since(start)
	speed := float64(res.Result.Size) / elapsed.Seconds() / (1 << 20)

	t.Logf("LIVE OK url=%s", url)
	t.Logf("  size=%d bytes  segments=%d  peak_conns=%d  retries=%d  resumed=%v",
		res.Result.Size, res.Segments, res.Result.PeakActive, res.Result.Retries, res.Resumed)
	t.Logf("  elapsed=%s  avg=%.2f MiB/s  limit=%d B/s  progress_samples=%d  last_pct=%.1f%%",
		elapsed.Round(time.Millisecond), speed, limit, samples.Load(), lastPct.load())
	t.Logf("  sha256=%s  read_back=%d bytes", h, readBack)

	if res.Segments < 2 {
		t.Logf("  note: only %d segment(s); server may not support ranges", res.Segments)
	}

	// When a limit is set, the achieved rate must stay near it: a throttle
	// that silently does nothing would show up as a much higher average.
	if limit > 0 {
		achieved := float64(res.Result.Size) / elapsed.Seconds()
		t.Logf("  achieved=%.0f B/s vs limit=%d B/s", achieved, limit)
		if achieved > float64(limit)*2.0 {
			t.Fatalf("throttle ineffective: achieved %.0f B/s, limit %d B/s", achieved, limit)
		}
	}
}

func hashFile(t *testing.T, path string) (string, int64) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), n
}
