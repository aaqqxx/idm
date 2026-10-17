package download

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUniquePath(t *testing.T) {
	dir := t.TempDir()
	base := filepath.Join(dir, "file.txt")

	if got := UniquePath(base); got != base {
		t.Fatalf("empty dir should keep the path, got %s", got)
	}

	if err := os.WriteFile(base, []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := UniquePath(base)
	if got == base {
		t.Fatal("existing file must force a new name")
	}
	if want := filepath.Join(dir, "file (1).txt"); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}

	// A pending .part file must also count as a collision so two tasks never
	// share a staging path.
	if err := os.WriteFile(got+".part", []byte("p"), 0o644); err != nil {
		t.Fatal(err)
	}
	third := UniquePath(base)
	if third == got {
		t.Fatal(".part collision must force another name")
	}
}

func TestThrottleUnlimited(t *testing.T) {
	th := NewThrottle(0)
	if th != nil {
		t.Fatal("zero rate should yield a nil (no-op) throttle")
	}
	if err := th.Take(context.Background(), 1<<20); err != nil {
		t.Fatalf("nil throttle must accept everything: %v", err)
	}
}

func TestThrottleRespectsRate(t *testing.T) {
	const rate = 256 * 1024 // bytes/sec
	th := NewThrottle(rate)
	if th == nil {
		t.Fatal("throttle should be created")
	}

	// Drain the initial burst, then measure the cost of one more second worth.
	if err := th.Take(context.Background(), int(rate/4)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := th.Take(context.Background(), int(rate)); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	// Consuming a full second of budget after draining the burst should take
	// on the order of a second; allow generous slack for CI.
	if elapsed < 200*time.Millisecond {
		t.Fatalf("throttle too permissive: %v for 1s of budget", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("throttle too strict: %v", elapsed)
	}
}

func TestThrottleCancels(t *testing.T) {
	th := NewThrottle(64 * 1024)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := th.Take(ctx, 1<<20); err == nil {
		t.Fatal("expected cancellation error")
	}
}

// The payload must land under the final name only after Commit, and never
// under the final name while incomplete.
func TestFileSinkPartialThenRename(t *testing.T) {
	dir := t.TempDir()
	finalPath := filepath.Join(dir, "out.bin")

	sink, err := NewFileSink(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Truncate(16); err != nil {
		t.Fatal(err)
	}
	if _, err := sink.WriterAt().WriteAt([]byte("hello world, idm"), 0); err != nil {
		t.Fatal(err)
	}

	// Before commit only the staging file exists.
	if _, statErr := os.Stat(finalPath); statErr == nil {
		t.Fatal("final path must not exist before Commit")
	}
	if _, statErr := os.Stat(finalPath + ".part"); statErr != nil {
		t.Fatalf("staging file should exist: %v", statErr)
	}

	if err := sink.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello world, idm" {
		t.Fatalf("got %q", got)
	}
	if _, statErr := os.Stat(finalPath + ".part"); statErr == nil {
		t.Fatal("staging file should be renamed away")
	}
}

// NoOverwrite must not clobber an existing file.
func TestRunSessionNoOverwrite(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 200*1024)
	srv, _ := rangeServer(t, payload, 0)
	defer srv.Close()

	dir := t.TempDir()
	savePath := filepath.Join(dir, "dup.bin")
	if err := os.WriteFile(savePath, []byte("ORIGINAL"), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := RunSession(context.Background(), SessionConfig{
		URL:         srv.URL + "/d.bin",
		SavePath:    savePath,
		NoOverwrite: true,
		MinChunk:    16 * 1024,
		BufferSize:  32 * 1024,
	})
	if err != nil {
		t.Fatalf("RunSession: %v", err)
	}
	if res.Path == savePath {
		t.Fatal("NoOverwrite should have chosen a new path")
	}
	orig, _ := os.ReadFile(savePath)
	if string(orig) != "ORIGINAL" {
		t.Fatalf("original file was modified: %q", orig)
	}
	got, err := os.ReadFile(res.Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("downloaded payload mismatch")
	}
}

// A redirect to another host must still yield a correct file: the probe
// records the final URL and range requests go there.
func TestRunSessionFollowsRedirect(t *testing.T) {
	payload := bytes.Repeat([]byte("redirect-me"), 40000)
	srv, _ := rangeServer(t, payload, 0)
	defer srv.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srv.URL+"/real.bin", http.StatusFound)
	})
	outer := httptest.NewServer(mux)
	defer outer.Close()

	dir := t.TempDir()
	savePath := filepath.Join(dir, "r.bin")
	res, err := RunSession(context.Background(), SessionConfig{
		URL:        outer.URL + "/start",
		SavePath:   savePath,
		MinChunk:   16 * 1024,
		BufferSize: 32 * 1024,
	})
	if err != nil {
		t.Fatalf("RunSession: %v", err)
	}
	got, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload mismatch: got %d want %d", len(got), len(payload))
	}
	if res.Probe.FinalURL != srv.URL+"/real.bin" {
		t.Fatalf("final URL = %q, want %q", res.Probe.FinalURL, srv.URL+"/real.bin")
	}
}
