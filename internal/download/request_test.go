package download

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// gatedServer requires a Referer, a cookie, and Basic Auth on every request.
// It is the common shape of "hard" real-world downloads.
func gatedServer(t *testing.T, payload []byte) *httptest.Server {
	t.Helper()
	etag := `"gated-v1"`
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Referer") != "https://example.com/page" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("bad referer"))
			return
		}
		if r.Header.Get("Cookie") != "session=abc123" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("bad cookie"))
			return
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "user" || pass != "pass" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("bad auth"))
			return
		}

		w.Header().Set("ETag", etag)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", `attachment; filename="gated.bin"`)

		// Serve the request, honouring Range. Shares parseRange with e2e_test.go.
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.WriteHeader(http.StatusOK)
			return
		}
		if rh := r.Header.Get("Range"); rh != "" {
			start, end, ok := parseRange(rh, int64(len(payload)))
			if !ok {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			chunk := payload[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
			w.Header().Set("Content-Length", strconv.Itoa(len(chunk)))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(chunk)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
}

func TestRequestOptionsValidation(t *testing.T) {
	for _, h := range []string{"Range", "Content-Length", "Accept-Encoding", "Connection", "Host"} {
		o := RequestOptions{Headers: map[string]string{h: "x"}}
		if err := o.Validate(); err == nil {
			t.Fatalf("header %q should be rejected", h)
		}
	}
	o := RequestOptions{
		Headers:  map[string]string{"X-Token": "v"},
		Referer:  "https://example.com",
		Username: "u",
	}
	if err := o.Validate(); err != nil {
		t.Fatalf("harmless options should pass: %v", err)
	}
}

// Without credentials the gated server must refuse the transfer.
func TestGatedServerRejectsBareRequests(t *testing.T) {
	payload := bytes.Repeat([]byte("g"), 65536)
	srv := gatedServer(t, payload)
	defer srv.Close()

	dir := t.TempDir()
	_, err := RunSession(context.Background(), SessionConfig{
		URL:      srv.URL + "/gated.bin",
		SavePath: filepath.Join(dir, "g.bin"),
		MinChunk: 16 * 1024,
	})
	if err == nil {
		t.Fatal("expected the gated server to refuse a bare request")
	}
}

// With all credentials the full segmented path must succeed.
func TestGatedServerAcceptsAuthedSession(t *testing.T) {
	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i % 211)
	}
	srv := gatedServer(t, payload)
	defer srv.Close()

	dir := t.TempDir()
	savePath := filepath.Join(dir, "g.bin")

	res, err := RunSession(context.Background(), SessionConfig{
		URL:      srv.URL + "/gated.bin",
		SavePath: savePath,
		MinChunk: 64 * 1024,
		Request: RequestOptions{
			Headers:  map[string]string{"X-Token": "token-123"},
			Referer:  "https://example.com/page",
			Cookies:  []string{"session=abc123"},
			Username: "user",
			Password: "pass",
		},
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
	if res.Probe.FileName != "gated.bin" {
		t.Fatalf("filename from disposition = %q", res.Probe.FileName)
	}
	if res.Result.Size != int64(len(payload)) {
		t.Fatalf("size = %d want %d", res.Result.Size, len(payload))
	}
}

// Auth must also flow through the segmented workers, not just the probe: every
// Range request is checked by the gated server, so a dropped header on workers
// fails the session.
func TestAuthSurvivesSegmentedWorkers(t *testing.T) {
	payload := make([]byte, 2<<20)
	for i := range payload {
		payload[i] = byte((i * 3) % 197)
	}
	srv := gatedServer(t, payload)
	defer srv.Close()

	dir := t.TempDir()
	savePath := filepath.Join(dir, "w.bin")

	res, err := RunSession(context.Background(), SessionConfig{
		URL:      srv.URL + "/gated.bin",
		SavePath: savePath,
		Engine:   EngineConfig{MaxConnections: 4, HardCap: 8},
		MinChunk: 64 * 1024,
		Request: RequestOptions{
			Referer:  "https://example.com/page",
			Cookies:  []string{"session=abc123"},
			Username: "user",
			Password: "pass",
		},
	})
	if err != nil {
		t.Fatalf("RunSession: %v", err)
	}
	got, err := os.ReadFile(savePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("segmented payload mismatch under auth")
	}
	if res.Segments < 2 {
		t.Fatalf("expected segmented transfer, got %d segment(s)", res.Segments)
	}
}

func TestProbeFailsCleanlyOnAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	_, err := RunSession(context.Background(), SessionConfig{
		URL:      srv.URL + "/x",
		SavePath: filepath.Join(t.TempDir(), "x.bin"),
		MinChunk: 16 * 1024,
	})
	if err == nil {
		t.Fatal("expected a clean error on 401")
	}
}
