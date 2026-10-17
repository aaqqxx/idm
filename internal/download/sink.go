package download

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Sink is the destination contract the engine writes downloaded bytes into.
// It must support concurrent writers because every connection writes at its
// own file offset. The returned io.WriterAt is expected to be safe for
// concurrent use at distinct offsets, which is true for *os.File and for a
// spooling implementation over an object store.
type Sink interface {
	// WriterAt returns the random-access writer to fill.
	WriterAt() io.WriterAt

	// Truncate ensures the file is exactly size bytes.
	Truncate(size int64) error

	// Commit finalises the payload: sync and publish. Called once on success.
	Commit(ctx context.Context) error

	// Abort discards partial work on failure.
	Abort(ctx context.Context) error
}

// FileSink writes directly to a local file. It is the fast path: the kernel
// page cache absorbs out-of-order writes and the final Sync flushes them.
//
// The payload is written to a sibling ".part" file which is renamed onto the
// final path only on Commit, so a partially downloaded file never appears
// under the real name. This mirrors IDM's temp-then-rename behaviour and keeps
// interrupted downloads obvious to the user.
type FileSink struct {
	path       string // final destination
	workPath   string // where bytes are actually written
	f          *os.File
	once       sync.Once
	committed  bool
	preallocate bool
}

// FileSinkOptions tunes FileSink behaviour.
type FileSinkOptions struct {
	// Partial disables the ".part" staging file and writes straight to path.
	// Used by spool sinks that manage their own lifecycle.
	Partial bool

	// Preallocate reserves the full size up front so that out-of-order writes
	// never fragment the file and a full disk fails early instead of midway.
	Preallocate bool
}

// NewFileSink opens (creating or resuming) a local file for random writes,
// staging through "<path>.part".
func NewFileSink(path string) (*FileSink, error) {
	return NewFileSinkWithOptions(path, FileSinkOptions{Preallocate: true})
}

// NewFileSinkWithOptions opens a sink with explicit options.
func NewFileSinkWithOptions(path string, opts FileSinkOptions) (*FileSink, error) {
	workPath := path
	if !opts.Partial {
		workPath = path + ".part"
	}
	if err := os.MkdirAll(filepath.Dir(workPath), 0o755); err != nil {
		return nil, fmt.Errorf("download: create dir for sink: %w", err)
	}
	f, err := os.OpenFile(workPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("download: open sink: %w", err)
	}
	return &FileSink{
		path:        path,
		workPath:    workPath,
		f:           f,
		preallocate: opts.Preallocate,
	}, nil
}

func (s *FileSink) WriterAt() io.WriterAt { return s.f }

func (s *FileSink) Truncate(size int64) error {
	if err := s.f.Truncate(size); err != nil {
		return fmt.Errorf("download: truncate sink: %w", err)
	}
	if s.preallocate {
		// A failed preallocation is advisory: some filesystems (and sparse
		// files on others) reject it, but the transfer remains correct, so
		// this is deliberately best-effort.
		_ = preallocate(s.f, size)
	}
	return nil
}

func (s *FileSink) Commit(ctx context.Context) error {
	if err := s.f.Sync(); err != nil {
		return fmt.Errorf("download: sync sink: %w", err)
	}
	if err := s.f.Close(); err != nil {
		return fmt.Errorf("download: close sink: %w", err)
	}
	if s.workPath != s.path {
		if err := os.Rename(s.workPath, s.path); err != nil {
			return fmt.Errorf("download: publish sink: %w", err)
		}
	}
	s.committed = true
	return nil
}

func (s *FileSink) Abort(_ context.Context) error {
	if s.committed {
		return nil
	}
	// Leave the .part file in place: it is resumable state, and the caller may
	// retry against the same sink. Only the handle is released here.
	return s.Close()
}

func (s *FileSink) Close() error {
	var err error
	s.once.Do(func() { err = s.f.Close() })
	return err
}

// Path exposes the final destination path.
func (s *FileSink) Path() string { return s.path }

// WorkPath exposes the staging path actually written during the transfer.
func (s *FileSink) WorkPath() string { return s.workPath }

// byteCounterSink wraps a Sink so the engine can observe aggregate progress
// without every worker having to report separately.
type progressSink struct {
	inner io.WriterAt
	add   func(int64)
}

func (p *progressSink) WriteAt(b []byte, off int64) (int, error) {
	n, err := p.inner.WriteAt(b, off)
	if n > 0 && p.add != nil {
		p.add(int64(n))
	}
	return n, err
}
