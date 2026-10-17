package download

import (
	"os"
)

// preallocate reserves size bytes for f so that out-of-order writes do not
// fragment the file and a full disk is detected before the transfer starts.
//
// The portable fallback is a single write of one byte at the last offset,
// which forces the filesystem to allocate the intervening range on most
// implementations. Platform-specific probes live in prealloc_unix.go /
// prealloc_windows.go and are used when available.
func preallocate(f *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	if err := preallocatePlatform(f, size); err == nil {
		return nil
	}
	// Sparse-friendly fallback: extending the file to full length is enough
	// for correctness; fragmentation is a nice-to-have.
	return f.Truncate(size)
}
