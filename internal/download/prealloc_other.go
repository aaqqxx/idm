//go:build !windows && !linux

package download

import (
	"os"
)

// preallocatePlatform is a no-op on platforms without an explicit allocation
// primitive (notably macOS, which has F_PREALLOCATE but no portable binding in
// the standard library). The caller's truncate-based fallback still extends the
// file to the right length, which is all correctness requires.
func preallocatePlatform(_ *os.File, _ int64) error {
	return os.ErrInvalid
}
