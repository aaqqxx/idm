//go:build linux

package download

import (
	"fmt"
	"os"
	"syscall"
)

// preallocatePlatform reserves the full size up front with fallocate so the
// filesystem allocates contiguous blocks before any bytes arrive.
func preallocatePlatform(f *os.File, size int64) error {
	if err := syscall.Fallocate(int(f.Fd()), 0, 0, size); err != nil {
		return fmt.Errorf("fallocate: %w", err)
	}
	return nil
}
