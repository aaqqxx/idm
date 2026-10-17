//go:build windows

package download

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

var (
	modkernel32          = windows.NewLazySystemDLL("kernel32.dll")
	procSetFileValidData = modkernel32.NewProc("SetFileValidData")
)

// preallocatePlatform reserves the full size up front so a full disk is
// detected before the transfer starts instead of midway through it.
//
// SetFileValidData additionally avoids the filesystem zeroing the range, but it
// requires SeManageVolumePrivilege, which normal processes lack. The zero-fill
// path is still a correct preallocation, so a privilege failure is not an
// error: the caller's truncate-based fallback covers the rest.
func preallocatePlatform(f *os.File, size int64) error {
	h := windows.Handle(f.Fd())

	cur, err := windows.Seek(h, 0, windows.FILE_CURRENT)
	if err != nil {
		return fmt.Errorf("preallocate: tell: %w", err)
	}
	if _, err := windows.Seek(h, size, windows.FILE_BEGIN); err != nil {
		return fmt.Errorf("preallocate: seek: %w", err)
	}
	if err := windows.SetEndOfFile(h); err != nil {
		return fmt.Errorf("preallocate: set end of file: %w", err)
	}
	if _, err := windows.Seek(h, cur, windows.FILE_BEGIN); err != nil {
		return fmt.Errorf("preallocate: restore offset: %w", err)
	}

	// Best effort: only succeeds with SeManageVolumePrivilege.
	_, _, _ = procSetFileValidData.Call(uintptr(h), uintptr(size))
	return nil
}
