package download

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// UniquePath returns a path that does not collide with an existing file,
// following the "name (1).ext" convention users expect from browsers and IDM.
//
// A pending ".part" staging file counts as a collision too, so two concurrent
// tasks never fight over the same staging path.
func UniquePath(path string) string {
	if !exists(path) && !exists(path+".part") {
		return path
	}

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)

	for i := 1; i < 10000; i++ {
		candidate := filepath.Join(dir, fmt.Sprintf("%s (%d)%s", stem, i, ext))
		if !exists(candidate) && !exists(candidate+".part") {
			return candidate
		}
	}
	return filepath.Join(dir, fmt.Sprintf("%s-%d%s", stem, os.Getpid(), ext))
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
