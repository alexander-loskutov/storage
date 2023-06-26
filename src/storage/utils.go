package storage

import (
	"os"
	"path/filepath"
)

func resolvePath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}

	current, _ := os.Executable()
	return filepath.Join(current, "..", path)
}
