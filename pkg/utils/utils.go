package utils

import (
	"fmt"
	"os"
	"path/filepath"
)

//SyncFileAndDirectory sync the file represented by a pointer and it's enclosing directory
func SyncFileAndDirectory(fp *os.File) error {
	if err := fp.Sync(); err != nil {
		return fmt.Errorf("could not sync file %s: %v", fp.Name(), err)
	}

	parentDir := filepath.Dir(fp.Name())

	// sync the directory entry
	dp, err := os.Open(parentDir)
	if err != nil {
		return fmt.Errorf("could not open directory %s to sync: %v", parentDir, err)
	}
	defer dp.Close()

	if err := dp.Sync(); err != nil {
		return fmt.Errorf("could not sync directory %s: %v", parentDir, err)
	}

	return nil
}
