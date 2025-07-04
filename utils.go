package mr

import (
	"fmt"
	"os"
)

// openFile reads the entire file at filePath and returns its contents as a string.
func openFile(filePath string) (string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("openFile: could not read %s, %w", filePath, err)
	}

	return string(data), nil
}
