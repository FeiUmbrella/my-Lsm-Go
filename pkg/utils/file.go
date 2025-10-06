package utils

import "sync"

// FileManager 文件操作的一个工具
type FileManager struct {
	dataDir string
	mu      sync.RWMutex
}

// NewFileManager creates a new file manager
func NewFileManager(dataDir string) *FileManager {
	return &FileManager{
		dataDir: dataDir,
	}
}
