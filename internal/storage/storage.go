package storage

import (
	"fmt"
	"os"
	"sync"
)

type LocalStorage struct {
	mutex        sync.Mutex
	relativePath string
}

func NewLocalStorage(filepath string) *LocalStorage {
	return &LocalStorage{relativePath: filepath}
}

func (l *LocalStorage) Write(p []byte) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	f, err := os.OpenFile(l.relativePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	output := fmt.Sprintf("%s\n", p)
	if _, err := f.WriteString(output); err != nil {
		return 0, err
	}

	return len(p), nil
}
