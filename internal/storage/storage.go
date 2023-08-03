package storage

import (
	"fmt"
	"io"
	"os"
	"sync"
)

type LocalStorage struct {
	Mutex        sync.Mutex
	relativePath string
	io.Writer
}

func NewLocalStorage(filepath string) *LocalStorage {
	return &LocalStorage{relativePath: filepath}
}

func (l *LocalStorage) Write(p []byte) (int, error) {
	l.Mutex.Lock()
	defer l.Mutex.Unlock()

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
