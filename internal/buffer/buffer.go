package buffer

import (
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
)

var BufferSize = 5

type Writer interface {
	Write(dest io.Writer, bytes []byte, entityID uuid.UUID) error
}

type MemoryBuffer struct {
	mutex sync.Mutex
	data  map[uuid.UUID][][]byte
}

func NewMemoryBuffer() *MemoryBuffer {
	return &MemoryBuffer{data: make(map[uuid.UUID][][]byte)}
}

func (m *MemoryBuffer) Write(dest io.Writer, bytes []byte, entityID uuid.UUID) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if dataSlice, ok := m.data[entityID]; ok {
		// Finalize
		if len(bytes) == 0 {
			for _, output := range m.data[entityID] {
				_, err := dest.Write(formatWithID(entityID, output))
				if err != nil {
					return nil
				}
			}
			delete(m.data, entityID)
			return nil
		}

		// Normal flow
		m.data[entityID] = append(m.data[entityID], bytes)
		if len(dataSlice) >= BufferSize {
			for _, output := range m.data[entityID] {
				_, err := dest.Write(formatWithID(entityID, output))
				if err != nil {
					return nil
				}
			}
			delete(m.data, entityID)
		}
	} else {
		m.data[entityID] = append(m.data[entityID], bytes)
	}

	return nil
}

func formatWithID(entityID uuid.UUID, output []byte) []byte {
	return []byte(fmt.Sprintf("ID: %s, %s", entityID, output))
}
