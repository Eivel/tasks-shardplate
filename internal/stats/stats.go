package stats

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
)

var (
	ErrPartialWrite = errors.New("some of the data was not saved to disk")
)

type Incrementer interface {
	Increment(entityID uuid.UUID)
}

type Writer interface {
	Write(entityID uuid.UUID, dest io.Writer) error
}

// Summary stores the metadata of the requests processed by each worker.
type Summary struct {
	mutex sync.Mutex // TODO privatize in the rest of the packages
	data  map[uuid.UUID]int
}

func NewSummary() *Summary {
	return &Summary{data: make(map[uuid.UUID]int)}
}

func (s *Summary) Increment(entityID uuid.UUID) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.data[entityID]; ok {
		s.data[entityID]++
	} else {
		s.data[entityID] = 1
	}
}

// Write stores the data into the dest io.Writer. The Write method should be executed on the worker's finalization.
func (s *Summary) Write(entityID uuid.UUID, dest io.Writer) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if count, ok := s.data[entityID]; ok {
		output := fmt.Sprintf("ID: %s, RequestsProcessed: %d", entityID, count)
		n, err := dest.Write([]byte(output))
		if n != len(output) {
			return ErrPartialWrite
		}
		return err
	}
	return nil
}
