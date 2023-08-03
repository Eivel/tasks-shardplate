package stats

import (
	"fmt"
	"io"
	"sync"

	"github.com/google/uuid"
)

type Incrementer interface {
	Increment(entityID uuid.UUID)
}

type Writer interface {
	Write(entityID uuid.UUID, dest io.Writer) error
}

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

func (s *Summary) Write(entityID uuid.UUID, dest io.Writer) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	fmt.Printf("ID: %s\n", entityID)
	// for k, _ := range s.Data {
	// 	fmt.Println(k)
	// }
	if count, ok := s.data[entityID]; ok {
		output := fmt.Sprintf("ID: %s, RequestsProcessed: %d", entityID, count)
		_, err := dest.Write([]byte(output))
		return err
	}
	return nil
}
