package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"shardplate/internal/payload"
	"sync"
	"time"
)

var ErrQueueEmpty = errors.New("queue is empty")

type internalData struct {
	Element  payload.Identifiable
	Reserved bool
}

type ShardedQueue struct {
	Mutex        sync.Mutex
	Shards       []Shard
	ShardsNumber int
}

type Shard struct {
	Mutex                    sync.Mutex
	Data                     []internalData
	outgoingDataChan         chan payload.Identifiable
	incomingConfirmationChan chan payload.Identifiable
}

func NewShard() *Shard {
	return &Shard{outgoingDataChan: make(chan payload.Identifiable), incomingConfirmationChan: make(chan payload.Identifiable), Data: []internalData{}}
}

func NewShardedQueue(shardsNumber int) *ShardedQueue {
	shards := make([]Shard, 0, shardsNumber)
	for i := 0; i < shardsNumber; i++ {
		shards = append(shards, *NewShard())
	}
	shardedQueue := &ShardedQueue{Shards: shards, ShardsNumber: shardsNumber}

	return shardedQueue
}

func (sq *ShardedQueue) Push(queueElement payload.Identifiable) error {
	sq.Shards[0].Mutex.Lock()
	defer sq.Shards[0].Mutex.Unlock()

	// fmt.Println("Push locked")
	// defer fmt.Println("Push unlocked")
	minShardSize := struct {
		size  int
		index int
	}{
		size:  len(sq.Shards[0].Data),
		index: 0,
	}

	for i := 1; i < sq.ShardsNumber; i++ {
		sq.Shards[i].Mutex.Lock()
		defer sq.Shards[i].Mutex.Unlock()
		if minShardSize.size > len(sq.Shards[i].Data) {
			minShardSize.size = len(sq.Shards[i].Data)
			minShardSize.index = i
		}
	}

	sq.Shards[minShardSize.index].Data = append(sq.Shards[minShardSize.index].Data, internalData{Element: queueElement})

	return nil
}

func (sq *ShardedQueue) Len(shardNumber int) int {
	sq.Shards[shardNumber].Mutex.Lock()
	defer sq.Shards[shardNumber].Mutex.Unlock()

	return len(sq.Shards[shardNumber].Data)
}

// TODO issues: long search for the reservations
func (sq *ShardedQueue) Pop(shardNumber int) (payload.Identifiable, error) {
	// fmt.Println("Pop locking")
	// defer fmt.Println("Pop unlocking")
	sq.Shards[shardNumber].Mutex.Lock()
	defer sq.Shards[shardNumber].Mutex.Unlock()

	if shardNumber < 0 || shardNumber >= sq.ShardsNumber {
		return nil, errors.New("the shard number is out of range")
	}

	shardLength := len(sq.Shards[shardNumber].Data)
	if shardLength == 0 {
		return nil, ErrQueueEmpty
	}

	for i := 0; i < len(sq.Shards[shardNumber].Data); i++ {
		if !sq.Shards[shardNumber].Data[i].Reserved {
			sq.Shards[shardNumber].Data[i].Reserved = true
			return sq.Shards[shardNumber].Data[i].Element, nil
		}
	}

	return nil, ErrQueueEmpty
}

func (sq *ShardedQueue) Restore(shardNumber int, queueElement payload.Identifiable) error {
	sq.Shards[shardNumber].Mutex.Lock()
	defer sq.Shards[shardNumber].Mutex.Unlock()

	if shardNumber < 0 || shardNumber >= sq.ShardsNumber {
		return errors.New("the shard number is out of range")
	}

	for index, el := range sq.Shards[shardNumber].Data {
		if el.Element.ID() == queueElement.ID() && el.Reserved {
			sq.Shards[shardNumber].Data[index].Reserved = false
		}
	}

	return nil
}

func (sq *ShardedQueue) Free(shardNumber int, queueElement payload.Identifiable) error {
	sq.Shards[shardNumber].Mutex.Lock()
	defer sq.Shards[shardNumber].Mutex.Unlock()

	if shardNumber < 0 || shardNumber >= sq.ShardsNumber {
		return errors.New("the shard number is out of range")
	}

	for index, el := range sq.Shards[shardNumber].Data {
		if el.Element.ID() == queueElement.ID() && el.Reserved {
			sq.Shards[shardNumber].Data = append(sq.Shards[shardNumber].Data[:index], sq.Shards[shardNumber].Data[index+1:]...)
			return nil
		}
	}

	return nil
}

func (sq *ShardedQueue) Start(ctx context.Context) {
	wg := &sync.WaitGroup{}
	for index := range sq.Shards {
		wg.Add(2)
		go sq.Produce(ctx, index, wg)
		go sq.Confirm(ctx, index, wg)
	}
	wg.Wait()
	fmt.Println("Exiting queue")
}

func (sq *ShardedQueue) Confirm(ctx context.Context, shardNumber int, wg *sync.WaitGroup) {
	go func(ctx context.Context, sq *ShardedQueue, shardNumber int, wg *sync.WaitGroup) {
		for {
			select {
			case confirmedData := <-sq.Shards[shardNumber].incomingConfirmationChan:
				err := sq.Free(shardNumber, confirmedData)
				if err != nil {
					log.Println(err)
					return
				}

			case <-ctx.Done():
				// Serialize reserved and unhandled elements to disk/db
				wg.Done()
				return
			}

		}
	}(ctx, sq, shardNumber, wg)
}

func (sq *ShardedQueue) Produce(ctx context.Context, shardNumber int, wg *sync.WaitGroup) {
	go func(ctx context.Context, sq *ShardedQueue, shardNumber int, wg *sync.WaitGroup) {
		for {
			select {
			case <-time.After(1 * time.Millisecond):
				queueElement, err := sq.Pop(shardNumber)
				if err == ErrQueueEmpty {
					continue
				} else if err != nil {
					log.Println(err)
					wg.Done()
					fmt.Println("Exiting producer")
					return
				}
				fmt.Printf("Popped %+v\n", queueElement)

				sq.Shards[shardNumber].outgoingDataChan <- queueElement

				fmt.Printf("Sent %+v\n", queueElement)

			case <-ctx.Done():
				// Serialize queued and unhandled elements to disk/db
				wg.Done()
				fmt.Println("Exiting producer")
				return
			}

		}
	}(ctx, sq, shardNumber, wg)
}

// TODO ideally that would register a consumer and create new channels for it, but I'm doing shortcuts
func (sq *ShardedQueue) Consume(shardNumber int) (chan payload.Identifiable, chan payload.Identifiable) {
	return sq.Shards[shardNumber].outgoingDataChan, sq.Shards[shardNumber].incomingConfirmationChan
}
