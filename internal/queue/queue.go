package queue

import (
	"context"
	"errors"
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

// ShardedQueue is a simple implementation of a concurrent safe queue with the use of Go's slices.
// The implementation is highly inefficient and serves only a demonstrational purpose.
// In the production uses it's recommended to use robust solutions such as RabbitMQ.
type ShardedQueue struct {
	mutex        sync.Mutex
	Shards       []Shard
	ShardsNumber int
}

// Shard represents a single subqueue. Such separation allows to use different data sharding algorithms and
// connect different worker pools to different shards.
type Shard struct {
	mutex                    sync.Mutex
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

// Push contains a simple logic of sharding the incoming data (based on the smallest shard) and
// stores it into the subqueue.
func (sq *ShardedQueue) Push(queueElement payload.Identifiable) error {
	sq.Shards[0].mutex.Lock()
	defer sq.Shards[0].mutex.Unlock()

	minShardSize := struct {
		size  int
		index int
	}{
		size:  len(sq.Shards[0].Data),
		index: 0,
	}

	for i := 1; i < sq.ShardsNumber; i++ {
		sq.Shards[i].mutex.Lock()
		defer sq.Shards[i].mutex.Unlock()
		if minShardSize.size > len(sq.Shards[i].Data) {
			minShardSize.size = len(sq.Shards[i].Data)
			minShardSize.index = i
		}
	}

	sq.Shards[minShardSize.index].Data = append(sq.Shards[minShardSize.index].Data, internalData{Element: queueElement})

	return nil
}

// Len returns the length of the shard's subqueue.
func (sq *ShardedQueue) Len(shardNumber int) int {
	sq.Shards[shardNumber].mutex.Lock()
	defer sq.Shards[shardNumber].mutex.Unlock()

	return len(sq.Shards[shardNumber].Data)
}

// Pop reserves a resource, leaving it in the queue for final confirmation that
// the resource has been processed successfully.
func (sq *ShardedQueue) Pop(shardNumber int) (payload.Identifiable, error) {
	sq.Shards[shardNumber].mutex.Lock()
	defer sq.Shards[shardNumber].mutex.Unlock()

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

// Restore undos the reservation made by Pop method, virtually "returning" the resource to be
// taken by a different worker.
// TODO: a separate process that restores resources reserved for too long.
func (sq *ShardedQueue) Restore(shardNumber int, queueElement payload.Identifiable) error {
	sq.Shards[shardNumber].mutex.Lock()
	defer sq.Shards[shardNumber].mutex.Unlock()

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

// Free hard removes the resource from the queue.
func (sq *ShardedQueue) Free(shardNumber int, queueElement payload.Identifiable) error {
	sq.Shards[shardNumber].mutex.Lock()
	defer sq.Shards[shardNumber].mutex.Unlock()

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

// Start runs the production and confirmation loops that explose the resources on the proper channels.
func (sq *ShardedQueue) Start(ctx context.Context) {
	wg := &sync.WaitGroup{}
	for index := range sq.Shards {
		wg.Add(2)
		go sq.Produce(ctx, index, wg)
		go sq.Confirm(ctx, index, wg)
	}
	wg.Wait()
	log.Println("exiting queue")
}

// Confirm takes the signals of properly processed resources and removes them from the queue permanently.
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
				// TODO: Serialize reserved and unhandled elements to disk/db
				wg.Done()
				return
			}

		}
	}(ctx, sq, shardNumber, wg)
}

// Produce takes the resources from the queue and exposes them to the workers via channels.
// It uses a soft removal by Pop method, only marking the resources as reserved.
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
					log.Println("exiting queue producer")
					return
				}
				sq.Shards[shardNumber].outgoingDataChan <- queueElement

			case <-ctx.Done():
				// Serialize queued and unhandled elements to disk/db
				wg.Done()
				log.Println("exiting queue producer")
				return
			}

		}
	}(ctx, sq, shardNumber, wg)
}

// Consume returns the channels for workers to use.
// TODO ideally that would register a consumer and create new channels for it, but I'm doing shortcuts
func (sq *ShardedQueue) Consume(shardNumber int) (chan payload.Identifiable, chan payload.Identifiable) {
	return sq.Shards[shardNumber].outgoingDataChan, sq.Shards[shardNumber].incomingConfirmationChan
}
