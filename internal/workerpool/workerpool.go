package workerpool

import (
	"context"
	"errors"
	"log"
	"shardplate/internal/payload"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

var (
	ErrConfigInvalid = errors.New("config contains illegal values")
)

// WorkerPool is the root-level abstraction of a worker pool. It stores all the necessary information to run the workers.
type WorkerPool struct {
	Config
	internalStats
}

// Config stores the configuration information for the worker pool.
// StartingWorkersNumber should be greater than 0.
// MaxWorkersNumber configures how high the pool will scale with demand.
// WorkerTTL is the time after which the worker will gracefully shutdown.
// AutoscalingInterval is the time between each tick of the autoscaling algorithm.
// WorkerFunc stores the logic that workers will execute.
// FinalizeWorkerFunc stores the logic that workers will execute once before they finish work.
type Config struct {
	StartingWorkersNumber int
	MaxWorkersNumber      int
	WorkerTTL             time.Duration
	AutoscalingInterval   time.Duration
	WorkerFunc            func(ctx context.Context, data payload.Identifiable, workerID uuid.UUID)
	FinalizeWorkerFunc    func(ctx context.Context, workerID uuid.UUID)
}

type internalStats struct {
	workersNumber            int64
	workersIdle              int64
	incomingEventChan        chan payload.Identifiable
	workersEventChan         chan payload.Identifiable
	signalEventProcessedChan chan payload.Identifiable
}

func NewWorkerPool(config Config, incomingEventsChan, signalEventProcessedChan chan payload.Identifiable) (*WorkerPool, error) {
	if !configValid(config) {
		return nil, ErrConfigInvalid
	}

	return &WorkerPool{
		Config: config,
		internalStats: internalStats{
			incomingEventChan:        incomingEventsChan,
			signalEventProcessedChan: signalEventProcessedChan,
		},
	}, nil
}

func configValid(config Config) bool {
	if config.StartingWorkersNumber < 0 ||
		config.MaxWorkersNumber <= 0 ||
		config.WorkerTTL < 1*time.Second ||
		config.AutoscalingInterval < 1*time.Second ||
		config.MaxWorkersNumber < config.StartingWorkersNumber {
		return false
	}
	return true
}

// Start runs the worker pool and creates the work depending on the configuration. Upon cancelling with context it
// gracefully shuts down.
func (wp *WorkerPool) Start(ctx context.Context) {
	workerCtx, workerCtxCancelFunc := context.WithDeadline(ctx, time.Now().Add(wp.Config.WorkerTTL))
	wg := &sync.WaitGroup{}

	for i := 0; i < wp.Config.StartingWorkersNumber; i++ {
		log.Println("starting worker")
		wg.Add(1)
		workerID := uuid.New()
		wp.startWorker(workerCtx, wg, workerID)
		atomic.AddInt64(&wp.internalStats.workersIdle, 1)
		atomic.AddInt64(&wp.internalStats.workersNumber, 1)
	}

	for {
		select {
		case <-time.After(wp.Config.AutoscalingInterval):
			if wp.internalStats.workersIdle == 0 && wp.internalStats.workersNumber < int64(wp.MaxWorkersNumber) {
				log.Println("starting additional worker")
				wg.Add(1)
				workerID := uuid.New()
				wp.startWorker(workerCtx, wg, workerID)
				atomic.AddInt64(&wp.internalStats.workersIdle, 1)
				atomic.AddInt64(&wp.internalStats.workersNumber, 1)
			} else if wp.internalStats.workersNumber < 1 {
				log.Println("starting additional worker")
				wg.Add(1)
				workerID := uuid.New()
				wp.startWorker(workerCtx, wg, workerID)
				atomic.AddInt64(&wp.internalStats.workersIdle, 1)
				atomic.AddInt64(&wp.internalStats.workersNumber, 1)
			}
		case <-ctx.Done():
			log.Println("worker pool received a cancellation signal, shutting down")
			workerCtxCancelFunc()
			log.Println("worker pool is waiting for the workers to shut down")
			wg.Wait()
			log.Println("worker pool finished waiting for the workers, all are closed")
			log.Println("exiting the pool")

			return
		}
	}
}

func (wp *WorkerPool) startWorker(ctx context.Context, wg *sync.WaitGroup, workerID uuid.UUID) {
	go func(workerCtx context.Context, incomingEventChan, signalEventProcessedChan chan payload.Identifiable) { // TODO channels needed here?
		for {
			select {
			case data := <-incomingEventChan:
				atomic.AddInt64(&wp.internalStats.workersIdle, -1)
				wp.WorkerFunc(ctx, data, workerID)
				wp.signalEventProcessedChan <- data
				atomic.AddInt64(&wp.internalStats.workersIdle, 1)
			case <-workerCtx.Done():
				log.Printf("worker %s received termination signal from the context, shutting down\n", workerID)
				if wp.Config.FinalizeWorkerFunc != nil {
					wp.Config.FinalizeWorkerFunc(ctx, workerID)
				}
				atomic.AddInt64(&wp.internalStats.workersIdle, -1)
				atomic.AddInt64(&wp.internalStats.workersNumber, -1)
				wg.Done()
				return
			}
		}
	}(ctx, wp.incomingEventChan, wp.signalEventProcessedChan)
}
