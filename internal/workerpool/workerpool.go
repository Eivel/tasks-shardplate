package workerpool

import (
	"context"
	"fmt"
	"log"
	"shardplate/internal/payload"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type WorkerPoolService interface {
	Init(config Config, incomindEventsChan, signalEventProcessedChan payload.Identifiable)
	Start(ctx context.Context, workerFunc func(data payload.Identifiable, workerID uuid.UUID))
}

type WorkerPool struct {
	Config
	internalStats
}

type Config struct {
	StartingWorkersNumber int
	MaxWorkersNumber      int
	WorkerTTL             time.Duration
	AutoscalingInterval   time.Duration
	WorkerFunc            func(ctx context.Context, data payload.Identifiable, workerID uuid.UUID)
	FinalizeWorkerFunc    func(ctx context.Context, workerID uuid.UUID)
}

type internalStats struct {
	workersNumber           int64
	workersIdle             int64
	incomingEventChan       chan payload.Identifiable
	workersEventChan        chan payload.Identifiable
	signalEvenProcessedChan chan payload.Identifiable
}

func (wp *WorkerPool) Init(config Config, incomingEventsChan, signalEventProcessedChan chan payload.Identifiable) {
	wp.Config = config
	wp.incomingEventChan = incomingEventsChan
	wp.signalEvenProcessedChan = signalEventProcessedChan
}

func (wp *WorkerPool) Start(ctx context.Context) {
	workerCtx, workerCtxCancelFunc := context.WithDeadline(ctx, time.Now().Add(wp.Config.WorkerTTL))
	wg := &sync.WaitGroup{}

	for i := 0; i < wp.Config.StartingWorkersNumber; i++ {
		fmt.Println("starting worker")
		wg.Add(1)
		workerID := uuid.New()
		wp.startWorker(workerCtx, wg, workerID)
		atomic.AddInt64(&wp.internalStats.workersIdle, 1)
		atomic.AddInt64(&wp.internalStats.workersNumber, 1)
	}

	for {
		select {
		case <-time.After(wp.Config.AutoscalingInterval):
			fmt.Printf("Workers number: %d, Workers idle: %d\n", wp.internalStats.workersNumber, wp.internalStats.workersIdle)
			if wp.internalStats.workersIdle == 0 && wp.internalStats.workersNumber < int64(wp.MaxWorkersNumber) {
				fmt.Println("starting additional worker")
				wg.Add(1)
				workerID := uuid.New()
				wp.startWorker(workerCtx, wg, workerID)
				atomic.AddInt64(&wp.internalStats.workersIdle, 1)
				atomic.AddInt64(&wp.internalStats.workersNumber, 1)
			} else if wp.internalStats.workersNumber < 1 {
				fmt.Println("starting additional worker")
				wg.Add(1)
				workerID := uuid.New()
				wp.startWorker(workerCtx, wg, workerID)
				atomic.AddInt64(&wp.internalStats.workersIdle, 1)
				atomic.AddInt64(&wp.internalStats.workersNumber, 1)
			}
		case <-ctx.Done():
			log.Println("Worker Pool received a cancellation signal, shutting down")
			workerCtxCancelFunc()
			log.Println("Worker Pool is waiting for the workers to shut down")
			wg.Wait()
			log.Println("Worker Pool finished waiting for the workers, all are closed")
			log.Println("Exiting the Pool")

			return
		}
	}
}

func (wp *WorkerPool) startWorker(ctx context.Context, wg *sync.WaitGroup, workerID uuid.UUID) {
	// fmt.Println("entering worker scope")
	go func(workerCtx context.Context, incomingEventChan, signalEventProcessedChan chan payload.Identifiable) { // TODO channels needed here?
		for {
			select {
			case data := <-incomingEventChan:
				atomic.AddInt64(&wp.internalStats.workersIdle, -1)
				wp.WorkerFunc(ctx, data, workerID)
				wp.signalEvenProcessedChan <- data
				fmt.Println("Worker sent the signal")
				atomic.AddInt64(&wp.internalStats.workersIdle, 1)
			case <-workerCtx.Done():
				log.Printf("Received termination signal from the context, shutting down")
				if wp.Config.FinalizeWorkerFunc != nil {
					wp.Config.FinalizeWorkerFunc(ctx, workerID)
				}
				atomic.AddInt64(&wp.internalStats.workersIdle, -1)
				atomic.AddInt64(&wp.internalStats.workersNumber, -1)
				wg.Done()
				return
			}
		}
	}(ctx, wp.incomingEventChan, wp.signalEvenProcessedChan)
}
