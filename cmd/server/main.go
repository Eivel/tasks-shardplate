package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"shardplate/internal/buffer"
	"shardplate/internal/payload"
	"shardplate/internal/queue"
	"shardplate/internal/stats"
	"shardplate/internal/storage"
	"shardplate/internal/workerpool"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
)

const (
	exitCodeErr       = 1
	exitCodeInterrupt = 2
	NumShards         = 5
)

func main() {
	// Creates queue and starts it.
	queueCtx, queueCtxCancelFunc := context.WithCancel(context.Background())
	defer queueCtxCancelFunc()
	q := queue.NewShardedQueue(NumShards)
	go q.Start(queueCtx)

	// Creates and starts worker pools, one for each queue shard.
	wpCtxs := [NumShards]struct {
		ctx        context.Context
		cancelFunc context.CancelFunc
	}{}
	for i := 0; i < NumShards; i++ {
		wpCtxs[i].ctx, wpCtxs[i].cancelFunc = context.WithCancel(context.Background())

		workersLogStorage := storage.NewLocalStorage("./workersOutput")
		wpCtxs[i].ctx = context.WithValue(wpCtxs[i].ctx, "workersLogStorage", workersLogStorage)

		workersStatsStorage := storage.NewLocalStorage("./workersStats")
		wpCtxs[i].ctx = context.WithValue(wpCtxs[i].ctx, "workersStatsStorage", workersStatsStorage)

		memoryBuffer := buffer.NewMemoryBuffer()
		wpCtxs[i].ctx = context.WithValue(wpCtxs[i].ctx, "memoryBuffer", memoryBuffer)

		summary := stats.NewSummary()
		wpCtxs[i].ctx = context.WithValue(wpCtxs[i].ctx, "summary", summary)

		workerFunc := func(ctx context.Context, data payload.Identifiable, workerID uuid.UUID) {
			fmt.Printf("Worker %s processing data: timestamp %s, value %+v\n", workerID, data.Payload().(payload.Payload).Timestamp, data.Payload().(payload.Payload).Value)
			bytes, err := json.Marshal(data.Payload().(payload.Payload))
			if err != nil {
				log.Printf("worker %s raised json marshalling error: %s", workerID, err)
			}
			workersLogStorage := ctx.Value("workersLogStorage").(io.Writer)
			err = ctx.Value("memoryBuffer").(buffer.Writer).Write(workersLogStorage, bytes, workerID)
			if err != nil {
				log.Printf("worker %s raised file write error: %s", workerID, err)
			}

			ctx.Value("summary").(stats.Incrementer).Increment(workerID)
			fmt.Printf("Worker %s finished processing data: timestamp %s, value %+v\n", workerID, data.Payload().(payload.Payload).Timestamp, data.Payload().(payload.Payload).Value)
		}

		finalizeWorkerFunc := func(ctx context.Context, workerID uuid.UUID) {
			workersLogStorage := ctx.Value("workersLogStorage").(io.Writer)
			err := ctx.Value("memoryBuffer").(buffer.Writer).Write(workersLogStorage, []byte{}, workerID)
			if err != nil {
				log.Printf("worker %s raised logs write error during finalization: %s", workerID, err)
			}

			workersStatsStorage := ctx.Value("workersStatsStorage").(io.Writer)
			err = ctx.Value("summary").(stats.Writer).Write(workerID, workersStatsStorage)
			if err != nil {
				log.Printf("worker %s raised stats write error during finalization: %s", workerID, err)
			}
		}

		wpConfig := workerpool.Config{
			StartingWorkersNumber: 3,
			MaxWorkersNumber:      4,
			WorkerTTL:             12 * time.Second,
			AutoscalingInterval:   1 * time.Second,
			WorkerFunc:            workerFunc,
			FinalizeWorkerFunc:    finalizeWorkerFunc,
		}

		wp := workerpool.WorkerPool{}
		dataChan, confirmationChan := q.Consume(i)
		wp.Init(wpConfig, dataChan, confirmationChan)
		go wp.Start(wpCtxs[i].ctx)
	}

	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	e.GET("/collect", func(c echo.Context) error {
		data := []payload.Payload{}
		if err := c.Bind(&data); err != nil {
			fmt.Println(err)
			return echo.NewHTTPError(http.StatusBadRequest, "incorrect data format")
		}

		for index := range data {
			id := uuid.New()
			data[index].Identifier = &id

			if err := q.Push(&data[index]); err != nil {
				fmt.Println(err)
				return echo.NewHTTPError(http.StatusInternalServerError, "error handling the request")
			}
		}

		return c.JSON(http.StatusOK, data)
	})

	go func() {
		if err := e.Start(":1323"); err != nil && err != http.ErrServerClosed {
			e.Logger.Fatal("shutting down the server")
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	serverCtx, serverCancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	defer serverCancelFunc()
	if err := e.Shutdown(serverCtx); err != nil {
		e.Logger.Fatal(err)
	}
	for _, wpCtx := range wpCtxs {
		wpCtx.cancelFunc()
	}
	queueCtxCancelFunc()

	for runtime.NumGoroutine() > 2 {
		time.Sleep(1 * time.Second)
	}
	fmt.Println("Exiting...")
}
