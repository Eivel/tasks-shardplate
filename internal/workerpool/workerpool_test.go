package workerpool

import (
	"context"
	"shardplate/internal/payload"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWorkerPool_Start(t *testing.T) {
	type fields struct {
		Config        Config
		internalStats internalStats
		data          payload.Identifiable
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		expectedErr error
	}{
		{
			name: "processes the message",
			fields: fields{
				Config: Config{
					StartingWorkersNumber: 1,
					MaxWorkersNumber:      4,
					WorkerTTL:             1 * time.Minute,
					AutoscalingInterval:   1 * time.Second,
					WorkerFunc: func(ctx context.Context, data payload.Identifiable, workerID uuid.UUID) {
						return
					},
					FinalizeWorkerFunc: func(ctx context.Context, workerID uuid.UUID) {
						return
					},
				},
				internalStats: internalStats{},
				data: payload.Payload{
					Identifier: func() *uuid.UUID {
						id := uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9")
						return &id
					}(),
					Timestamp: "1.2.2023",
					Value:     nil,
				},
			},
			args: args{
				ctx: context.Background(),
			},
			expectedErr: nil,
		},
		{
			name: "incorrect config",
			fields: fields{
				Config: Config{
					StartingWorkersNumber: -1,
					MaxWorkersNumber:      4,
					WorkerTTL:             1 * time.Minute,
					AutoscalingInterval:   1 * time.Second,
					WorkerFunc: func(ctx context.Context, data payload.Identifiable, workerID uuid.UUID) {
						return
					},
					FinalizeWorkerFunc: func(ctx context.Context, workerID uuid.UUID) {
						return
					},
				},
				internalStats: internalStats{},
				data: payload.Payload{
					Identifier: func() *uuid.UUID {
						id := uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9")
						return &id
					}(),
					Timestamp: "1.2.2023",
					Value:     nil,
				},
			},
			args: args{
				ctx: context.Background(),
			},
			expectedErr: ErrConfigInvalid,
		},
		{
			name: "large workers values",
			fields: fields{
				Config: Config{
					StartingWorkersNumber: 1000,
					MaxWorkersNumber:      1000,
					WorkerTTL:             1 * time.Minute,
					AutoscalingInterval:   1 * time.Second,
					WorkerFunc: func(ctx context.Context, data payload.Identifiable, workerID uuid.UUID) {
						return
					},
					FinalizeWorkerFunc: func(ctx context.Context, workerID uuid.UUID) {
						return
					},
				},
				internalStats: internalStats{},
				data: payload.Payload{
					Identifier: func() *uuid.UUID {
						id := uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9")
						return &id
					}(),
					Timestamp: "1.2.2023",
					Value:     nil,
				},
			},
			args: args{
				ctx: context.Background(),
			},
			expectedErr: nil,
		},
		// TODO: more test cases for the failing conditions and different missing attributes
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataChan, confirmationChan := make(chan payload.Identifiable), make(chan payload.Identifiable)
			wp, err := NewWorkerPool(tt.fields.Config, dataChan, confirmationChan)
			if err != tt.expectedErr {
				t.Errorf("WorkerPool.Start() error = %v, expectedErr %v", err, tt.expectedErr)
				return
			}
			if err != nil {
				return
			}
			go wp.Start(tt.args.ctx)
			// TODO: resolve the issue with failing channels stopping the whole test suite
			dataChan <- tt.fields.data
			<-confirmationChan
		})
	}
}
