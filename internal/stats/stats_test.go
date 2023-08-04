package stats

import (
	"bytes"
	"io"
	"testing"
	"testing/iotest"

	"github.com/google/uuid"
)

// TODO: Use the mocking library for easier creation of test cases
type failureWriter struct{}

func (t *failureWriter) Write(p []byte) (int, error) {
	return 0, ErrPartialWrite
}

func TestSummary_Write(t *testing.T) {
	type fields struct {
		data map[uuid.UUID]int
	}
	type args struct {
		entityID uuid.UUID
		dest     io.Writer
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "saves the stats successfully",
			fields: fields{
				data: func() map[uuid.UUID]int {
					d := make(map[uuid.UUID]int)
					d[uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9")] = 3
					return d
				}(),
			},
			args: args{
				entityID: uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9"),
				dest:     &bytes.Buffer{},
			},
			wantErr: false,
		},
		{
			name: "returns error if not the whole message got saved",
			fields: fields{
				data: func() map[uuid.UUID]int {
					d := make(map[uuid.UUID]int)
					d[uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9")] = 3
					return d
				}(),
			},
			args: args{
				entityID: uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9"),
				dest:     iotest.TruncateWriter(&failureWriter{}, 2),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Summary{data: tt.fields.data}
			err := s.Write(tt.args.entityID, tt.args.dest)
			if (err != nil) != tt.wantErr {
				t.Errorf("Summary.Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestSummary_Increment(t *testing.T) {
	type fields struct {
		data map[uuid.UUID]int
	}
	type args struct {
		entityID uuid.UUID
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		expectedNum int
	}{
		{
			name: "increments the stats successfully",
			fields: fields{
				data: func() map[uuid.UUID]int {
					d := make(map[uuid.UUID]int)
					d[uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9")] = 1
					return d
				}(),
			},
			args: args{
				entityID: uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9"),
			},
			expectedNum: 2,
		},
		{
			name: "when ID is not in the stats, creates it with counter equals 1",
			fields: fields{
				data: make(map[uuid.UUID]int),
			},
			args: args{
				entityID: uuid.MustParse("d69ad8d4-b637-4d08-9571-a7b6676b5ae9"),
			},
			expectedNum: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Summary{data: tt.fields.data}
			s.Increment(tt.args.entityID)
			if tt.expectedNum != tt.fields.data[tt.args.entityID] {
				t.Errorf("Summary.Increment() counter = %v, expected %v", tt.fields.data[tt.args.entityID], tt.expectedNum)
				return
			}
		})
	}
}
