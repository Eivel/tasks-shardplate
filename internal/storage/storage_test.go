package storage

import (
	"fmt"
	"os"
	"testing"
)

func TestLocalStorage_Write(t *testing.T) {
	type fields struct {
		relativePath string
	}
	type args struct {
		p []byte
	}
	tests := []struct {
		name                string
		fields              fields
		args                args
		expectedBytesNumber int
		wantErr             bool
	}{
		{
			name: "saves the file successfully",
			fields: fields{
				relativePath: fmt.Sprintf("%s%c%s", t.TempDir(), os.PathSeparator, "testfile"),
			},
			args: args{
				p: []byte("teststring"),
			},
			expectedBytesNumber: 10,
			wantErr:             false,
		},
		{
			name: "incorrect path",
			fields: fields{
				relativePath: t.TempDir(),
			},
			args: args{
				p: []byte("teststring"),
			},
			expectedBytesNumber: 0,
			wantErr:             true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := NewLocalStorage(tt.fields.relativePath)
			got, err := l.Write(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("LocalStorage.Write() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.expectedBytesNumber {
				t.Errorf("LocalStorage.Write() = %v, expectedBytesNumber %v", got, tt.expectedBytesNumber)
				return
			}
		})
	}
}
