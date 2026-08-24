package organizer

import (
	"context"
	"github.com/f2e/f2e/internal/adapter/inbound/s3event"
	"github.com/f2e/f2e/internal/platform/config"
	"io"
	"testing"
)

func TestJobs(t *testing.T) {
	s := Service{Store: fakeHead{}, Config: config.Config{RecordLength: 10, RecordsPerChunk: 10}}
	b := []byte(`{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"f2e-input"},"object":{"key":"input%2Fa+b.txt"}}}]}`)
	references, e := s3event.Parse(b)
	if e != nil {
		t.Fatal(e)
	}
	j, e := s.Jobs(context.Background(), references)
	if e != nil || len(j) != 3 || j[2].RecordCount != 5 || j[1].StartByte != 100 || j[2].EndByteInclusive != 249 {
		t.Fatalf("jobs=%+v err=%v", j, e)
	}
}

type fakeHead struct{}

func (fakeHead) Head(context.Context, string, string) (int64, string, error) { return 250, "etag", nil }
func (fakeHead) GetRange(context.Context, string, string, int64, int64) (io.ReadCloser, error) {
	return nil, nil
}
