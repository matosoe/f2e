package worker

import (
	"context"
	"encoding/json"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
	"io"
	"strings"
	"testing"
)

type store struct{ data string }

func (s store) Head(context.Context, string, string) (int64, string, error) { return 0, "", nil }
func (s store) GetRange(context.Context, string, string, int64, int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data)), nil
}

type queue struct{ batches [][]string }

func (q *queue) Send(_ context.Context, _ string, b []string) ([]int, error) {
	q.batches = append(q.batches, append([]string(nil), b...))
	return nil, nil
}
func TestProcessBatchesAndStableIDs(t *testing.T) {
	q := &queue{}
	s := Service{Store: store{"aaa\nbbb\nccc\n"}, Queue: q, Config: config.Config{RecordLength: 4, OutputQueueURL: "out"}}
	j := f2e.ChunkJob{SchemaVersion: "1", FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", RecordCount: 3, RecordLengthBytes: 4, EndByteInclusive: 11}
	b, _ := json.Marshal(j)
	if e := s.Process(context.Background(), b); e != nil {
		t.Fatal(e)
	}
	if len(q.batches) != 1 || len(q.batches[0]) != 3 {
		t.Fatalf("batches=%v", q.batches)
	}
	var e f2e.OutputEvent
	json.Unmarshal([]byte(q.batches[0][0]), &e)
	if e.RecordNumber != 1 || e.EventID == "" {
		t.Fatalf("event=%+v", e)
	}
}
