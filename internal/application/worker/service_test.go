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

func TestJSONLValidationAndBypass(t *testing.T) {
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", RecordCount: 1, StartByte: 0, EndByteInclusive: 8, DataType: f2e.DataTypeJSONL}
	body, _ := json.Marshal(job)
	if err := (Service{Store: store{"{bad}\n"}, Queue: &queue{}, Config: config.Config{OutputQueueURL: "out"}}).Process(context.Background(), body); err == nil {
		t.Fatal("expected invalid JSON error")
	}
	job.Options.BypassJSONValidation = true
	body, _ = json.Marshal(job)
	q := &queue{}
	if err := (Service{Store: store{"{bad}\n"}, Queue: q, Config: config.Config{OutputQueueURL: "out"}}).Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(q.batches) != 1 || len(q.batches[0]) != 1 {
		t.Fatalf("batches=%v", q.batches)
	}
}

func TestVariableChunksDoNotDuplicateRecordsInPadding(t *testing.T) {
	q := &queue{}
	s := Service{Store: rangedStore{"aa\nbbb\ncccc\nd\n"}, Queue: q, Config: config.Config{OutputQueueURL: "out"}}
	jobs := []f2e.ChunkJob{
		{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", StartByte: 0, EndByteInclusive: 11, MaxRecordLengthBytes: 5, TrailingPaddingBytes: 2, DataType: f2e.DataTypeText},
		{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "2", Bucket: "b", Key: "k", StartByte: 10, EndByteInclusive: 13, MaxRecordLengthBytes: 5, DataType: f2e.DataTypeText},
	}
	for _, job := range jobs {
		body, _ := json.Marshal(job)
		if err := s.Process(context.Background(), body); err != nil {
			t.Fatal(err)
		}
	}
	var raws []string
	for _, batch := range q.batches {
		for _, body := range batch {
			var event f2e.OutputEvent
			_ = json.Unmarshal([]byte(body), &event)
			raws = append(raws, event.Payload.Raw)
		}
	}
	if strings.Join(raws, ",") != "aa,bbb,cccc,d" {
		t.Fatalf("records=%v", raws)
	}
}

type rangedStore struct{ data string }

func (s rangedStore) Head(context.Context, string, string) (int64, string, error) {
	return int64(len(s.data)), "", nil
}
func (s rangedStore) GetRange(_ context.Context, _ string, _ string, start, end int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data[start : end+1])), nil
}
