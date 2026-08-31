package worker

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

type store struct{ data string }

func (s store) Head(context.Context, string, string) (int64, string, string, error) {
	return 0, "", "", nil
}
func (s store) GetRange(context.Context, string, string, int64, int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data)), nil
}

type queue struct{ batches [][]string }

func (q *queue) Send(_ context.Context, _ string, b []string, _ map[string]port.MessageAttribute) ([]int, error) {
	q.batches = append(q.batches, append([]string(nil), b...))
	return nil, nil
}
func TestProcessBatchesAndStableIDs(t *testing.T) {
	q := &queue{}
	s := Service{Store: store{"aaa\nbbb\nccc\n"}, Queue: q, Config: config.Config{RecordLength: 4, OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	j := f2e.ChunkJob{SchemaVersion: "1", FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", RecordCount: 3, RecordLengthBytes: 4, EndByteInclusive: 11}
	b, _ := json.Marshal(j)
	if e := s.Process(context.Background(), b); e != nil {
		t.Fatal(e)
	}
	if len(q.batches) != 1 || len(q.batches[0]) != 3 {
		t.Fatalf("batches=%v", q.batches)
	}
	var env f2e.Envelope[f2e.RecordPayload]
	json.Unmarshal([]byte(q.batches[0][0]), &env)
	if env.Processing.RecordNumber == nil || *env.Processing.RecordNumber != 1 || env.Metadata.EventID == "" {
		t.Fatalf("env=%+v", env)
	}
	if env.Source.Bucket != "b" || env.Source.Key != "k" || env.Source.FileName != "k" {
		t.Fatalf("source=%+v", env.Source)
	}
	if env.Processing.JobID != "j" || env.Processing.ChunkID != "00000001" {
		t.Fatalf("processing=%+v", env.Processing)
	}
	if env.Metadata.Schema.ID != "test" || env.Metadata.Schema.Version != "1" || env.Metadata.Format != "json" {
		t.Fatalf("metadata=%+v", env.Metadata)
	}
}

func TestJSONLValidationAndBypass(t *testing.T) {
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", RecordCount: 1, StartByte: 0, EndByteInclusive: 8, DataType: f2e.DataTypeJSONL}
	body, _ := json.Marshal(job)
	if err := (Service{Store: store{"{bad}\n"}, Queue: &queue{}, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}).Process(context.Background(), body); err == nil {
		t.Fatal("expected invalid JSON error")
	}
	job.Options.BypassJSONValidation = true
	body, _ = json.Marshal(job)
	q := &queue{}
	if err := (Service{Store: store{"{bad}\n"}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}).Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(q.batches) != 1 || len(q.batches[0]) != 1 {
		t.Fatalf("batches=%v", q.batches)
	}
}

func TestVariableChunksDoNotDuplicateRecordsInPadding(t *testing.T) {
	q := &queue{}
	s := Service{Store: rangedStore{"aa\nbbb\ncccc\nd\n"}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
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
			var env f2e.Envelope[f2e.RecordPayload]
			_ = json.Unmarshal([]byte(body), &env)
			raws = append(raws, env.Data.Raw)
		}
	}
	if strings.Join(raws, ",") != "aa,bbb,cccc,d" {
		t.Fatalf("records=%v", raws)
	}
}

type rangedStore struct{ data string }

func (s rangedStore) Head(context.Context, string, string) (int64, string, string, error) {
	return int64(len(s.data)), "", "", nil
}
func (s rangedStore) GetRange(_ context.Context, _ string, _ string, start, end int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data[start : end+1])), nil
}

func TestMultiLineChunkEmitsJoinedRecords(t *testing.T) {
	// Single chunk covering the whole file; no padding.
	data := "1abc\n2def\n1xyz\n2uvw\n"
	q := &queue{}
	s := Service{Store: rangedStore{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
	layout := f2e.MultiLineLayout{BreakMarker: "1", AcceptedPrefixes: []string{"2"}, LineSeparator: "\x1C", MaxBytesPerRecord: 10}
	job := f2e.ChunkJob{
		SchemaVersion:        f2e.SchemaVersion,
		FileID:               "f",
		JobID:                "j",
		ChunkID:              "00000001",
		Bucket:               "b",
		Key:                  "k",
		StartByte:            0,
		EndByteInclusive:     19,
		MaxRecordLengthBytes: 10,
		DataType:             f2e.DataTypeMultiLine,
		MultiLineLayout:      layout,
	}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	var raws []string
	for _, batch := range q.batches {
		for _, b := range batch {
			var env f2e.Envelope[f2e.RecordPayload]
			_ = json.Unmarshal([]byte(b), &env)
			raws = append(raws, env.Data.Raw)
		}
	}
	if len(raws) != 2 {
		t.Fatalf("expected 2 records, got %d: %v", len(raws), raws)
	}
	if raws[0] != "1abc\x1C2def" || raws[1] != "1xyz\x1C2uvw" {
		t.Fatalf("records=%v", raws)
	}
}

func TestMultiLineChunksDoNotDuplicateRecords(t *testing.T) {
	// Two chunks; chunk 1 owns bytes [0,9] with trailing padding 3 (ends at byte 12).
	// Chunk 2 starts at byte 10, re-reads from byte 0, but only owns bytes [10,19].
	//
	//   Bytes 0-4:   "1abc\n"
	//   Bytes 5-9:   "2def\n"
	//   Bytes 10-14: "1xyz\n"
	//   Bytes 15-19: "2uvw\n"
	data := "1abc\n2def\n1xyz\n2uvw\n"
	layout := f2e.MultiLineLayout{BreakMarker: "1", AcceptedPrefixes: []string{"2"}, LineSeparator: "\x1C", MaxBytesPerRecord: 10}
	jobs := []f2e.ChunkJob{
		{
			SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1",
			Bucket: "b", Key: "k",
			StartByte: 0, EndByteInclusive: 9,
			MaxRecordLengthBytes: 10, TrailingPaddingBytes: 0,
			DataType: f2e.DataTypeMultiLine, MultiLineLayout: layout,
		},
		{
			SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "2",
			Bucket: "b", Key: "k",
			StartByte: 10, EndByteInclusive: 19,
			MaxRecordLengthBytes: 10, TrailingPaddingBytes: 0,
			DataType: f2e.DataTypeMultiLine, MultiLineLayout: layout,
		},
	}
	q := &queue{}
	s := Service{Store: rangedStore{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
	for _, job := range jobs {
		body, _ := json.Marshal(job)
		if err := s.Process(context.Background(), body); err != nil {
			t.Fatal(err)
		}
	}
	var raws []string
	for _, batch := range q.batches {
		for _, b := range batch {
			var env f2e.Envelope[f2e.RecordPayload]
			_ = json.Unmarshal([]byte(b), &env)
			raws = append(raws, env.Data.Raw)
		}
	}
	if strings.Join(raws, ",") != "1abc\x1C2def,1xyz\x1C2uvw" {
		t.Fatalf("records=%v", raws)
	}
}
