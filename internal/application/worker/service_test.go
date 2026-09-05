package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/f2e/f2e/internal/application/organizer"
	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

type store struct{ data string }

func (s store) OpenChunkRange(_ context.Context, _ f2e.ChunkJob, _, _ int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data)), nil
}

type closeErrorStore struct{ data string }

type closeErrorReader struct{ io.Reader }

func (closeErrorReader) Close() error { return errors.New("close failed") }
func (s closeErrorStore) OpenChunkRange(_ context.Context, _ f2e.ChunkJob, _, _ int64) (io.ReadCloser, error) {
	return closeErrorReader{Reader: strings.NewReader(s.data)}, nil
}

type queue struct{ batches [][]string }

func (q *queue) Send(_ context.Context, _ string, messages []port.OutboundMessage) ([]int, error) {
	bodies := make([]string, len(messages))
	for i := range messages {
		bodies[i] = messages[i].Body
	}
	q.batches = append(q.batches, bodies)
	return nil, nil
}

type partialQueue struct {
	calls [][]string
}

type messageQueue struct{ messages []port.OutboundMessage }

func (q *messageQueue) Send(_ context.Context, _ string, messages []port.OutboundMessage) ([]int, error) {
	q.messages = append(q.messages, messages...)
	return nil, nil
}

type processorFunc func(context.Context, f2e.Envelope[f2e.RecordPayload]) (f2e.RecordDecision, error)

func (f processorFunc) Process(ctx context.Context, envelope f2e.Envelope[f2e.RecordPayload]) (f2e.RecordDecision, error) {
	return f(ctx, envelope)
}

func (q *partialQueue) Send(_ context.Context, _ string, messages []port.OutboundMessage) ([]int, error) {
	bodies := make([]string, len(messages))
	for i := range messages {
		bodies[i] = messages[i].Body
	}
	q.calls = append(q.calls, bodies)
	if len(q.calls) == 1 {
		return []int{1}, nil
	}
	return nil, nil
}

func TestPartialBatchRetriesOnlyFailedMessages(t *testing.T) {
	q := &partialQueue{}
	s := Service{Resolver: store{"aaa\nbbb\n"}, Queue: q, Config: config.Config{BatchSize: 2, OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 7, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(q.calls) != 2 || len(q.calls[0]) != 2 || len(q.calls[1]) != 1 || q.calls[1][0] != q.calls[0][1] {
		t.Fatalf("calls=%v", q.calls)
	}
}

func TestProcessReportsSourceCloseFailure(t *testing.T) {
	s := Service{Resolver: closeErrorStore{"aaa\n"}, Queue: &queue{}, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 3, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err == nil || !strings.Contains(err.Error(), "close source stream") {
		t.Fatalf("expected close error, got %v", err)
	}
}

func TestProcessHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := Service{Resolver: store{"aaa\n"}, Queue: &queue{}, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 3, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	body, _ := json.Marshal(job)
	if err := s.Process(ctx, body); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
}

func TestAttributesUseFinalEnvelopeAndCorporateContext(t *testing.T) {
	q := &messageQueue{}
	processor := processorFunc(func(_ context.Context, env f2e.Envelope[f2e.RecordPayload]) (f2e.RecordDecision, error) {
		env.Metadata.Schema.Version = "2"
		env.Metadata.Format = "application/json"
		return f2e.PublishRecord(env), nil
	})
	s := Service{Resolver: store{"aaa\n"}, Queue: q, Processor: processor, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 3, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4, Context: f2e.CorporateContext{TransactionID: "tx", CorrelationID: "corr", TraceID: "trace", SourceSystem: "erp"}}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(q.messages) != 1 || q.messages[0].Attributes["schema"].Value != "test:2" || q.messages[0].Attributes["format"].Value != "application/json" || q.messages[0].Attributes["transactionId"].Value != "tx" || q.messages[0].Attributes["sourceSystem"].Value != "erp" {
		t.Fatalf("unexpected message attributes: %+v", q.messages)
	}
}

func TestProcessorCannotRemoveTechnicalIdentity(t *testing.T) {
	processor := processorFunc(func(_ context.Context, env f2e.Envelope[f2e.RecordPayload]) (f2e.RecordDecision, error) {
		env.Metadata.SourceRecordID = ""
		return f2e.PublishRecord(env), nil
	})
	s := Service{Resolver: store{"aaa\n"}, Queue: &queue{}, Processor: processor, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 3, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err == nil || !strings.Contains(err.Error(), "mandatory technical fields") {
		t.Fatalf("expected processor invariant error, got %v", err)
	}
}

func TestProcessorDecisionsCountPublishRejectAndIgnore(t *testing.T) {
	q := &messageQueue{}
	processor := processorFunc(func(_ context.Context, env f2e.Envelope[f2e.RecordPayload]) (f2e.RecordDecision, error) {
		switch env.Data.Raw {
		case "reject":
			return f2e.RecordDecision{Kind: f2e.RecordReject, Reason: f2e.RejectionProcessorRejected}, nil
		case "ignore":
			return f2e.RecordDecision{Kind: f2e.RecordIgnore, Reason: f2e.IgnoreProcessorFiltered}, nil
		default:
			return f2e.PublishRecord(env), nil
		}
	})
	s := Service{Queue: q, Processor: processor, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 20, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 7}
	counts, err := s.streamWithMetrics(context.Background(), job, strings.NewReader("publish\nreject\nignore\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if counts.RecordsRead != 3 || counts.RecordsPublished != 1 || counts.RecordsRejected != 1 || counts.RecordsIgnored != 1 || len(q.messages) != 1 {
		t.Fatalf("counts=%+v messages=%d", counts, len(q.messages))
	}
}
func TestProcessBatchesAndStableIDs(t *testing.T) {
	q := &queue{}
	s := Service{Resolver: store{"aaa\nbbb\nccc\n"}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	j := f2e.ChunkJob{SchemaVersion: "1", FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", EndByteInclusive: 11, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	b, _ := json.Marshal(j)
	if e := s.Process(context.Background(), b); e != nil {
		t.Fatal(e)
	}
	if len(q.batches) != 1 || len(q.batches[0]) != 3 {
		t.Fatalf("batches=%v", q.batches)
	}
	var env f2e.Envelope[f2e.RecordPayload]
	json.Unmarshal([]byte(q.batches[0][0]), &env)
	if env.Processing.RecordNumber == nil || *env.Processing.RecordNumber != 1 || env.Metadata.EventID == "" || env.Metadata.SourceRecordID == "" {
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

func TestRetryPreservesEventAndSourceRecordIDs(t *testing.T) {
	q := &queue{}
	s := Service{Resolver: store{"aaa\n"}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "same-occurrence", ChunkID: "00000001", Bucket: "b", Key: "k", EndByteInclusive: 3, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if err := s.ProcessAttempt(context.Background(), body, 2); err != nil {
		t.Fatal(err)
	}
	var first, retry f2e.Envelope[f2e.RecordPayload]
	_ = json.Unmarshal([]byte(q.batches[0][0]), &first)
	_ = json.Unmarshal([]byte(q.batches[1][0]), &retry)
	if first.Metadata.EventID != retry.Metadata.EventID || first.Metadata.SourceRecordID != retry.Metadata.SourceRecordID {
		t.Fatalf("retry changed identities: first=%+v retry=%+v", first.Metadata, retry.Metadata)
	}
}

func TestProcessUsesConfiguredBatchSize(t *testing.T) {
	q := &queue{}
	s := Service{Resolver: store{"aaa\nbbb\nccc\n"}, Queue: q, Config: config.Config{BatchSize: 2, OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 11, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(q.batches) != 2 || len(q.batches[0]) != 2 || len(q.batches[1]) != 1 {
		t.Fatalf("batches=%v", q.batches)
	}
}

func TestProcessRejectsOversizedSerializedEvent(t *testing.T) {
	q := &queue{}
	s := Service{Resolver: store{strings.Repeat("x", 1100) + "\n"}, Queue: q, Config: config.Config{MaxEventBytes: 1024, OutputQueueURL: "out", EventSchemaID: "test", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 1100, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 1101}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err == nil {
		t.Fatal("expected oversized event error")
	}
	if len(q.batches) != 0 {
		t.Fatalf("oversized event was published: %v", q.batches)
	}
}

func TestLegacyDataTypesAreRejected(t *testing.T) {
	for _, dataType := range []f2e.DataType{"", "fixed-width", "jsonl", "ndjson", "csv", "binary"} {
		job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", StartByte: 0, EndByteInclusive: 3, DataType: dataType}
		body, _ := json.Marshal(job)
		err := (Service{Resolver: store{"aaa\n"}, Queue: &queue{}, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}).Process(context.Background(), body)
		if err == nil || !strings.Contains(err.Error(), "unsupported data type") {
			t.Fatalf("dataType=%q err=%v", dataType, err)
		}
	}
}

// TestTextBoundariesWithoutFinalLFAndWithCRLF covers two contracts:
//   - The contract now requires every line to have a terminator; a file ending
//     without one is an error.
//   - CRLF (\r\n) is a valid single terminator.
func TestTextBoundariesWithoutFinalLFAndWithCRLF(t *testing.T) {
	// no-final-lf: last line without terminator is now an error per the new contract.
	t.Run("no-final-lf", func(t *testing.T) {
		data := "1234"
		q := &queue{}
		job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", StartByte: 0, EndByteInclusive: int64(len(data) - 1), MaxRecordLengthBytes: int64(len(data)), DataType: f2e.DataTypeText}
		body, _ := json.Marshal(job)
		s := Service{Resolver: store{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
		if err := s.Process(context.Background(), body); err == nil {
			t.Fatal("expected error for last line without terminator, got nil")
		}
	})
	// crlf: CRLF is one terminator; the record should be emitted once.
	t.Run("crlf", func(t *testing.T) {
		data := "{}\r\n"
		q := &queue{}
		job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", StartByte: 0, EndByteInclusive: int64(len(data) - 1), MaxRecordLengthBytes: int64(len(data)), DataType: f2e.DataTypeText}
		body, _ := json.Marshal(job)
		s := Service{Resolver: store{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
		if err := s.Process(context.Background(), body); err != nil || len(q.batches) != 1 {
			t.Fatalf("err=%v batches=%v", err, q.batches)
		}
	})
	// Line exceeding maxRecordLengthBytes produces an error that identifies
	// the byte offset and the limit.
	t.Run("exceeds-max-record-bytes", func(t *testing.T) {
		data := "12345"
		job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001", Bucket: "b", Key: "k", StartByte: 0, EndByteInclusive: 4, MaxRecordLengthBytes: 4, DataType: f2e.DataTypeText}
		body, _ := json.Marshal(job)
		s := Service{Resolver: store{data}, Queue: &queue{}, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
		err := s.Process(context.Background(), body)
		if err == nil {
			t.Fatal("expected error for record exceeding max bytes, got nil")
		}
		if !strings.Contains(err.Error(), "exceeds maximum") {
			t.Fatalf("expected 'exceeds maximum' in error, got: %v", err)
		}
	})
}

func TestVariableChunksDoNotDuplicateRecordsInPadding(t *testing.T) {
	q := &queue{}
	s := Service{Resolver: rangedStore{"aa\nbbb\ncccc\nd\n"}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
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

func (s rangedStore) OpenChunkRange(_ context.Context, _ f2e.ChunkJob, start, end int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data[start : end+1])), nil
}

func (s rangedStore) Head(_ context.Context, bucket, key string) (f2e.ObjectIdentity, error) {
	return f2e.ObjectIdentity{Bucket: bucket, Key: key, Size: int64(len(s.data)), ETag: "etag"}, nil
}

func (s rangedStore) GetRange(_ context.Context, _ f2e.ObjectIdentity, start, end int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.data[start : end+1])), nil
}

func TestMultiLineChunkEmitsJoinedRecords(t *testing.T) {
	// Single chunk covering the whole file; no padding.
	data := "1abc\n2def\n1xyz\n2uvw\n"
	q := &queue{}
	s := Service{Resolver: rangedStore{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
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
	s := Service{Resolver: rangedStore{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
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

func TestJSONArrayWorkerSingleChunk(t *testing.T) {
	// File: [{"a":1},{"b":2},{"c":3}] — array starts at byte 1, 3 elements.
	data := `[{"a":1},{"b":2},{"c":3}]`
	q := &queue{}
	layout := f2e.JSONArrayLayout{ArrayPath: "", MaxBytesPerElement: 10}
	job := f2e.ChunkJob{
		SchemaVersion:        f2e.SchemaVersion,
		FileID:               "f",
		JobID:                "j",
		ChunkID:              "00000001",
		Bucket:               "b",
		Key:                  "k",
		StartByte:            1, // after '['
		EndByteInclusive:     int64(len(data) - 1),
		MaxRecordLengthBytes: 10,
		DataType:             f2e.DataTypeJSON,
		JSONArrayLayout:      layout,
		JSONArrayOffset:      1,
	}
	body, _ := json.Marshal(job)
	s := Service{
		Resolver: rangedStore{data},
		Queue:    q,
		Config:   config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"},
	}
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
	if strings.Join(raws, ",") != `{"a":1},{"b":2},{"c":3}` {
		t.Fatalf("records=%v", raws)
	}
}

func TestJSONArrayWorkerNestedAndMultiChunk(t *testing.T) {
	// File: {"items":[{"a":1},{"b":2},{"c":3},{"d":4}]}
	// Array starts at byte 10 (after '{"items":[').
	data := `{"items":[{"a":1},{"b":2},{"c":3},{"d":4}]}`
	// Manually create two jobs that would result from planning.
	layout := f2e.JSONArrayLayout{ArrayPath: "items", MaxBytesPerElement: 8}
	jobs := []f2e.ChunkJob{
		{
			SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001",
			Bucket: "b", Key: "k",
			// owns bytes [10..24], padding to end of {"b":2}
			StartByte: 10, EndByteInclusive: 24, MaxRecordLengthBytes: 8,
			TrailingPaddingBytes: 0,
			DataType:             f2e.DataTypeJSON, JSONArrayLayout: layout, JSONArrayOffset: 10,
		},
		{
			SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000002",
			Bucket: "b", Key: "k",
			StartByte: 26, EndByteInclusive: int64(len(data) - 1), MaxRecordLengthBytes: 8,
			TrailingPaddingBytes: 0,
			DataType:             f2e.DataTypeJSON, JSONArrayLayout: layout, JSONArrayOffset: 10,
		},
	}
	q := &queue{}
	s := Service{
		Resolver: rangedStore{data},
		Queue:    q,
		Config:   config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"},
	}
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
	if len(raws) != 4 {
		t.Fatalf("expected 4 elements, got %d: %v", len(raws), raws)
	}
}

func TestJSONArrayPlanningAndWorkersPreserveTenThousandElements(t *testing.T) {
	var data strings.Builder
	data.WriteByte('[')
	for i := 1; i <= 10_000; i++ {
		if i > 1 {
			data.WriteByte(',')
		}
		fmt.Fprintf(&data, `{"id":%d,"value":"record-%d"}`, i, i)
	}
	data.WriteByte(']')

	store := rangedStore{data: data.String()}
	layout := f2e.JSONArrayLayout{MaxBytesPerElement: 256}
	planner := organizer.Service{Store: store, Config: config.Config{RecordsPerChunk: 1_000}}
	jobs, err := planner.Plan(context.Background(), f2e.OrganizerRequest{SchemaVersion: f2e.SchemaVersion, Files: []f2e.FileRequest{{Bucket: "b", Key: "k", DataType: f2e.DataTypeJSON, JSONArrayLayout: layout}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(jobs))
	}
	for i := 1; i < len(jobs); i++ {
		if jobs[i].StartByte != jobs[i-1].EndByteInclusive+1 {
			t.Fatalf("non-contiguous chunks %d and %d", i, i+1)
		}
	}

	q := &queue{}
	processor := Service{Resolver: store, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json", MaxChunkBytes: 64 * 1024 * 1024}}
	for _, job := range jobs {
		body, _ := json.Marshal(job)
		if err := processor.Process(context.Background(), body); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	for _, batch := range q.batches {
		count += len(batch)
	}
	if count != 10_000 {
		t.Fatalf("expected 10000 elements, got %d", count)
	}
}
