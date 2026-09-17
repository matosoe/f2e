package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

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

type failureLedger struct {
	port.JobLedger
	failures []f2e.ChunkResult
}

type completionLedger struct {
	port.JobLedger
	intent    *f2e.CompletionIntent
	delivered int
	markErr   error
}

func (l *completionLedger) StartChunk(context.Context, string, string, int) error { return nil }
func (l *completionLedger) CompleteChunk(context.Context, f2e.ChunkResult) error  { return nil }
func (l *completionLedger) ReconcileCompletion(context.Context, string) (*f2e.CompletionIntent, error) {
	return l.intent, nil
}
func (l *completionLedger) MarkIntentDelivered(context.Context, string, int64) error {
	l.delivered++
	return l.markErr
}

func (l *failureLedger) FailChunk(_ context.Context, result f2e.ChunkResult) error {
	l.failures = append(l.failures, result)
	return nil
}

type shutdownLedger struct {
	port.JobLedger
	failures          []f2e.ChunkResult
	failureContextErr error
}

func (l *shutdownLedger) StartChunk(context.Context, string, string, int) error { return nil }

func (l *shutdownLedger) FailChunk(ctx context.Context, result f2e.ChunkResult) error {
	l.failureContextErr = ctx.Err()
	l.failures = append(l.failures, result)
	return nil
}

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

type messageQueue struct {
	mu       sync.Mutex
	messages []port.OutboundMessage
}

func (q *messageQueue) Send(_ context.Context, _ string, messages []port.OutboundMessage) ([]int, error) {
	q.mu.Lock()
	q.messages = append(q.messages, messages...)
	q.mu.Unlock()
	return nil, nil
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
	counts, err := s.streamWithMetrics(context.Background(), job, strings.NewReader("aaa\nbbb\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.calls) != 2 || len(q.calls[0]) != 2 || len(q.calls[1]) != 1 || q.calls[1][0] != q.calls[0][1] {
		t.Fatalf("calls=%v", q.calls)
	}
	if counts.RecordsPublished != 2 || counts.MessagesPublished != 2 || counts.SendMessageBatchCalls != 2 {
		t.Fatalf("retry counters=%+v", counts)
	}
}

func TestCompletionIsSentBeforeItsIntentIsMarkedDelivered(t *testing.T) {
	q := &messageQueue{}
	intent := &f2e.CompletionIntent{JobID: "job", Version: 1, Event: f2e.CompletionEvent{EventID: f2e.CompletionEventID("job", 1), JobID: "job"}}
	ledger := &completionLedger{intent: intent}
	s := Service{Resolver: store{"record\n"}, Queue: q, Ledger: ledger, CompletionLedger: ledger, Config: config.Config{OutputQueueURL: "out", CompletionQueueURL: "completion", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "job", ChunkID: "1", Bucket: "b", Key: "k", EndByteInclusive: 6, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 7}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if ledger.delivered != 1 || len(q.messages) != 2 {
		t.Fatalf("delivered=%d messages=%d", ledger.delivered, len(q.messages))
	}
	var event f2e.CompletionEvent
	if err := json.Unmarshal([]byte(q.messages[1].Body), &event); err != nil || event.EventID != intent.Event.EventID {
		t.Fatalf("event=%+v err=%v", event, err)
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
		if err == nil || !strings.Contains(err.Error(), "tipo de dado não suportado") {
			t.Fatalf("dataType=%q err=%v", dataType, err)
		}
	}
}

func TestProcessAttemptStoresClearReasonInLedger(t *testing.T) {
	ledger := &failureLedger{}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", StartByte: 0, EndByteInclusive: 9, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	body, _ := json.Marshal(job)

	err := (Service{Resolver: store{""}, Queue: &queue{}, Ledger: ledger, Config: config.Config{MaxChunkBytes: 5}}).ProcessAttempt(context.Background(), body, 1)
	if err == nil {
		t.Fatal("expected chunk size validation error")
	}
	if len(ledger.failures) != 1 || !strings.Contains(ledger.failures[0].Error, "o chunk possui 10 bytes e excede o limite configurado de 5 bytes") {
		t.Fatalf("ledger failures=%+v", ledger.failures)
	}
}

func TestProcessAttemptStopsOutputAndMarksChunkIncompleteWhenDeadlineExpires(t *testing.T) {
	ledger := &shutdownLedger{}
	q := &queue{}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", StartByte: 0, EndByteInclusive: 3, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4}
	body, _ := json.Marshal(job)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := (Service{Resolver: store{"aaa\n"}, Queue: q, Ledger: ledger, Config: config.Config{BatchSize: 1, OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}).ProcessAttempt(ctx, body, 1)
	if !errors.Is(err, ErrLambdaExecutionLimitExceeded) {
		t.Fatalf("err=%v, want execution-limit error", err)
	}
	if len(q.batches) != 0 {
		t.Fatalf("output was sent after deadline: %+v", q.batches)
	}
	if len(ledger.failures) != 1 || !strings.Contains(ledger.failures[0].Error, ErrLambdaExecutionLimitExceeded.Error()) {
		t.Fatalf("ledger failures=%+v", ledger.failures)
	}
	if ledger.failureContextErr != nil {
		t.Fatalf("failure ledger received cancelled context: %v", ledger.failureContextErr)
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
		if !strings.Contains(err.Error(), "um dos registros excede o limite configurado") {
			t.Fatalf("expected clear record-limit error, got: %v", err)
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

func TestNominalTextChunksDiscoverBoundariesAndPublishEachRecordOnce(t *testing.T) {
	data := "one\r\ntwo\r\nthree\r\nfour" // EOF without final newline is valid.
	q := &queue{}
	s := Service{Resolver: rangedStore{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
	jobs := []f2e.ChunkJob{
		{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", StartByte: 0, EndByteInclusive: 8, FileSize: int64(len(data)), MaxRecordLengthBytes: 5, DataType: f2e.DataTypeText},
		{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "2", Bucket: "b", Key: "k", StartByte: 9, EndByteInclusive: int64(len(data) - 1), FileSize: int64(len(data)), MaxRecordLengthBytes: 5, DataType: f2e.DataTypeText},
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
	if strings.Join(raws, ",") != "one,two,three,four" {
		t.Fatalf("records=%v", raws)
	}
}

func TestNominalTextChunkFailsBeforePublishingWhenBoundaryExceedsNineTimesLimit(t *testing.T) {
	data := strings.Repeat("x", 100) + "\nnext\n"
	q := &queue{}
	job := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "1", Bucket: "b", Key: "k", StartByte: 50, EndByteInclusive: int64(len(data) - 1), FileSize: int64(len(data)), MaxRecordLengthBytes: 10, DataType: f2e.DataTypeText}
	body, _ := json.Marshal(job)
	err := (Service{Resolver: rangedStore{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}).Process(context.Background(), body)
	if err == nil || !strings.Contains(err.Error(), "não foi localizada uma quebra de registro") {
		t.Fatalf("err=%v", err)
	}
	if len(q.batches) != 0 {
		t.Fatalf("published before boundary validation: %v", q.batches)
	}
}

func TestMultiLineChunkEmitsJoinedRecords(t *testing.T) {
	// Single chunk covering the whole file; no padding.
	data := "1abc\n2def\n1xyz\n2uvw\n"
	q := &queue{}
	s := Service{Resolver: rangedStore{data}, Queue: q, Config: config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"}}
	layout := f2e.MultiLineLayout{BreakFields: []f2e.LineMatchField{{StartByte: 0, LengthBytes: 1, Value: "1"}}, IncludeFields: []f2e.LineMatchField{{StartByte: 0, LengthBytes: 1, Value: "2"}}, LineSeparator: "\x1C", MaxBytesPerRecord: 10}
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
	layout := f2e.MultiLineLayout{BreakFields: []f2e.LineMatchField{{StartByte: 0, LengthBytes: 1, Value: "1"}}, IncludeFields: []f2e.LineMatchField{{StartByte: 0, LengthBytes: 1, Value: "2"}}, LineSeparator: "\x1C", MaxBytesPerRecord: 10}
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
	data := `[{"a":1},{"a":2},{"a":3}]`
	q := &queue{}
	layout := f2e.JSONArrayLayout{ArrayPath: "", FirstFieldName: "a", MaxBytesPerElement: 10}
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
	if strings.Join(raws, ",") != `{"a":1},{"a":2},{"a":3}` {
		t.Fatalf("records=%v", raws)
	}
}

func TestJSONArrayWorkerNestedAndMultiChunk(t *testing.T) {
	// File: {"items":[{"a":1},{"b":2},{"c":3},{"d":4}]}
	// Array starts at byte 10 (after '{"items":[').
	data := `{"items":[{"a":1},{"a":2},{"a":3},{"a":4}]}`
	// Manually create two jobs that would result from planning.
	layout := f2e.JSONArrayLayout{ArrayPath: "items", FirstFieldName: "a", MaxBytesPerElement: 8}
	jobs := []f2e.ChunkJob{
		{
			SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001",
			Bucket: "b", Key: "k",
			// owns bytes [10..24], padding to end of {"b":2}
			StartByte: 10, EndByteInclusive: 24, MaxRecordLengthBytes: 8,
			TrailingPaddingBytes: 0,
			DataType:             f2e.DataTypeJSON, JSONArrayLayout: layout,
		},
		{
			SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000002",
			Bucket: "b", Key: "k",
			StartByte: 26, EndByteInclusive: int64(len(data) - 1), MaxRecordLengthBytes: 8,
			TrailingPaddingBytes: 0,
			DataType:             f2e.DataTypeJSON, JSONArrayLayout: layout,
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

// ── T16: Bundle output mode integration tests ─────────────────────────────────

// bundleJob creates a ChunkJob with OutputMode=bundle and the given bundle settings.
func bundleJob(maxEnv, maxBytes int) f2e.ChunkJob {
	return f2e.ChunkJob{
		SchemaVersion:        f2e.SchemaVersion,
		FileID:               "f",
		JobID:                "j",
		ChunkID:              "00000001",
		Bucket:               "b",
		Key:                  "k",
		EndByteInclusive:     11, // "aaa\nbbb\nccc\n"
		DataType:             f2e.DataTypeText,
		MaxRecordLengthBytes: 4,
		Configuration: f2e.JobConfiguration{
			BatchSize: 10, MaxEventBytes: 256 * 1024,
			EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json",
			OutputMode:             f2e.OutputModeBundle,
			MaxEnvelopesPerMessage: maxEnv,
			MaxMessageBytes:        maxBytes,
		},
	}
}

// TestBundleModePacksAllEnvelopesIntoOneBundleMessage verifies that three
// records with maxEnvelopes=10 end up in a single SQS message.
func TestBundleModePacksAllEnvelopesIntoOneBundleMessage(t *testing.T) {
	q := &queue{}
	s := Service{
		Resolver: store{"aaa\nbbb\nccc\n"},
		Queue:    q,
		Config:   config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"},
	}
	job := bundleJob(10, 256*1024)
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	// Expect exactly one SQS batch with one bundle message.
	if len(q.batches) != 1 || len(q.batches[0]) != 1 {
		t.Fatalf("want 1 batch with 1 bundle, got %v batches", q.batches)
	}
	var bundle f2e.BundleEnvelope
	if err := json.Unmarshal([]byte(q.batches[0][0]), &bundle); err != nil {
		t.Fatalf("decode bundle: %v", err)
	}
	if bundle.SchemaVersion != f2e.BundleSchemaVersion {
		t.Fatalf("schemaVersion: %q", bundle.SchemaVersion)
	}
	if len(bundle.Items) != 3 {
		t.Fatalf("want 3 items, got %d", len(bundle.Items))
	}
}

// TestBundleModeFlushesOnCountLimit verifies that maxEnvelopes=2 creates
// two bundle messages from three records.
func TestBundleModeFlushesOnCountLimit(t *testing.T) {
	q := &queue{}
	s := Service{
		Resolver: store{"aaa\nbbb\nccc\n"},
		Queue:    q,
		Config:   config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"},
	}
	job := bundleJob(2, 256*1024)
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	totalMsgs := 0
	for _, batch := range q.batches {
		totalMsgs += len(batch)
	}
	if totalMsgs != 2 {
		t.Fatalf("want 2 SQS messages (2 bundles), got %d", totalMsgs)
	}
	// First bundle has 2 items, second has 1.
	var b1, b2 f2e.BundleEnvelope
	allBodies := []string{}
	for _, batch := range q.batches {
		allBodies = append(allBodies, batch...)
	}
	json.Unmarshal([]byte(allBodies[0]), &b1)
	json.Unmarshal([]byte(allBodies[1]), &b2)
	if len(b1.Items) != 2 || len(b2.Items) != 1 {
		t.Fatalf("bundle sizes: b1=%d b2=%d", len(b1.Items), len(b2.Items))
	}
}

// TestBundleModeRecordsPublishedCountsEnvelopes verifies that RecordsPublished
// reflects the number of envelopes (logical records), not the number of bundle messages.
func TestBundleModeRecordsPublishedCountsEnvelopes(t *testing.T) {
	job := f2e.ChunkJob{
		SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001",
		Bucket: "b", Key: "k", EndByteInclusive: 11, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4,
		Configuration: f2e.JobConfiguration{
			OutputMode: f2e.OutputModeBundle, MaxEnvelopesPerMessage: 2, MaxMessageBytes: 256 * 1024,
			MaxEventBytes: 256 * 1024, EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json",
		},
	}
	counts, err := (&Service{Queue: &messageQueue{}}).streamWithMetrics(
		context.Background(), job, strings.NewReader("aaa\nbbb\nccc\n"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if counts.RecordsPublished != 3 {
		t.Fatalf("want RecordsPublished=3, got %d", counts.RecordsPublished)
	}
}

// TestBundleModeSingleModeUnchanged verifies that single mode still sends
// one SQS message per envelope (backward compatibility).
func TestBundleModeSingleModeUnchanged(t *testing.T) {
	q := &queue{}
	s := Service{
		Resolver: store{"aaa\nbbb\n"},
		Queue:    q,
		Config:   config.Config{OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json"},
	}
	job := f2e.ChunkJob{
		SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001",
		Bucket: "b", Key: "k", EndByteInclusive: 7, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4,
		Configuration: f2e.JobConfiguration{
			OutputMode: f2e.OutputModeSingle, MaxEventBytes: 256 * 1024,
			EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json",
		},
	}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	totalMsgs := 0
	for _, batch := range q.batches {
		totalMsgs += len(batch)
	}
	if totalMsgs != 2 {
		t.Fatalf("single mode: want 2 messages, got %d", totalMsgs)
	}
	// Verify message body is a plain Envelope, not a BundleEnvelope.
	var probe struct {
		SchemaVersion string `json:"schemaVersion"`
	}
	json.Unmarshal([]byte(q.batches[0][0]), &probe)
	if probe.SchemaVersion == f2e.BundleSchemaVersion {
		t.Fatal("single mode emitted a bundle envelope")
	}
}

// TestBundleModeRetryPreservesEventIDs verifies that re-running the same
// chunk in bundle mode produces the same eventIds inside bundles.
func TestBundleModeRetryPreservesEventIDs(t *testing.T) {
	q1, q2 := &messageQueue{}, &messageQueue{}
	job := f2e.ChunkJob{
		SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001",
		Bucket: "b", Key: "k", EndByteInclusive: 3, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4,
		Configuration: f2e.JobConfiguration{
			OutputMode: f2e.OutputModeBundle, MaxEnvelopesPerMessage: 10, MaxMessageBytes: 256 * 1024,
			MaxEventBytes: 256 * 1024, EventSchemaID: "s", EventSchemaVersion: "1", EventFormat: "json",
		},
	}
	svc1 := Service{Queue: q1}
	svc2 := Service{Queue: q2}
	svc1.streamWithMetrics(context.Background(), job, strings.NewReader("aaa\n"), 0)
	svc2.streamWithMetrics(context.Background(), job, strings.NewReader("aaa\n"), 0)

	var b1, b2 f2e.BundleEnvelope
	json.Unmarshal([]byte(q1.messages[0].Body), &b1)
	json.Unmarshal([]byte(q2.messages[0].Body), &b2)
	if b1.BundleID != b2.BundleID {
		t.Fatalf("bundleId differs on retry: %s vs %s", b1.BundleID, b2.BundleID)
	}
	if b1.Items[0].Metadata.EventID != b2.Items[0].Metadata.EventID {
		t.Fatalf("eventId differs on retry")
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
	layout := f2e.JSONArrayLayout{FirstFieldName: "id", MaxBytesPerElement: 256}
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
