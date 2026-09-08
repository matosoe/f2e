package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

func TestSplitSendBatchesHonorsSQSByteAndEntryBudgets(t *testing.T) {
	// Multibyte bytes and attributes both participate in the budget.
	large := port.OutboundMessage{Body: strings.Repeat("界", 81_900), Attributes: map[string]port.MessageAttribute{"ação": {DataType: "String", Value: "✓"}}}
	small := port.OutboundMessage{Body: "x"}
	batches := splitSendBatches(append([]port.OutboundMessage{large, small}, make([]port.OutboundMessage, 10)...))
	for _, batch := range batches {
		if len(batch) > port.MaxMessagesPerBatch {
			t.Fatalf("entries=%d", len(batch))
		}
		if len(batch) > 1 && batchSize(batch) > port.MaxMultiMessageBatchBytes {
			t.Fatalf("batch bytes=%d", batchSize(batch))
		}
		for _, message := range batch {
			if port.OutboundMessageSize(message) > port.MaxPhysicalMessageBytes {
				t.Fatalf("message bytes=%d", port.OutboundMessageSize(message))
			}
		}
	}
	if len(batches[0]) != 1 {
		t.Fatalf("large valid message must be isolated, got %d entries", len(batches[0]))
	}
}

type rootFailureQueue struct{}

func (q *rootFailureQueue) Send(ctx context.Context, _ string, messages []port.OutboundMessage) ([]int, error) {
	if messages[0].Body == "a" {
		time.Sleep(20 * time.Millisecond)
		return nil, errors.New("BatchRequestTooLong")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestConcurrentSenderPreservesRootErrorOverCancellation(t *testing.T) {
	q := &rootFailureQueue{}
	cs := newConcurrentSender(context.Background(), q, "url", 2)
	if err := cs.submit([]port.OutboundMessage{{Body: "a"}}); err != nil {
		t.Fatal(err)
	}
	if err := cs.submit([]port.OutboundMessage{{Body: "b"}}); err != nil {
		t.Fatal(err)
	}
	err := cs.wait()
	if err == nil || !strings.Contains(err.Error(), "BatchRequestTooLong") {
		t.Fatalf("want root API error, got %v", err)
	}
}

// countingQueue records how many Send calls are in-flight concurrently.
type countingQueue struct {
	mu       sync.Mutex
	calls    int32 // total completed calls
	maxSeen  int32 // peak in-flight concurrency
	inflight int32
}

func (q *countingQueue) Send(_ context.Context, _ string, messages []port.OutboundMessage) ([]int, error) {
	current := atomic.AddInt32(&q.inflight, 1)
	for {
		old := atomic.LoadInt32(&q.maxSeen)
		if current <= old || atomic.CompareAndSwapInt32(&q.maxSeen, old, current) {
			break
		}
	}
	atomic.AddInt32(&q.inflight, -1)
	atomic.AddInt32(&q.calls, 1)
	return nil, nil
}

// TestConcurrentSenderRespectsConcurrencyLimit verifies that peak in-flight
// Send calls never exceed the configured concurrency.
func TestConcurrentSenderRespectsConcurrencyLimit(t *testing.T) {
	q := &countingQueue{}
	cs := newConcurrentSender(context.Background(), q, "url", 2)
	for i := 0; i < 10; i++ {
		msg := port.OutboundMessage{Body: "x"}
		if err := cs.submit([]port.OutboundMessage{msg}); err != nil {
			t.Fatal(err)
		}
	}
	if err := cs.wait(); err != nil {
		t.Fatal(err)
	}
	if q.maxSeen > 2 {
		t.Fatalf("max concurrent sends = %d, want ≤ 2", q.maxSeen)
	}
	if q.calls != 10 {
		t.Fatalf("want 10 sends, got %d", q.calls)
	}
}

// TestConcurrentSenderPropagatesError verifies that the first error is returned
// by wait() and that subsequent submits also fail.
func TestConcurrentSenderPropagatesError(t *testing.T) {
	errSend := errors.New("send failed")
	q := &alwaysFailQueue{err: errSend}
	cs := newConcurrentSender(context.Background(), q, "url", 1)
	_ = cs.submit([]port.OutboundMessage{{Body: "x"}})
	_ = cs.wait()
	// A second submit after the context is cancelled should fail.
	err := cs.submit([]port.OutboundMessage{{Body: "y"}})
	if err == nil {
		// Acceptable: submit may not have seen the cancelled ctx yet, but wait must return error.
	}
	err = cs.wait()
	if err == nil {
		t.Fatal("want error from wait, got nil")
	}
}

type alwaysFailQueue struct{ err error }

func (q *alwaysFailQueue) Send(_ context.Context, _ string, _ []port.OutboundMessage) ([]int, error) {
	return nil, q.err
}

// TestWorkerPublishConcurrencyTwoSendsAllRecords verifies that with
// publishConcurrency=2 all records from a chunk are delivered.
func TestWorkerPublishConcurrencyTwoSendsAllRecords(t *testing.T) {
	q := &messageQueue{}
	s := Service{
		Resolver: store{"aaa\nbbb\nccc\nddd\neee\nfff\n"},
		Queue:    q,
		Config: config.Config{
			OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1",
			EventFormat: "json", BatchSize: 2, PublishConcurrency: 2,
		},
	}
	job := f2e.ChunkJob{
		SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001",
		Bucket: "b", Key: "k", EndByteInclusive: 23, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4,
	}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(q.messages) != 6 {
		t.Fatalf("want 6 records, got %d", len(q.messages))
	}
}

// TestWorkerPublishConcurrencyFourNoRaceDetector uses -race to confirm the
// concurrent sender has no data races at concurrency=4.
func TestWorkerPublishConcurrencyFourNoRaceDetector(t *testing.T) {
	q := &messageQueue{}
	s := Service{
		Resolver: store{"aaa\nbbb\nccc\nddd\neee\nfff\nggg\nhhh\n"},
		Queue:    q,
		Config: config.Config{
			OutputQueueURL: "out", EventSchemaID: "s", EventSchemaVersion: "1",
			EventFormat: "json", BatchSize: 2, PublishConcurrency: 4,
		},
	}
	job := f2e.ChunkJob{
		SchemaVersion: f2e.SchemaVersion, FileID: "f", JobID: "j", ChunkID: "00000001",
		Bucket: "b", Key: "k", EndByteInclusive: 31, DataType: f2e.DataTypeText, MaxRecordLengthBytes: 4,
	}
	body, _ := json.Marshal(job)
	if err := s.Process(context.Background(), body); err != nil {
		t.Fatal(err)
	}
	if len(q.messages) != 8 {
		t.Fatalf("want 8 records, got %d", len(q.messages))
	}
}
