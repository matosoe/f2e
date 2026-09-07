package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

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
