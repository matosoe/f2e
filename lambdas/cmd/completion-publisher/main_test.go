package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/f2e/f2e/internal/domain/f2e"
)

// --- helpers ----------------------------------------------------------------

func makeIntentAttr(intent f2e.CompletionIntent) map[string]events.DynamoDBAttributeValue {
	payload, _ := json.Marshal(intent)
	return map[string]events.DynamoDBAttributeValue{
		"pk":            events.NewStringAttribute("JOB#" + intent.JobID),
		"sk":            events.NewStringAttribute("COMPLETION_INTENT#00000000000000000001"),
		"intentPending": events.NewStringAttribute("1"),
		"payload":       events.NewStringAttribute(string(payload)),
	}
}

// stubBackend implements completionBackend for unit tests.
type stubBackend struct {
	sent      []string
	delivered []string
}

func (s *stubBackend) SendCompletion(_ context.Context, _ string, body string) error {
	s.sent = append(s.sent, body)
	return nil
}

func (s *stubBackend) MarkIntentDelivered(_ context.Context, jobID string, _ int64) error {
	s.delivered = append(s.delivered, jobID)
	return nil
}

func (s *stubBackend) PendingCompletionIntents(_ context.Context, _ int) ([]f2e.CompletionIntent, error) {
	return nil, nil
}

// withBackend injects the stub into the package-level publisher for tests.
func withBackend(b completionBackend, queueURL string) {
	publisher.backend = b
	publisher.cfg.CompletionQueueURL = queueURL
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestHandlerSkipsNonInsertEvents(t *testing.T) {
	intent := f2e.CompletionIntent{
		JobID:   "j1",
		Version: 1,
		Status:  f2e.JobStateCompleted,
		Event:   f2e.CompletionEvent{EventID: "evt1"},
	}
	stub := &stubBackend{}
	withBackend(stub, "test-queue")
	ev := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		{EventName: "MODIFY", Change: events.DynamoDBStreamRecord{NewImage: makeIntentAttr(intent)}},
		{EventName: "REMOVE", Change: events.DynamoDBStreamRecord{NewImage: makeIntentAttr(intent)}},
	}}
	if err := handler(context.Background(), ev); err != nil {
		t.Fatalf("expected no error for MODIFY/REMOVE events, got %v", err)
	}
	if len(stub.sent) != 0 {
		t.Fatalf("expected no sends for non-INSERT events, got %d", len(stub.sent))
	}
}

func TestHandlerSkipsNonIntentSortKeys(t *testing.T) {
	stub := &stubBackend{}
	withBackend(stub, "test-queue")
	ev := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{{
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			NewImage: map[string]events.DynamoDBAttributeValue{
				"pk":            events.NewStringAttribute("JOB#j2"),
				"sk":            events.NewStringAttribute("JOB#j2"), // not COMPLETION_INTENT#
				"intentPending": events.NewStringAttribute("1"),
			},
		},
	}}}
	if err := handler(context.Background(), ev); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(stub.sent) != 0 {
		t.Fatalf("should not send for non-intent sort key, got %d sends", len(stub.sent))
	}
}

func TestPublishIntentMarshalAndDeliver(t *testing.T) {
	now := time.Now().UTC()
	intent := f2e.CompletionIntent{
		JobID:     "job-abc",
		Version:   1,
		Status:    f2e.JobStateCompleted,
		CreatedAt: now,
		Event: f2e.CompletionEvent{
			SchemaVersion: f2e.CompletionEventVersion,
			EventID:       "eid",
			JobID:         "job-abc",
			Status:        f2e.JobStateCompleted,
			Timestamps: f2e.CompletionTimestamps{
				ReceivedAt:  &now,
				CompletedAt: &now,
			},
		},
	}
	stub := &stubBackend{}
	withBackend(stub, "test-queue")
	if err := publishIntent(context.Background(), intent); err != nil {
		t.Fatalf("publishIntent: %v", err)
	}
	if len(stub.sent) != 1 {
		t.Fatalf("expected 1 sent, got %d", len(stub.sent))
	}
	if !strings.Contains(stub.sent[0], `"eventId":"eid"`) {
		t.Fatalf("sent body does not contain eventId: %s", stub.sent[0])
	}
	if len(stub.delivered) != 1 || stub.delivered[0] != "job-abc" {
		t.Fatalf("unexpected delivered: %v", stub.delivered)
	}
}

func TestHandlerPublishesInsertIntentEvent(t *testing.T) {
	now := time.Now().UTC()
	intent := f2e.CompletionIntent{
		JobID:   "job-xyz",
		Version: 1,
		Status:  f2e.JobStateCompleted,
		Event: f2e.CompletionEvent{
			SchemaVersion: f2e.CompletionEventVersion,
			EventID:       "eid-xyz",
			JobID:         "job-xyz",
			Status:        f2e.JobStateCompleted,
			Timestamps:    f2e.CompletionTimestamps{CompletedAt: &now},
		},
	}
	stub := &stubBackend{}
	withBackend(stub, "test-queue")
	ev := events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{{
		EventName: "INSERT",
		Change:    events.DynamoDBStreamRecord{NewImage: makeIntentAttr(intent)},
	}}}
	if err := handler(context.Background(), ev); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if len(stub.sent) != 1 {
		t.Fatalf("expected 1 send, got %d", len(stub.sent))
	}
	if !strings.Contains(stub.sent[0], `"eventId":"eid-xyz"`) {
		t.Fatalf("wrong eventId in payload: %s", stub.sent[0])
	}
	if len(stub.delivered) != 1 || stub.delivered[0] != "job-xyz" {
		t.Fatalf("unexpected delivered: %v", stub.delivered)
	}
}
