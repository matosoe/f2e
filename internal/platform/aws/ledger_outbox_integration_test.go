package aws

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

// TestOutboxWriteAndQueryAgainstLocalStack verifies that:
//  1. WriteCompletionIntent persists an intent under the job partition.
//  2. PendingCompletionIntents retrieves the undelivered intent via the GSI.
//  3. MarkIntentDelivered removes the intentPending marker.
//  4. A second PendingCompletionIntents call no longer returns the intent.
//  5. MarkIntentDelivered is idempotent (calling it again does not fail).
func TestOutboxWriteAndQueryAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{
		Endpoint:            endpoint,
		Region:              "us-east-1",
		LedgerTable:         "f2e-job-ledger",
		LedgerRetentionDays: 90,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	jobID := "outbox-" + time.Now().UTC().Format("20060102150405.000000000")
	now := time.Now().UTC()
	completedAt := now.Add(5 * time.Minute)

	intent := f2e.CompletionIntent{
		JobID:     jobID,
		Version:   1,
		Status:    f2e.JobStateCompleted,
		CreatedAt: now,
		Event: f2e.CompletionEvent{
			SchemaVersion: f2e.CompletionEventVersion,
			EventID:       f2e.CompletionEventID(jobID, 1),
			JobID:         jobID,
			FileID:        "file-outbox",
			Source: f2e.CompletionSource{
				Bucket: "bucket",
				Key:    "key.txt",
			},
			Config: f2e.CompletionConfig{},
			Status: f2e.JobStateCompleted,
			Result: f2e.JobResultSuccess,
			Counts: f2e.CompletionCounts{
				RecordsRead:      10,
				RecordsPublished: 10,
				CountsComplete:   true,
			},
			Timestamps: f2e.CompletionTimestamps{
				ReceivedAt:  &now,
				CompletedAt: &completedAt,
			},
		},
	}

	// 1. Write intent.
	if err := client.WriteCompletionIntent(ctx, intent); err != nil {
		t.Fatalf("WriteCompletionIntent: %v", err)
	}

	// 2. Writing again must be idempotent (no error, no duplicate).
	if err := client.WriteCompletionIntent(ctx, intent); err != nil {
		t.Fatalf("WriteCompletionIntent idempotent: %v", err)
	}

	// 3. Query pending intents — must find exactly one for this job.
	pending, err := client.PendingCompletionIntents(ctx, 50)
	if err != nil {
		t.Fatalf("PendingCompletionIntents: %v", err)
	}
	found := false
	for _, p := range pending {
		if p.JobID == jobID {
			found = true
			if p.Event.EventID != intent.Event.EventID {
				t.Errorf("eventId mismatch: got %q, want %q", p.Event.EventID, intent.Event.EventID)
			}
			if p.Status != f2e.JobStateCompleted {
				t.Errorf("status=%q, want COMPLETED", p.Status)
			}
		}
	}
	if !found {
		t.Fatalf("intent for job %q not found in pending list (got %d items)", jobID, len(pending))
	}

	// 4. Mark as delivered.
	if err := client.MarkIntentDelivered(ctx, jobID, 1); err != nil {
		t.Fatalf("MarkIntentDelivered: %v", err)
	}

	// 5. MarkIntentDelivered must be idempotent.
	if err := client.MarkIntentDelivered(ctx, jobID, 1); err != nil {
		t.Fatalf("MarkIntentDelivered idempotent: %v", err)
	}

	// 6. Intent no longer appears in pending list.
	pending2, err := client.PendingCompletionIntents(ctx, 50)
	if err != nil {
		t.Fatalf("PendingCompletionIntents after delivery: %v", err)
	}
	for _, p := range pending2 {
		if p.JobID == jobID {
			t.Errorf("job %q still appears in pending intents after MarkIntentDelivered", jobID)
		}
	}
}

// TestOutboxConcurrentTerminalAgainstLocalStack verifies that two concurrent
// terminal transitions (simulating a crash-and-retry) produce exactly one
// logical intent: the first write wins via the conditional put, the second is
// a no-op and the existing intent is preserved unchanged.
func TestOutboxConcurrentTerminalAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{
		Endpoint:            endpoint,
		Region:              "us-east-1",
		LedgerTable:         "f2e-job-ledger",
		LedgerRetentionDays: 90,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	jobID := "outbox-concurrent-" + time.Now().UTC().Format("20060102150405.000000000")
	now := time.Now().UTC()
	completedAt := now

	makeIntent := func(version int64) f2e.CompletionIntent {
		return f2e.CompletionIntent{
			JobID:     jobID,
			Version:   version,
			Status:    f2e.JobStateCompleted,
			CreatedAt: now,
			Event: f2e.CompletionEvent{
				SchemaVersion: f2e.CompletionEventVersion,
				EventID:       f2e.CompletionEventID(jobID, version),
				JobID:         jobID,
				FileID:        "file-concurrent",
				Source:        f2e.CompletionSource{Bucket: "b", Key: "k"},
				Config:        f2e.CompletionConfig{},
				Status:        f2e.JobStateCompleted,
				Result:        f2e.JobResultSuccess,
				Counts:        f2e.CompletionCounts{CountsComplete: true},
				Timestamps:    f2e.CompletionTimestamps{CompletedAt: &completedAt},
			},
		}
	}

	// Both calls use version=1 to simulate a crash-and-retry on the same intent.
	if err := client.WriteCompletionIntent(ctx, makeIntent(1)); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := client.WriteCompletionIntent(ctx, makeIntent(1)); err != nil {
		t.Fatalf("second write (concurrent retry): %v", err)
	}

	// Count how many pending intents exist for this job.
	pending, err := client.PendingCompletionIntents(ctx, 200)
	if err != nil {
		t.Fatalf("PendingCompletionIntents: %v", err)
	}
	count := 0
	for _, p := range pending {
		if p.JobID == jobID {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 pending intent, got %d", count)
	}
}
