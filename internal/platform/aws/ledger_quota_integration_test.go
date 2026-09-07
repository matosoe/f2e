package aws

import (
	"os"
	"testing"
	"time"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

// TestQuotaReserveAndReleaseAgainstLocalStack verifies the full quota lifecycle:
//
//   - ReserveSlot succeeds up to maxActiveJobs times
//   - The (N+1)th attempt returns ErrQuotaExceeded
//   - After ReleaseSlot the counter drops and a new reservation succeeds
//
// Requires a running LocalStack with a pre-created DynamoDB table
// (set F2E_TEST_AWS_ENDPOINT).
func TestQuotaReserveAndReleaseAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{Endpoint: endpoint, Region: "us-east-1", LedgerTable: "f2e-job-ledger", LedgerRetentionDays: 90, MaxReceiveCount: 1})
	if err != nil {
		t.Fatal(err)
	}

	const maxJobs = 2
	prefixID := "quota-test-" + time.Now().UTC().Format("20060102150405.000000000")

	// Reserve two slots — both should succeed.
	if err := client.ReserveSlot(t.Context(), prefixID, maxJobs); err != nil {
		t.Fatalf("first reservation: %v", err)
	}
	if err := client.ReserveSlot(t.Context(), prefixID, maxJobs); err != nil {
		t.Fatalf("second reservation: %v", err)
	}

	// Third attempt must be rejected.
	if err := client.ReserveSlot(t.Context(), prefixID, maxJobs); err == nil {
		t.Fatal("expected ErrQuotaExceeded on third reservation, got nil")
	} else if err != port.ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}

	// Release one slot; now reservation must succeed again.
	if err := client.ReleaseSlot(t.Context(), prefixID); err != nil {
		t.Fatalf("release slot: %v", err)
	}
	if err := client.ReserveSlot(t.Context(), prefixID, maxJobs); err != nil {
		t.Fatalf("reservation after release: %v", err)
	}
}

// TestQuotaReleaseIdempotentAgainstLocalStack verifies that calling ReleaseSlot
// when the counter is already 0 is a no-op (no error, no negative counter).
func TestQuotaReleaseIdempotentAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{Endpoint: endpoint, Region: "us-east-1", LedgerTable: "f2e-job-ledger", LedgerRetentionDays: 90, MaxReceiveCount: 1})
	if err != nil {
		t.Fatal(err)
	}

	prefixID := "quota-idempotent-" + time.Now().UTC().Format("20060102150405.000000000")

	// ReleaseSlot on a counter that has never been created must not return an error.
	for i := range 3 {
		if err := client.ReleaseSlot(t.Context(), prefixID); err != nil {
			t.Fatalf("idempotent release %d: %v", i, err)
		}
	}
}

// TestQuotaReleasedOnRejectAgainstLocalStack verifies that RejectJob releases
// the prefix quota slot so the next admission can proceed.
func TestQuotaReleasedOnRejectAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{Endpoint: endpoint, Region: "us-east-1", LedgerTable: "f2e-job-ledger", LedgerRetentionDays: 90, MaxReceiveCount: 1})
	if err != nil {
		t.Fatal(err)
	}

	prefixID := "quota-reject-" + time.Now().UTC().Format("20060102150405.000000000")
	fileID := "quota-reject-file-" + time.Now().UTC().Format("20060102150405.000000000")

	// Reserve the only available slot, then admit a job.
	if err := client.ReserveSlot(t.Context(), prefixID, 1); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	outcome, err := client.Admit(t.Context(), f2e.Receipt{
		ReceiptID:  "rcv-quota-reject",
		FileID:     fileID,
		Source:     f2e.ObjectIdentity{Bucket: "b", Key: "k", VersionID: "v", ETag: "e"},
		PrefixID:   prefixID,
		ReceivedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("admit: %v", err)
	}
	if outcome != f2e.Acquired {
		t.Fatalf("expected Acquired, got %v", outcome)
	}

	// The slot is occupied — a second reservation must fail.
	if err := client.ReserveSlot(t.Context(), prefixID, 1); err != port.ErrQuotaExceeded {
		t.Fatalf("expected ErrQuotaExceeded, got %v", err)
	}

	// Reject the job; releaseQuotaIfNeeded should free the slot.
	if err := client.RejectJob(t.Context(), fileID, f2e.Rejection{Reason: f2e.RejectionEmptyFile}); err != nil {
		t.Fatalf("reject: %v", err)
	}

	// Slot must now be available again.
	if err := client.ReserveSlot(t.Context(), prefixID, 1); err != nil {
		t.Fatalf("reserve after reject: %v", err)
	}
}
