package aws

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

func newPhasedClient(t *testing.T) *AWS {
	t.Helper()
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{Endpoint: endpoint, Region: "us-east-1", LedgerTable: "f2e-job-ledger", LedgerRetentionDays: 90, MaxReceiveCount: 3})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// seedJob writes a job aggregate item directly, emulating the state a later
// task (T09/T10) would have persisted before the phased transition under test.
func seedJob(t *testing.T, client *AWS, jobID string, status f2e.JobStatus) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	item := map[string]types.AttributeValue{
		"pk":        text("JOB#" + jobID),
		"sk":        text("JOB"),
		"jobId":     text(jobID),
		"fileId":    text("file-" + jobID),
		"status":    text(string(status)),
		"revision":  number(0),
		"history":   &types.AttributeValueMemberL{Value: []types.AttributeValue{}},
		"createdAt": text(now),
		"updatedAt": text(now),
		"expiresAt": number(time.Now().UTC().Add(90 * 24 * time.Hour).Unix()),
	}
	if _, err := client.DynamoDB.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String("f2e-job-ledger"), Item: item}); err != nil {
		t.Fatal(err)
	}
}

// seedChunk writes a PENDING chunk item directly, emulating a sealed manifest.
func seedChunk(t *testing.T, client *AWS, jobID, chunkID string) {
	t.Helper()
	item := map[string]types.AttributeValue{
		"pk":        text("JOB#" + jobID),
		"sk":        text("CHUNK#" + chunkID),
		"jobId":     text(jobID),
		"chunkId":   text(chunkID),
		"status":    text(string(f2e.ChunkStatePending)),
		"attempt":   number(0),
		"revision":  number(0),
		"expiresAt": number(time.Now().UTC().Add(90 * 24 * time.Hour).Unix()),
	}
	if _, err := client.DynamoDB.PutItem(t.Context(), &dynamodb.PutItemInput{TableName: aws.String("f2e-job-ledger"), Item: item}); err != nil {
		t.Fatal(err)
	}
}

// TestPhasedAdmitLifecycleAgainstLocalStack exercises admission through the
// validation/planning transitions, proving idempotency, terminal detection and
// transition-history retention.
func TestPhasedAdmitLifecycleAgainstLocalStack(t *testing.T) {
	client := newPhasedClient(t)
	jobID := "phased-admit-" + time.Now().UTC().Format("20060102150405.000000000")
	fileID := "file-" + jobID
	receipt := f2e.Receipt{
		ReceiptID:   jobID,
		FileID:      fileID,
		Source:      f2e.ObjectIdentity{Bucket: "bucket", Key: "key", VersionID: "v1", ETag: "etag", Size: 10},
		Environment: "test",
		ReceivedAt:  time.Now().UTC(),
	}

	if got, err := client.Admit(t.Context(), receipt); err != nil || got != f2e.Acquired {
		t.Fatalf("admit: got=%s err=%v, want acquired", got, err)
	}
	// Re-admission while in-flight reports Busy (not a new job).
	if got, _ := client.Admit(t.Context(), receipt); got != f2e.Busy {
		t.Fatalf("re-admit: got=%s, want busy", got)
	}
	if err := client.BeginValidation(t.Context(), fileID); err != nil {
		t.Fatalf("begin validation: %v", err)
	}
	// Idempotent.
	if err := client.BeginValidation(t.Context(), fileID); err != nil {
		t.Fatalf("begin validation (repeat): %v", err)
	}
	if err := client.BeginPlanning(t.Context(), fileID); err != nil {
		t.Fatalf("begin planning: %v", err)
	}

	out, err := client.DynamoDB.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("f2e-job-ledger"), Key: ledgerKey(fileID, "JOB"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if status := out.Item["status"].(*types.AttributeValueMemberS).Value; status != string(f2e.JobStatePlanning) {
		t.Fatalf("status=%s, want PLANNING", status)
	}
	if hist, ok := out.Item["history"].(*types.AttributeValueMemberL); !ok || len(hist.Value) == 0 {
		t.Fatalf("history missing or empty: %v", out.Item["history"])
	}
	if tok, ok := out.Item["token"].(*types.AttributeValueMemberS); !ok || tok.Value == "" {
		t.Fatalf("token missing or empty")
	}
}

// TestPhasedRejectAgainstLocalStack verifies the REJECTED terminal path and
// that a rejected job reports AlreadyCompleted on re-admission.
func TestPhasedRejectAgainstLocalStack(t *testing.T) {
	client := newPhasedClient(t)
	jobID := "phased-reject-" + time.Now().UTC().Format("20060102150405.000000000")
	fileID := "file-" + jobID
	receipt := f2e.Receipt{ReceiptID: jobID, FileID: fileID, Source: f2e.ObjectIdentity{Bucket: "b", Key: "k", VersionID: "v1", ETag: "e"}, ReceivedAt: time.Now().UTC()}
	if got, err := client.Admit(t.Context(), receipt); err != nil || got != f2e.Acquired {
		t.Fatalf("admit: got=%s err=%v", got, err)
	}
	if err := client.RejectJob(t.Context(), fileID, f2e.Rejection{Reason: f2e.RejectionEmptyFile}); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if got, _ := client.Admit(t.Context(), receipt); got != f2e.AlreadyCompleted {
		t.Fatalf("admit after reject: got=%s, want alreadyCompleted", got)
	}
	out, err := client.DynamoDB.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("f2e-job-ledger"), Key: ledgerKey(fileID, "JOB"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if status := out.Item["status"].(*types.AttributeValueMemberS).Value; status != string(f2e.JobStateRejected) {
		t.Fatalf("status=%s, want REJECTED", status)
	}
}

// TestPhasedFinalizeAgainstLocalStack verifies FinalizeJob records the terminal
// result + counts atomically and never regresses.
func TestPhasedFinalizeAgainstLocalStack(t *testing.T) {
	client := newPhasedClient(t)
	jobID := "phased-finalize-" + time.Now().UTC().Format("20060102150405.000000000")
	seedJob(t, client, jobID, f2e.JobStateProcessing)

	counts := f2e.Counts{RecordsRead: 5, RecordsPublished: 4, RecordsRejected: 1, MessagesPublished: 4, CountsComplete: true}
	if err := client.FinalizeJob(t.Context(), jobID, f2e.JobResultWithRejections, counts); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	// Idempotent — a duplicate finalize must not regress or error.
	if err := client.FinalizeJob(t.Context(), jobID, f2e.JobResultWithRejections, counts); err != nil {
		t.Fatalf("finalize (repeat): %v", err)
	}

	out, err := client.DynamoDB.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("f2e-job-ledger"), Key: ledgerKey(jobID, "JOB"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if status := out.Item["status"].(*types.AttributeValueMemberS).Value; status != string(f2e.JobStateCompleted) {
		t.Fatalf("status=%s, want COMPLETED", status)
	}
	if result := out.Item["result"].(*types.AttributeValueMemberS).Value; result != string(f2e.JobResultWithRejections) {
		t.Fatalf("result=%s, want WITH_REJECTIONS", result)
	}
	if _, ok := out.Item["counts"].(*types.AttributeValueMemberM); !ok {
		t.Fatalf("counts missing")
	}
	if _, ok := out.Item["completedAt"].(*types.AttributeValueMemberS); !ok {
		t.Fatalf("completedAt missing")
	}
}

// TestPhasedNoRegressionAgainstLocalStack proves a terminal job never regresses:
// validation, planning and rejection writes all fail without changing state.
func TestPhasedNoRegressionAgainstLocalStack(t *testing.T) {
	client := newPhasedClient(t)
	jobID := "phased-noregress-" + time.Now().UTC().Format("20060102150405.000000000")
	seedJob(t, client, jobID, f2e.JobStateCompleted)

	if err := client.BeginValidation(t.Context(), jobID); err == nil {
		t.Fatalf("begin validation on completed job should fail")
	}
	if err := client.BeginPlanning(t.Context(), jobID); err == nil {
		t.Fatalf("begin planning on completed job should fail")
	}
	if err := client.RejectJob(t.Context(), jobID, f2e.Rejection{Reason: f2e.RejectionEmptyFile}); err == nil {
		t.Fatalf("reject on completed job should fail")
	}

	out, err := client.DynamoDB.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("f2e-job-ledger"), Key: ledgerKey(jobID, "JOB"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if status := out.Item["status"].(*types.AttributeValueMemberS).Value; status != string(f2e.JobStateCompleted) {
		t.Fatalf("status=%s, want COMPLETED (no regression)", status)
	}
}

// TestPhasedAcquireChunkAgainstLocalStack verifies chunk acquisition: first
// claim wins, a repeat reports Busy.
func TestPhasedAcquireChunkAgainstLocalStack(t *testing.T) {
	client := newPhasedClient(t)
	jobID := "phased-chunk-" + time.Now().UTC().Format("20060102150405.000000000")
	seedChunk(t, client, jobID, "00000001")

	if got, err := client.AcquireChunk(t.Context(), jobID, "00000001"); err != nil || got != f2e.Acquired {
		t.Fatalf("acquire: got=%s err=%v, want acquired", got, err)
	}
	if got, _ := client.AcquireChunk(t.Context(), jobID, "00000001"); got != f2e.Busy {
		t.Fatalf("re-acquire: got=%s, want busy", got)
	}
}

func TestPhasedConcurrentChunkClaimAgainstLocalStack(t *testing.T) {
	client := newPhasedClient(t)
	jobID := "phased-concurrent-" + time.Now().UTC().Format("20060102150405.000000000")
	seedChunk(t, client, jobID, "00000001")

	const workers = 8
	results := make(chan f2e.AcquisitionResult, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, _ := client.AcquireChunk(context.Background(), jobID, "00000001")
			results <- got
		}()
	}
	wg.Wait()
	close(results)
	acquired := 0
	for r := range results {
		if r == f2e.Acquired {
			acquired++
		}
	}
	if acquired != 1 {
		t.Fatalf("acquired=%d, want exactly 1", acquired)
	}
}

func TestPhasedAdmissionDeduplicatesDifferentReceiptsForSameFileAgainstLocalStack(t *testing.T) {
	client := newPhasedClient(t)
	fileID := "file-version-" + time.Now().UTC().Format("20060102150405.000000000")
	source := f2e.ObjectIdentity{Bucket: "bucket", Key: "key", VersionID: "version-1", ETag: "etag", Size: 10}
	first := f2e.Receipt{ReceiptID: "receipt-a-" + fileID, FileID: fileID, Source: source, ReceivedAt: time.Now().UTC()}
	second := first
	second.ReceiptID = "receipt-b-" + fileID

	if got, err := client.Admit(t.Context(), first); err != nil || got != f2e.Acquired {
		t.Fatalf("first admit: got=%s err=%v", got, err)
	}
	if got, err := client.Admit(t.Context(), second); err != nil || got != f2e.Busy {
		t.Fatalf("second receipt for same version: got=%s err=%v, want busy", got, err)
	}
}
