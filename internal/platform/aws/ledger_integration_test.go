package aws

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

func TestLedgerLifecycleAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{Endpoint: endpoint, Region: "us-east-1", LedgerTable: "f2e-job-ledger", LedgerRetentionDays: 90, MaxReceiveCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	chunk := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: "integration-job", FileID: "file", ChunkID: "00000001", Bucket: "bucket", Key: "key", ETag: "etag", FileSize: 10, StartByte: 0, EndByteInclusive: 9, DataType: f2e.DataTypeText}
	plan := f2e.JobPlan{JobID: chunk.JobID, FileID: chunk.FileID, Bucket: chunk.Bucket, Key: chunk.Key, ETag: chunk.ETag, ExpectedChunks: 1, CreatedAt: now}
	if err := client.Plan(context.Background(), plan, []f2e.ChunkJob{chunk}); err != nil {
		t.Fatalf("plan: %v", err)
	}
	if err := client.MarkScheduled(t.Context(), []string{chunk.JobID}); err != nil {
		t.Fatalf("scheduled: %v", err)
	}
	if err := client.StartChunk(t.Context(), chunk.JobID, chunk.ChunkID, 1); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := client.CompleteChunk(t.Context(), f2e.ChunkResult{JobID: chunk.JobID, ChunkID: chunk.ChunkID, Attempt: 1, RecordsProduced: 1, BytesProcessed: 10, OccurredAt: now}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// A duplicate terminal acknowledgement must not increment job counters.
	if err := client.CompleteChunk(t.Context(), f2e.ChunkResult{JobID: chunk.JobID, ChunkID: chunk.ChunkID, Attempt: 1, RecordsProduced: 1, BytesProcessed: 10, OccurredAt: now}); err != nil {
		t.Fatalf("idempotent complete: %v", err)
	}
}

func TestLedgerConcurrentCompletionAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{Endpoint: endpoint, Region: "us-east-1", LedgerTable: "f2e-job-ledger", LedgerRetentionDays: 90, MaxReceiveCount: 3})
	if err != nil {
		t.Fatal(err)
	}
	const count = 8
	now := time.Now().UTC()
	chunks := make([]f2e.ChunkJob, count)
	for i := range chunks {
		chunks[i] = f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: "integration-concurrent", FileID: "file", ChunkID: fmt.Sprintf("%08d", i+1), Bucket: "bucket", Key: "key", ETag: "etag", FileSize: count, StartByte: int64(i), EndByteInclusive: int64(i), DataType: f2e.DataTypeText}
	}
	plan := f2e.JobPlan{JobID: "integration-concurrent", FileID: "file", Bucket: "bucket", Key: "key", ETag: "etag", ExpectedChunks: count, CreatedAt: now}
	if err := client.Plan(t.Context(), plan, chunks); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, count*2)
	for _, chunk := range chunks {
		for duplicate := 0; duplicate < 2; duplicate++ {
			wg.Add(1)
			go func(chunk f2e.ChunkJob) {
				defer wg.Done()
				if err := client.StartChunk(context.Background(), chunk.JobID, chunk.ChunkID, 1); err != nil {
					errs <- err
					return
				}
				errs <- client.CompleteChunk(context.Background(), f2e.ChunkResult{JobID: chunk.JobID, ChunkID: chunk.ChunkID, Attempt: 1, RecordsProduced: 1, BytesProcessed: 1, OccurredAt: now})
			}(chunk)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	out, err := client.DynamoDB.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("f2e-job-ledger"), Key: ledgerKey(plan.JobID, "JOB"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	status := out.Item["status"].(*types.AttributeValueMemberS).Value
	completed, _ := strconv.Atoi(out.Item["completedChunks"].(*types.AttributeValueMemberN).Value)
	if status != string(f2e.JobCompleted) || completed != count {
		t.Fatalf("status=%s completed=%d, want COMPLETED/%d", status, completed, count)
	}
}
