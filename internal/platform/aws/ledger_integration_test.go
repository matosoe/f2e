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
	jobID := "integration-job-" + time.Now().UTC().Format("20060102150405.000000000")
	chunk := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: jobID, FileID: "file", ChunkID: "00000001", Bucket: "bucket", Key: "key", ETag: "etag", FileSize: 10, StartByte: 0, EndByteInclusive: 9, DataType: f2e.DataTypeText}
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
	jobID := "integration-concurrent-" + time.Now().UTC().Format("20060102150405.000000000")
	chunks := make([]f2e.ChunkJob, count)
	for i := range chunks {
		chunks[i] = f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: jobID, FileID: "file", ChunkID: fmt.Sprintf("%08d", i+1), Bucket: "bucket", Key: "key", ETag: "etag", FileSize: count, StartByte: int64(i), EndByteInclusive: int64(i), DataType: f2e.DataTypeText}
	}
	plan := f2e.JobPlan{JobID: jobID, FileID: "file", Bucket: "bucket", Key: "key", ETag: "etag", ExpectedChunks: count, CreatedAt: now}
	if err := client.Plan(t.Context(), plan, chunks); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, count*2)
	completedExecutions := make(chan struct{}, count*2)
	for _, chunk := range chunks {
		for duplicate := 0; duplicate < 2; duplicate++ {
			wg.Add(1)
			go func(chunk f2e.ChunkJob) {
				defer wg.Done()
				if err := client.StartChunk(context.Background(), chunk.JobID, chunk.ChunkID, 1); err != nil {
					// Only one concurrent claim is entitled to execute. A losing
					// delivery is retried by SQS and must not count as completion.
					return
				}
				if err := client.CompleteChunk(context.Background(), f2e.ChunkResult{JobID: chunk.JobID, ChunkID: chunk.ChunkID, Attempt: 1, RecordsProduced: 1, BytesProcessed: 1, OccurredAt: now}); err != nil {
					errs <- err
					return
				}
				completedExecutions <- struct{}{}
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
	if got := len(completedExecutions); got != count {
		t.Fatalf("executed completions=%d, want %d", got, count)
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

func TestLedgerManifestScheduleCheckpointsAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{Endpoint: endpoint, Region: "us-east-1", LedgerTable: "f2e-job-ledger", LedgerRetentionDays: 90})
	if err != nil {
		t.Fatal(err)
	}
	jobID := "manifest-" + time.Now().UTC().Format("20060102150405.000000000")
	chunks := []f2e.ChunkJob{
		{SchemaVersion: f2e.SchemaVersion, JobID: jobID, FileID: "file", ChunkID: "00000001", Bucket: "bucket", Key: "key", VersionID: "v1", ETag: "etag", FileSize: 2, StartByte: 0, EndByteInclusive: 0, DataType: f2e.DataTypeText},
		{SchemaVersion: f2e.SchemaVersion, JobID: jobID, FileID: "file", ChunkID: "00000002", Bucket: "bucket", Key: "key", VersionID: "v1", ETag: "etag", FileSize: 2, StartByte: 1, EndByteInclusive: 1, DataType: f2e.DataTypeText},
	}
	if err := client.Plan(t.Context(), f2e.JobPlan{JobID: jobID, FileID: "file", Bucket: "bucket", Key: "key", VersionID: "v1", ETag: "etag", ExpectedChunks: len(chunks), CreatedAt: time.Now().UTC()}, chunks); err != nil {
		t.Fatal(err)
	}
	unscheduled, err := client.UnscheduledChunks(t.Context(), jobID)
	if err != nil || len(unscheduled) != 2 {
		t.Fatalf("before checkpoints: chunks=%d err=%v, want 2", len(unscheduled), err)
	}
	if err := client.MarkChunksScheduled(t.Context(), chunks[:1]); err != nil {
		t.Fatal(err)
	}
	unscheduled, err = client.UnscheduledChunks(t.Context(), jobID)
	if err != nil || len(unscheduled) != 1 || unscheduled[0].ChunkID != "00000002" {
		t.Fatalf("after checkpoint: chunks=%+v err=%v, want only 00000002", unscheduled, err)
	}
}

func TestLedgerAggregatesReasonCountsAgainstLocalStack(t *testing.T) {
	endpoint := os.Getenv("F2E_TEST_AWS_ENDPOINT")
	if endpoint == "" {
		t.Skip("set F2E_TEST_AWS_ENDPOINT to run the LocalStack integration test")
	}
	client, err := New(t.Context(), config.Config{Endpoint: endpoint, Region: "us-east-1", LedgerTable: "f2e-job-ledger", LedgerRetentionDays: 90})
	if err != nil {
		t.Fatal(err)
	}
	jobID := "reasons-" + time.Now().UTC().Format("20060102150405.000000000")
	chunk := f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: jobID, FileID: "file", ChunkID: "00000001", Bucket: "bucket", Key: "key", VersionID: "v1", ETag: "etag", FileSize: 1, EndByteInclusive: 0, DataType: f2e.DataTypeText}
	if err := client.Plan(t.Context(), f2e.JobPlan{JobID: jobID, FileID: "file", Bucket: "bucket", Key: "key", VersionID: "v1", ETag: "etag", ExpectedChunks: 1, CreatedAt: time.Now().UTC()}, []f2e.ChunkJob{chunk}); err != nil {
		t.Fatal(err)
	}
	if err := client.StartChunk(t.Context(), jobID, chunk.ChunkID, 1); err != nil {
		t.Fatal(err)
	}
	counts := f2e.Counts{RecordsRead: 2, RecordsPublished: 1, RecordsRejected: 1, CountsComplete: true, Reasons: map[f2e.RejectionReason]int64{f2e.RejectionProcessorRejected: 1}}
	if err := client.CompleteChunk(t.Context(), f2e.ChunkResult{JobID: jobID, ChunkID: chunk.ChunkID, Attempt: 1, RecordsProduced: 1, BytesProcessed: 1, OccurredAt: time.Now().UTC(), Counts: counts}); err != nil {
		t.Fatal(err)
	}
	out, err := client.DynamoDB.GetItem(t.Context(), &dynamodb.GetItemInput{TableName: aws.String("f2e-job-ledger"), Key: ledgerKey(jobID, "JOB"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if got := out.Item["reasonCount_PROCESSOR_REJECTED"].(*types.AttributeValueMemberN).Value; got != "1" {
		t.Fatalf("reason count=%s, want 1", got)
	}
}
