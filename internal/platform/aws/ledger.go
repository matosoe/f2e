package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
)

var _ port.JobLedger = (*AWS)(nil)

func text(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }
func number(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func (a *AWS) Replay(ctx context.Context, sourceJobID, newJobID string, chunkIDs []string) ([]f2e.ChunkJob, error) {
	if a.LedgerTable == "" || sourceJobID == "" || newJobID == "" || sourceJobID == newJobID {
		return nil, fmt.Errorf("invalid replay request")
	}
	wanted := make(map[string]struct{}, len(chunkIDs))
	for _, id := range chunkIDs {
		wanted[id] = struct{}{}
	}
	query := &dynamodb.QueryInput{
		TableName: aws.String(a.LedgerTable), KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": text("JOB#" + sourceJobID)}, ConsistentRead: aws.Bool(true),
	}
	jobs := make([]f2e.ChunkJob, 0)
	for {
		out, err := a.DynamoDB.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			payload, ok := item["payload"].(*types.AttributeValueMemberS)
			if !ok {
				continue
			}
			var job f2e.ChunkJob
			if err := json.Unmarshal([]byte(payload.Value), &job); err != nil {
				return nil, fmt.Errorf("decode replay chunk: %w", err)
			}
			if len(wanted) > 0 {
				if _, ok := wanted[job.ChunkID]; !ok {
					continue
				}
			}
			job.JobID = newJobID
			jobs = append(jobs, job)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		query.ExclusiveStartKey = out.LastEvaluatedKey
	}
	if len(jobs) == 0 || (len(wanted) > 0 && len(jobs) != len(wanted)) {
		return nil, fmt.Errorf("replay chunks not found or incomplete")
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ChunkID < jobs[j].ChunkID })
	now := time.Now().UTC()
	retentionDays := a.LedgerRetentionDays
	if retentionDays < 1 {
		retentionDays = 90
	}
	auditBody, _ := json.Marshal(map[string]any{"sourceJobId": sourceJobID, "newJobId": newJobID, "chunkIds": chunkIDs, "createdAt": now.Format(time.RFC3339Nano)})
	_, err := a.DynamoDB.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String(a.LedgerTable), Item: map[string]types.AttributeValue{
		"pk": text("JOB#" + sourceJobID), "sk": text("AUDIT#REPLAY#" + now.Format(time.RFC3339Nano) + "#" + newJobID), "payload": text(string(auditBody)), "expiresAt": number(now.Add(time.Duration(retentionDays) * 24 * time.Hour).Unix()),
	}, ConditionExpression: aws.String("attribute_not_exists(pk) AND attribute_not_exists(sk)")})
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (a *AWS) Plan(ctx context.Context, plan f2e.JobPlan, chunks []f2e.ChunkJob) error {
	if a.LedgerTable == "" {
		return fmt.Errorf("ledger table is not configured")
	}
	configSnapshotJSON, _ := json.Marshal(plan.ConfigSnapshot)
	values := map[string]types.AttributeValue{
		":jobId": text(plan.JobID), ":fileId": text(plan.FileID), ":bucket": text(plan.Bucket), ":key": text(plan.Key), ":versionId": text(plan.VersionID), ":etag": text(plan.ETag),
		":pending": text(string(f2e.JobPending)), ":expected": number(int64(plan.ExpectedChunks)), ":zero": number(0), ":created": text(plan.CreatedAt.Format(time.RFC3339Nano)),
		":configSnapshot": text(string(configSnapshotJSON)),
	}
	retentionDays := a.LedgerRetentionDays
	if retentionDays < 1 {
		retentionDays = 90
	}
	expiresAt := number(plan.CreatedAt.Add(time.Duration(retentionDays) * 24 * time.Hour).Unix())
	values[":expiresAt"] = expiresAt
	if _, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(a.LedgerTable), Key: ledgerKey(plan.JobID, "JOB"),
		ConditionExpression:      aws.String("attribute_not_exists(pk) OR (fileId = :fileId AND expectedChunks = :expected)"),
		UpdateExpression:         aws.String("SET jobId = :jobId, fileId = :fileId, #bucket = :bucket, #key = :key, versionId = :versionId, etag = :etag, #s = if_not_exists(#s, :pending), expectedChunks = :expected, completedChunks = if_not_exists(completedChunks, :zero), failedChunks = if_not_exists(failedChunks, :zero), recordsProduced = if_not_exists(recordsProduced, :zero), createdAt = if_not_exists(createdAt, :created), expiresAt = if_not_exists(expiresAt, :expiresAt), configSnapshot = if_not_exists(configSnapshot, :configSnapshot)"),
		ExpressionAttributeNames: map[string]string{"#s": "status", "#bucket": "bucket", "#key": "key"}, ExpressionAttributeValues: values,
	}); err != nil {
		return err
	}
	for _, chunk := range chunks {
		persistedChunk := chunk
		persistedChunk.PresignedURL = "" // credentials are never written to the ledger
		payload, err := json.Marshal(persistedChunk)
		if err != nil {
			return fmt.Errorf("encode ledger chunk: %w", err)
		}
		chunkValues := map[string]types.AttributeValue{
			":jobId": text(plan.JobID), ":chunkId": text(chunk.ChunkID), ":pending": text(string(f2e.JobPending)), ":start": number(chunk.StartByte), ":end": number(chunk.EndByteInclusive), ":zero": number(0), ":expiresAt": expiresAt, ":payload": text(string(payload)),
		}
		if _, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(a.LedgerTable), Key: ledgerKey(plan.JobID, "CHUNK#"+chunk.ChunkID),
			ConditionExpression:      aws.String("attribute_not_exists(sk) OR (startByte = :start AND endByte = :end)"),
			UpdateExpression:         aws.String("SET jobId = :jobId, chunkId = :chunkId, #s = if_not_exists(#s, :pending), startByte = :start, endByte = :end, attempt = if_not_exists(attempt, :zero), expiresAt = if_not_exists(expiresAt, :expiresAt), payload = if_not_exists(payload, :payload)"),
			ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: chunkValues,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *AWS) MarkScheduled(ctx context.Context, jobIDs []string) error {
	return a.markJobs(ctx, jobIDs, f2e.JobScheduled, "")
}

func (a *AWS) MarkSchedulingFailed(ctx context.Context, jobIDs []string, message string) error {
	return a.markJobs(ctx, jobIDs, f2e.JobSchedulingFailed, message)
}

func (a *AWS) markJobs(ctx context.Context, jobIDs []string, status f2e.JobStatus, message string) error {
	for _, jobID := range jobIDs {
		values := map[string]types.AttributeValue{":status": text(string(status)), ":now": text(time.Now().UTC().Format(time.RFC3339Nano))}
		update := "SET #s = :status, updatedAt = :now"
		if message != "" {
			values[":error"] = text(message)
			update += ", lastError = :error"
		}
		if _, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "JOB"), ConditionExpression: aws.String("attribute_exists(pk)"), UpdateExpression: aws.String(update), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: values}); err != nil {
			return err
		}
	}
	return nil
}

func (a *AWS) StartChunk(ctx context.Context, jobID, chunkID string, attempt int) error {
	_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "CHUNK#"+chunkID), ConditionExpression: aws.String("attribute_exists(pk) AND #s <> :completed"), UpdateExpression: aws.String("SET #s = :running, attempt = :attempt, startedAt = if_not_exists(startedAt, :now)"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":running": text(string(f2e.JobRunning)), ":completed": text(string(f2e.JobCompleted)), ":attempt": number(int64(attempt)), ":now": text(time.Now().UTC().Format(time.RFC3339Nano))}})
	if err != nil {
		status, readErr := a.chunkStatus(ctx, jobID, chunkID)
		if readErr == nil && status == string(f2e.JobCompleted) {
			return nil
		}
	}
	return err
}

func (a *AWS) CompleteChunk(ctx context.Context, result f2e.ChunkResult) error {
	status, err := a.chunkStatus(ctx, result.JobID, result.ChunkID)
	if err != nil {
		return err
	}
	if status == string(f2e.JobCompleted) {
		return nil
	}
	_, err = a.DynamoDB.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "CHUNK#"+result.ChunkID), ConditionExpression: aws.String("#s <> :completed"), UpdateExpression: aws.String("SET #s = :completed, recordsProduced = :records, bytesProcessed = :bytes, completedAt = :now"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":completed": text(string(f2e.JobCompleted)), ":records": number(result.RecordsProduced), ":bytes": number(result.BytesProcessed), ":now": text(result.OccurredAt.Format(time.RFC3339Nano))}}},
		{Update: &types.Update{TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "JOB"), UpdateExpression: aws.String("SET #s = :running ADD completedChunks :one, recordsProduced :records"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":one": number(1), ":records": number(result.RecordsProduced), ":running": text(string(f2e.JobRunning))}}},
	}})
	if err != nil {
		status, readErr := a.chunkStatus(ctx, result.JobID, result.ChunkID)
		if readErr != nil || status != string(f2e.JobCompleted) {
			return err
		}
		return nil
	}
	return a.reconcileJob(ctx, result.JobID)
}

func (a *AWS) FailChunk(ctx context.Context, result f2e.ChunkResult) error {
	maxReceiveCount := a.MaxReceiveCount
	if maxReceiveCount < 1 {
		maxReceiveCount = 3
	}
	if result.Attempt < maxReceiveCount {
		_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "CHUNK#"+result.ChunkID),
			UpdateExpression:         aws.String("SET #s = :pending, attempt = :attempt, lastError = :error, lastAttemptAt = :now"),
			ExpressionAttributeNames: map[string]string{"#s": "status"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pending": text(string(f2e.JobPending)), ":attempt": number(int64(result.Attempt)), ":error": text(result.Error), ":now": text(result.OccurredAt.Format(time.RFC3339Nano)),
			},
		})
		return err
	}
	status, err := a.chunkStatus(ctx, result.JobID, result.ChunkID)
	if err != nil {
		return err
	}
	if status == string(f2e.JobFailed) || status == string(f2e.JobCompleted) {
		return nil
	}
	_, err = a.DynamoDB.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "CHUNK#"+result.ChunkID), ConditionExpression: aws.String("#s <> :failed AND #s <> :completed"), UpdateExpression: aws.String("SET #s = :failed, lastError = :error, completedAt = :now"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":failed": text(string(f2e.JobFailed)), ":completed": text(string(f2e.JobCompleted)), ":error": text(result.Error), ":now": text(result.OccurredAt.Format(time.RFC3339Nano))}}},
		{Update: &types.Update{TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "JOB"), UpdateExpression: aws.String("SET #s = :running ADD failedChunks :one"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":one": number(1), ":running": text(string(f2e.JobRunning))}}},
	}})
	if err != nil {
		status, readErr := a.chunkStatus(ctx, result.JobID, result.ChunkID)
		if readErr != nil || (status != string(f2e.JobFailed) && status != string(f2e.JobCompleted)) {
			return err
		}
		return nil
	}
	return a.reconcileJob(ctx, result.JobID)
}

func (a *AWS) chunkStatus(ctx context.Context, jobID, chunkID string) (string, error) {
	out, err := a.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "CHUNK#"+chunkID), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return "", err
	}
	if value, ok := out.Item["status"].(*types.AttributeValueMemberS); ok {
		return value.Value, nil
	}
	return "", nil
}

func (a *AWS) reconcileJob(ctx context.Context, jobID string) error {
	out, err := a.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "JOB"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return err
	}
	read := func(name string) int64 {
		if value, ok := out.Item[name].(*types.AttributeValueMemberN); ok {
			n, _ := strconv.ParseInt(value.Value, 10, 64)
			return n
		}
		return 0
	}
	expected, completed, failed := read("expectedChunks"), read("completedChunks"), read("failedChunks")
	if expected == 0 || completed+failed < expected {
		return nil
	}
	status := f2e.JobCompleted
	if failed > 0 {
		status = f2e.JobFailed
	}
	_, err = a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "JOB"), UpdateExpression: aws.String("SET #s = :status, completedAt = :now"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":status": text(string(status)), ":now": text(time.Now().UTC().Format(time.RFC3339Nano))}})
	return err
}

func ledgerKey(jobID, sortKey string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": text("JOB#" + jobID), "sk": text(sortKey)}
}
