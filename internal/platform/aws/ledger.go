package aws

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
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

// boolAttr wraps a Go bool for use in an UpdateItem value map.
func boolAttr(v bool) types.AttributeValue {
	return &types.AttributeValueMemberBOOL{Value: v}
}

// emptyList returns an empty DynamoDB list, used with if_not_exists before
// list_append so transition history can be appended on the first transition.
func emptyList() types.AttributeValue {
	return &types.AttributeValueMemberL{Value: []types.AttributeValue{}}
}

// singleList wraps a single transition entry so it can be appended to the
// history list via list_append(if_not_exists(history, :empty), :entry).
func singleList(v types.AttributeValue) types.AttributeValue {
	return &types.AttributeValueMemberL{Value: []types.AttributeValue{v}}
}

// transitionEntry encodes a Transition into a DynamoDB map for the history list.
func transitionEntry(from, to, at, reason string) types.AttributeValue {
	m := map[string]types.AttributeValue{
		"from": text(from),
		"to":   text(to),
		"at":   text(at),
	}
	if reason != "" {
		m["reason"] = text(reason)
	}
	return &types.AttributeValueMemberM{Value: m}
}

// countsMap encodes Counts into a nested DynamoDB map written to the job item.
func countsMap(c f2e.Counts) types.AttributeValue {
	values := map[string]types.AttributeValue{
		"recordsRead":          number(c.RecordsRead),
		"recordsPublished":     number(c.RecordsPublished),
		"recordsRejected":      number(c.RecordsRejected),
		"recordsIgnored":       number(c.RecordsIgnored),
		"messagesPublished":    number(c.MessagesPublished),
		"physicalLinesIgnored": number(c.PhysicalLinesIgnored),
		"countsComplete":       boolAttr(c.CountsComplete),
	}
	reasons := map[string]types.AttributeValue{}
	for reason, count := range c.Reasons {
		reasons[string(reason)] = number(count)
	}
	values["reasonCounts"] = &types.AttributeValueMemberM{Value: reasons}
	return &types.AttributeValueMemberM{Value: values}
}

// conditionalConflict reports whether err is a DynamoDB conditional-check
// failure, which is the signal that a state-guarded write did not win.
func conditionalConflict(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
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
		ConditionExpression:      aws.String("attribute_not_exists(pk) OR (fileId = :fileId AND (attribute_not_exists(expectedChunks) OR expectedChunks = :expected))"),
		UpdateExpression:         aws.String("SET jobId = :jobId, fileId = :fileId, #bucket = :bucket, #key = :key, versionId = :versionId, etag = :etag, #s = if_not_exists(#s, :pending), expectedChunks = :expected, completedChunks = if_not_exists(completedChunks, :zero), failedChunks = if_not_exists(failedChunks, :zero), recordsProduced = if_not_exists(recordsProduced, :zero), createdAt = if_not_exists(createdAt, :created), expiresAt = if_not_exists(expiresAt, :expiresAt), configSnapshot = if_not_exists(configSnapshot, :configSnapshot), manifestSealed = if_not_exists(manifestSealed, :false)"),
		ExpressionAttributeNames: map[string]string{"#s": "status", "#bucket": "bucket", "#key": "key"}, ExpressionAttributeValues: func() map[string]types.AttributeValue { values[":false"] = boolAttr(false); return values }(),
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
	// Seal only after every individual chunk write succeeded. This intentionally
	// avoids a DynamoDB transaction sized by the number of chunks; a crash before
	// this point is resumed by repeating the idempotent Plan call.
	sealedStatus := f2e.JobPending
	if len(chunks) == 0 {
		sealedStatus = f2e.JobCompleted
	}
	_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(a.LedgerTable), Key: ledgerKey(plan.JobID, "JOB"),
		ConditionExpression:       aws.String("fileId = :fileId AND expectedChunks = :expected AND (attribute_not_exists(manifestSealed) OR manifestSealed = :false)"),
		UpdateExpression:          aws.String("SET manifestSealed = :true, sealedAt = :now, #s = :status, pendingChunks = :expected, updatedAt = :now"),
		ExpressionAttributeNames:  map[string]string{"#s": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{":fileId": text(plan.FileID), ":expected": number(int64(plan.ExpectedChunks)), ":false": boolAttr(false), ":true": boolAttr(true), ":now": text(time.Now().UTC().Format(time.RFC3339Nano)), ":status": text(string(sealedStatus))},
	})
	if err != nil && !conditionalConflict(err) {
		return err
	}
	return nil
}

// MarkChunksScheduled saves one durable checkpoint per confirmed SQS message.
// It deliberately does not change chunk status: a Worker still owns the
// PENDING -> RUNNING transition.
func (a *AWS) MarkChunksScheduled(ctx context.Context, chunks []f2e.ChunkJob) error {
	for _, chunk := range chunks {
		_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(chunk.JobID, "CHUNK#"+chunk.ChunkID), ConditionExpression: aws.String("#s = :pending"), UpdateExpression: aws.String("SET scheduledAt = if_not_exists(scheduledAt, :now)"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":pending": text(string(f2e.ChunkStatePending)), ":now": text(time.Now().UTC().Format(time.RFC3339Nano))}})
		if err != nil && !conditionalConflict(err) {
			return err
		}
	}
	return nil
}

func (a *AWS) UnscheduledChunks(ctx context.Context, jobID string) ([]f2e.ChunkJob, error) {
	query := &dynamodb.QueryInput{TableName: aws.String(a.LedgerTable), KeyConditionExpression: aws.String("pk = :pk AND begins_with(sk, :chunk)"), FilterExpression: aws.String("attribute_not_exists(scheduledAt)"), ExpressionAttributeValues: map[string]types.AttributeValue{":pk": text("JOB#" + jobID), ":chunk": text("CHUNK#")}, ConsistentRead: aws.Bool(true)}
	var jobs []f2e.ChunkJob
	for {
		out, err := a.DynamoDB.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			payload, ok := item["payload"].(*types.AttributeValueMemberS)
			if !ok {
				return nil, fmt.Errorf("chunk %s has no resumable payload", jobID)
			}
			var chunk f2e.ChunkJob
			if err := json.Unmarshal([]byte(payload.Value), &chunk); err != nil {
				return nil, fmt.Errorf("decode resumable chunk: %w", err)
			}
			jobs = append(jobs, chunk)
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		query.ExclusiveStartKey = out.LastEvaluatedKey
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ChunkID < jobs[j].ChunkID })
	return jobs, nil
}

func (a *AWS) MarkScheduled(ctx context.Context, jobIDs []string) error {
	return a.markJobs(ctx, jobIDs, f2e.JobScheduled, "")
}

func (a *AWS) MarkSchedulingFailed(ctx context.Context, jobIDs []string, message string) error {
	return a.markJobs(ctx, jobIDs, f2e.JobSchedulingFailed, message)
}

func (a *AWS) markJobs(ctx context.Context, jobIDs []string, status f2e.JobStatus, message string) error {
	for _, jobID := range jobIDs {
		values := map[string]types.AttributeValue{":status": text(string(status)), ":now": text(time.Now().UTC().Format(time.RFC3339Nano)), ":pending": text(string(f2e.JobPending)), ":planning": text(string(f2e.JobStatePlanning)), ":scheduled": text(string(f2e.JobScheduled))}
		update := "SET #s = :status, updatedAt = :now"
		condition := "attribute_exists(pk) AND manifestSealed = :sealed AND (#s = :pending OR #s = :planning OR #s = :scheduled)"
		if status == f2e.JobSchedulingFailed {
			values[":schedulingFailed"] = text(string(f2e.JobSchedulingFailed))
			condition = "attribute_exists(pk) AND manifestSealed = :sealed AND (#s = :pending OR #s = :planning OR #s = :schedulingFailed)"
		}
		if message != "" {
			values[":error"] = text(message)
			update += ", lastError = :error"
		}
		values[":sealed"] = boolAttr(true)
		if _, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "JOB"), ConditionExpression: aws.String(condition), UpdateExpression: aws.String(update), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: values}); err != nil {
			return err
		}
	}
	return nil
}

func (a *AWS) StartChunk(ctx context.Context, jobID, chunkID string, attempt int) error {
	// A worker may only start an unclaimed pending attempt. In particular, a
	// delayed delivery cannot move a completed/failed chunk back to RUNNING.
	_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "CHUNK#"+chunkID), ConditionExpression: aws.String("attribute_exists(pk) AND (#s = :pending OR #s = :retryPending)"), UpdateExpression: aws.String("SET #s = :running, attempt = :attempt, startedAt = if_not_exists(startedAt, :now), revision = if_not_exists(revision, :zero) + :one"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":running": text(string(f2e.ChunkStateRunning)), ":pending": text(string(f2e.ChunkStatePending)), ":retryPending": text(string(f2e.ChunkStateRetryPending)), ":attempt": number(int64(attempt)), ":now": text(time.Now().UTC().Format(time.RFC3339Nano)), ":zero": number(0), ":one": number(1)}})
	if err != nil {
		status, readErr := a.chunkStatus(ctx, jobID, chunkID)
		if readErr == nil && status == string(f2e.ChunkStateCompleted) {
			return port.ErrAlreadyCompleted
		}
	}
	return err
}

func (a *AWS) CompleteChunk(ctx context.Context, result f2e.ChunkResult) error {
	counts := result.Counts
	if counts.RecordsRead == 0 && counts.RecordsPublished == 0 && counts.RecordsRejected == 0 && counts.RecordsIgnored == 0 && result.RecordsProduced > 0 {
		counts = f2e.Counts{RecordsRead: result.RecordsProduced, RecordsPublished: result.RecordsProduced, MessagesPublished: result.RecordsProduced, CountsComplete: true}
	}
	status, err := a.chunkStatus(ctx, result.JobID, result.ChunkID)
	if err != nil {
		return err
	}
	if status == string(f2e.ChunkStateCompleted) {
		return nil
	}
	jobUpdate := "SET #s = :running ADD completedChunks :one, recordsProduced :records, recordsRead :read, recordsPublished :published, recordsRejected :rejected, recordsIgnored :ignored, messagesPublished :messages, physicalLinesIgnored :physicalIgnored"
	jobNames := map[string]string{"#s": "status"}
	jobValues := map[string]types.AttributeValue{":one": number(1), ":records": number(counts.RecordsPublished), ":read": number(counts.RecordsRead), ":published": number(counts.RecordsPublished), ":rejected": number(counts.RecordsRejected), ":ignored": number(counts.RecordsIgnored), ":messages": number(counts.MessagesPublished), ":physicalIgnored": number(counts.PhysicalLinesIgnored), ":pending": text(string(f2e.JobPending)), ":scheduled": text(string(f2e.JobScheduled)), ":running": text(string(f2e.JobRunning))}
	i := 0
	for reason, count := range counts.Reasons {
		if count == 0 {
			continue
		}
		name, value := fmt.Sprintf("#reason%d", i), fmt.Sprintf(":reason%d", i)
		jobUpdate += ", " + name + " " + value
		jobNames[name] = "reasonCount_" + string(reason)
		jobValues[value] = number(count)
		i++
	}
	_, err = a.DynamoDB.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "CHUNK#"+result.ChunkID), ConditionExpression: aws.String("#s = :running AND attempt = :attempt"), UpdateExpression: aws.String("SET #s = :completed, recordsProduced = :records, bytesProcessed = :bytes, counts = :counts, completedAt = :now, revision = if_not_exists(revision, :zero) + :one"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":completed": text(string(f2e.ChunkStateCompleted)), ":running": text(string(f2e.ChunkStateRunning)), ":attempt": number(int64(result.Attempt)), ":records": number(counts.RecordsPublished), ":counts": countsMap(counts), ":bytes": number(result.BytesProcessed), ":now": text(result.OccurredAt.Format(time.RFC3339Nano)), ":zero": number(0), ":one": number(1)}}},
		{Update: &types.Update{TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "JOB"), ConditionExpression: aws.String("#s = :pending OR #s = :scheduled OR #s = :running"), UpdateExpression: aws.String(jobUpdate), ExpressionAttributeNames: jobNames, ExpressionAttributeValues: jobValues}},
	}})
	if err != nil {
		status, readErr := a.chunkStatus(ctx, result.JobID, result.ChunkID)
		if readErr != nil || status != string(f2e.ChunkStateCompleted) {
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
			ConditionExpression:      aws.String("#s = :running AND attempt = :attempt"),
			UpdateExpression:         aws.String("SET #s = :retryPending, lastError = :error, lastAttemptAt = :now, revision = if_not_exists(revision, :zero) + :one"),
			ExpressionAttributeNames: map[string]string{"#s": "status"},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":retryPending": text(string(f2e.ChunkStateRetryPending)), ":running": text(string(f2e.ChunkStateRunning)), ":attempt": number(int64(result.Attempt)), ":error": text(result.Error), ":now": text(result.OccurredAt.Format(time.RFC3339Nano)), ":zero": number(0), ":one": number(1),
			},
		})
		if conditionalConflict(err) {
			status, readErr := a.chunkStatus(ctx, result.JobID, result.ChunkID)
			if readErr == nil && (status == string(f2e.ChunkStateCompleted) || status == string(f2e.ChunkStateFailed)) {
				return nil // explicit idempotent skip; never revive a terminal chunk.
			}
		}
		return err
	}
	status, err := a.chunkStatus(ctx, result.JobID, result.ChunkID)
	if err != nil {
		return err
	}
	if status == string(f2e.ChunkStateFailed) || status == string(f2e.ChunkStateCompleted) {
		return nil
	}
	_, err = a.DynamoDB.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
		{Update: &types.Update{TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "CHUNK#"+result.ChunkID), ConditionExpression: aws.String("#s = :running AND attempt = :attempt"), UpdateExpression: aws.String("SET #s = :failed, lastError = :error, completedAt = :now, revision = if_not_exists(revision, :zero) + :one"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":failed": text(string(f2e.ChunkStateFailed)), ":running": text(string(f2e.ChunkStateRunning)), ":attempt": number(int64(result.Attempt)), ":error": text(result.Error), ":now": text(result.OccurredAt.Format(time.RFC3339Nano)), ":zero": number(0), ":one": number(1)}}},
		{Update: &types.Update{TableName: aws.String(a.LedgerTable), Key: ledgerKey(result.JobID, "JOB"), ConditionExpression: aws.String("#s = :pending OR #s = :scheduled OR #s = :running"), UpdateExpression: aws.String("SET #s = :running ADD failedChunks :one"), ExpressionAttributeNames: map[string]string{"#s": "status"}, ExpressionAttributeValues: map[string]types.AttributeValue{":one": number(1), ":pending": text(string(f2e.JobPending)), ":scheduled": text(string(f2e.JobScheduled)), ":running": text(string(f2e.JobRunning))}}},
	}})
	if err != nil {
		status, readErr := a.chunkStatus(ctx, result.JobID, result.ChunkID)
		if readErr != nil || (status != string(f2e.ChunkStateFailed) && status != string(f2e.ChunkStateCompleted)) {
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
	sealed, _ := out.Item["manifestSealed"].(*types.AttributeValueMemberBOOL)
	if sealed == nil || !sealed.Value || expected == 0 || completed+failed < expected {
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

// ---------------------------------------------------------------------------
// T08 — conditional persistence for the phased lifecycle
// ---------------------------------------------------------------------------
//
// The methods below implement the ADR 0006 state machine with conditional
// writes so terminals never regress, an old attempt never overwrites a new one,
// and late planning/scheduling never overwrites completion. Every job and chunk
// item carries a revision counter, an ownership token (random lease) and a
// transition history list whose retention is bounded by the item TTL
// (expiresAt), following the LedgerRetentionDays setting.

// jobToken generates a random ownership/lease token for a newly admitted job.
func jobToken() string { return rand.Text() }

func (a *AWS) jobRetention() int64 {
	retentionDays := a.LedgerRetentionDays
	if retentionDays < 1 {
		retentionDays = 90
	}
	return time.Now().UTC().Add(time.Duration(retentionDays) * 24 * time.Hour).Unix()
}

func admissionLeaseExpiry() int64 { return time.Now().UTC().Add(15 * time.Minute).Unix() }

// jobStatus reads the current status of a job aggregate item.
func (a *AWS) jobStatus(ctx context.Context, jobID string) (string, error) {
	out, err := a.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "JOB"), ConsistentRead: aws.Bool(true)})
	if err != nil {
		return "", err
	}
	if value, ok := out.Item["status"].(*types.AttributeValueMemberS); ok {
		return value.Value, nil
	}
	return "", nil
}

// advanceJob conditionally moves a job from one of `from` states into `to`,
// recording the transition in the history list and bumping the revision. It is
// the single writer for job state changes, guaranteeing no terminal regresses.
func (a *AWS) advanceJob(ctx context.Context, jobID string, from []f2e.JobStatus, to f2e.JobStatus, reason string, extraSet string, extraValues map[string]types.AttributeValue) error {
	names := map[string]string{"#s": "status"}
	values := map[string]types.AttributeValue{
		":to":    text(string(to)),
		":now":   text(time.Now().UTC().Format(time.RFC3339Nano)),
		":one":   number(1),
		":zero":  number(0),
		":empty": emptyList(),
	}
	condition := ""
	for i, f := range from {
		key := ":from" + strconv.Itoa(i)
		values[key] = text(string(f))
		if i > 0 {
			condition += " OR "
		}
		condition += "#s = " + key
	}
	entry := transitionEntry(string(from[0]), string(to), values[":now"].(*types.AttributeValueMemberS).Value, reason)
	values[":entry"] = singleList(entry)
	// statusIndex mirrors the status value so the status-time-index GSI (T14)
	// can range-query by updatedAt without needing a table Scan.
	values[":statusIndex"] = text(string(to))
	set := "SET #s = :to, statusIndex = :statusIndex, updatedAt = :now, revision = if_not_exists(revision, :zero) + :one, history = list_append(if_not_exists(history, :empty), :entry)"
	if reason != "" {
		values[":reason"] = text(reason)
		set += ", lastReason = :reason"
	}
	if extraSet != "" {
		set += ", " + extraSet
	}
	if extraSet != "" && strings.Contains(extraSet, "#result") {
		names["#result"] = "result"
	}
	for k, v := range extraValues {
		values[k] = v
	}
	_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                 aws.String(a.LedgerTable),
		Key:                       ledgerKey(jobID, "JOB"),
		ConditionExpression:       aws.String(condition),
		UpdateExpression:          aws.String(set),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	})
	return err
}

// Admit binds a receipt to a job item in the RECEIVED state. jobId is the
// immutable fileId, so a different SQS receipt for the same physical version
// links to the existing normal execution rather than creating a duplicate.
// rather than creating a duplicate. It returns Acquired on first write,
// AlreadyCompleted if the job is terminal, or Busy if another execution owns it.
func (a *AWS) Admit(ctx context.Context, receipt f2e.Receipt) (f2e.AcquisitionResult, error) {
	if a.LedgerTable == "" {
		return f2e.Busy, fmt.Errorf("ledger table is not configured")
	}
	if receipt.ReceiptID == "" || receipt.FileID == "" || receipt.Source.VersionID == "" {
		return f2e.Busy, fmt.Errorf("receiptId, fileId and source versionId are required for admission")
	}
	jobID := receipt.FileID
	now := time.Now().UTC().Format(time.RFC3339Nano)
	snapshot, err := json.Marshal(receipt.ConfigSnapshot)
	if err != nil {
		return f2e.Busy, fmt.Errorf("encode admission configuration snapshot: %w", err)
	}
	item := map[string]types.AttributeValue{
		"pk":             text("JOB#" + jobID),
		"sk":             text("JOB"),
		"jobId":          text(jobID),
		"fileId":         text(receipt.FileID),
		"receiptId":      text(receipt.ReceiptID),
		"status":         text(string(f2e.JobStateReceived)),
		"statusIndex":    text(string(f2e.JobStateReceived)),
		"bucket":         text(receipt.Source.Bucket),
		"key":            text(receipt.Source.Key),
		"versionId":      text(receipt.Source.VersionID),
		"etag":           text(receipt.Source.ETag),
		"size":           number(receipt.Source.Size),
		"environment":    text(receipt.Environment),
		"token":          text(jobToken()),
		"revision":       number(0),
		"receivedAt":     text(receipt.ReceivedAt.UTC().Format(time.RFC3339Nano)),
		"createdAt":      text(now),
		"updatedAt":      text(now),
		"expiresAt":      number(a.jobRetention()),
		"leaseExpiresAt": number(admissionLeaseExpiry()),
		"configSnapshot": text(string(snapshot)),
	}
	// T22: persist prefixId so FinalizeJob/RejectJob can release the quota slot.
	if receipt.PrefixID != "" {
		item["prefixId"] = text(receipt.PrefixID)
	}
	// The RECEIVED state is the initial state, so no transition entry is
	// appended on admission (history is seeded empty).
	item["history"] = &types.AttributeValueMemberL{Value: []types.AttributeValue{}}
	_, err = a.DynamoDB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(a.LedgerTable),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(pk)"),
	})
	if err == nil {
		return f2e.Acquired, nil
	}
	if !conditionalConflict(err) {
		return f2e.Busy, err
	}
	status, readErr := a.jobStatus(ctx, jobID)
	if readErr != nil {
		return f2e.Busy, readErr
	}
	switch f2e.JobStatus(status) {
	case f2e.JobStateCompleted, f2e.JobStateFailed, f2e.JobStateRejected:
		return f2e.AlreadyCompleted, nil
	case f2e.JobStateReceived, f2e.JobStateValidating, f2e.JobStatePlanning:
		// A crashed organizer leaves a renewable lease rather than a permanent
		// lock. Only one later delivery can reclaim it after expiry.
		nowUnix := time.Now().UTC().Unix()
		_, reclaimErr := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(a.LedgerTable), Key: ledgerKey(jobID, "JOB"),
			ConditionExpression:       aws.String("leaseExpiresAt <= :now AND (#s = :received OR #s = :validating OR #s = :planning)"),
			UpdateExpression:          aws.String("SET #token = :token, leaseExpiresAt = :lease, updatedAt = :updated, revision = if_not_exists(revision, :zero) + :one"),
			ExpressionAttributeNames:  map[string]string{"#s": "status", "#token": "token"},
			ExpressionAttributeValues: map[string]types.AttributeValue{":now": number(nowUnix), ":lease": number(admissionLeaseExpiry()), ":token": text(jobToken()), ":updated": text(time.Now().UTC().Format(time.RFC3339Nano)), ":zero": number(0), ":one": number(1), ":received": text(string(f2e.JobStateReceived)), ":validating": text(string(f2e.JobStateValidating)), ":planning": text(string(f2e.JobStatePlanning))},
		})
		if reclaimErr == nil {
			return f2e.Acquired, nil
		}
		if !conditionalConflict(reclaimErr) {
			return f2e.Busy, reclaimErr
		}
		return f2e.Busy, nil
	default:
		return f2e.Busy, nil
	}
}

// BeginValidation moves a RECEIVED job into VALIDATING.
func (a *AWS) BeginValidation(ctx context.Context, jobID string) error {
	err := a.advanceJob(ctx, jobID, []f2e.JobStatus{f2e.JobStateReceived}, f2e.JobStateValidating, "", "", nil)
	if err == nil {
		return nil
	}
	if !conditionalConflict(err) {
		return err
	}
	// Idempotent: if already past VALIDATING, this is a no-op rather than a
	// regression. A terminal state must not be overwritten.
	status, readErr := a.jobStatus(ctx, jobID)
	if readErr != nil {
		return readErr
	}
	if f2e.JobStatus(status) == f2e.JobStateValidating || f2e.JobStatus(status) == f2e.JobStatePlanning {
		return nil
	}
	return err
}

// releaseQuotaIfNeeded reads the prefixId from the job item and, if set,
// decrements the per-prefix active-job counter. It is called by RejectJob and
// FinalizeJob after a successful terminal transition so the freed slot becomes
// immediately available to new admissions.
func (a *AWS) releaseQuotaIfNeeded(ctx context.Context, jobID string) {
	out, err := a.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(a.LedgerTable),
		Key:                  ledgerKey(jobID, "JOB"),
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("prefixId"),
	})
	if err != nil || out.Item == nil {
		return
	}
	if v, ok := out.Item["prefixId"].(*types.AttributeValueMemberS); ok && v.Value != "" {
		_ = a.ReleaseSlot(ctx, v.Value)
	}
}

// RejectJob moves a RECEIVED or VALIDATING job into REJECTED with a reason.
func (a *AWS) RejectJob(ctx context.Context, jobID string, rejection f2e.Rejection) error {
	reason := string(rejection.Reason)
	if rejection.Detail != "" {
		reason += ": " + rejection.Detail
	}
	rejectionJSON, _ := json.Marshal(rejection)
	extraValues := map[string]types.AttributeValue{":rejection": text(string(rejectionJSON))}
	err := a.advanceJob(ctx, jobID, []f2e.JobStatus{f2e.JobStateReceived, f2e.JobStateValidating}, f2e.JobStateRejected, reason, "rejection = :rejection", extraValues)
	if err == nil {
		a.releaseQuotaIfNeeded(ctx, jobID)
		return nil
	}
	if !conditionalConflict(err) {
		return err
	}
	status, readErr := a.jobStatus(ctx, jobID)
	if readErr != nil {
		return readErr
	}
	if f2e.JobStatus(status) == f2e.JobStateRejected {
		return nil
	}
	return err
}

// BeginPlanning moves a VALIDATING job into PLANNING.
func (a *AWS) BeginPlanning(ctx context.Context, jobID string) error {
	err := a.advanceJob(ctx, jobID, []f2e.JobStatus{f2e.JobStateValidating}, f2e.JobStatePlanning, "", "", nil)
	if err == nil {
		return nil
	}
	if !conditionalConflict(err) {
		return err
	}
	status, readErr := a.jobStatus(ctx, jobID)
	if readErr != nil {
		return readErr
	}
	if f2e.JobStatus(status) == f2e.JobStatePlanning {
		return nil
	}
	return err
}

// AcquireChunk claims a PENDING chunk for execution, writing a fresh ownership
// token and bumping the attempt counter. It returns Acquired on a successful
// claim, AlreadyCompleted if the chunk is terminal, or Busy if another worker
// already holds it.
func (a *AWS) AcquireChunk(ctx context.Context, jobID, chunkID string) (f2e.AcquisitionResult, error) {
	now := text(time.Now().UTC().Format(time.RFC3339Nano))
	_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:                aws.String(a.LedgerTable),
		Key:                      ledgerKey(jobID, "CHUNK#"+chunkID),
		ConditionExpression:      aws.String("attribute_exists(pk) AND #s = :pending"),
		UpdateExpression:         aws.String("SET #s = :running, #token = :token, acquiredAt = :now, revision = if_not_exists(revision, :zero) + :one ADD attempt :one"),
		ExpressionAttributeNames: map[string]string{"#s": "status", "#token": "token"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":running": text(string(f2e.ChunkStateRunning)),
			":pending": text(string(f2e.ChunkStatePending)),
			":token":   text(jobToken()),
			":now":     now,
			":zero":    number(0),
			":one":     number(1),
		},
	})
	if err == nil {
		return f2e.Acquired, nil
	}
	if !conditionalConflict(err) {
		return f2e.Busy, err
	}
	status, readErr := a.chunkStatus(ctx, jobID, chunkID)
	if readErr != nil {
		return f2e.Busy, readErr
	}
	switch f2e.ChunkStatus(status) {
	case f2e.ChunkStateCompleted, f2e.ChunkStateFailed:
		return f2e.AlreadyCompleted, nil
	default:
		return f2e.Busy, nil
	}
}

// FinalizeJob records the terminal result and counts of a PROCESSING job. It
// moves the job to COMPLETED (never regressing a terminal) and stores the
// aggregate counts with the countsComplete flag.
func (a *AWS) FinalizeJob(ctx context.Context, jobID string, result f2e.JobResult, counts f2e.Counts) error {
	reason := string(result)
	extraValues := map[string]types.AttributeValue{":result": text(string(result)), ":counts": countsMap(counts)}
	err := a.advanceJob(ctx, jobID, []f2e.JobStatus{f2e.JobStateProcessing}, f2e.JobStateCompleted, reason, "#result = :result, counts = :counts, completedAt = :now", extraValues)
	if err == nil {
		a.releaseQuotaIfNeeded(ctx, jobID)
		return nil
	}
	if !conditionalConflict(err) {
		return err
	}
	status, readErr := a.jobStatus(ctx, jobID)
	if readErr != nil {
		return readErr
	}
	if f2e.JobStatus(status) == f2e.JobStateCompleted {
		return nil
	}
	return err
}

// ---------------------------------------------------------------------------
// T12 — completion outbox
// ---------------------------------------------------------------------------
//
// The completion intent pattern ensures that every terminal job transition
// produces exactly one logical delivery record. The intent is written to a
// dedicated sort-key COMPLETION_INTENT#<version> under the same partition as
// the job. A sparse GSI (pending-intents-index) on the intentPending attribute
// allows the publisher (T13) to query pending intents without a full Scan.
//
// Item layout:
//   pk: JOB#<jobId>
//   sk: COMPLETION_INTENT#<paddedVersion>
//   intentPending: "1"  (removed on delivery to drop the item from the GSI)
//   payload: <JSON-encoded CompletionIntent>
//   expiresAt: <job retention TTL>

const intentPendingMarker = "1"
const intentSortKeyPrefix = "COMPLETION_INTENT#"

func intentSortKey(version int64) string {
	return fmt.Sprintf("%s%020d", intentSortKeyPrefix, version)
}

// WriteCompletionIntent writes an outbox record atomically under the job
// partition. It uses attribute_not_exists to guarantee idempotence: if a
// concurrent terminal transition already wrote the intent, this call is a
// no-op and preserves the existing one.
func (a *AWS) WriteCompletionIntent(ctx context.Context, intent f2e.CompletionIntent) error {
	if a.LedgerTable == "" {
		return fmt.Errorf("ledger table is not configured")
	}
	payload, err := json.Marshal(intent)
	if err != nil {
		return fmt.Errorf("encode completion intent: %w", err)
	}
	item := map[string]types.AttributeValue{
		"pk":            text("JOB#" + intent.JobID),
		"sk":            text(intentSortKey(intent.Version)),
		"jobId":         text(intent.JobID),
		"intentVersion": number(intent.Version),
		"intentStatus":  text(string(intent.Status)),
		"intentPending": text(intentPendingMarker),
		"payload":       text(string(payload)),
		"createdAt":     text(intent.CreatedAt.UTC().Format(time.RFC3339Nano)),
		"expiresAt":     number(a.jobRetention()),
	}
	_, err = a.DynamoDB.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(a.LedgerTable),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(pk) AND attribute_not_exists(sk)"),
	})
	if err != nil {
		if conditionalConflict(err) {
			// Intent already exists (concurrent terminal transition). No-op.
			return nil
		}
		return fmt.Errorf("write completion intent: %w", err)
	}
	return nil
}

// PendingCompletionIntents queries the sparse GSI for intents whose
// intentPending attribute is still set. The publisher calls this to recover
// undelivered intents after a DynamoDB Streams expiration or a publisher crash.
func (a *AWS) PendingCompletionIntents(ctx context.Context, limit int) ([]f2e.CompletionIntent, error) {
	if a.LedgerTable == "" {
		return nil, fmt.Errorf("ledger table is not configured")
	}
	if limit <= 0 {
		limit = 100
	}
	lim := int32(limit)
	out, err := a.DynamoDB.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(a.LedgerTable),
		IndexName:              aws.String("pending-intents-index"),
		KeyConditionExpression: aws.String("intentPending = :pending"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pending": text(intentPendingMarker),
		},
		Limit: &lim,
	})
	if err != nil {
		return nil, fmt.Errorf("query pending completion intents: %w", err)
	}
	intents := make([]f2e.CompletionIntent, 0, len(out.Items))
	for _, item := range out.Items {
		payloadAttr, ok := item["payload"].(*types.AttributeValueMemberS)
		if !ok {
			continue
		}
		var intent f2e.CompletionIntent
		if err := json.Unmarshal([]byte(payloadAttr.Value), &intent); err != nil {
			return nil, fmt.Errorf("decode completion intent: %w", err)
		}
		intents = append(intents, intent)
	}
	return intents, nil
}

// quotaKey returns the DynamoDB primary key for a per-prefix active-job counter.
// The item uses pk = "QUOTA#<prefixID>" and sk = "QUOTA" so it is co-located
// with job items but separated by a different sort-key prefix.
func quotaKey(prefixID string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": text("QUOTA#" + prefixID),
		"sk": text("QUOTA"),
	}
}

// ReserveSlot atomically increments the per-prefix active-job counter, returning
// ErrQuotaExceeded if it would exceed maxActiveJobs. When maxActiveJobs is 0 the
// call is a no-op. Thread-safe via a DynamoDB conditional update.
func (a *AWS) ReserveSlot(ctx context.Context, prefixID string, maxActiveJobs int) error {
	if a.LedgerTable == "" {
		return fmt.Errorf("ledger table is not configured")
	}
	if maxActiveJobs == 0 || prefixID == "" {
		return nil
	}
	// Attempt to increment if count < maxActiveJobs.
	_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(a.LedgerTable),
		Key:       quotaKey(prefixID),
		// Create the counter at 1 if it doesn't exist yet (if_not_exists(activeJobs,0)+1),
		// but only if the resulting value does not exceed the quota.
		ConditionExpression:      aws.String("attribute_not_exists(activeJobs) OR activeJobs < :max"),
		UpdateExpression:         aws.String("SET activeJobs = if_not_exists(activeJobs, :zero) + :one, prefixId = :prefix, updatedAt = :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":max":    number(int64(maxActiveJobs)),
			":zero":   number(0),
			":one":    number(1),
			":prefix": text(prefixID),
			":now":    text(time.Now().UTC().Format(time.RFC3339Nano)),
		},
	})
	if err != nil {
		if conditionalConflict(err) {
			return port.ErrQuotaExceeded
		}
		return fmt.Errorf("reserve quota slot for prefix %q: %w", prefixID, err)
	}
	return nil
}

// ReleaseSlot atomically decrements the per-prefix active-job counter.
// It is idempotent: if the counter is already 0 it stays at 0.
func (a *AWS) ReleaseSlot(ctx context.Context, prefixID string) error {
	if a.LedgerTable == "" {
		return fmt.Errorf("ledger table is not configured")
	}
	if prefixID == "" {
		return nil
	}
	// Decrement only if activeJobs > 0 to prevent negative counters.
	_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(a.LedgerTable),
		Key:                 quotaKey(prefixID),
		ConditionExpression: aws.String("attribute_exists(activeJobs) AND activeJobs > :zero"),
		UpdateExpression:    aws.String("SET activeJobs = activeJobs - :one, updatedAt = :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":zero": number(0),
			":one":  number(1),
			":now":  text(time.Now().UTC().Format(time.RFC3339Nano)),
		},
	})
	if err != nil {
		if conditionalConflict(err) {
			// Counter is already 0; no-op (idempotent).
			return nil
		}
		return fmt.Errorf("release quota slot for prefix %q: %w", prefixID, err)
	}
	return nil
}

// MarkIntentDelivered removes the intentPending attribute from the intent item,
// dropping it from the sparse GSI. It is called by the publisher only after SQS
// confirms the send; a crash between send and mark may duplicate delivery, which
// consumers handle via the stable, deterministic eventId.
func (a *AWS) MarkIntentDelivered(ctx context.Context, jobID string, version int64) error {
	if a.LedgerTable == "" {
		return fmt.Errorf("ledger table is not configured")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := a.DynamoDB.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(a.LedgerTable),
		Key:       ledgerKey(jobID, intentSortKey(version)),
		// Only remove the GSI marker if the intent is still pending; idempotent
		// if already delivered.
		ConditionExpression:      aws.String("attribute_exists(pk) AND attribute_exists(intentPending)"),
		UpdateExpression:         aws.String("REMOVE intentPending SET deliveredAt = :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":now": text(now)},
	})
	if err != nil {
		if conditionalConflict(err) {
			// Already marked delivered; idempotent.
			return nil
		}
		return fmt.Errorf("mark intent delivered: %w", err)
	}
	return nil
}
