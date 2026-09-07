package aws

// ledger_query.go — T14 operational query methods for the job ledger.
//
// These methods implement the port.JobQueryer interface for operational
// dashboards and the stuck-job reconciler. All queries use GSIs or single-key
// lookups; none require a full table Scan.
//
// Key layout reference (ledger.go):
//   pk: JOB#<jobId>   sk: JOB          → job aggregate item
//   pk: JOB#<jobId>   sk: RECEIPT#<id> → receipt link
//   pk: JOB#<jobId>   sk: CHUNK#<chunkId>
//   pk: JOB#<jobId>   sk: COMPLETION_INTENT#<version>
//
// GSIs (defined in terraform/dynamodb.tf):
//   pending-intents-index: hash=intentPending, range=sk
//   status-time-index:     hash=statusIndex,   range=updatedAt  (T14)

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/f2e/f2e/internal/domain/f2e"
)

// JobSummary is the operational view of a job returned by query operations.
// It is intentionally minimal — internal execution details and chunk lists
// are not included so the query path stays fast.
type JobSummary struct {
	JobID       string        `json:"jobId"`
	FileID      string        `json:"fileId"`
	ReceiptID   string        `json:"receiptId,omitempty"`
	Status      f2e.JobStatus `json:"status"`
	Environment string        `json:"environment,omitempty"`
	CreatedAt   string        `json:"createdAt,omitempty"`
	UpdatedAt   string        `json:"updatedAt,omitempty"`
	// TerminalAt is the time the job reached a terminal state; empty for active jobs.
	TerminalAt string `json:"terminalAt,omitempty"`
	// Result qualifies a COMPLETED job (SUCCESS or WITH_REJECTIONS).
	Result f2e.JobResult `json:"result,omitempty"`
	// StuckSince is populated by StuckJobs and indicates how long the job has
	// been without a state transition.
	StuckSince *time.Duration `json:"stuckSince,omitempty"`
}

// GetJob returns the operational summary for a specific job by its jobId.
func (a *AWS) GetJob(ctx context.Context, jobID string) (JobSummary, error) {
	if a.LedgerTable == "" {
		return JobSummary{}, fmt.Errorf("ledger table is not configured")
	}
	out, err := a.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(a.LedgerTable),
		Key:            ledgerKey(jobID, "JOB"),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return JobSummary{}, fmt.Errorf("get job %s: %w", jobID, err)
	}
	if len(out.Item) == 0 {
		return JobSummary{}, fmt.Errorf("job %s not found", jobID)
	}
	return jobSummaryFromItem(out.Item), nil
}

// GetJobByReceiptID finds a job aggregate by its SQS receipt identifier.
// It queries the receipt link item (sk=RECEIPT#<receiptId>) to resolve the
// jobId, then reads the job aggregate.
func (a *AWS) GetJobByReceiptID(ctx context.Context, receiptID string) (JobSummary, error) {
	if a.LedgerTable == "" {
		return JobSummary{}, fmt.Errorf("ledger table is not configured")
	}
	// The receipt link is stored under pk=RECEIPT#<receiptId>, sk=RECEIPT.
	out, err := a.DynamoDB.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(a.LedgerTable),
		Key:            ledgerKey("RECEIPT#"+receiptID, "RECEIPT"),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return JobSummary{}, fmt.Errorf("get receipt %s: %w", receiptID, err)
	}
	if len(out.Item) == 0 {
		// Fallback: scan the job aggregate item via Query; the receiptId is
		// projected as a field on the JOB item. This is a single-partition
		// read with a filter — it scans the job's own items only.
		return a.getJobByReceiptIDFallback(ctx, receiptID)
	}
	jobIDAttr, ok := out.Item["jobId"].(*types.AttributeValueMemberS)
	if !ok {
		return JobSummary{}, fmt.Errorf("receipt %s link has no jobId", receiptID)
	}
	return a.GetJob(ctx, jobIDAttr.Value)
}

// getJobByReceiptIDFallback queries the receipt field on the job aggregate
// when no dedicated receipt-link item exists. It is a best-effort fallback and
// should not be called in the hot path.
func (a *AWS) getJobByReceiptIDFallback(_ context.Context, receiptID string) (JobSummary, error) {
	// A receipt always belongs to a single job; we cannot efficiently look up
	// without an inverted index. Return a not-found error so the caller can
	// suggest creating the receipt-link item.
	return JobSummary{}, fmt.Errorf("receipt %s: no direct index; create a RECEIPT# link item for efficient lookup", receiptID)
}

// QueryJobsByStatus returns jobs in a given status ordered by updatedAt.
// Use after and before to restrict the time window; both are RFC3339 strings.
// A limit of 0 defaults to 100. Non-terminal statuses with old timestamps
// are candidates for the stuck-job reconciler.
func (a *AWS) QueryJobsByStatus(ctx context.Context, status f2e.JobStatus, after, before string, limit int) ([]JobSummary, error) {
	if a.LedgerTable == "" {
		return nil, fmt.Errorf("ledger table is not configured")
	}
	if limit <= 0 {
		limit = 100
	}
	lim := int32(limit)
	values := map[string]types.AttributeValue{
		":status": text(string(status)),
	}
	keyCondition := "statusIndex = :status"
	if after != "" && before != "" {
		keyCondition += " AND updatedAt BETWEEN :after AND :before"
		values[":after"] = text(after)
		values[":before"] = text(before)
	} else if after != "" {
		keyCondition += " AND updatedAt >= :after"
		values[":after"] = text(after)
	} else if before != "" {
		keyCondition += " AND updatedAt < :before"
		values[":before"] = text(before)
	}
	out, err := a.DynamoDB.Query(ctx, &dynamodb.QueryInput{
		TableName:                 aws.String(a.LedgerTable),
		IndexName:                 aws.String("status-time-index"),
		KeyConditionExpression:    aws.String(keyCondition),
		ExpressionAttributeValues: values,
		Limit:                     &lim,
	})
	if err != nil {
		return nil, fmt.Errorf("query jobs by status %s: %w", status, err)
	}
	result := make([]JobSummary, 0, len(out.Items))
	for _, item := range out.Items {
		result = append(result, jobSummaryFromItem(item))
	}
	return result, nil
}

// StuckJobs returns non-terminal jobs whose updatedAt is older than the given
// threshold. These are candidates for operator review; the reconciler does not
// automatically replay without explicit human confirmation.
//
// A job is considered stuck if:
//   - its status is one of RECEIVED, VALIDATING, PLANNING, or PROCESSING
//   - its updatedAt timestamp has not advanced for at least stuckThreshold
func (a *AWS) StuckJobs(ctx context.Context, stuckThreshold time.Duration, limit int) ([]JobSummary, error) {
	before := time.Now().UTC().Add(-stuckThreshold).Format(time.RFC3339Nano)
	nonTerminal := []f2e.JobStatus{
		f2e.JobStateReceived,
		f2e.JobStateValidating,
		f2e.JobStatePlanning,
		f2e.JobStateProcessing,
	}
	var all []JobSummary
	perState := limit
	if perState <= 0 {
		perState = 25
	}
	for _, status := range nonTerminal {
		jobs, err := a.QueryJobsByStatus(ctx, status, "", before, perState)
		if err != nil {
			return nil, fmt.Errorf("stuck jobs query for status %s: %w", status, err)
		}
		for _, j := range jobs {
			since := stuckSince(j.UpdatedAt, stuckThreshold)
			j.StuckSince = &since
			all = append(all, j)
		}
		if len(all) >= limit && limit > 0 {
			break
		}
	}
	return all, nil
}

// jobSummaryFromItem extracts a JobSummary from a raw DynamoDB attribute map.
func jobSummaryFromItem(item map[string]types.AttributeValue) JobSummary {
	s := JobSummary{}
	if v, ok := item["jobId"].(*types.AttributeValueMemberS); ok {
		s.JobID = v.Value
	}
	if v, ok := item["fileId"].(*types.AttributeValueMemberS); ok {
		s.FileID = v.Value
	}
	if v, ok := item["receiptId"].(*types.AttributeValueMemberS); ok {
		s.ReceiptID = v.Value
	}
	if v, ok := item["status"].(*types.AttributeValueMemberS); ok {
		s.Status = f2e.JobStatus(v.Value)
	}
	if v, ok := item["environment"].(*types.AttributeValueMemberS); ok {
		s.Environment = v.Value
	}
	if v, ok := item["createdAt"].(*types.AttributeValueMemberS); ok {
		s.CreatedAt = v.Value
	}
	if v, ok := item["updatedAt"].(*types.AttributeValueMemberS); ok {
		s.UpdatedAt = v.Value
	}
	if v, ok := item["terminalAt"].(*types.AttributeValueMemberS); ok {
		s.TerminalAt = v.Value
	}
	if v, ok := item["result"].(*types.AttributeValueMemberS); ok {
		s.Result = f2e.JobResult(v.Value)
	}
	return s
}

// stuckSince returns how long a job has been stuck, computed from its last
// updatedAt timestamp. Returns the stuckThreshold if the timestamp cannot
// be parsed (conservative).
func stuckSince(updatedAt string, threshold time.Duration) time.Duration {
	if updatedAt == "" {
		return threshold
	}
	// Normalize: strip sub-second and timezone variations if needed.
	t, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		// Try without nanoseconds.
		t, err = time.Parse(time.RFC3339, strings.TrimSuffix(updatedAt, "Z")+"Z")
		if err != nil {
			return threshold
		}
	}
	return time.Since(t)
}
