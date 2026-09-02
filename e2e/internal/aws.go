package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	localStackEndpoint = "http://localhost:4566"
	awsRegion          = "us-east-1"
	inputBucket        = "f2e-input"
	intakeQueueName    = "file-intake"
	outputQueueName    = "output-events"
	ledgerTableName    = "f2e-job-ledger"
	schemaVersion      = "1"
)

// AWSClient wraps S3 and SQS clients pre-configured for LocalStack.
type AWSClient struct {
	s3Client       *s3.Client
	sqsClient      *sqs.Client
	dynamoClient   *dynamodb.Client
	intakeQueueURL string
	outputQueueURL string
	bucket         string
}

// LocalStackAvailable returns true when the LocalStack health endpoint responds.
func LocalStackAvailable() bool {
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(localStackEndpoint + "/_localstack/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

// NewAWSClient creates S3 and SQS clients pointing to LocalStack and resolves queue URLs.
func NewAWSClient() *AWSClient {
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(awsRegion),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
		awsconfig.WithBaseEndpoint(localStackEndpoint),
	)
	if err != nil {
		panic(fmt.Sprintf("aws config: %v", err))
	}

	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	sqsc := sqs.NewFromConfig(cfg)

	return &AWSClient{
		s3Client:       s3c,
		sqsClient:      sqsc,
		dynamoClient:   dynamodb.NewFromConfig(cfg),
		intakeQueueURL: mustQueueURL(ctx, sqsc, intakeQueueName),
		outputQueueURL: mustQueueURL(ctx, sqsc, outputQueueName),
		bucket:         inputBucket,
	}
}

// AssertLedgerComplete proves that every planned E2E job and chunk reached a
// reconciled terminal state, with no double-counted or missing chunk.
func (c *AWSClient) AssertLedgerComplete(ctx context.Context, expectedJobs int) error {
	input := &dynamodb.ScanInput{TableName: aws.String(ledgerTableName), ConsistentRead: aws.Bool(true)}
	jobs, chunks := 0, 0
	for {
		out, err := c.dynamoClient.Scan(ctx, input)
		if err != nil {
			return fmt.Errorf("scan ledger: %w", err)
		}
		for _, item := range out.Items {
			sk, _ := item["sk"].(*dynamodbtypes.AttributeValueMemberS)
			status, _ := item["status"].(*dynamodbtypes.AttributeValueMemberS)
			if sk == nil || status == nil {
				continue
			}
			if sk.Value == "JOB" {
				jobs++
				expected := ledgerNumber(item, "expectedChunks")
				completed := ledgerNumber(item, "completedChunks")
				failed := ledgerNumber(item, "failedChunks")
				if status.Value != "COMPLETED" || expected < 1 || completed != expected || failed != 0 {
					return fmt.Errorf("unreconciled job: status=%s expected=%d completed=%d failed=%d", status.Value, expected, completed, failed)
				}
			} else if strings.HasPrefix(sk.Value, "CHUNK#") {
				chunks++
				if status.Value != "COMPLETED" {
					return fmt.Errorf("unreconciled chunk %s: status=%s", sk.Value, status.Value)
				}
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	if jobs != expectedJobs || chunks < jobs {
		return fmt.Errorf("unexpected ledger cardinality: jobs=%d (want %d), chunks=%d", jobs, expectedJobs, chunks)
	}
	return nil
}

func ledgerNumber(item map[string]dynamodbtypes.AttributeValue, name string) int64 {
	value, _ := item[name].(*dynamodbtypes.AttributeValueMemberN)
	if value == nil {
		return 0
	}
	n, _ := strconv.ParseInt(value.Value, 10, 64)
	return n
}

func mustQueueURL(ctx context.Context, c *sqs.Client, name string) string {
	out, err := c.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		panic(fmt.Sprintf("GetQueueUrl %q: %v", name, err))
	}
	return *out.QueueUrl
}

// UploadFile puts data under key in the configured S3 bucket.
func (c *AWSClient) UploadFile(ctx context.Context, key string, data []byte) error {
	cl := int64(len(data))
	_, err := c.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: &cl,
	})
	return err
}

// SendOrganizerRequest marshals and enqueues an OrganizerRequest to the file-intake queue.
func (c *AWSClient) SendOrganizerRequest(ctx context.Context, req OrganizerRequest) error {
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = c.sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    aws.String(c.intakeQueueURL),
		MessageBody: aws.String(string(body)),
	})
	return err
}

// DrainOutputQueue removes all messages from the dedicated output queue after a
// count-only scenario. The E2E suite is sequential and provisions this queue
// exclusively, so it never touches an application queue shared with a user.
func (c *AWSClient) DrainOutputQueue(ctx context.Context) error {
	for {
		out, err := c.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.outputQueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     0,
		})
		if err != nil {
			return fmt.Errorf("drain receive: %w", err)
		}
		if len(out.Messages) == 0 {
			return nil
		}
		entries := make([]sqstypes.DeleteMessageBatchRequestEntry, len(out.Messages))
		for i, m := range out.Messages {
			entries[i] = sqstypes.DeleteMessageBatchRequestEntry{Id: m.MessageId, ReceiptHandle: m.ReceiptHandle}
		}
		if _, err = c.sqsClient.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(c.outputQueueURL),
			Entries:  entries,
		}); err != nil {
			return fmt.Errorf("drain delete: %w", err)
		}
	}
}

// ApproximateCount returns the approximate number of messages available in the output queue.
func (c *AWSClient) ApproximateCount(ctx context.Context) (int, error) {
	return c.approximateCount(ctx, c.outputQueueURL)
}

func (c *AWSClient) approximateCount(ctx context.Context, queueURL string) (int, error) {
	out, err := c.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{"ApproximateNumberOfMessages"},
	})
	if err != nil {
		return 0, err
	}
	n, _ := strconv.Atoi(out.Attributes["ApproximateNumberOfMessages"])
	return n, nil
}

// DLQCounts returns visible poison messages after the suite has allowed all
// redrive attempts to settle.
func (c *AWSClient) DLQCounts(ctx context.Context) (intake, chunks int, err error) {
	intakeURL := mustQueueURL(ctx, c.sqsClient, "file-intake-dlq")
	chunkURL := mustQueueURL(ctx, c.sqsClient, "chunk-jobs-dlq")
	if intake, err = c.approximateCount(ctx, intakeURL); err != nil {
		return 0, 0, err
	}
	chunks, err = c.approximateCount(ctx, chunkURL)
	return intake, chunks, err
}

// WaitForCount polls until at least expected messages appear in the output queue or timeout elapses.
// Returns the last observed count and an error when the timeout is exceeded.
func (c *AWSClient) WaitForCount(ctx context.Context, expected int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		count, err := c.ApproximateCount(ctx)
		if err != nil {
			return 0, err
		}
		if count >= expected {
			return count, nil
		}
		if time.Now().After(deadline) {
			return count, fmt.Errorf("timeout: expected %d messages, got %d", expected, count)
		}
		select {
		case <-ctx.Done():
			return count, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// ConsumeAll drains up to limit messages from the output queue, waiting up to timeout for
// the first expected messages to arrive.  Each message body is JSON-unmarshaled into an Envelope.
// The caller should set limit = expected * 2 to detect unexpected duplicates.
func (c *AWSClient) ConsumeAll(ctx context.Context, expected int, timeout time.Duration) ([]Envelope, error) {
	if _, err := c.WaitForCount(ctx, expected, timeout); err != nil {
		return nil, err
	}

	limit := expected*2 + 20
	var envelopes []Envelope
	emptyRuns := 0

	for len(envelopes) < limit && emptyRuns < 3 {
		out, err := c.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(c.outputQueueURL),
			MaxNumberOfMessages:   10,
			WaitTimeSeconds:       1,
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			return nil, err
		}
		if len(out.Messages) == 0 {
			emptyRuns++
			continue
		}
		emptyRuns = 0

		entries := make([]sqstypes.DeleteMessageBatchRequestEntry, 0, len(out.Messages))
		for _, m := range out.Messages {
			var env Envelope
			if err := json.Unmarshal([]byte(*m.Body), &env); err != nil {
				return nil, fmt.Errorf("unmarshal envelope (msgId=%s): %w", *m.MessageId, err)
			}
			schema := env.Metadata.Schema.ID + ":" + env.Metadata.Schema.Version
			if attr, ok := m.MessageAttributes["schema"]; !ok || aws.ToString(attr.StringValue) != schema {
				return nil, fmt.Errorf("message attribute schema diverges from body (msgId=%s)", aws.ToString(m.MessageId))
			}
			if attr, ok := m.MessageAttributes["format"]; !ok || aws.ToString(attr.StringValue) != env.Metadata.Format {
				return nil, fmt.Errorf("message attribute format diverges from body (msgId=%s)", aws.ToString(m.MessageId))
			}
			envelopes = append(envelopes, env)
			entries = append(entries, sqstypes.DeleteMessageBatchRequestEntry{
				Id:            m.MessageId,
				ReceiptHandle: m.ReceiptHandle,
			})
		}
		if _, err = c.sqsClient.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(c.outputQueueURL),
			Entries:  entries,
		}); err != nil {
			return nil, err
		}
	}
	return envelopes, nil
}
