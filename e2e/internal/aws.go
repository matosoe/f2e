package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
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
	schemaVersion      = "1"
)

// AWSClient wraps S3 and SQS clients pre-configured for LocalStack.
type AWSClient struct {
	s3Client       *s3.Client
	sqsClient      *sqs.Client
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
		intakeQueueURL: mustQueueURL(ctx, sqsc, intakeQueueName),
		outputQueueURL: mustQueueURL(ctx, sqsc, outputQueueName),
		bucket:         inputBucket,
	}
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

// DrainOutputQueue removes all messages currently in the output-events queue.
// Used in the Before hook to isolate each scenario.
func (c *AWSClient) DrainOutputQueue(ctx context.Context) error {
	emptyRuns := 0
	for emptyRuns < 3 {
		out, err := c.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.outputQueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     0,
		})
		if err != nil {
			return fmt.Errorf("drain receive: %w", err)
		}
		if len(out.Messages) == 0 {
			emptyRuns++
			continue
		}
		emptyRuns = 0
		entries := make([]sqstypes.DeleteMessageBatchRequestEntry, len(out.Messages))
		for i, m := range out.Messages {
			entries[i] = sqstypes.DeleteMessageBatchRequestEntry{
				Id:            m.MessageId,
				ReceiptHandle: m.ReceiptHandle,
			}
		}
		if _, err = c.sqsClient.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
			QueueUrl: aws.String(c.outputQueueURL),
			Entries:  entries,
		}); err != nil {
			return fmt.Errorf("drain delete: %w", err)
		}
	}
	return nil
}

// ApproximateCount returns the approximate number of messages available in the output queue.
func (c *AWSClient) ApproximateCount(ctx context.Context) (int, error) {
	out, err := c.sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(c.outputQueueURL),
		AttributeNames: []sqstypes.QueueAttributeName{"ApproximateNumberOfMessages"},
	})
	if err != nil {
		return 0, err
	}
	n, _ := strconv.Atoi(out.Attributes["ApproximateNumberOfMessages"])
	return n, nil
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
			QueueUrl:            aws.String(c.outputQueueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     1,
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
