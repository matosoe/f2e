package internal

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// MessageReport records one physical SQS message per JSONL line. Timestamp fields
// in the body are rendered in the report process timezone.
type MessageReport struct {
	Queue     string `json:"queue"`
	MessageID string `json:"messageId"`
	Body      string `json:"body"`
	SentAt    string `json:"sentAt,omitempty"`
}

type MessageRecorder struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	err    error
	seen   map[string]struct{}
}

func NewMessageRecorder(path string) (*MessageRecorder, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &MessageRecorder{file: f, writer: bufio.NewWriter(f), seen: make(map[string]struct{})}, nil
}

func (r *MessageRecorder) Record(queue string, msg sqstypes.Message) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return
	}
	if _, exists := r.seen[aws.ToString(msg.MessageId)]; exists {
		return
	}
	r.seen[aws.ToString(msg.MessageId)] = struct{}{}
	entry := MessageReport{Queue: queue, MessageID: aws.ToString(msg.MessageId), Body: localizeReportJSON(aws.ToString(msg.Body))}
	entry.SentAt = localSentAt(msg.Attributes["SentTimestamp"])
	data, err := json.Marshal(entry)
	if err == nil {
		_, err = r.writer.Write(append(data, '\n'))
	}
	if err != nil {
		r.err = err
	}
}

func (r *MessageRecorder) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.writer.Flush(); err != nil && r.err == nil {
		r.err = err
	}
	if err := r.file.Close(); err != nil && r.err == nil {
		r.err = err
	}
	return r.err
}

// SnapshotQueue reads messages without deleting them, then restores visibility.
// The queue should be dedicated to this E2E environment.
func (c *AWSClient) SnapshotQueue(ctx context.Context, name string, started time.Time) ([]MessageReport, error) {
	url := mustQueueURL(ctx, c.sqsClient, name)
	count, err := c.approximateCount(ctx, url)
	if err != nil {
		return nil, err
	}
	result := make([]MessageReport, 0, count)
	seen := make(map[string]struct{})
	handles := make([]string, 0, count)
	defer func() {
		for _, handle := range handles {
			_, _ = c.sqsClient.ChangeMessageVisibility(context.Background(), &sqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(url), ReceiptHandle: aws.String(handle), VisibilityTimeout: 0})
		}
	}()
	for len(seen) < count {
		out, receiveErr := c.sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl: aws.String(url), MaxNumberOfMessages: 10, WaitTimeSeconds: 1,
			VisibilityTimeout: 60, MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{"SentTimestamp"},
		})
		if receiveErr != nil {
			return result, receiveErr
		}
		if len(out.Messages) == 0 {
			break
		}
		for _, msg := range out.Messages {
			id := aws.ToString(msg.MessageId)
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			handles = append(handles, aws.ToString(msg.ReceiptHandle))
			if ms := msg.Attributes["SentTimestamp"]; ms != "" {
				var unixMs int64
				if _, err := fmt.Sscan(ms, &unixMs); err == nil && time.UnixMilli(unixMs).Before(started.Add(-time.Second)) {
					continue
				}
			}
			result = append(result, MessageReport{Queue: name, MessageID: id, Body: localizeReportJSON(aws.ToString(msg.Body)), SentAt: localSentAt(msg.Attributes["SentTimestamp"])})
		}
	}
	return result, nil
}

// localSentAt translates the SQS epoch-millisecond system attribute to an
// RFC3339 timestamp using the timezone of the process that runs the E2E suite.
func localSentAt(milliseconds string) string {
	if milliseconds == "" {
		return ""
	}
	ms, err := strconv.ParseInt(milliseconds, 10, 64)
	if err != nil {
		return ""
	}
	return localTimestamp(time.UnixMilli(ms))
}

// localTimestamp formats an instant in the timezone of the process producing
// the report. This keeps report timestamps aligned even when their source
// (such as SQS or the Worker) uses UTC.
func localTimestamp(value time.Time) string {
	return value.In(time.Local).Format(time.RFC3339Nano)
}

// localRFC3339Timestamp translates an RFC3339 timestamp into the timezone of
// the process producing the report. Invalid values are left untouched so a
// malformed message remains visible in the report for diagnosis.
func localRFC3339Timestamp(value string) string {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return value
	}
	return localTimestamp(parsed)
}

// localizeReportJSON converts timestamp values in a captured JSON document.
// This is deliberately applied only to the persisted report copy; it never
// changes the source SQS message.
func localizeReportJSON(value string) string {
	var document any
	if err := json.Unmarshal([]byte(value), &document); err != nil {
		return value
	}
	localizeReportValue(document, "")
	data, err := json.Marshal(document)
	if err != nil {
		return value
	}
	return string(data)
}

func localizeReportValue(value any, name string) {
	switch typed := value.(type) {
	case map[string]any:
		for field, child := range typed {
			if isTimestampField(field) {
				if timestamp, ok := child.(string); ok {
					typed[field] = localRFC3339Timestamp(timestamp)
					continue
				}
			}
			localizeReportValue(child, field)
		}
	case []any:
		for _, child := range typed {
			localizeReportValue(child, name)
		}
	}
}

func isTimestampField(name string) bool {
	return strings.HasSuffix(name, "At") || name == "timestamp"
}

func (c *AWSClient) SnapshotLedger(ctx context.Context) ([]map[string]dynamodbtypes.AttributeValue, error) {
	input := &dynamodb.ScanInput{TableName: aws.String(c.ledgerTable), ConsistentRead: aws.Bool(true)}
	var result []map[string]dynamodbtypes.AttributeValue
	for {
		out, err := c.dynamoClient.Scan(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			result = append(result, localizeLedgerReportItem(item))
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		input.ExclusiveStartKey = out.LastEvaluatedKey
	}
	return result, nil
}

func localizeLedgerReportItem(item map[string]dynamodbtypes.AttributeValue) map[string]dynamodbtypes.AttributeValue {
	localized := make(map[string]dynamodbtypes.AttributeValue, len(item))
	for name, value := range item {
		localized[name] = localizeLedgerReportValue(value, name)
	}
	return localized
}

func localizeLedgerReportValue(value dynamodbtypes.AttributeValue, name string) dynamodbtypes.AttributeValue {
	switch typed := value.(type) {
	case *dynamodbtypes.AttributeValueMemberS:
		copy := *typed
		if isTimestampField(name) {
			copy.Value = localRFC3339Timestamp(copy.Value)
		} else if name == "payload" {
			copy.Value = localizeReportJSON(copy.Value)
		}
		return &copy
	case *dynamodbtypes.AttributeValueMemberM:
		copy := *typed
		copy.Value = localizeLedgerReportItem(typed.Value)
		return &copy
	case *dynamodbtypes.AttributeValueMemberL:
		copy := *typed
		copy.Value = make([]dynamodbtypes.AttributeValue, len(typed.Value))
		for index, child := range typed.Value {
			copy.Value[index] = localizeLedgerReportValue(child, name)
		}
		return &copy
	default:
		return value
	}
}
