// Package aws provides AWS implementations of the application ports.
package aws

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

type AWS struct {
	S3                  *s3.Client
	SQS                 *sqs.Client
	DynamoDB            *dynamodb.Client
	LedgerTable         string
	MaxReceiveCount     int
	LedgerRetentionDays int
}

type PresignedCredentialError struct{ StatusCode int }

func (e PresignedCredentialError) Error() string {
	return fmt.Sprintf("pre-signed credential rejected with status %d", e.StatusCode)
}

func (PresignedCredentialError) Transient() bool { return true }

var _ port.ObjectStore = (*AWS)(nil)
var _ port.Queue = (*AWS)(nil)
var _ port.SourceResolver = (*AWS)(nil)

func New(ctx context.Context, c config.Config) (*AWS, error) {
	opts := []func(*awscfg.LoadOptions) error{awscfg.WithRegion(c.Region)}
	if c.Endpoint != "" {
		opts = append(opts, awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")), awscfg.WithBaseEndpoint(c.Endpoint))
	}
	cfg, err := awscfg.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &AWS{S3: s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), SQS: sqs.NewFromConfig(cfg), DynamoDB: dynamodb.NewFromConfig(cfg), LedgerTable: c.LedgerTable, MaxReceiveCount: c.MaxReceiveCount, LedgerRetentionDays: c.LedgerRetentionDays}, nil
}
func (a *AWS) Head(ctx context.Context, b, k string) (f2e.ObjectIdentity, error) {
	out, e := a.S3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(k)})
	if e != nil {
		return f2e.ObjectIdentity{}, e
	}
	return f2e.ObjectIdentity{Bucket: b, Key: k, Size: aws.ToInt64(out.ContentLength), ETag: aws.ToString(out.ETag), VersionID: aws.ToString(out.VersionId)}, nil
}
func (a *AWS) GetRange(ctx context.Context, object f2e.ObjectIdentity, start, end int64) (io.ReadCloser, error) {
	if start < 0 || end < start || object.Size < 1 || end >= object.Size {
		return nil, fmt.Errorf("invalid immutable object range %d-%d for size %d", start, end, object.Size)
	}
	in := &s3.GetObjectInput{Bucket: aws.String(object.Bucket), Key: aws.String(object.Key), Range: aws.String(fmt.Sprintf("bytes=%d-%d", start, end))}
	if object.VersionID != "" {
		in.VersionId = aws.String(object.VersionID)
	} else if object.ETag != "" {
		in.IfMatch = aws.String(object.ETag)
	}
	out, e := a.S3.GetObject(ctx, in)
	if e != nil {
		return nil, e
	}
	if err := validateS3RangeIdentity(object, start, end, aws.ToInt64(out.ContentLength), aws.ToString(out.ContentRange), aws.ToString(out.ETag), aws.ToString(out.VersionId)); err != nil {
		out.Body.Close()
		return nil, err
	}
	return out.Body, nil
}
func (a *AWS) Send(ctx context.Context, url string, messages []port.OutboundMessage) ([]int, error) {
	if len(messages) == 0 {
		return nil, nil
	}
	entries := make([]sqsTypes.SendMessageBatchRequestEntry, len(messages))
	for i, message := range messages {
		if err := validateOutboundMessage(message); err != nil {
			return nil, fmt.Errorf("message %d: %w", i, err)
		}
		id := fmt.Sprintf("%d", i)
		body := message.Body
		entry := sqsTypes.SendMessageBatchRequestEntry{Id: &id, MessageBody: &body}
		if len(message.Attributes) > 0 {
			entry.MessageAttributes = make(map[string]sqsTypes.MessageAttributeValue, len(message.Attributes))
			for k, v := range message.Attributes {
				dt, val := v.DataType, v.Value
				entry.MessageAttributes[k] = sqsTypes.MessageAttributeValue{DataType: &dt, StringValue: &val}
			}
		}
		entries[i] = entry
	}
	out, e := a.SQS.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{QueueUrl: &url, Entries: entries})
	if e != nil {
		return nil, e
	}
	failed := make([]int, 0, len(out.Failed))
	for _, f := range out.Failed {
		var i int
		if _, e := fmt.Sscanf(aws.ToString(f.Id), "%d", &i); e == nil {
			failed = append(failed, i)
		}
	}
	return failed, nil
}

var messageAttributeName = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,256}$`)

func validateOutboundMessage(message port.OutboundMessage) error {
	if message.Body == "" {
		return fmt.Errorf("empty message body")
	}
	if len(message.Attributes) > 10 {
		return fmt.Errorf("more than 10 message attributes")
	}
	size := len(message.Body)
	for name, attribute := range message.Attributes {
		if !messageAttributeName.MatchString(name) || strings.HasPrefix(strings.ToLower(name), "aws.") || strings.HasPrefix(strings.ToLower(name), "amazon.") {
			return fmt.Errorf("invalid message attribute name %q", name)
		}
		if attribute.DataType != "String" || attribute.Value == "" {
			return fmt.Errorf("invalid message attribute %q", name)
		}
		size += len(name) + len(attribute.DataType) + len(attribute.Value)
	}
	if size > 256*1024 {
		return fmt.Errorf("message and attributes exceed 256 KiB: %d", size)
	}
	return nil
}

// OpenChunkRange implements port.SourceResolver. It uses the pre-signed URL when
// present, falling back to S3 SDK access via bucket/key.
func (a *AWS) OpenChunkRange(ctx context.Context, job f2e.ChunkJob, start, end int64) (io.ReadCloser, error) {
	if job.PresignedURL != "" {
		return presignedGetRange(ctx, job.PresignedURL, start, end)
	}
	if start < 0 || end < start || job.FileSize < 1 || end >= job.FileSize {
		return nil, fmt.Errorf("invalid immutable object range %d-%d for size %d", start, end, job.FileSize)
	}
	in := &s3.GetObjectInput{
		Bucket: aws.String(job.Bucket),
		Key:    aws.String(job.Key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}
	if job.VersionID != "" {
		in.VersionId = aws.String(job.VersionID)
	} else if job.ETag != "" {
		in.IfMatch = aws.String(job.ETag)
	}
	out, err := a.S3.GetObject(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("read immutable object %s/%s: %w", job.Bucket, job.Key, err)
	}
	identity := f2e.ObjectIdentity{Bucket: job.Bucket, Key: job.Key, VersionID: job.VersionID, ETag: job.ETag, Size: job.FileSize}
	if err := validateS3RangeIdentity(identity, start, end, aws.ToInt64(out.ContentLength), aws.ToString(out.ContentRange), aws.ToString(out.ETag), aws.ToString(out.VersionId)); err != nil {
		out.Body.Close()
		return nil, err
	}
	return out.Body, nil
}

func validateS3RangeIdentity(object f2e.ObjectIdentity, start, end, contentLength int64, contentRange, etag, versionID string) error {
	expectedLength := end - start + 1
	if contentLength != expectedLength || !strings.HasPrefix(contentRange, fmt.Sprintf("bytes %d-%d/", start, end)) {
		return fmt.Errorf("immutable object range changed: contentLength=%d contentRange=%q", contentLength, contentRange)
	}
	if object.ETag != "" && etag != "" && object.ETag != etag {
		return fmt.Errorf("immutable object ETag changed")
	}
	if object.VersionID != "" && versionID != "" && object.VersionID != versionID {
		return fmt.Errorf("immutable object version changed")
	}
	return nil
}

func presignedGetRange(ctx context.Context, rawURL string, start, end int64) (io.ReadCloser, error) {
	u, err := url.Parse(rawURL)
	if err != nil || u == nil {
		return nil, fmt.Errorf("invalid pre-signed URL")
	}
	host := strings.ToLower(u.Hostname())
	isS3Host := host == "s3.amazonaws.com" || (strings.HasSuffix(host, ".amazonaws.com") && (strings.Contains(host, ".s3.") || strings.Contains(host, ".s3-")))
	if u.Scheme != "https" || u.Host == "" || !isS3Host || u.User != nil {
		return nil, fmt.Errorf("invalid pre-signed URL")
	}
	if start < 0 || end < start {
		return nil, fmt.Errorf("invalid byte range %d-%d", start, end)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("presigned range request failed")
	}
	if resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return nil, PresignedCredentialError{StatusCode: resp.StatusCode}
		}
		return nil, fmt.Errorf("presigned range request failed with status %d", resp.StatusCode)
	}
	expectedRange := fmt.Sprintf("bytes %d-%d/", start, end)
	if resp.ContentLength != end-start+1 || !strings.HasPrefix(resp.Header.Get("Content-Range"), expectedRange) {
		resp.Body.Close()
		return nil, fmt.Errorf("presigned response range does not match requested bytes")
	}
	return resp.Body, nil
}
