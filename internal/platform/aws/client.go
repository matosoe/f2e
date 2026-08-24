// Package aws provides AWS implementations of the application ports.
package aws

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsTypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/platform/config"
	"io"
)

type AWS struct {
	S3  *s3.Client
	SQS *sqs.Client
}

var _ port.ObjectStore = (*AWS)(nil)
var _ port.Queue = (*AWS)(nil)

func New(ctx context.Context, c config.Config) (*AWS, error) {
	opts := []func(*awscfg.LoadOptions) error{awscfg.WithRegion(c.Region)}
	if c.Endpoint != "" {
		opts = append(opts, awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")), awscfg.WithBaseEndpoint(c.Endpoint))
	}
	cfg, err := awscfg.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &AWS{S3: s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true }), SQS: sqs.NewFromConfig(cfg)}, nil
}
func (a *AWS) Head(ctx context.Context, b, k string) (int64, string, error) {
	out, e := a.S3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(b), Key: aws.String(k)})
	if e != nil {
		return 0, "", e
	}
	return aws.ToInt64(out.ContentLength), aws.ToString(out.ETag), nil
}
func (a *AWS) GetRange(ctx context.Context, b, k string, start, end int64) (io.ReadCloser, error) {
	out, e := a.S3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(b), Key: aws.String(k), Range: aws.String(fmt.Sprintf("bytes=%d-%d", start, end))})
	if e != nil {
		return nil, e
	}
	return out.Body, nil
}
func (a *AWS) Send(ctx context.Context, url string, bodies []string) ([]int, error) {
	if len(bodies) == 0 {
		return nil, nil
	}
	entries := make([]sqsTypes.SendMessageBatchRequestEntry, len(bodies))
	for i, b := range bodies {
		id := fmt.Sprintf("%d", i)
		entries[i] = sqsTypes.SendMessageBatchRequestEntry{Id: &id, MessageBody: &b}
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
