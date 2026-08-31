// Package port defines the dependencies required by application services.
package port

import (
	"context"
	"io"

	"github.com/f2e/f2e/internal/domain/f2e"
)

// MessageAttribute is an SQS/SNS message attribute.
type MessageAttribute struct {
	DataType string
	Value    string
}

type ObjectStore interface {
	Head(context.Context, string, string) (size int64, eTag string, versionID string, err error)
	GetRange(context.Context, string, string, int64, int64) (io.ReadCloser, error)
}

type Queue interface {
	Send(context.Context, string, []string, map[string]MessageAttribute) ([]int, error)
}

// SourceResolver fetches a byte range for a chunk job, hiding the underlying
// access method (S3 bucket/key or pre-signed URL).
type SourceResolver interface {
	OpenChunkRange(ctx context.Context, job f2e.ChunkJob, start, end int64) (io.ReadCloser, error)
}

// RecordProcessor is an optional extension point invoked before each event is published.
// Returning nil discards the record; the framework skips it without error.
type RecordProcessor interface {
	Process(ctx context.Context, envelope f2e.Envelope[f2e.RecordPayload]) (*f2e.Envelope[f2e.RecordPayload], error)
}
