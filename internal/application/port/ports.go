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

// OutboundMessage keeps broker metadata coupled to the exact serialized body.
// This prevents attributes from diverging when an extension changes an envelope.
type OutboundMessage struct {
	Body       string
	Attributes map[string]MessageAttribute
}

type ObjectStore interface {
	Head(context.Context, string, string) (f2e.ObjectIdentity, error)
	GetRange(context.Context, f2e.ObjectIdentity, int64, int64) (io.ReadCloser, error)
}

type PrefixConfigurationResolver interface {
	ResolvePrefixConfiguration(context.Context, string, string) (f2e.PrefixConfiguration, f2e.ConfigurationSnapshot, error)
	LoadGlobalLimits(context.Context) (f2e.GlobalLimits, int64, string, error)
}

type Queue interface {
	Send(context.Context, string, []OutboundMessage) ([]int, error)
}

// JobLedger persists the technical state required for reconciliation and replay.
// Implementations must make every operation idempotent and concurrency-safe.
type JobLedger interface {
	Plan(context.Context, f2e.JobPlan, []f2e.ChunkJob) error
	MarkScheduled(context.Context, []string) error
	MarkSchedulingFailed(context.Context, []string, string) error
	StartChunk(context.Context, string, string, int) error
	CompleteChunk(context.Context, f2e.ChunkResult) error
	FailChunk(context.Context, f2e.ChunkResult) error
	Replay(context.Context, string, string, []string) ([]f2e.ChunkJob, error)
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
