// Package port defines the dependencies required by application services.
package port

import (
	"context"
	"errors"
	"io"

	"github.com/f2e/f2e/internal/domain/f2e"
)

// ErrAlreadyCompleted tells a Worker to acknowledge a duplicate chunk delivery
// without processing or publishing its records again.
var ErrAlreadyCompleted = errors.New("chunk already completed")

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

// VersionedObjectStore reads an exact immutable object version. Implementations
// must never silently replace a requested VersionID with the current key.
type VersionedObjectStore interface {
	HeadObject(context.Context, f2e.ObjectIdentity) (f2e.ObjectIdentity, error)
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
//
// The lifecycle is split into the phases defined by ADR 0006:
// admission (Admit), validation (BeginValidation/RejectJob), planning
// (BeginPlanning/Plan), scheduling (MarkScheduled/MarkSchedulingFailed),
// execution (AcquireChunk/StartChunk/CompleteChunk/FailChunk) and completion
// (FinalizeJob). The phased methods below are declared now (T07) but full
// persistence is introduced in T08; implementations may return
// ErrNotImplemented until then.
type JobLedger interface {
	// Admission binds a receipt to a jobId/fileId and returns whether the job
	// was newly acquired, already completed, or busy in another execution.
	Admit(context.Context, f2e.Receipt) (f2e.AcquisitionResult, error)

	// BeginValidation moves a RECEIVED job into VALIDATING.
	BeginValidation(context.Context, string) error
	// RejectJob moves a VALIDATING (or RECEIVED) job into REJECTED with a reason.
	RejectJob(context.Context, string, f2e.Rejection) error

	// BeginPlanning moves a VALIDATING job into PLANNING.
	BeginPlanning(context.Context, string) error

	// Plan (legacy) seals the chunk manifest and moves the job from PLANNING to
	// PROCESSING (or COMPLETED when the plan is empty).
	Plan(context.Context, f2e.JobPlan, []f2e.ChunkJob) error
	// MarkChunksScheduled records the durable publication checkpoint after SQS
	// confirms a chunk message. Replaying an uncheckpointed chunk keeps its ID.
	MarkChunksScheduled(context.Context, []f2e.ChunkJob) error
	// UnscheduledChunks returns manifest chunks whose SQS publication has not
	// been durably checkpointed, for safe resume after an organizer crash.
	UnscheduledChunks(context.Context, string) ([]f2e.ChunkJob, error)
	MarkScheduled(context.Context, []string) error
	MarkSchedulingFailed(context.Context, []string, string) error

	// AcquireChunk claims a PENDING chunk for execution, returning whether the
	// claim was acquired, the chunk was already completed, or is busy.
	AcquireChunk(context.Context, string, string) (f2e.AcquisitionResult, error)
	StartChunk(context.Context, string, string, int) error
	CompleteChunk(context.Context, f2e.ChunkResult) error
	FailChunk(context.Context, f2e.ChunkResult) error

	// FinalizeJob records the terminal result and counts of a job.
	FinalizeJob(context.Context, string, f2e.JobResult, f2e.Counts) error

	Replay(context.Context, string, string, []string) ([]f2e.ChunkJob, error)
}

// ErrNotImplemented is returned by phased ledger operations whose persistence
// is introduced in a later task (T08) but are already declared in the port.
type ErrNotImplemented struct{}

func (ErrNotImplemented) Error() string { return "not implemented" }

// SourceResolver fetches a byte range for a chunk job, hiding the underlying
// access method (S3 bucket/key or pre-signed URL).
type SourceResolver interface {
	OpenChunkRange(ctx context.Context, job f2e.ChunkJob, start, end int64) (io.ReadCloser, error)
}

// RecordProcessor is an optional extension point invoked before each event is
// published. It returns an explicit publish/reject/ignore decision.
type RecordProcessor interface {
	Process(ctx context.Context, envelope f2e.Envelope[f2e.RecordPayload]) (f2e.RecordDecision, error)
}
