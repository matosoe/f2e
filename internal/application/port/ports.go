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

// Deprecated compatibility types remain private to the DynamoDB adapter while
// historic ledger rows are drained. They are not part of JobLedger.
var ErrQuotaExceeded = errors.New("prefix active-job quota exceeded")

type WaitingAdmission struct {
	FileID   string
	PrefixID string
	Body     string
	QueuedAt string
}

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
	// LogicalEvents is the number of logical output events carried by this
	// physical message. Zero means one (the single-envelope mode).
	LogicalEvents int
}

const (
	// MaxSQSMessageBytes is the AWS hard limit for an individual message and
	// the total payload of a SendMessageBatch request: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SendMessageBatch.html
	MaxSQSMessageBytes = 1024 * 1024
	// DefaultMaxEventBytes leaves room for SQS message attributes and their wire
	// representation while keeping an individual event close to the 1 MiB limit.
	DefaultMaxEventBytes = MaxSQSMessageBytes - 4*1024

	MaxPhysicalMessageBytes   = MaxSQSMessageBytes
	MaxMultiMessageBatchBytes = MaxSQSMessageBytes
	MaxMessagesPerBatch       = 10
)

// OutboundMessageSize returns a conservative UTF-8 byte budget for an SQS
// message. Besides the body, SQS counts attribute names, types and values; the
// fixed allowance covers their wire representation and entry metadata.
func OutboundMessageSize(message OutboundMessage) int {
	size := len(message.Body) + 64
	for name, attribute := range message.Attributes {
		size += len(name) + len(attribute.DataType) + len(attribute.Value) + 32
	}
	return size
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

// CompletionLedger is the deliberately small part of the ledger used by the
// Worker to close a job and deliver its outbox entry.  Keeping it separate
// avoids making record-processing tests depend on terminal-job persistence.
// ReconcileCompletion is safe to call after a duplicate chunk delivery.
type CompletionLedger interface {
	ReconcileCompletion(context.Context, string) (*f2e.CompletionIntent, error)
	MarkIntentDelivered(context.Context, string, int64) error
}

// JobLedger persists the technical state required for reconciliation and replay.
// Implementations must make every operation idempotent and concurrency-safe.
//
// The lifecycle is split into the following phases:
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

	Replay(context.Context, string, string, []string) ([]f2e.ChunkJob, error)

	// ReserveSlot atomically increments the active-job counter for prefixID and
	// returns ErrQuotaExceeded if the counter would exceed maxActiveJobs (>0).
	// When maxActiveJobs is 0 the call is a no-op (unlimited). The counter is
	// stored as a separate item so the admission PutItem and the quota update
	// can be issued in a TransactWriteItems for atomicity.
	//
	// The caller must pair every successful ReserveSlot with a corresponding
	// ReleaseSlot when the job reaches a terminal state, and must call
	// RecoverQuotaLeaks during startup to release slots for jobs that crashed
	// before reaching terminal.

	// ReleaseSlot atomically decrements the active-job counter for prefixID.
	// ReleaseSlot is idempotent: if the counter is already 0 it remains 0.
	// It must be called exactly once per terminal job state transition, even if
	// the completion event publication fails. A second call due to a retry or
	// duplicate terminal is a no-op.

	// EnqueueWaitingAdmission durably records an admission that could not obtain
	// quota. Repeated S3/SQS deliveries for the same physical file are a no-op.
	// ClaimWaitingAdmissions returns the oldest waiting admission for each
	// prefix and marks it RELEASING. The caller must either remove it after a
	// successful admission or return it to WAITING.
}

// SourceResolver fetches a byte range for a chunk job, hiding the underlying
// access method (S3 bucket/key or pre-signed URL).
type SourceResolver interface {
	OpenChunkRange(ctx context.Context, job f2e.ChunkJob, start, end int64) (io.ReadCloser, error)
}
