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

// ErrQuotaExceeded is returned by ReserveSlot when the prefix has reached its
// maxActiveJobs limit. The Organizer should return the SQS message to the
// queue (do not add it to BatchItemFailures) so it can be retried naturally
// when an active job completes and releases its slot.
var ErrQuotaExceeded = errors.New("prefix active-job quota exceeded")

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

	// WriteCompletionIntent atomically writes an outbox record when a job
	// reaches a terminal state. The intent is created together with the
	// terminal transition so a crash cannot leave a terminal job without a
	// pending delivery intent.
	//
	// If an intent already exists for this jobId (concurrent terminal from a
	// retry), the call is a no-op and the existing intent is preserved.
	WriteCompletionIntent(context.Context, f2e.CompletionIntent) error

	// PendingCompletionIntents returns completion intents that have not been
	// marked as delivered yet. The publisher (T13) uses this for recovery after
	// a DynamoDB Streams expiration or a publisher crash.
	//
	// The implementation must not return intents whose TTL has already expired
	// without delivery; the caller is responsible for deciding whether to
	// re-derive the event from the job item.
	PendingCompletionIntents(ctx context.Context, limit int) ([]f2e.CompletionIntent, error)

	// MarkIntentDelivered records that a completion intent has been
	// successfully sent to the completion queue. The publisher calls this only
	// after SQS confirms the send; a crash between send and mark may cause a
	// duplicate delivery, which consumers handle via the stable eventId.
	MarkIntentDelivered(ctx context.Context, jobID string, version int64) error

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
	ReserveSlot(ctx context.Context, prefixID string, maxActiveJobs int) error

	// ReleaseSlot atomically decrements the active-job counter for prefixID.
	// ReleaseSlot is idempotent: if the counter is already 0 it remains 0.
	// It must be called exactly once per terminal job state transition, even if
	// the completion event publication fails. A second call due to a retry or
	// duplicate terminal is a no-op.
	ReleaseSlot(ctx context.Context, prefixID string) error
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
