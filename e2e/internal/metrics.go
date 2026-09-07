package internal

import (
	"sync/atomic"
	"time"
)

// PhaseTimer records the wall-clock duration of a named sub-stage within a scenario.
// All durations are in milliseconds and use the local clock exclusively.
// AWS-side timestamps (e.g. envelope.metadata.createdAt) are captured separately
// and must not be subtracted from local timestamps to compute latency.
type PhaseTimer struct {
	start time.Time
}

func StartPhase() PhaseTimer { return PhaseTimer{start: time.Now()} }
func (p PhaseTimer) ElapsedMs() int64 {
	if p.start.IsZero() {
		return 0
	}
	return time.Since(p.start).Milliseconds()
}

// APICounters accumulates raw AWS API call and message counts for one scenario.
// All fields are updated atomically so they can be incremented from any goroutine.
type APICounters struct {
	S3PutCount        int64 // PutObject calls
	SQSSendCount      int64 // SendMessage calls (intake)
	SQSReceiveCount   int64 // ReceiveMessage calls (output)
	SQSDeleteBatch    int64 // DeleteMessageBatch calls (output)
	SQSGetAttrsCount  int64 // GetQueueAttributes calls (approximate count polling)
	MessagesReceived  int64 // total SQS messages received across all ReceiveMessage calls
	MessagesDeleted   int64 // total SQS messages deleted across all DeleteMessageBatch calls
	EmptyReceives     int64 // ReceiveMessage calls that returned zero messages
}

func (c *APICounters) incS3Put()                       { atomic.AddInt64(&c.S3PutCount, 1) }
func (c *APICounters) incSQSSend()                     { atomic.AddInt64(&c.SQSSendCount, 1) }
func (c *APICounters) incSQSReceive(msgs, empty int64) {
	atomic.AddInt64(&c.SQSReceiveCount, 1)
	atomic.AddInt64(&c.MessagesReceived, msgs)
	atomic.AddInt64(&c.EmptyReceives, empty)
}
func (c *APICounters) incSQSDeleteBatch(msgs int64) {
	atomic.AddInt64(&c.SQSDeleteBatch, 1)
	atomic.AddInt64(&c.MessagesDeleted, msgs)
}
func (c *APICounters) incSQSGetAttrs() { atomic.AddInt64(&c.SQSGetAttrsCount, 1) }

// ScenarioMetrics holds all instrumentation collected during one Godog scenario.
type ScenarioMetrics struct {
	ScenarioName string `json:"scenario"`
	DataType     string `json:"dataType"`
	RecordCount  int    `json:"recordCount"`

	// Phase durations (local clock, milliseconds).
	SetupMs    int64 `json:"setupMs"`    // file generation
	UploadMs   int64 `json:"uploadMs"`   // S3 PutObject
	IntakeMs   int64 `json:"intakeMs"`   // SQS SendMessage to intake queue
	WaitMs     int64 `json:"waitMs"`     // waiting for first envelopes (WaitForCount)
	ConsumeMs  int64 `json:"consumeMs"`  // ReceiveMessage + DeleteMessageBatch loop
	ValidateMs int64 `json:"validateMs"` // envelope validation
	TotalMs    int64 `json:"totalMs"`    // entire scenario (Before → After)

	// AWS API counters (local view; does not represent server-side latency).
	API APICounters `json:"api"`

	// Envelope timestamps from received messages (AWS-side clock, RFC3339).
	// Must not be subtracted from local timestamps.
	EarliestEnvelopeCreatedAt string `json:"earliestEnvelopeCreatedAt,omitempty"`
	LatestEnvelopeCreatedAt   string `json:"latestEnvelopeCreatedAt,omitempty"`
}

// RunReport is the top-level document written as JSON after a complete test run.
type RunReport struct {
	// RunID is the UTC start time of the test run, used as a stable identifier.
	RunID     string `json:"runId"`
	StartedAt string `json:"startedAt"`
	EndedAt   string `json:"endedAt"`
	TotalMs   int64  `json:"totalMs"`

	// Target distinguishes "localstack" from "aws" runs. AWS-side timing data
	// is absent for LocalStack runs; the 50-minute figure has not been measured
	// and is not reproduced here as a baseline.
	Target string `json:"target"`

	Scenarios []ScenarioMetrics `json:"scenarios"`

	// Suite-level DLQ counts measured after all scenarios complete.
	IntakeDLQCount int `json:"intakeDlqCount"`
	ChunkDLQCount  int `json:"chunkDlqCount"`

	// Benchmark holds a side-by-side comparison of sequential vs parallel runs.
	// Present only when E2E_BENCHMARK=true.
	Benchmark *BenchmarkComparison `json:"benchmark,omitempty"`

	// Notes contains caveats declared at collection time.
	Notes []string `json:"notes"`
}

// BenchmarkComparison records the result of running the same scenario set
// sequentially and then in parallel. The SpeedupFactor is the ratio of the
// sequential total duration to the parallel total duration. A value > 1
// indicates actual measured speedup on the current target.
type BenchmarkComparison struct {
	Concurrency       int     `json:"concurrency"`
	SequentialTotalMs int64   `json:"sequentialTotalMs"`
	ParallelTotalMs   int64   `json:"parallelTotalMs"`
	SpeedupFactor     float64 `json:"speedupFactor"`
	Note              string  `json:"note"`
}
