// Package worker processes chunk jobs and publishes output events.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path"
	"time"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/domain/lineio"
	"github.com/f2e/f2e/internal/domain/multiline"
	"github.com/f2e/f2e/internal/platform/config"
)

type Service struct {
	Resolver  port.SourceResolver
	Processor port.RecordProcessor
	Queue     port.Queue
	Ledger    port.JobLedger
	Config    config.Config
}

// ChunkProcessingResult contains metrics from processing a chunk
type ChunkProcessingResult struct {
	JobID                string
	FileID               string
	ChunkID              string
	Bucket               string
	Key                  string
	StartByte            int64
	EndByte              int64
	Attempt              int
	BytesProcessed       int64
	RecordsProcessed     int64
	ProcessingTimeMillis int64
	TPS                  float64
}

func hash(s string) string { x := sha256.Sum256([]byte(s)); return hex.EncodeToString(x[:]) }
func (s Service) Process(ctx context.Context, body []byte) error {
	return s.ProcessAttempt(ctx, body, 1)
}

// ProcessAttempt records the delivery attempt reported by SQS in the ledger.
func (s Service) ProcessAttempt(ctx context.Context, body []byte, attempt int) error {
	result, err := s.processWithMetrics(ctx, body, attempt)
	if err != nil && s.Ledger != nil {
		var job f2e.ChunkJob
		if json.Unmarshal(body, &job) == nil && job.JobID != "" {
			_ = s.Ledger.FailChunk(ctx, f2e.ChunkResult{JobID: job.JobID, ChunkID: job.ChunkID, Attempt: attempt, Error: err.Error(), OccurredAt: time.Now().UTC()})
		}
	}
	if err == nil && result != nil {
		logChunkSummary(result)
	}
	return err
}

// ProcessWithMetrics processes a chunk and returns metrics
func (s Service) ProcessWithMetrics(ctx context.Context, body []byte) (*ChunkProcessingResult, error) {
	return s.processWithMetrics(ctx, body, 1)
}

func (s Service) processWithMetrics(ctx context.Context, body []byte, attempt int) (*ChunkProcessingResult, error) {
	startTime := f2e.CurrentTimeMillis()

	var j f2e.ChunkJob
	if e := json.Unmarshal(body, &j); e != nil {
		return nil, fmt.Errorf("decode job: %w", e)
	}
	if j.Configuration.BatchSize > 0 {
		s.Config.BatchSize = j.Configuration.BatchSize
	}
	if j.Configuration.MaxEventBytes > 0 {
		s.Config.MaxEventBytes = j.Configuration.MaxEventBytes
	}
	if j.Configuration.MaxChunkBytes > 0 {
		s.Config.MaxChunkBytes = j.Configuration.MaxChunkBytes
	}
	if j.Configuration.EventSchemaID != "" {
		s.Config.EventSchemaID = j.Configuration.EventSchemaID
	}
	if j.Configuration.EventSchemaVersion != "" {
		s.Config.EventSchemaVersion = j.Configuration.EventSchemaVersion
	}
	if j.Configuration.EventFormat != "" {
		s.Config.EventFormat = j.Configuration.EventFormat
	}
	// T20: per-prefix output queue routing — the job carries its own queue URL
	// when the prefix has a dedicated output queue; otherwise fall back to the
	// Lambda environment's default.
	if j.Configuration.OutputQueueURL != "" {
		s.Config.OutputQueueURL = j.Configuration.OutputQueueURL
	}
	if e := s.valid(j); e != nil {
		return nil, e
	}
	if s.Ledger != nil {
		if err := s.Ledger.StartChunk(ctx, j.JobID, j.ChunkID, attempt); err != nil {
			if errors.Is(err, port.ErrAlreadyCompleted) {
				return nil, nil
			}
			return nil, fmt.Errorf("start chunk ledger: %w", err)
		}
	}
	readStart := j.StartByte
	if j.MaxRecordLengthBytes > 0 && readStart > 0 && j.DataType != f2e.DataTypeJSON {
		readStart -= j.MaxRecordLengthBytes
		if readStart < 0 {
			readStart = 0
		}
	}
	r, e := s.Resolver.OpenChunkRange(ctx, j, readStart, j.EndByteInclusive)
	if e != nil {
		return nil, e
	}
	counts, err := s.streamWithMetrics(ctx, j, r, readStart)
	closeErr := r.Close()
	if err != nil {
		return nil, fmt.Errorf("process job %s chunk %s: %w", j.JobID, j.ChunkID, err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close source stream: %w", closeErr)
	}

	endTime := f2e.CurrentTimeMillis()
	duration := endTime - startTime

	result := &ChunkProcessingResult{
		JobID:                j.JobID,
		FileID:               j.FileID,
		ChunkID:              j.ChunkID,
		Bucket:               j.Bucket,
		Key:                  j.Key,
		StartByte:            j.StartByte,
		EndByte:              j.EndByteInclusive,
		Attempt:              attempt,
		BytesProcessed:       j.EndByteInclusive - readStart + 1,
		RecordsProcessed:     counts.RecordsRead,
		ProcessingTimeMillis: duration,
		TPS:                  f2e.CalculateTPS(counts.RecordsRead, duration),
	}
	if s.Ledger != nil {
		if err := s.Ledger.CompleteChunk(ctx, f2e.ChunkResult{JobID: j.JobID, ChunkID: j.ChunkID, Attempt: attempt, RecordsProduced: counts.RecordsPublished, BytesProcessed: j.EndByteInclusive - readStart + 1, OccurredAt: time.Now().UTC(), Counts: counts}); err != nil {
			return nil, fmt.Errorf("complete chunk ledger: %w", err)
		}
	}
	return result, nil
}

// logChunkSummary logs the chunk processing summary
func logChunkSummary(result *ChunkProcessingResult) {
	slog.Info("chunk processing completed", "service", "worker", "jobId", result.JobID, "fileId", result.FileID, "chunkId", result.ChunkID, "bucket", result.Bucket, "key", result.Key, "startByte", result.StartByte, "endByte", result.EndByte, "attempt", result.Attempt, "bytesProcessed", result.BytesProcessed, "recordsProduced", result.RecordsProcessed, "durationMs", result.ProcessingTimeMillis, "tps", result.TPS)
}
func (s Service) valid(j f2e.ChunkJob) error {
	if j.SchemaVersion != f2e.SchemaVersion || j.Bucket == "" || j.Key == "" || j.EndByteInclusive < j.StartByte {
		return fmt.Errorf("invalid chunk job")
	}
	maxChunkBytes := s.Config.MaxChunkBytes
	if maxChunkBytes == 0 {
		maxChunkBytes = 64 * 1024 * 1024
	}
	if j.EndByteInclusive-j.StartByte+1 > maxChunkBytes {
		return fmt.Errorf("chunk exceeds maximum %d bytes", maxChunkBytes)
	}
	if j.DataType != f2e.DataTypeText && j.DataType != f2e.DataTypeMultiLine && j.DataType != f2e.DataTypeJSON {
		return fmt.Errorf("unsupported data type %q", j.DataType)
	}
	if j.MaxRecordLengthBytes > 0 && (j.TrailingPaddingBytes < 0 || j.TrailingPaddingBytes > j.MaxRecordLengthBytes || j.EndByteInclusive-j.StartByte+1 < j.TrailingPaddingBytes) {
		return fmt.Errorf("invalid variable-record chunk job")
	}
	if j.DataType == f2e.DataTypeMultiLine && j.MultiLineLayout.BreakMarker == "" {
		return fmt.Errorf("multi-line chunk job missing breakMarker")
	}
	if j.DataType == f2e.DataTypeJSON && j.JSONArrayLayout.MaxBytesPerElement < 1 {
		return fmt.Errorf("json array chunk job missing maxBytesPerElement")
	}
	return nil
}
func (s Service) stream(ctx context.Context, j f2e.ChunkJob, r io.Reader, readStart int64) error {
	_, err := s.streamWithMetrics(ctx, j, r, readStart)
	return err
}

// streamWithMetrics processes a chunk and returns the number of records processed
func (s Service) streamWithMetrics(ctx context.Context, j f2e.ChunkJob, r io.Reader, readStart int64) (f2e.Counts, error) {
	batchSize := s.Config.BatchSize
	if batchSize == 0 { // keeps direct callers that omit Config.BatchSize compatible.
		batchSize = 10
	}
	concurrency := s.Config.PublishConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	batch := make([]port.OutboundMessage, 0, batchSize)
	counts := f2e.Counts{CountsComplete: true}

	// Determine output mode from the job configuration (T16).
	outputMode := j.Configuration.OutputMode
	if outputMode == "" {
		outputMode = f2e.OutputModeSingle
	}
	var packer *bundlePacker
	if outputMode == f2e.OutputModeBundle {
		packer = newBundlePacker(j, j.Configuration)
	}

	// sender dispatches full batches concurrently; production order is preserved
	// because batches are assembled deterministically before being submitted.
	sender := newConcurrentSender(ctx, s.Queue, s.Config.OutputQueueURL, concurrency)

	sendBatch := func() error {
		if err := sender.submit(batch); err != nil {
			return err
		}
		batch = nil
		return nil
	}

	// enqueueSingle adds a message to the SQS batch and flushes when full.
	// This is the original single-envelope path.
	enqueueSingle := func(msg port.OutboundMessage) error {
		batch = append(batch, msg)
		counts.RecordsPublished++
		if len(batch) == batchSize {
			return sendBatch()
		}
		return nil
	}

	// enqueueBundle adds an envelope to the packer; any flushed bundle goes to
	// the SQS batch. The SQS batch is flushed when full.
	enqueueBundle := func(env f2e.Envelope[f2e.RecordPayload], serialised []byte) error {
		flushed, err := packer.add(env, serialised)
		if err != nil {
			return err
		}
		if flushed != nil {
			batch = append(batch, *flushed)
			// Bundle messages are logical messages; do not increment RecordsPublished here —
			// that is done per-envelope inside the packer path below.
			if len(batch) == batchSize {
				return sendBatch()
			}
		}
		return nil
	}

	flush := func() error {
		if packer != nil {
			// Flush any remaining items in the packer before the final SQS batch flush.
			remaining, err := packer.flush()
			if err != nil {
				return err
			}
			if remaining != nil {
				batch = append(batch, *remaining)
			}
		}
		if len(batch) > 0 {
			if err := sendBatch(); err != nil {
				return err
			}
		}
		// Wait for all in-flight sends to complete.
		return sender.wait()
	}

	publish := func(n, off int64, raw string, physicalLength int64) error {
		// Every callback is one logical record; rejected and ignored records are
		// reconciled separately from messages actually published.
		counts.RecordsRead++
		byteOffset := j.StartByte + off
		if j.MaxRecordLengthBytes > 0 {
			byteOffset = readStart + off
		}
		// sourceRecordId (v2) is derived solely from the immutable physical source
		// and the record's starting byte offset, independent of chunking, bundle
		// grouping and schema/configuration version (kept as separate metadata).
		sourceRecordID := f2e.SourceRecordID(j.FileID, byteOffset)
		eventID := hash(j.JobID + "/" + sourceRecordID)
		recNum := j.StartRecord + n + 1
		var recordNumber *int64
		// For chunked variable-length formats only the first chunk has a
		// provably global ordinal.
		if j.ChunkID == "00000001" || j.StartByte == 0 {
			recordNumber = &recNum
		}
		var byteLen *int64
		if physicalLength > 0 {
			byteLen = &physicalLength
		}
		switch j.DataType {
		case f2e.DataTypeJSON, f2e.DataTypeText, f2e.DataTypeMultiLine:
			if raw != "" {
				bl := int64(len(raw))
				byteLen = &bl
			}
		}
		env := f2e.Envelope[f2e.RecordPayload]{
			Metadata: f2e.Metadata{
				EventID:        eventID,
				SourceRecordID: sourceRecordID,
				Schema:         f2e.Schema{ID: s.Config.EventSchemaID, Version: s.Config.EventSchemaVersion},
				Format:         s.Config.EventFormat,
				CreatedAt:      time.Now().UTC().Format(time.RFC3339),
				TransactionID:  j.Context.TransactionID,
				CorrelationID:  j.Context.CorrelationID,
				TraceID:        j.Context.TraceID,
			},
			Source: f2e.Source{
				Type:       "s3",
				Bucket:     j.Bucket,
				Key:        j.Key,
				VersionID:  j.VersionID,
				ETag:       j.ETag,
				FileName:   path.Base(j.Key),
				FileFormat: string(j.DataType),
				FileSize:   j.FileSize,
				System:     j.Context.SourceSystem,
			},
			Processing: f2e.Processing{
				JobID:        j.JobID,
				ChunkID:      j.ChunkID,
				RecordNumber: recordNumber,
				ByteOffset:   &byteOffset,
				ByteLength:   byteLen,
			},
			Data: f2e.RecordPayload{Raw: raw},
		}
		if s.Processor != nil {
			original := env
			decision, er := s.Processor.Process(ctx, env)
			if er != nil {
				return er
			}
			switch decision.Kind {
			case f2e.RecordReject, f2e.RecordIgnore:
				if decision.Reason == "" {
					return fmt.Errorf("record processor returned %s without reasonCode", decision.Kind)
				}
				if decision.Kind == f2e.RecordReject {
					counts.RecordsRejected++
				} else {
					counts.RecordsIgnored++
				}
				if counts.Reasons == nil {
					counts.Reasons = make(map[f2e.RejectionReason]int64)
				}
				counts.Reasons[decision.Reason]++
				return nil
			case f2e.RecordPublish:
				if decision.Envelope == nil {
					return fmt.Errorf("record processor returned publish without envelope")
				}
				env = *decision.Envelope
			default:
				return fmt.Errorf("record processor returned invalid decision %q", decision.Kind)
			}
			if env.Metadata.EventID != original.Metadata.EventID || env.Metadata.SourceRecordID != original.Metadata.SourceRecordID || env.Metadata.Schema.ID == "" || env.Metadata.Schema.Version == "" || env.Metadata.Format == "" || env.Processing.JobID != original.Processing.JobID || env.Processing.ChunkID != original.Processing.ChunkID || env.Source.Bucket != original.Source.Bucket || env.Source.Key != original.Source.Key {
				return fmt.Errorf("record processor changed or removed mandatory technical fields")
			}
		}
		b, er := json.Marshal(env)
		if er != nil {
			return er
		}
		maxEventBytes := s.Config.MaxEventBytes
		if maxEventBytes == 0 {
			maxEventBytes = 256 * 1024
		}
		if len(b) > maxEventBytes {
			return fmt.Errorf("serialized event exceeds limit: %d > %d bytes", len(b), maxEventBytes)
		}
		attrs := map[string]port.MessageAttribute{
			"schema": {DataType: "String", Value: env.Metadata.Schema.ID + ":" + env.Metadata.Schema.Version},
			"format": {DataType: "String", Value: env.Metadata.Format},
		}
		for name, value := range map[string]string{
			"transactionId": env.Metadata.TransactionID,
			"correlationId": env.Metadata.CorrelationID,
			"traceId":       env.Metadata.TraceID,
			"sourceSystem":  env.Source.System,
		} {
			if value != "" {
				attrs[name] = port.MessageAttribute{DataType: "String", Value: value}
			}
		}
		if len(attrs) > 10 {
			return fmt.Errorf("message attribute count exceeds SQS limit")
		}

		if packer != nil {
			// Bundle mode: accumulate in packer; counts.RecordsPublished is
			// incremented here so it reflects envelopes queued, not bundles sent.
			counts.RecordsPublished++
			return enqueueBundle(env, b)
		}
		return enqueueSingle(port.OutboundMessage{Body: string(b), Attributes: attrs})
	}

	var err error
	switch j.DataType {
	case f2e.DataTypeText:
		lineLimit := j.MaxRecordLengthBytes
		if lineLimit == 0 {
			lineLimit = int64(s.Config.MaxEventBytes)
			if lineLimit == 0 {
				lineLimit = 256 * 1024
			}
		}
		err = lineio.ReadLines(ctx, r, lineLimit, j.MaxRecordLengthBytes > 0 && readStart > 0, func(n, off int64, raw string) error {
			if j.MaxRecordLengthBytes > 0 && (readStart+off < j.StartByte || readStart+off > j.EndByteInclusive-j.TrailingPaddingBytes) {
				return nil
			}
			return publish(n, off, raw, 0)
		})
	case f2e.DataTypeMultiLine:
		layout := j.MultiLineLayout
		stats, multiErr := multiline.ReadMultiLineWithStats(ctx, r, layout.BreakPosition, layout.BreakMarker, layout.AcceptedPrefixes, layout.LineSeparator, layout.MaxBytesPerRecord, func(n, off int64, raw string) error {
			if readStart+off < j.StartByte || readStart+off > j.EndByteInclusive-j.TrailingPaddingBytes {
				return nil
			}
			return publish(n, off, raw, 0)
		})
		counts.PhysicalLinesIgnored += stats.HeaderLinesIgnored + stats.TrailerLinesIgnored
		if stats.HeaderLinesIgnored > 0 || stats.TrailerLinesIgnored > 0 {
			if counts.Reasons == nil {
				counts.Reasons = make(map[f2e.RejectionReason]int64)
			}
			counts.Reasons[f2e.IgnoreMultiLineHeader] += stats.HeaderLinesIgnored
			counts.Reasons[f2e.IgnoreMultiLineTrailer] += stats.TrailerLinesIgnored
		}
		err = multiErr
	case f2e.DataTypeJSON:
		err = readJSONArray(ctx, r, j, readStart, func(n, off int64, raw string) error {
			return publish(n, off, raw, int64(len(raw)))
		})
	}
	if err != nil {
		return f2e.Counts{}, err
	}
	// flush handles packer remainder + any pending SQS batch.
	if err := flush(); err != nil {
		return f2e.Counts{}, err
	}
	return counts, nil
}

// readJSONArray streams elements from a JSON array within the chunk's owned byte range.
func readJSONArray(ctx context.Context, r io.Reader, j f2e.ChunkJob, readStart int64, fn func(int64, int64, string) error) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	// Trim the buffer to start at the array content.
	var scanStart int64
	if j.JSONArrayOffset >= readStart {
		scanStart = j.JSONArrayOffset - readStart
	}
	trimmed := data[scanStart:]
	bufferBase := readStart + scanStart
	ownedEnd := j.EndByteInclusive - j.TrailingPaddingBytes
	pos := 0
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		elemStart, elemEnd := nextCompleteElement(trimmed, pos)
		if elemStart < 0 {
			break
		}
		fileOffset := bufferBase + int64(elemStart)
		if fileOffset > ownedEnd {
			break
		}
		if fileOffset >= j.StartByte {
			off := fileOffset - readStart
			if err := fn(n, off, string(trimmed[elemStart:elemEnd])); err != nil {
				return err
			}
			n++
		}
		pos = elemEnd
	}
	return nil
}

// nextCompleteElement returns the [start, end) byte positions of the next
// complete JSON value at an element boundary.
func nextCompleteElement(data []byte, pos int) (int, int) {
	return f2e.NextCompleteJSONElement(data, pos)
}
