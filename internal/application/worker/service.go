// Package worker processes chunk jobs and publishes output events.
package worker

import (
	"bytes"
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
	Resolver         port.SourceResolver
	Queue            port.Queue
	Ledger           port.JobLedger
	CompletionLedger port.CompletionLedger
	Config           config.Config
}

// ErrLambdaExecutionLimitExceeded identifies work deliberately stopped before
// the Lambda runtime can terminate the invocation. Callers use this to record
// an incomplete chunk with a reason that is actionable in the ledger.
var ErrLambdaExecutionLimitExceeded = errors.New("excedeu o limite de execução da lambda")

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
		// The worker uses a deadline slightly before the Lambda deadline. Do not
		// reuse that cancelled context to write the terminal/retry state: this
		// write is precisely what makes the shutdown graceful.
		if errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w: %v", ErrLambdaExecutionLimitExceeded, err)
		}
		var job f2e.ChunkJob
		if json.Unmarshal(body, &job) == nil && job.JobID != "" {
			failureCtx := ctx
			var cancel context.CancelFunc
			if ctx.Err() != nil {
				// Preserve request values (for tracing) but detach cancellation and
				// bound the final ledger write so it fits in the reserved margin.
				failureCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				defer cancel()
			}
			if failErr := s.Ledger.FailChunk(failureCtx, f2e.ChunkResult{JobID: job.JobID, ChunkID: job.ChunkID, Attempt: attempt, Error: err.Error(), OccurredAt: time.Now().UTC()}); failErr != nil {
				slog.Error("could not mark incomplete chunk", "service", "worker", "jobId", job.JobID, "chunkId", job.ChunkID, "error", failErr)
			}
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
	if j.Control == "completion" {
		if j.JobID == "" {
			return nil, fmt.Errorf("completion control without jobId")
		}
		return nil, s.publishCompletion(ctx, j.JobID)
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
	// The job carries its own queue URL for optional per-prefix routing.
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
				return nil, s.publishCompletion(ctx, j.JobID)
			}
			return nil, fmt.Errorf("start chunk ledger: %w", err)
		}
	}
	readStart, readEnd, e := chunkReadRange(j)
	if e != nil {
		return nil, e
	}
	r, e := s.Resolver.OpenChunkRange(ctx, j, readStart, readEnd)
	if e != nil {
		return nil, e
	}
	var source io.Reader = r
	// Text chunks use nominal ranges. Validate both discoverable line boundaries
	// before streaming anything, so a bad maxRecordLengthBytes cannot publish a
	// partial chunk.
	if j.DataType == f2e.DataTypeText && j.MaxRecordLengthBytes > 0 {
		data, readErr := io.ReadAll(r)
		if readErr != nil {
			_ = r.Close()
			return nil, fmt.Errorf("read chunk boundaries: %w", readErr)
		}
		if err := validateTextBoundaries(j, readStart, readEnd, data); err != nil {
			_ = r.Close()
			return nil, err
		}
		// lineio intentionally requires a terminator. EOF is nevertheless a
		// valid final boundary for the nominal-range contract, so add a synthetic
		// LF only to the in-memory parsing view (never to the source object).
		if readEnd == j.FileSize-1 && len(data) > 0 && data[len(data)-1] != '\n' && data[len(data)-1] != '\r' {
			data = append(data, '\n')
		}
		source = bytes.NewReader(data)
	}
	counts, err := s.streamWithMetrics(ctx, j, source, readStart)
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
		BytesProcessed:       readEnd - readStart + 1,
		RecordsProcessed:     counts.RecordsRead,
		ProcessingTimeMillis: duration,
		TPS:                  f2e.CalculateTPS(counts.RecordsRead, duration),
	}
	if s.Ledger != nil {
		if err := s.Ledger.CompleteChunk(ctx, f2e.ChunkResult{JobID: j.JobID, ChunkID: j.ChunkID, Attempt: attempt, RecordsProduced: counts.RecordsPublished, BytesProcessed: readEnd - readStart + 1, OccurredAt: time.Now().UTC(), Counts: counts}); err != nil {
			return nil, fmt.Errorf("complete chunk ledger: %w", err)
		}
		if err := s.publishCompletion(ctx, j.JobID); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// publishCompletion sends only a durable, pending intent.  SQS acknowledgement
// is deliberately after Send succeeds; a crash between them can redeliver the
// same deterministic eventId, which is the intended at-least-once contract.
func (s Service) publishCompletion(ctx context.Context, jobID string) error {
	if s.CompletionLedger == nil {
		return nil
	}
	intent, err := s.CompletionLedger.ReconcileCompletion(ctx, jobID)
	if err != nil || intent == nil {
		return err
	}
	if s.Config.CompletionQueueURL == "" {
		return fmt.Errorf("completion queue is not configured")
	}
	body, err := json.Marshal(intent.Event)
	if err != nil {
		return fmt.Errorf("encode completion event: %w", err)
	}
	failed, err := s.Queue.Send(ctx, s.Config.CompletionQueueURL, []port.OutboundMessage{{Body: string(body)}})
	if err != nil {
		return fmt.Errorf("send completion event: %w", err)
	}
	if len(failed) != 0 {
		return fmt.Errorf("send completion event: partial batch failure")
	}
	if err := s.CompletionLedger.MarkIntentDelivered(ctx, intent.JobID, intent.Version); err != nil {
		return fmt.Errorf("mark completion delivery: %w", err)
	}
	return nil
}

func chunkReadRange(j f2e.ChunkJob) (int64, int64, error) {
	if j.DataType == f2e.DataTypeJSON && j.FileSize > 0 {
		overlap := j.JSONArrayLayout.MaxBytesPerElement
		if overlap < 1 {
			return 0, 0, fmt.Errorf("JSON chunk is missing maxBytesPerElement")
		}
		start, end := j.StartByte-overlap, j.EndByteInclusive+overlap
		if start < 0 {
			start = 0
		}
		if end >= j.FileSize {
			end = j.FileSize - 1
		}
		return start, end, nil
	}
	if j.DataType != f2e.DataTypeText || j.MaxRecordLengthBytes == 0 {
		return j.StartByte, j.EndByteInclusive, nil
	}
	// Jobs created before nominal-range planning did not carry FileSize. Keep
	// them processable during rollout; all newly planned text jobs include it.
	if j.FileSize < 1 {
		return j.StartByte, j.EndByteInclusive, nil
	}
	overlap := 9 * j.MaxRecordLengthBytes
	if overlap/j.MaxRecordLengthBytes != 9 {
		return 0, 0, fmt.Errorf("text chunk overlap overflow")
	}
	start := j.StartByte - overlap
	if start < 0 {
		start = 0
	}
	end := j.EndByteInclusive + overlap
	if end >= j.FileSize {
		end = j.FileSize - 1
	}
	return start, end, nil
}

func validateTextBoundaries(j f2e.ChunkJob, readStart, readEnd int64, data []byte) error {
	// A line may start at byte zero or immediately after LF/CR. In the latter
	// case the left overlap must contain a terminator. EOF is an equally valid
	// right boundary for a final record without a newline.
	if j.FileSize < 1 {
		return nil
	}
	if j.StartByte > 0 && !hasLineTerminator(data[:j.StartByte-readStart]) {
		return boundaryError(j, "start", j.StartByte, readEnd-readStart+1)
	}
	if j.EndByteInclusive < j.FileSize-1 {
		offset := j.EndByteInclusive + 1 - readStart
		if offset < 0 || offset > int64(len(data)) || !hasLineTerminator(data[offset:]) {
			return boundaryError(j, "end", j.EndByteInclusive, readEnd-readStart+1)
		}
	}
	return nil
}

func hasLineTerminator(data []byte) bool {
	for _, b := range data {
		if b == '\n' || b == '\r' {
			return true
		}
	}
	return false
}

func boundaryError(j f2e.ChunkJob, side string, offset, searched int64) error {
	return fmt.Errorf("não foi localizada uma quebra de registro na fronteira %s (byte %d) após buscar %d bytes; um dos registros pode ser maior que o limite configurado de %d bytes", side, offset, searched, j.MaxRecordLengthBytes)
}

// logChunkSummary logs the chunk processing summary
func logChunkSummary(result *ChunkProcessingResult) {
	slog.Info("chunk processing completed", "service", "worker", "jobId", result.JobID, "fileId", result.FileID, "chunkId", result.ChunkID, "bucket", result.Bucket, "key", result.Key, "startByte", result.StartByte, "endByte", result.EndByte, "attempt", result.Attempt, "bytesProcessed", result.BytesProcessed, "recordsProduced", result.RecordsProcessed, "durationMs", result.ProcessingTimeMillis, "tps", result.TPS)
}
func (s Service) valid(j f2e.ChunkJob) error {
	if j.SchemaVersion != f2e.SchemaVersion || j.Bucket == "" || j.Key == "" || j.EndByteInclusive < j.StartByte {
		return fmt.Errorf("dados do chunk inválidos: versão, origem ou intervalo de bytes inconsistente")
	}
	maxChunkBytes := s.Config.MaxChunkBytes
	if maxChunkBytes == 0 {
		maxChunkBytes = 64 * 1024 * 1024
	}
	if j.EndByteInclusive-j.StartByte+1 > maxChunkBytes {
		return fmt.Errorf("o chunk possui %d bytes e excede o limite configurado de %d bytes", j.EndByteInclusive-j.StartByte+1, maxChunkBytes)
	}
	if j.DataType != f2e.DataTypeText && j.DataType != f2e.DataTypeMultiLine && j.DataType != f2e.DataTypeJSON {
		return fmt.Errorf("tipo de dado não suportado no chunk: %q", j.DataType)
	}
	if j.MaxRecordLengthBytes > 0 && (j.TrailingPaddingBytes < 0 || j.TrailingPaddingBytes > j.MaxRecordLengthBytes || j.EndByteInclusive-j.StartByte+1 < j.TrailingPaddingBytes) {
		return fmt.Errorf("dados do chunk com registros de tamanho variável inválidos: padding ou limite de registro inconsistente")
	}
	if j.DataType == f2e.DataTypeMultiLine && len(j.MultiLineLayout.BreakFields) == 0 {
		return fmt.Errorf("configuração do chunk multi-linha inválida: breakFields não informado")
	}
	if j.DataType == f2e.DataTypeJSON && (j.JSONArrayLayout.MaxBytesPerElement < 1 || j.JSONArrayLayout.FirstFieldName == "") {
		return fmt.Errorf("configuração do chunk JSON inválida: firstFieldName ou limite máximo por elemento não informado")
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
		if err := sender.wait(); err != nil {
			return err
		}
		counts.MessagesPublished, counts.RecordsPublished, counts.SendMessageBatchCalls = sender.counts()
		return nil
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
		b, er := json.Marshal(env)
		if er != nil {
			return er
		}
		maxEventBytes := s.Config.MaxEventBytes
		if maxEventBytes == 0 {
			maxEventBytes = port.DefaultMaxEventBytes
		}
		// A configured limit below the SQS safety budget remains a hard
		// contract. At/above that budget, let the physical-message check below
		// reject this individual record with ledger-reconcilable context.
		if len(b) > maxEventBytes && maxEventBytes < port.MaxPhysicalMessageBytes {
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
			if err := enqueueBundle(env, b); err != nil {
				var tooLarge f2e.ErrEnvelopeTooLarge
				if errors.As(err, &tooLarge) {
					slog.Warn("record rejected: physical SQS message exceeds budget", "service", "worker", "jobId", j.JobID, "chunkId", j.ChunkID, "eventId", tooLarge.EventID, "actualBytes", tooLarge.ActualBytes, "limitBytes", tooLarge.LimitBytes)
					counts.RecordsRejected++
					if counts.Reasons == nil {
						counts.Reasons = make(map[f2e.RejectionReason]int64)
					}
					counts.Reasons[f2e.RejectionMessageTooLarge]++
					return nil
				}
				return err
			}
			return nil
		}
		message := port.OutboundMessage{Body: string(b), Attributes: attrs, LogicalEvents: 1}
		if port.OutboundMessageSize(message) > port.MaxPhysicalMessageBytes {
			slog.Warn("record rejected: physical SQS message exceeds budget", "service", "worker", "jobId", j.JobID, "chunkId", j.ChunkID, "eventId", env.Metadata.EventID, "actualBytes", port.OutboundMessageSize(message), "limitBytes", port.MaxPhysicalMessageBytes)
			counts.RecordsRejected++
			if counts.Reasons == nil {
				counts.Reasons = make(map[f2e.RejectionReason]int64)
			}
			counts.Reasons[f2e.RejectionMessageTooLarge]++
			return nil
		}
		return enqueueSingle(message)
	}

	var err error
	switch j.DataType {
	case f2e.DataTypeText:
		lineLimit := j.MaxRecordLengthBytes
		if lineLimit == 0 {
			lineLimit = int64(s.Config.MaxEventBytes)
			if lineLimit == 0 {
				lineLimit = int64(port.DefaultMaxEventBytes)
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
		stats, multiErr := multiline.ReadMultiLineLayoutWithStats(ctx, r, layout, func(n, off int64, raw string) error {
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
	data, readStart = alignJSONRead(data, readStart, j.JSONArrayLayout.FirstFieldName)
	var n int64
	for _, element := range f2e.FindJSONObjectStarts(data, j.JSONArrayLayout.FirstFieldName) {
		if err := ctx.Err(); err != nil {
			return err
		}
		fileOffset := readStart + int64(element[0])
		if fileOffset >= j.StartByte && fileOffset <= j.EndByteInclusive {
			if int64(element[1]-element[0]) > j.JSONArrayLayout.MaxBytesPerElement {
				return fmt.Errorf("JSON element at byte %d exceeds maxBytesPerElement", fileOffset)
			}
			if err := fn(n, fileOffset-readStart, string(data[element[0]:element[1]])); err != nil {
				return err
			}
			n++
		}
	}
	return nil
}

// alignJSONRead descarta o prefixo parcial de uma leitura com sobreposição até
// o primeiro objeto completo. Sem esse realinhamento, uma faixa que começa no
// meio de uma string deixa o analisador léxico fora de sincronia e faz com que
// ele ignore os objetos seguintes.
func alignJSONRead(data []byte, readStart int64, firstFieldName string) ([]byte, int64) {
	for offset := 0; offset < len(data); {
		relative := bytes.IndexByte(data[offset:], '{')
		if relative < 0 {
			break
		}
		offset += relative
		starts := f2e.FindJSONObjectStarts(data[offset:], firstFieldName)
		if len(starts) != 0 && starts[0][0] == 0 {
			return data[offset:], readStart + int64(offset)
		}
		offset++
	}
	return data, readStart
}

// nextCompleteElement returns the [start, end) byte positions of the next
// complete JSON value at an element boundary.
func nextCompleteElement(data []byte, pos int) (int, int) {
	return f2e.NextCompleteJSONElement(data, pos)
}
