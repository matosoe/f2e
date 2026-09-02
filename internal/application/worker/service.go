// Package worker processes chunk jobs and publishes output events.
package worker

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"path"
	"strings"
	"time"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/domain/fixedwidth"
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
	// Jobs produced before the explicit dataType contract were fixed-width.
	if j.DataType == "" {
		j.DataType = f2e.DataTypeFixedWidth
	}
	if e := s.valid(j); e != nil {
		return nil, e
	}
	if s.Ledger != nil {
		if err := s.Ledger.StartChunk(ctx, j.JobID, j.ChunkID, attempt); err != nil {
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
	recordCount, err := s.streamWithMetrics(ctx, j, r, readStart)
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
		RecordsProcessed:     recordCount,
		ProcessingTimeMillis: duration,
		TPS:                  f2e.CalculateTPS(recordCount, duration),
	}
	if s.Ledger != nil {
		if err := s.Ledger.CompleteChunk(ctx, f2e.ChunkResult{JobID: j.JobID, ChunkID: j.ChunkID, Attempt: attempt, RecordsProduced: recordCount, BytesProcessed: j.EndByteInclusive - readStart + 1, OccurredAt: time.Now().UTC()}); err != nil {
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
	if j.DataType == f2e.DataTypeFixedWidth && (j.RecordCount < 1 || j.RecordLengthBytes != int64(s.Config.RecordLength) || j.EndByteInclusive-j.StartByte+1 != j.RecordCount*j.RecordLengthBytes) {
		return fmt.Errorf("invalid fixed-width chunk job")
	}
	if j.DataType != f2e.DataTypeFixedWidth && j.DataType != f2e.DataTypeJSONL && j.DataType != f2e.DataTypeNDJSON && j.DataType != f2e.DataTypeCSV && j.DataType != f2e.DataTypeBinary && j.DataType != f2e.DataTypeText && j.DataType != f2e.DataTypeMultiLine && j.DataType != f2e.DataTypeJSON {
		return fmt.Errorf("unsupported data type %q", j.DataType)
	}
	if (j.DataType == f2e.DataTypeCSV || j.DataType == f2e.DataTypeJSON) && s.Config.Environment != "" && !s.Config.EnablePreviewFormats {
		return fmt.Errorf("preview data type %q is not enabled", j.DataType)
	}
	if (j.DataType == f2e.DataTypeBinary || j.DataType == f2e.DataTypeMultiLine) && s.Config.Environment != "" && !s.Config.EnableExperimentalFormats {
		return fmt.Errorf("experimental data type %q is not enabled", j.DataType)
	}
	if j.Options.BypassJSONValidation && j.DataType != f2e.DataTypeJSONL && j.DataType != f2e.DataTypeNDJSON {
		return fmt.Errorf("invalid JSON validation option")
	}
	if j.MaxRecordLengthBytes > 0 && (j.DataType == f2e.DataTypeFixedWidth || j.DataType == f2e.DataTypeBinary || j.TrailingPaddingBytes < 0 || j.TrailingPaddingBytes > j.MaxRecordLengthBytes || j.EndByteInclusive-j.StartByte+1 < j.TrailingPaddingBytes) {
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
func (s Service) streamWithMetrics(ctx context.Context, j f2e.ChunkJob, r io.Reader, readStart int64) (int64, error) {
	batchSize := s.Config.BatchSize
	if batchSize == 0 { // keeps direct callers that omit Config.BatchSize compatible.
		batchSize = 10
	}
	batch := make([]port.OutboundMessage, 0, batchSize)
	var recordsProcessed int64 = 0

	flush := func() error {
		pending := append([]port.OutboundMessage(nil), batch...)
		for attempt := 0; attempt < 3; attempt++ {
			failed, e := s.Queue.Send(ctx, s.Config.OutputQueueURL, pending)
			if e == nil && len(failed) == 0 {
				batch = nil
				return nil
			}
			if e != nil && attempt == 2 {
				return e
			}
			if e == nil {
				next := make([]port.OutboundMessage, 0, len(failed))
				for _, index := range failed {
					if index >= 0 && index < len(pending) {
						next = append(next, pending[index])
					}
				}
				pending = next
			}
			delay := time.Duration((1<<attempt)*50+rand.IntN(50)) * time.Millisecond
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		return fmt.Errorf("output batch partial failure after retries")
	}

	publish := func(n, off int64, raw string, fields []string, binary []byte, physicalLength int64) error {
		byteOffset := j.StartByte + off
		if j.MaxRecordLengthBytes > 0 {
			byteOffset = readStart + off
		}
		sourceRecordID := hash(j.FileID + "/" + j.ChunkID + "/" + fmt.Sprint(j.StartRecord+n+1) + "/" + s.Config.EventSchemaID + "/" + s.Config.EventSchemaVersion)
		if j.MaxRecordLengthBytes > 0 {
			sourceRecordID = hash(j.FileID + "/" + fmt.Sprint(byteOffset) + "/" + s.Config.EventSchemaID + "/" + s.Config.EventSchemaVersion)
		}
		eventID := hash(j.JobID + "/" + sourceRecordID)
		recNum := j.StartRecord + n + 1
		var recordNumber *int64
		// Fixed-width chunks carry an exact starting record. For variable
		// formats only the first chunk has a provably global ordinal.
		if j.DataType == f2e.DataTypeFixedWidth || j.ChunkID == "00000001" || j.StartByte == 0 {
			recordNumber = &recNum
		}
		var byteLen *int64
		if physicalLength > 0 {
			byteLen = &physicalLength
		}
		switch j.DataType {
		case f2e.DataTypeFixedWidth:
			bl := j.RecordLengthBytes
			byteLen = &bl
		case f2e.DataTypeJSON, f2e.DataTypeJSONL, f2e.DataTypeNDJSON, f2e.DataTypeText, f2e.DataTypeMultiLine:
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
			Data: f2e.RecordPayload{Raw: raw, Fields: fields},
		}
		if binary != nil {
			env.Data.Base64 = base64.StdEncoding.EncodeToString(binary)
		}
		if s.Processor != nil {
			original := env
			processed, er := s.Processor.Process(ctx, env)
			if er != nil {
				return er
			}
			if processed == nil {
				return nil
			}
			env = *processed
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
		batch = append(batch, port.OutboundMessage{Body: string(b), Attributes: attrs})
		recordsProcessed++
		if len(batch) == batchSize {
			return flush()
		}
		return nil
	}

	var err error
	switch j.DataType {
	case f2e.DataTypeFixedWidth:
		err = fixedwidth.Read(ctx, r, j.RecordLengthBytes, j.RecordCount, func(n, off int64, raw string) error { return publish(n, off, raw, nil, nil, j.RecordLengthBytes) })
	case f2e.DataTypeJSONL, f2e.DataTypeNDJSON, f2e.DataTypeText:
		lineLimit := j.MaxRecordLengthBytes
		if lineLimit == 0 {
			lineLimit = int64(s.Config.MaxEventBytes)
			if lineLimit == 0 {
				lineLimit = 256 * 1024
			}
		}
		err = readLines(ctx, r, j.MaxRecordLengthBytes > 0 && readStart > 0, lineLimit, func(n, off int64, raw string) error {
			if j.MaxRecordLengthBytes > 0 && (readStart+off < j.StartByte || readStart+off > j.EndByteInclusive-j.TrailingPaddingBytes) {
				return nil
			}
			if (j.DataType == f2e.DataTypeJSONL || j.DataType == f2e.DataTypeNDJSON) && !j.Options.BypassJSONValidation {
				var value any
				if e := json.Unmarshal([]byte(raw), &value); e != nil {
					return fmt.Errorf("invalid JSON at record %d: %w", n+1, e)
				}
			}
			return publish(n, off, raw, nil, nil, 0)
		})
	case f2e.DataTypeCSV:
		err = readCSV(ctx, r, func(n, off, length int64, fields []string) error { return publish(n, off, "", fields, nil, length) })
	case f2e.DataTypeBinary:
		var body []byte
		limit := int64(s.Config.MaxEventBytes)
		if limit == 0 {
			limit = 256 * 1024
		}
		body, err = io.ReadAll(io.LimitReader(r, limit+1))
		if int64(len(body)) > limit {
			err = fmt.Errorf("binary record exceeds event limit: %d > %d bytes", len(body), limit)
		}
		if err == nil {
			err = publish(0, 0, "", nil, body, int64(len(body)))
		}
	case f2e.DataTypeMultiLine:
		layout := j.MultiLineLayout
		err = fixedwidth.ReadMultiLine(ctx, r, layout.BreakPosition, layout.BreakMarker, layout.AcceptedPrefixes, layout.LineSeparator, func(n, off int64, raw string) error {
			if readStart+off < j.StartByte || readStart+off > j.EndByteInclusive-j.TrailingPaddingBytes {
				return nil
			}
			return publish(n, off, raw, nil, nil, 0)
		})
	case f2e.DataTypeJSON:
		err = readJSONArray(ctx, r, j, readStart, func(n, off int64, raw string) error {
			return publish(n, off, raw, nil, nil, int64(len(raw)))
		})
	}
	if err != nil {
		return 0, err
	}
	if len(batch) > 0 {
		if err := flush(); err != nil {
			return 0, err
		}
	}
	return recordsProcessed, nil
}

func readLines(ctx context.Context, r io.Reader, discardFirst bool, maxRecordBytes int64, fn func(int64, int64, string) error) error {
	br := bufio.NewReader(r)
	var offset, number int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := readBoundedLine(br, maxRecordBytes)
		if len(line) > 0 {
			raw := strings.TrimSuffix(string(line), "\n")
			raw = strings.TrimSuffix(raw, "\r")
			if !discardFirst || number > 0 {
				if e := fn(number, offset, raw); e != nil {
					return e
				}
			}
			number++
			offset += int64(len(line))
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("record at byte %d: %w", offset, err)
		}
	}
}

func readBoundedLine(br *bufio.Reader, maxRecordBytes int64) ([]byte, error) {
	line := make([]byte, 0, min(maxRecordBytes, 64*1024))
	for {
		fragment, err := br.ReadSlice('\n')
		if int64(len(line)+len(fragment)) > maxRecordBytes {
			return nil, fmt.Errorf("record exceeds maximum %d bytes", maxRecordBytes)
		}
		line = append(line, fragment...)
		if err == bufio.ErrBufferFull {
			continue
		}
		return line, err
	}
}

func readCSV(ctx context.Context, r io.Reader, fn func(int64, int64, int64, []string) error) error {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	for n := int64(0); ; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		off := cr.InputOffset()
		fields, err := cr.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("invalid CSV at record %d: %w", n+1, err)
		}
		if err := fn(n, off, cr.InputOffset()-off, fields); err != nil {
			return err
		}
	}
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
