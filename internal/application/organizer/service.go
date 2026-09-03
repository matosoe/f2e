// Package organizer plans and schedules fixed-width file chunks.
package organizer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

type Service struct {
	Store  port.ObjectStore
	Queue  port.Queue
	Ledger port.JobLedger
	Config config.Config
}

// ExecutionSummary contains metrics from a planning execution
type ExecutionSummary struct {
	Summary *f2e.OrganizerSummary
	Jobs    []f2e.ChunkJob
}

func hash(s string) string { x := sha256.Sum256([]byte(s)); return hex.EncodeToString(x[:]) }
func fileID(bucket, key, versionID, etag string, size int64) string {
	return hash(fmt.Sprintf("%s/%s/%s/%s/%d", bucket, key, versionID, etag, size))
}
func (s Service) Jobs(ctx context.Context, references []f2e.FileReference) ([]f2e.ChunkJob, error) {
	return s.JobsWithExecutionID(ctx, references, "")
}

func (s Service) JobsWithExecutionID(ctx context.Context, references []f2e.FileReference, executionID string) ([]f2e.ChunkJob, error) {
	files := make([]f2e.FileRequest, len(references))
	for i, reference := range references {
		files[i] = f2e.FileRequest{Bucket: reference.Bucket, Key: reference.Key, DataType: f2e.DataTypeFixedWidth}
	}
	return s.Plan(ctx, f2e.OrganizerRequest{SchemaVersion: f2e.SchemaVersion, ExecutionID: executionID, Files: files})
}

// Plan converts the explicit organizer contract into explicit worker jobs.
func (s Service) Plan(ctx context.Context, request f2e.OrganizerRequest) ([]f2e.ChunkJob, error) {
	if request.SchemaVersion != f2e.SchemaVersion || len(request.Files) == 0 {
		return nil, fmt.Errorf("invalid organizer request")
	}
	var jobs []f2e.ChunkJob
	for _, file := range request.Files {
		if file.Bucket == "" || file.Key == "" || !s.validType(file.DataType) || (file.Options.BypassJSONValidation && file.DataType != f2e.DataTypeJSONL && file.DataType != f2e.DataTypeNDJSON) {
			return nil, fmt.Errorf("invalid file request")
		}
		object, e := s.Store.Head(ctx, file.Bucket, file.Key)
		if e != nil {
			return nil, fmt.Errorf("head %s/%s: %w", file.Bucket, file.Key, e)
		}
		size, etag, versionID := object.Size, object.ETag, object.VersionID
		maxFileBytes := s.Config.MaxFileBytes
		if maxFileBytes == 0 {
			maxFileBytes = 10 * 1024 * 1024 * 1024
		}
		if size > maxFileBytes {
			return nil, fmt.Errorf("object size %d exceeds maximum %d bytes", size, maxFileBytes)
		}
		executionID := rand.Text()
		if request.ExecutionID != "" {
			executionID = hash(request.ExecutionID + "/" + file.Bucket + "/" + file.Key)
		}
		if file.DataType == f2e.DataTypeMultiLine {
			planned, err := s.multiLineJobs(ctx, file, size, etag, versionID, executionID)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, planned...)
			continue
		}
		if file.DataType == f2e.DataTypeJSON {
			planned, err := s.jsonArrayJobs(ctx, file, size, etag, versionID, executionID)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, planned...)
			continue
		}
		if file.MaxRecordLengthBytes > 0 && (file.DataType == f2e.DataTypeJSONL || file.DataType == f2e.DataTypeNDJSON || file.DataType == f2e.DataTypeText) {
			planned, err := s.variableJobs(ctx, file, size, etag, versionID, executionID)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, planned...)
			continue
		}
		if file.DataType != f2e.DataTypeFixedWidth {
			if size == 0 {
				return nil, fmt.Errorf("empty object")
			}
			if size > s.maxChunkBytes() {
				return nil, fmt.Errorf("non-splittable %s object size %d exceeds chunk maximum %d", file.DataType, size, s.maxChunkBytes())
			}
			if file.DataType == f2e.DataTypeBinary && size > s.maxBinaryBytes() {
				return nil, fmt.Errorf("binary object size %d exceeds maximum payload %d bytes", size, s.maxBinaryBytes())
			}
			fileID := fileID(file.Bucket, file.Key, versionID, etag, size)
			jobs = append(jobs, f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: executionID, FileID: fileID, ChunkID: "00000001", Bucket: file.Bucket, Key: file.Key, PresignedURL: file.PresignedURL, ETag: etag, VersionID: versionID, FileSize: size, RecordCount: 1, StartByte: 0, EndByteInclusive: size - 1, MaxRecordLengthBytes: file.MaxRecordLengthBytes, DataType: file.DataType, Options: file.Options, Context: file.Context})
			continue
		}
		if size == 0 || size%s.safeLen() != 0 {
			return nil, fmt.Errorf("invalid fixed-width object size %d", size)
		}
		if int64(s.Config.RecordsPerChunk)*s.safeLen() > s.maxChunkBytes() {
			return nil, fmt.Errorf("fixed-width chunk exceeds maximum %d bytes", s.maxChunkBytes())
		}
		fileID := fileID(file.Bucket, file.Key, versionID, etag, size)
		total := size / s.safeLen()
		for start, chunk := int64(0), int64(1); start < total; start, chunk = start+int64(s.Config.RecordsPerChunk), chunk+1 {
			count := int64(s.Config.RecordsPerChunk)
			if total-start < count {
				count = total - start
			}
			startByte := start * s.safeLen()
			jobs = append(jobs, f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: executionID, FileID: fileID, ChunkID: fmt.Sprintf("%08d", chunk), Bucket: file.Bucket, Key: file.Key, PresignedURL: file.PresignedURL, ETag: etag, VersionID: versionID, FileSize: size, StartRecord: start, RecordCount: count, RecordLengthBytes: s.safeLen(), StartByte: startByte, EndByteInclusive: startByte + count*s.safeLen() - 1, DataType: file.DataType, Options: file.Options, Context: file.Context})
		}
	}
	return jobs, nil
}

// variableJobs divides a newline-delimited object into nominal ranges. Each
// range is extended only far enough to finish the record crossing its end.
func (s Service) variableJobs(ctx context.Context, file f2e.FileRequest, size int64, etag, versionID, executionID string) ([]f2e.ChunkJob, error) {
	if size == 0 {
		return nil, fmt.Errorf("empty object")
	}
	if file.MaxRecordLengthBytes < 1 {
		return nil, fmt.Errorf("invalid maximum record length")
	}
	nominalSize := int64(s.Config.RecordsPerChunk) * file.MaxRecordLengthBytes
	if nominalSize < file.MaxRecordLengthBytes {
		return nil, fmt.Errorf("variable chunk size overflow")
	}
	if nominalSize > s.maxChunkBytes() {
		return nil, fmt.Errorf("variable chunk size %d exceeds maximum %d", nominalSize, s.maxChunkBytes())
	}
	fileID := fileID(file.Bucket, file.Key, versionID, etag, size)
	var jobs []f2e.ChunkJob
	for start, chunk := int64(0), int64(1); start < size; start, chunk = start+nominalSize, chunk+1 {
		ownedEnd := start + nominalSize - 1
		if ownedEnd >= size {
			ownedEnd = size - 1
		}
		padding := int64(0)
		if ownedEnd < size-1 {
			lookEnd := ownedEnd + file.MaxRecordLengthBytes
			if lookEnd >= size {
				lookEnd = size - 1
			}
			r, err := s.Store.GetRange(ctx, f2e.ObjectIdentity{Bucket: file.Bucket, Key: file.Key, VersionID: versionID, ETag: etag, Size: size}, ownedEnd+1, lookEnd)
			if err != nil {
				return nil, fmt.Errorf("read variable-record padding: %w", err)
			}
			data, err := io.ReadAll(r)
			closeErr := r.Close()
			if err != nil {
				return nil, err
			}
			if closeErr != nil {
				return nil, closeErr
			}
			at := bytes.IndexByte(data, '\n')
			if at < 0 {
				return nil, fmt.Errorf("record after byte %d exceeds maxRecordLengthBytes %d", ownedEnd, file.MaxRecordLengthBytes)
			}
			padding = int64(at + 1)
		}
		jobs = append(jobs, f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: executionID, FileID: fileID, ChunkID: fmt.Sprintf("%08d", chunk), Bucket: file.Bucket, Key: file.Key, PresignedURL: file.PresignedURL, ETag: etag, VersionID: versionID, FileSize: size, StartByte: start, EndByteInclusive: ownedEnd + padding, MaxRecordLengthBytes: file.MaxRecordLengthBytes, TrailingPaddingBytes: padding, DataType: file.DataType, Options: file.Options, Context: file.Context})
	}
	return jobs, nil
}
func (s Service) validType(t f2e.DataType) bool {
	switch t {
	case f2e.DataTypeFixedWidth, f2e.DataTypeJSONL, f2e.DataTypeNDJSON,
		f2e.DataTypeText, f2e.DataTypeCSV, f2e.DataTypeJSON,
		f2e.DataTypeBinary, f2e.DataTypeMultiLine:
		return true
	default:
		return false
	}
}
func (s Service) safeLen() int64 { return int64(s.Config.RecordLength) }
func (s Service) maxChunkBytes() int64 {
	if s.Config.MaxChunkBytes > 0 {
		return s.Config.MaxChunkBytes
	}
	return 64 * 1024 * 1024
}

// maxBinaryBytes reserves space for Base64 expansion and the mandatory envelope
// metadata. The Worker performs the authoritative serialized-size validation.
func (s Service) maxBinaryBytes() int64 {
	maxEventBytes := int64(s.Config.MaxEventBytes)
	if maxEventBytes == 0 {
		maxEventBytes = 256 * 1024
	}
	const envelopeOverhead = 4096
	if maxEventBytes <= envelopeOverhead {
		return 0
	}
	return (maxEventBytes - envelopeOverhead) / 4 * 3
}

// nextBreakOffset scans data line-by-line and returns the byte offset within data
// of the first line whose content at breakPosition starts with breakMarker.
// Returns -1 when no such line is found.
func nextBreakOffset(data []byte, breakPosition int, breakMarker string) int {
	i := 0
	for i < len(data) {
		eol := bytes.IndexByte(data[i:], '\n')
		var line []byte
		var next int
		if eol < 0 {
			line = data[i:]
			next = len(data)
		} else {
			line = data[i : i+eol]
			next = i + eol + 1
		}
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if breakPosition < len(line) && strings.HasPrefix(string(line[breakPosition:]), breakMarker) {
			return i
		}
		if eol < 0 {
			break
		}
		i = next
	}
	return -1
}

// multiLineJobs divides a multi-line object into chunk ranges whose boundaries
// always fall between logical records (never inside a record's constituent lines).
func (s Service) multiLineJobs(ctx context.Context, file f2e.FileRequest, size int64, etag, versionID, executionID string) ([]f2e.ChunkJob, error) {
	layout := file.MultiLineLayout
	if layout.BreakMarker == "" {
		return nil, fmt.Errorf("multi-line layout requires breakMarker")
	}
	if layout.MaxBytesPerRecord < 1 {
		return nil, fmt.Errorf("multi-line layout requires maxBytesPerRecord")
	}
	if size == 0 {
		return nil, fmt.Errorf("empty object")
	}
	nominalSize := int64(s.Config.RecordsPerChunk) * layout.MaxBytesPerRecord
	if nominalSize < layout.MaxBytesPerRecord {
		return nil, fmt.Errorf("multi-line chunk size overflow")
	}
	if nominalSize > s.maxChunkBytes() {
		return nil, fmt.Errorf("multi-line chunk size %d exceeds maximum %d", nominalSize, s.maxChunkBytes())
	}
	fileID := fileID(file.Bucket, file.Key, versionID, etag, size)
	var jobs []f2e.ChunkJob
	for start, chunk := int64(0), int64(1); start < size; start, chunk = start+nominalSize, chunk+1 {
		ownedEnd := start + nominalSize - 1
		if ownedEnd >= size {
			ownedEnd = size - 1
		}
		padding := int64(0)
		if ownedEnd < size-1 {
			lookEnd := ownedEnd + layout.MaxBytesPerRecord
			if lookEnd >= size {
				lookEnd = size - 1
			}
			r, err := s.Store.GetRange(ctx, f2e.ObjectIdentity{Bucket: file.Bucket, Key: file.Key, VersionID: versionID, ETag: etag, Size: size}, ownedEnd+1, lookEnd)
			if err != nil {
				return nil, fmt.Errorf("read multi-line padding: %w", err)
			}
			data, err := io.ReadAll(r)
			closeErr := r.Close()
			if err != nil {
				return nil, err
			}
			if closeErr != nil {
				return nil, closeErr
			}
			at := nextBreakOffset(data, layout.BreakPosition, layout.BreakMarker)
			if at < 0 {
				if lookEnd < size-1 {
					return nil, fmt.Errorf("no record break found after byte %d within maxBytesPerRecord %d", ownedEnd, layout.MaxBytesPerRecord)
				}
				// Lookahead reached EOF without a break: remaining bytes are the last record.
				padding = size - 1 - ownedEnd
			} else {
				padding = int64(at)
			}
		}
		jobs = append(jobs, f2e.ChunkJob{
			SchemaVersion:        f2e.SchemaVersion,
			JobID:                executionID,
			FileID:               fileID,
			ChunkID:              fmt.Sprintf("%08d", chunk),
			Bucket:               file.Bucket,
			Key:                  file.Key,
			PresignedURL:         file.PresignedURL,
			ETag:                 etag,
			VersionID:            versionID,
			FileSize:             size,
			StartByte:            start,
			EndByteInclusive:     ownedEnd + padding,
			MaxRecordLengthBytes: layout.MaxBytesPerRecord,
			TrailingPaddingBytes: padding,
			DataType:             file.DataType,
			MultiLineLayout:      layout,
			Options:              file.Options,
			Context:              file.Context,
		})
	}
	return jobs, nil
}

// PlanWithSummary converts the organizer request into worker jobs and returns execution summary
func (s Service) PlanWithSummary(ctx context.Context, request f2e.OrganizerRequest) (*ExecutionSummary, error) {
	startTime := f2e.CurrentTimeMillis()

	jobs, err := s.Plan(ctx, request)
	if err != nil {
		return nil, err
	}

	endTime := f2e.CurrentTimeMillis()
	duration := endTime - startTime

	// Build summary
	summary := &f2e.OrganizerSummary{
		SchemaVersion:        f2e.SchemaVersion,
		StartTimeMillis:      startTime,
		EndTimeMillis:        endTime,
		ProcessingTimeMillis: duration,
		FilesProcessed:       len(request.Files),
		TotalChunksGenerated: int64(len(jobs)),
		Files:                []f2e.FileSummary{},
	}

	// Create file summaries
	for _, file := range request.Files {
		object, err := s.Store.Head(ctx, file.Bucket, file.Key)
		size := object.Size
		if err != nil {
			// Log error but continue
			fmt.Printf("organizer_summary: error getting file size for %s/%s: %v\n", file.Bucket, file.Key, err)
		}

		// Count chunks for this file
		chunksForFile := int64(0)
		for _, job := range jobs {
			if job.Bucket == file.Bucket && job.Key == file.Key {
				chunksForFile++
			}
		}

		summary.Files = append(summary.Files, f2e.FileSummary{
			Bucket:               file.Bucket,
			Key:                  file.Key,
			SizeBytes:            size,
			ChunksGenerated:      chunksForFile,
			ProcessingTimeMillis: duration,
		})
	}

	return &ExecutionSummary{
		Summary: summary,
		Jobs:    jobs,
	}, nil
}

func (s Service) Publish(ctx context.Context, jobs []f2e.ChunkJob) error {
	jobIDs := make([]string, 0)
	if s.Ledger != nil {
		groups := make(map[string][]f2e.ChunkJob)
		for _, job := range jobs {
			groups[job.JobID] = append(groups[job.JobID], job)
		}
		for _, chunks := range groups {
			first := chunks[0]
			plan := f2e.JobPlan{JobID: first.JobID, FileID: first.FileID, Bucket: first.Bucket, Key: first.Key, VersionID: first.VersionID, ETag: first.ETag, ExpectedChunks: len(chunks), CreatedAt: time.Now().UTC()}
			if err := s.Ledger.Plan(ctx, plan, chunks); err != nil {
				return fmt.Errorf("persist job plan: %w", err)
			}
			jobIDs = append(jobIDs, first.JobID)
		}
	}
	finish := func(err error) error {
		if s.Ledger == nil {
			return err
		}
		if err != nil {
			if ledgerErr := s.Ledger.MarkSchedulingFailed(ctx, jobIDs, err.Error()); ledgerErr != nil {
				return fmt.Errorf("%v; mark scheduling failed: %w", err, ledgerErr)
			}
			return err
		}
		if ledgerErr := s.Ledger.MarkScheduled(ctx, jobIDs); ledgerErr != nil {
			return fmt.Errorf("mark jobs scheduled: %w", ledgerErr)
		}
		return nil
	}
	messages := make([]port.OutboundMessage, 0, 10)
	flush := func() error {
		pending := append([]port.OutboundMessage(nil), messages...)
		for attempt := 0; attempt < 3; attempt++ {
			failed, err := s.Queue.Send(ctx, s.Config.ChunkQueueURL, pending)
			if err == nil && len(failed) == 0 {
				messages = nil
				return nil
			}
			if err != nil {
				if attempt == 2 {
					return err
				}
			} else {
				next := make([]port.OutboundMessage, 0, len(failed))
				for _, index := range failed {
					if index < 0 || index >= len(pending) {
						return fmt.Errorf("chunk batch returned invalid failed index %d", index)
					}
					next = append(next, pending[index])
				}
				pending = next
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(50*(1<<attempt)) * time.Millisecond):
			}
		}
		return fmt.Errorf("chunk batch partial failure after retries")
	}
	for _, j := range jobs {
		b, e := json.Marshal(j)
		if e != nil {
			return finish(e)
		}
		messages = append(messages, port.OutboundMessage{Body: string(b)})
		if len(messages) == 10 {
			if e := flush(); e != nil {
				return finish(e)
			}
		}
	}
	if len(messages) > 0 {
		if err := flush(); err != nil {
			return finish(err)
		}
	}
	return finish(nil)
}

// Replay republishes the immutable chunk contracts retained by the ledger.
// A new job ID creates a new event occurrence while file/source identities stay unchanged.
func (s Service) Replay(ctx context.Context, sourceJobID string, chunkIDs []string) ([]f2e.ChunkJob, error) {
	if s.Ledger == nil || sourceJobID == "" {
		return nil, fmt.Errorf("replay requires a configured ledger and source job ID")
	}
	jobs, err := s.Ledger.Replay(ctx, sourceJobID, rand.Text(), chunkIDs)
	if err != nil {
		return nil, fmt.Errorf("load replay plan: %w", err)
	}
	if err := s.Publish(ctx, jobs); err != nil {
		return nil, fmt.Errorf("publish replay: %w", err)
	}
	return jobs, nil
}

// jsonArrayJobs divides a JSON file's target array into chunk ranges whose boundaries
// always fall between array elements (never inside an element's JSON structure).
func (s Service) jsonArrayJobs(ctx context.Context, file f2e.FileRequest, size int64, etag, versionID, executionID string) ([]f2e.ChunkJob, error) {
	layout := file.JSONArrayLayout
	if layout.MaxBytesPerElement < 1 {
		return nil, fmt.Errorf("json array layout requires maxBytesPerElement")
	}
	if size == 0 {
		return nil, fmt.Errorf("empty object")
	}
	searchBytes := s.Config.JSONArraySearchBytes
	if searchBytes == 0 {
		searchBytes = 1024 * 1024
	}
	// The target path must be found within the explicit bounded search window.
	probeEnd := int64(searchBytes - 1)
	if probeEnd >= size {
		probeEnd = size - 1
	}
	arrayOffset, err := s.findJSONArrayOffset(ctx, f2e.ObjectIdentity{Bucket: file.Bucket, Key: file.Key, VersionID: versionID, ETag: etag, Size: size}, probeEnd, layout.ArrayPath)
	if err != nil {
		return nil, fmt.Errorf("find json array offset for %q within %d bytes: %w", layout.ArrayPath, searchBytes, err)
	}
	nominalSize := int64(s.Config.RecordsPerChunk) * layout.MaxBytesPerElement
	if nominalSize < layout.MaxBytesPerElement {
		return nil, fmt.Errorf("json array chunk size overflow")
	}
	if nominalSize > s.maxChunkBytes()-layout.MaxBytesPerElement {
		return nil, fmt.Errorf("json array chunk plus boundary padding exceeds maximum %d", s.maxChunkBytes())
	}
	fileID := fileID(file.Bucket, file.Key, versionID, etag, size)
	var jobs []f2e.ChunkJob
	for start, chunk := arrayOffset, int64(1); start < size; chunk++ {
		ownedEnd := start + nominalSize - 1
		if ownedEnd >= size {
			ownedEnd = size - 1
		}
		padding := int64(0)
		if ownedEnd < size-1 {
			// start is an element boundary. Scanning from there preserves JSON
			// string/escape state; beginning in the middle of an element can skip
			// otherwise valid records at every chunk boundary.
			lookEnd := ownedEnd + layout.MaxBytesPerElement
			if lookEnd >= size {
				lookEnd = size - 1
			}
			r, err := s.Store.GetRange(ctx, f2e.ObjectIdentity{Bucket: file.Bucket, Key: file.Key, VersionID: versionID, ETag: etag, Size: size}, start, lookEnd)
			if err != nil {
				return nil, fmt.Errorf("read json array padding: %w", err)
			}
			data, err := io.ReadAll(r)
			closeErr := r.Close()
			if err != nil {
				return nil, err
			}
			if closeErr != nil {
				return nil, closeErr
			}
			relPos := int(ownedEnd - start)
			at := f2e.JSONElementEndAfter(data, relPos)
			if at < 0 {
				// ownedEnd already falls between elements — no padding needed.
				padding = 0
			} else {
				// at is the exclusive end position within the window.
				// file position of that end = contextStart + at - 1
				// padding = that position - ownedEnd
				padding = start + int64(at) - 1 - ownedEnd
			}
		}
		jobs = append(jobs, f2e.ChunkJob{
			SchemaVersion:        f2e.SchemaVersion,
			JobID:                executionID,
			FileID:               fileID,
			ChunkID:              fmt.Sprintf("%08d", chunk),
			Bucket:               file.Bucket,
			Key:                  file.Key,
			PresignedURL:         file.PresignedURL,
			ETag:                 etag,
			VersionID:            versionID,
			FileSize:             size,
			StartByte:            start,
			EndByteInclusive:     ownedEnd + padding,
			MaxRecordLengthBytes: layout.MaxBytesPerElement,
			TrailingPaddingBytes: padding,
			DataType:             file.DataType,
			JSONArrayLayout:      layout,
			JSONArrayOffset:      arrayOffset,
			Options:              file.Options,
			Context:              file.Context,
		})
		start = ownedEnd + padding + 1
	}
	return jobs, nil
}

// findJSONArrayOffset reads the start of the file and returns the byte offset
// immediately after the '[' of the target array.
func (s Service) findJSONArrayOffset(ctx context.Context, object f2e.ObjectIdentity, probeEnd int64, arrayPath string) (int64, error) {
	r, err := s.Store.GetRange(ctx, object, 0, probeEnd)
	if err != nil {
		return 0, err
	}
	data, err := io.ReadAll(r)
	closeErr := r.Close()
	if err != nil {
		return 0, err
	}
	if closeErr != nil {
		return 0, closeErr
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := navigateToArrayStart(dec, arrayPath); err != nil {
		return 0, err
	}
	return dec.InputOffset(), nil
}

// navigateToArrayStart advances dec to just after the '[' of the target array.
func navigateToArrayStart(dec *json.Decoder, arrayPath string) error {
	if arrayPath == "" {
		t, err := dec.Token()
		if err != nil {
			return fmt.Errorf("read JSON root: %w", err)
		}
		if t != json.Delim('[') {
			return fmt.Errorf("root is not a JSON array")
		}
		return nil
	}
	return navigateObjectPath(dec, strings.Split(arrayPath, "."))
}

// navigateObjectPath navigates through nested JSON objects following parts,
// leaving dec positioned just after the '[' of the final array value.
func navigateObjectPath(dec *json.Decoder, parts []string) error {
	t, err := dec.Token()
	if err != nil {
		return err
	}
	if t != json.Delim('{') {
		return fmt.Errorf("expected JSON object, got %v", t)
	}
	target, rest := parts[0], parts[1:]
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return err
		}
		ks, _ := kt.(string)
		if ks != target {
			var skip json.RawMessage
			if err := dec.Decode(&skip); err != nil {
				return err
			}
			continue
		}
		if len(rest) == 0 {
			t, err := dec.Token()
			if err != nil {
				return err
			}
			if t != json.Delim('[') {
				return fmt.Errorf("key %q is not a JSON array", target)
			}
			return nil
		}
		return navigateObjectPath(dec, rest)
	}
	return fmt.Errorf("key %q not found in JSON object", target)
}
