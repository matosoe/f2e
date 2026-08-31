// Package organizer plans and schedules fixed-width file chunks.
package organizer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/f2e/f2e/internal/application/port"
	"github.com/f2e/f2e/internal/domain/f2e"
	"github.com/f2e/f2e/internal/platform/config"
)

type Service struct {
	Store  port.ObjectStore
	Queue  port.Queue
	Config config.Config
}

// ExecutionSummary contains metrics from a planning execution
type ExecutionSummary struct {
	Summary *f2e.OrganizerSummary
	Jobs    []f2e.ChunkJob
}

func hash(s string) string { x := sha256.Sum256([]byte(s)); return hex.EncodeToString(x[:]) }
func (s Service) Jobs(ctx context.Context, references []f2e.FileReference) ([]f2e.ChunkJob, error) {
	files := make([]f2e.FileRequest, len(references))
	for i, reference := range references {
		files[i] = f2e.FileRequest{Bucket: reference.Bucket, Key: reference.Key, DataType: f2e.DataTypeFixedWidth}
	}
	return s.Plan(ctx, f2e.OrganizerRequest{SchemaVersion: f2e.SchemaVersion, Files: files})
}

// Plan converts the explicit organizer contract into explicit worker jobs.
func (s Service) Plan(ctx context.Context, request f2e.OrganizerRequest) ([]f2e.ChunkJob, error) {
	if request.SchemaVersion != f2e.SchemaVersion || len(request.Files) == 0 {
		return nil, fmt.Errorf("invalid organizer request")
	}
	var jobs []f2e.ChunkJob
	for _, file := range request.Files {
		if file.Bucket == "" || file.Key == "" || !validType(file.DataType) || (file.Options.BypassJSONValidation && file.DataType != f2e.DataTypeJSONL && file.DataType != f2e.DataTypeNDJSON) {
			return nil, fmt.Errorf("invalid file request")
		}
		size, etag, e := s.Store.Head(ctx, file.Bucket, file.Key)
		if e != nil {
			return nil, fmt.Errorf("head %s/%s: %w", file.Bucket, file.Key, e)
		}
		if file.DataType == f2e.DataTypeMultiLine {
			planned, err := s.multiLineJobs(ctx, file, size, etag)
			if err != nil {
				return nil, err
			}
			jobs = append(jobs, planned...)
			continue
		}
		if file.MaxRecordLengthBytes > 0 && (file.DataType == f2e.DataTypeJSONL || file.DataType == f2e.DataTypeNDJSON || file.DataType == f2e.DataTypeText) {
			planned, err := s.variableJobs(ctx, file, size, etag)
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
			fileID := hash(file.Bucket + "/" + file.Key + "/" + etag)
			jobs = append(jobs, f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: hash(fileID), FileID: fileID, ChunkID: "00000001", Bucket: file.Bucket, Key: file.Key, ETag: etag, RecordCount: 1, StartByte: 0, EndByteInclusive: size - 1, MaxRecordLengthBytes: file.MaxRecordLengthBytes, DataType: file.DataType, Options: file.Options})
			continue
		}
		if size == 0 || size%s.safeLen() != 0 {
			return nil, fmt.Errorf("invalid fixed-width object size %d", size)
		}
		fileID := hash(file.Bucket + "/" + file.Key + "/" + etag)
		total := size / s.safeLen()
		for start, chunk := int64(0), int64(1); start < total; start, chunk = start+int64(s.Config.RecordsPerChunk), chunk+1 {
			count := int64(s.Config.RecordsPerChunk)
			if total-start < count {
				count = total - start
			}
			startByte := start * s.safeLen()
			jobs = append(jobs, f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: hash(fileID), FileID: fileID, ChunkID: fmt.Sprintf("%08d", chunk), Bucket: file.Bucket, Key: file.Key, ETag: etag, StartRecord: start, RecordCount: count, RecordLengthBytes: s.safeLen(), StartByte: startByte, EndByteInclusive: startByte + count*s.safeLen() - 1, DataType: file.DataType, Options: file.Options})
		}
	}
	return jobs, nil
}

// variableJobs divides a newline-delimited object into nominal ranges. Each
// range is extended only far enough to finish the record crossing its end.
func (s Service) variableJobs(ctx context.Context, file f2e.FileRequest, size int64, etag string) ([]f2e.ChunkJob, error) {
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
	fileID := hash(file.Bucket + "/" + file.Key + "/" + etag)
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
			r, err := s.Store.GetRange(ctx, file.Bucket, file.Key, ownedEnd+1, lookEnd)
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
		jobs = append(jobs, f2e.ChunkJob{SchemaVersion: f2e.SchemaVersion, JobID: hash(fileID), FileID: fileID, ChunkID: fmt.Sprintf("%08d", chunk), Bucket: file.Bucket, Key: file.Key, ETag: etag, StartByte: start, EndByteInclusive: ownedEnd + padding, MaxRecordLengthBytes: file.MaxRecordLengthBytes, TrailingPaddingBytes: padding, DataType: file.DataType, Options: file.Options})
	}
	return jobs, nil
}
func validType(t f2e.DataType) bool {
	return t == f2e.DataTypeFixedWidth || t == f2e.DataTypeJSONL || t == f2e.DataTypeNDJSON || t == f2e.DataTypeCSV || t == f2e.DataTypeBinary || t == f2e.DataTypeText || t == f2e.DataTypeMultiLine
}
func (s Service) safeLen() int64 { return int64(s.Config.RecordLength) }

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
func (s Service) multiLineJobs(ctx context.Context, file f2e.FileRequest, size int64, etag string) ([]f2e.ChunkJob, error) {
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
	fileID := hash(file.Bucket + "/" + file.Key + "/" + etag)
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
			r, err := s.Store.GetRange(ctx, file.Bucket, file.Key, ownedEnd+1, lookEnd)
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
			JobID:                hash(fileID),
			FileID:               fileID,
			ChunkID:              fmt.Sprintf("%08d", chunk),
			Bucket:               file.Bucket,
			Key:                  file.Key,
			ETag:                 etag,
			StartByte:            start,
			EndByteInclusive:     ownedEnd + padding,
			MaxRecordLengthBytes: layout.MaxBytesPerRecord,
			TrailingPaddingBytes: padding,
			DataType:             file.DataType,
			MultiLineLayout:      layout,
			Options:              file.Options,
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
		size, _, err := s.Store.Head(ctx, file.Bucket, file.Key)
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
	bodies := make([]string, 0, 10)
	flush := func() error {
		f, e := s.Queue.Send(ctx, s.Config.ChunkQueueURL, bodies)
		if e != nil {
			return e
		}
		if len(f) > 0 {
			return fmt.Errorf("chunk batch partial failure: %v", f)
		}
		bodies = nil
		return nil
	}
	for _, j := range jobs {
		b, e := json.Marshal(j)
		if e != nil {
			return e
		}
		bodies = append(bodies, string(b))
		if len(bodies) == 10 {
			if e := flush(); e != nil {
				return e
			}
		}
	}
	if len(bodies) > 0 {
		return flush()
	}
	return nil
}
